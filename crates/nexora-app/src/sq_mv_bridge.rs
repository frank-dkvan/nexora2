//! Standing Query to Materialized View integration.
//!
//! Automatically updates materialized views when Standing Query patterns match.
#![allow(dead_code)]

use nexora_core::materialized_view::{MaterializedRow, MaterializedViewManager};
use nexora_standing_query::StandingQueryResult;
use std::collections::HashMap;
use std::sync::Arc;

/// Bridge between Standing Query results and Materialized View updates
pub struct SQMaterializedViewBridge {
    mv_manager: Arc<MaterializedViewManager>,
    /// Mapping: Standing Query ID → Materialized View ID
    sq_to_mv: Arc<tokio::sync::RwLock<HashMap<String, String>>>,
}

impl SQMaterializedViewBridge {
    pub fn new(mv_manager: Arc<MaterializedViewManager>) -> Self {
        Self {
            mv_manager,
            sq_to_mv: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Register a Standing Query to update a Materialized View
    pub async fn register_sq_for_mv(&self, sq_id: String, mv_id: String) {
        tracing::info!(sq_id = %sq_id, mv_id = %mv_id, "Registered SQ → MV mapping");
        self.sq_to_mv.write().await.insert(sq_id, mv_id);
    }

    /// Unregister a Standing Query from materialized view updates
    pub async fn unregister_sq(&self, sq_id: &str) {
        self.sq_to_mv.write().await.remove(sq_id);
        tracing::info!(sq_id = %sq_id, "Unregistered SQ → MV mapping");
    }

    /// Handle a Standing Query result and update the corresponding materialized view
    pub async fn on_sq_result(&self, result: &StandingQueryResult) {
        let sq_to_mv = self.sq_to_mv.read().await;

        if let Some(mv_id) = sq_to_mv.get(&result.sq_id.to_string()) {
            match result.result_type {
                nexora_standing_query::ResultType::Matched => {
                    // Node matched - add/update in materialized view
                    let row = MaterializedRow {
                        key: result.qid.to_string(),
                        values: result.matched_properties.clone(),
                        version: 1,
                        updated_at: chrono::Utc::now(),
                    };

                    if let Err(e) = self.mv_manager.upsert_row(mv_id, row).await {
                        tracing::error!(
                            mv_id = %mv_id,
                            node = %result.qid,
                            error = %e,
                            "Failed to update materialized view"
                        );
                    } else {
                        tracing::debug!(
                            mv_id = %mv_id,
                            node = %result.qid,
                            "Materialized view updated from SQ match"
                        );
                    }
                }
                nexora_standing_query::ResultType::Unmatched => {
                    // Node no longer matches - remove from materialized view
                    if let Err(e) = self
                        .mv_manager
                        .delete_row(mv_id, &result.qid.to_string())
                        .await
                    {
                        tracing::error!(
                            mv_id = %mv_id,
                            node = %result.qid,
                            error = %e,
                            "Failed to remove from materialized view"
                        );
                    } else {
                        tracing::debug!(
                            mv_id = %mv_id,
                            node = %result.qid,
                            "Removed from materialized view (unmatched)"
                        );
                    }
                }
            }
        }
    }

    /// Create a callback closure for Standing Query manager
    pub fn create_callback(
        bridge: Arc<Self>,
    ) -> impl Fn(
        StandingQueryResult,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
           + Send
           + Sync {
        move |result: StandingQueryResult| {
            let bridge = bridge.clone();
            Box::pin(async move {
                bridge.on_sq_result(&result).await;
            })
        }
    }
}

/// Helper: Create a materialized view from a Standing Query
pub async fn create_mv_from_sq(
    mv_manager: &MaterializedViewManager,
    bridge: &SQMaterializedViewBridge,
    sq_id: &str,
    mv_name: String,
    query: String,
) -> Result<String, Box<dyn std::error::Error>> {
    use nexora_core::materialized_view::{ColumnDef, DataType, RefreshMode};

    // Infer schema from query (simplified - in production, parse Cypher RETURN clause)
    let schema = vec![ColumnDef {
        name: "node_id".to_string(),
        data_type: DataType::String,
    }];

    // Create materialized view
    let mv_id = mv_manager
        .create_view(mv_name, query, schema, RefreshMode::Incremental)
        .await?;

    // Register SQ → MV mapping
    bridge
        .register_sq_for_mv(sq_id.to_string(), mv_id.clone())
        .await;

    Ok(mv_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_id::{NexoraId, PropertyValue};
    use nexora_standing_query::ResultType;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_sq_to_mv_bridge() {
        let mv_manager = Arc::new(MaterializedViewManager::new());
        let bridge = SQMaterializedViewBridge::new(mv_manager.clone());

        // Create a materialized view
        let mv_id = mv_manager
            .create_view(
                "test_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                nexora_core::materialized_view::RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Register SQ → MV mapping
        let sq_id = Uuid::new_v4();
        bridge
            .register_sq_for_mv(sq_id.to_string(), mv_id.clone())
            .await;

        // Simulate SQ match
        let qid = NexoraId::from_bytes(b"test-node".to_vec());
        let mut props = HashMap::new();
        props.insert("name".to_string(), PropertyValue::String("Alice".into()));

        let result = StandingQueryResult::new(
            sq_id,
            "test",
            qid.clone(),
            props,
            ResultType::Matched,
            chrono::Utc::now(),
        );

        // Process result
        bridge.on_sq_result(&result).await;

        // Verify row was added
        let row = mv_manager.get_row(&mv_id, &qid.to_string()).await.unwrap();
        assert!(row.is_some());
    }

    /// GAP-3: End-to-end event-driven refresh. This mirrors the main.rs wiring:
    /// subscribe to the SQ manager's result broadcast, forward each result to the
    /// bridge, and confirm a match (then an unmatch) drives the MV incrementally.
    #[tokio::test]
    async fn test_sq_broadcast_drives_mv_refresh() {
        use nexora_standing_query::StandingQueryManager;

        let mv_manager = Arc::new(MaterializedViewManager::new());
        let bridge = Arc::new(SQMaterializedViewBridge::new(mv_manager.clone()));
        let sq_manager = Arc::new(StandingQueryManager::new(64));

        let mv_id = mv_manager
            .create_view(
                "live_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                nexora_core::materialized_view::RefreshMode::Incremental,
            )
            .await
            .unwrap();
        let sq_id = Uuid::new_v4();
        bridge
            .register_sq_for_mv(sq_id.to_string(), mv_id.clone())
            .await;

        // Wire the subscription exactly as main.rs does.
        let mut rx = sq_manager.subscribe();
        let bridge_for_mv = bridge.clone();
        let handle = tokio::spawn(async move {
            while let Ok(result) = rx.recv().await {
                bridge_for_mv.on_sq_result(&result).await;
            }
        });

        let qid = NexoraId::from_bytes(b"live-node".to_vec());
        let mut props = HashMap::new();
        props.insert("value".to_string(), PropertyValue::Integer(100));

        // Publish a Matched result through the SQ manager's broadcast channel.
        sq_manager
            .publish_result_for_test(StandingQueryResult::new(
                sq_id,
                "live",
                qid.clone(),
                props,
                ResultType::Matched,
                chrono::Utc::now(),
            ))
            .expect("broadcast send should succeed");

        // The subscriber runs on another task; poll until the row appears.
        let mut inserted = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if mv_manager
                .get_row(&mv_id, &qid.to_string())
                .await
                .unwrap()
                .is_some()
            {
                inserted = true;
                break;
            }
        }
        assert!(inserted, "SQ match broadcast should insert the MV row");

        // Now publish an Unmatched result; the row should be removed.
        sq_manager
            .publish_result_for_test(StandingQueryResult::new(
                sq_id,
                "live",
                qid.clone(),
                HashMap::new(),
                ResultType::Unmatched,
                chrono::Utc::now(),
            ))
            .expect("broadcast send should succeed");

        let mut removed = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if mv_manager
                .get_row(&mv_id, &qid.to_string())
                .await
                .unwrap()
                .is_none()
            {
                removed = true;
                break;
            }
        }
        assert!(removed, "SQ unmatch broadcast should delete the MV row");

        handle.abort();
    }
}
