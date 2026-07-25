//! P1.2: Standing Query → Materialized View Integration Test
//!
//! This test demonstrates the end-to-end flow:
//! 1. Create a Standing Query that matches high-value nodes
//! 2. Create a Materialized View with incremental refresh
//! 3. Insert nodes that match the SQ pattern
//! 4. Verify that MV deltas are generated and applied automatically

use nexora_core::materialized_view::{
    ColumnDef, DataType, DeltaOperation, MVDelta, MaterializedRow, MaterializedViewManager,
    RefreshMode,
};
use nexora_core::persistor::InMemoryPersistor;
use nexora_core::GraphService;
use nexora_id::{NexoraId, PropertyValue};
use std::collections::HashMap;
use std::sync::Arc;

/// Simulate SQ match → MV delta generation
fn sq_result_to_mv_delta(
    view_id: &str,
    node_id: &NexoraId,
    properties: &HashMap<String, PropertyValue>,
) -> MVDelta {
    let mut values = HashMap::new();
    values.insert("id".to_string(), PropertyValue::String(node_id.to_hex()));

    if let Some(value) = properties.get("value") {
        values.insert("value".to_string(), value.clone());
    }
    if let Some(status) = properties.get("status") {
        values.insert("status".to_string(), status.clone());
    }

    MVDelta {
        view_id: view_id.to_string(),
        operation: DeltaOperation::Insert,
        row: MaterializedRow {
            key: node_id.to_hex(),
            values,
            version: 1,
            updated_at: chrono::Utc::now(),
        },
        timestamp: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn test_sq_to_mv_incremental_flow() {
    // Setup: Create graph and MV manager
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = nexora_core::graph::GraphServiceConfig::default();
    let graph = Arc::new(GraphService::new(config, persistor));
    let mv_manager = Arc::new(MaterializedViewManager::new());

    // Step 1: Create MV with incremental refresh
    let view_id = mv_manager
        .create_view(
            "high_value_nodes".to_string(),
            "MATCH (n) WHERE n.value > 100 RETURN n.id, n.value, n.status".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "value".to_string(),
                    data_type: DataType::Integer,
                },
                ColumnDef {
                    name: "status".to_string(),
                    data_type: DataType::String,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Step 2: Simulate SQ matching nodes with value > 100
    let matched_nodes = vec![
        (NexoraId::new_random(), 150),
        (NexoraId::new_random(), 200),
        (NexoraId::new_random(), 120),
    ];

    for (node_id, value) in &matched_nodes {
        // Insert node into graph
        graph
            .set_property(node_id, "value", PropertyValue::Integer(*value))
            .await
            .unwrap();
        graph
            .set_property(node_id, "status", PropertyValue::String("active".into()))
            .await
            .unwrap();

        // Simulate SQ evaluation → generate delta
        let mut props = HashMap::new();
        props.insert("value".to_string(), PropertyValue::Integer(*value));
        props.insert("status".to_string(), PropertyValue::String("active".into()));

        let delta = sq_result_to_mv_delta(&view_id, node_id, &props);

        // Apply delta to MV
        mv_manager.apply_delta(delta).await.unwrap();
    }

    // Step 3: Verify MV contains all matched rows
    let rows = mv_manager.scan_view(&view_id, None).await.unwrap();
    assert_eq!(rows.len(), 3, "MV should contain 3 rows from SQ matches");

    // Verify each row has correct values
    for row in &rows {
        let value = row.values.get("value").unwrap();
        if let PropertyValue::Integer(v) = value {
            assert!(*v > 100, "All values should be > 100");
        }
    }

    // Step 4: Verify delta log
    let log = mv_manager.get_delta_log(&view_id, 10).await.unwrap();
    assert_eq!(log.len(), 3, "Delta log should contain 3 insert operations");

    println!("✅ SQ → MV incremental flow test passed");
    println!("   - {} nodes matched SQ pattern", matched_nodes.len());
    println!("   - {} rows in MV", rows.len());
    println!("   - {} deltas in log", log.len());
}

#[tokio::test]
async fn test_sq_unmatch_triggers_mv_delete() {
    let mv_manager = Arc::new(MaterializedViewManager::new());

    let view_id = mv_manager
        .create_view(
            "active_devices".to_string(),
            "MATCH (d:Device) WHERE d.status = 'active' RETURN d.id, d.status".to_string(),
            vec![],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Step 1: Node matches → insert delta
    let node_id = NexoraId::new_random();
    let mut props = HashMap::new();
    props.insert("status".to_string(), PropertyValue::String("active".into()));

    let insert_delta = sq_result_to_mv_delta(&view_id, &node_id, &props);
    mv_manager.apply_delta(insert_delta).await.unwrap();

    // Verify row exists
    let row = mv_manager
        .get_row(&view_id, &node_id.to_hex())
        .await
        .unwrap();
    assert!(row.is_some(), "Row should exist after insert");

    // Step 2: Node no longer matches → delete delta
    props.insert(
        "status".to_string(),
        PropertyValue::String("inactive".into()),
    );

    let delete_delta = MVDelta {
        view_id: view_id.clone(),
        operation: DeltaOperation::Delete,
        row: MaterializedRow {
            key: node_id.to_hex(),
            values: props,
            version: 1,
            updated_at: chrono::Utc::now(),
        },
        timestamp: chrono::Utc::now(),
    };

    mv_manager.apply_delta(delete_delta).await.unwrap();

    // Verify row deleted
    let row = mv_manager
        .get_row(&view_id, &node_id.to_hex())
        .await
        .unwrap();
    assert!(row.is_none(), "Row should be deleted after unmatch");

    println!("✅ SQ unmatch → MV delete test passed");
}

#[tokio::test]
async fn test_mv_update_on_property_change() {
    let mv_manager = Arc::new(MaterializedViewManager::new());

    let view_id = mv_manager
        .create_view(
            "device_metrics".to_string(),
            "MATCH (d:Device) RETURN d.id, d.temperature, d.status".to_string(),
            vec![],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    let node_id = NexoraId::new_random();

    // Step 1: Initial insert
    let mut props = HashMap::new();
    props.insert("temperature".to_string(), PropertyValue::Float(25.0));
    props.insert("status".to_string(), PropertyValue::String("normal".into()));

    let insert_delta = sq_result_to_mv_delta(&view_id, &node_id, &props);
    mv_manager.apply_delta(insert_delta).await.unwrap();

    // Step 2: Property change → update delta
    props.insert("temperature".to_string(), PropertyValue::Float(85.0));
    props.insert(
        "status".to_string(),
        PropertyValue::String("warning".into()),
    );

    let update_delta = MVDelta {
        view_id: view_id.clone(),
        operation: DeltaOperation::Update { old_version: 1 },
        row: MaterializedRow {
            key: node_id.to_hex(),
            values: props,
            version: 2,
            updated_at: chrono::Utc::now(),
        },
        timestamp: chrono::Utc::now(),
    };

    mv_manager.apply_delta(update_delta).await.unwrap();

    // Verify updated values
    let row = mv_manager
        .get_row(&view_id, &node_id.to_hex())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.version, 2);
    assert_eq!(
        row.values.get("temperature"),
        Some(&PropertyValue::Float(85.0))
    );
    assert_eq!(
        row.values.get("status"),
        Some(&PropertyValue::String("warning".into()))
    );

    println!("✅ MV update on property change test passed");
}

#[tokio::test]
async fn test_concurrent_delta_application() {
    let mv_manager = Arc::new(MaterializedViewManager::new());

    let view_id = mv_manager
        .create_view(
            "concurrent_test".to_string(),
            "MATCH (n) RETURN n.id, n.value".to_string(),
            vec![],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Apply 100 deltas concurrently
    let mut handles = vec![];
    for i in 0..100 {
        let mv = mv_manager.clone();
        let vid = view_id.clone();

        let handle = tokio::spawn(async move {
            let node_id = NexoraId::new_random();
            let mut props = HashMap::new();
            props.insert("value".to_string(), PropertyValue::Integer(i));

            let delta = sq_result_to_mv_delta(&vid, &node_id, &props);
            mv.apply_delta(delta).await.unwrap();
        });

        handles.push(handle);
    }

    // Wait for all deltas to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all rows inserted
    let rows = mv_manager.scan_view(&view_id, None).await.unwrap();
    assert_eq!(
        rows.len(),
        100,
        "All 100 concurrent deltas should be applied"
    );

    println!("✅ Concurrent delta application test passed");
}
