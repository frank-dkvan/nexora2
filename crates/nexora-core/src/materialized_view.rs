//! Materialized View Storage — persistent storage for computed results.
//!
//! Architecture:
//! - Standing Query results → MaterializedView
//! - Incremental updates with delta tracking
//! - Persistent storage in RocksDB
//! - Query optimization through indexed access

use crate::control_plane_store::{ControlPlaneStore, Namespace, RocksDbControlPlaneStore};
use nexora_id::PropertyValue;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A materialized view definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedView {
    /// Unique identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Source query (Cypher pattern)
    pub source_query: String,
    /// Refresh strategy
    pub refresh_mode: RefreshMode,
    /// Created timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last refreshed timestamp
    pub last_refreshed: Option<chrono::DateTime<chrono::Utc>>,
    /// Result schema
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    String,
    Integer,
    Float,
    Boolean,
    List,
    Map,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RefreshMode {
    /// Incremental refresh on every change
    Incremental,
    /// Manual refresh only
    Manual,
    /// Scheduled refresh (cron expression)
    Scheduled(String),
}

/// A row in a materialized view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedRow {
    /// Primary key (node ID or composite key)
    pub key: String,
    /// Column values
    pub values: HashMap<String, PropertyValue>,
    /// Version for optimistic locking
    pub version: u64,
    /// Last updated timestamp
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// ============================================================
// P1.2: Incremental Refresh Delta Types
// ============================================================

/// A delta operation for incremental MV refresh
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVDelta {
    /// View ID this delta applies to
    pub view_id: String,
    /// The operation type
    pub operation: DeltaOperation,
    /// The row being inserted/updated/deleted
    pub row: MaterializedRow,
    /// When this delta was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Type of delta operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaOperation {
    /// Insert a new row (fails if key exists)
    Insert,
    /// Update an existing row (upsert semantics)
    Update { old_version: u64 },
    /// Delete a row
    Delete,
}

/// Result of a refresh operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshResult {
    pub rows_inserted: usize,
    pub rows_updated: usize,
    pub rows_deleted: usize,
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Materialized View Manager
pub struct MaterializedViewManager {
    /// View definitions: view_id -> definition
    views: Arc<RwLock<HashMap<String, MaterializedView>>>,
    /// View data storage: view_id -> row storage
    storage: Arc<RwLock<HashMap<String, MaterializedViewStorage>>>,
    /// A1: unified control-plane store backend (optional). Definitions go under
    /// [`Namespace::MvDef`] keyed by view id; row data under [`Namespace::MvData`]
    /// keyed by `{view_id}:{row_key}`. `None` = in-memory only. Replaces the
    /// former direct RocksDB handle so all metadata shares one durable backend.
    store: Option<Arc<dyn ControlPlaneStore>>,
}

/// Storage for a single materialized view
struct MaterializedViewStorage {
    /// Rows indexed by key
    rows: HashMap<String, MaterializedRow>,
    /// Secondary indexes: column -> serialized value -> keys
    indexes: HashMap<String, HashMap<String, Vec<String>>>,
    /// P1.2: Delta log (last N deltas for debugging/audit)
    delta_log: VecDeque<MVDelta>,
}

/// The in-memory state loaded from the control-plane store on startup: view
/// definitions and their per-view storage, keyed by view id.
type LoadedViews = (
    HashMap<String, MaterializedView>,
    HashMap<String, MaterializedViewStorage>,
);

impl MaterializedViewManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
            storage: Arc::new(RwLock::new(HashMap::new())),
            store: None,
        }
    }

    /// Create with RocksDB persistence.
    ///
    /// A1: the RocksDB handle is now wrapped in a [`RocksDbControlPlaneStore`] so
    /// MV metadata shares the unified control-plane backend. On open, any view
    /// definitions and rows persisted by a prior run are loaded back into memory
    /// (A0 behavior, preserved).
    pub fn with_rocksdb(path: &Path) -> Result<Self, MaterializedViewError> {
        let store = RocksDbControlPlaneStore::open(path)
            .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;
        Self::with_store(Arc::new(store))
    }

    /// A1: build a manager over any [`ControlPlaneStore`] backend. Lets MV share
    /// the *same* physical store as the shard map and SQ definitions (one store
    /// per node) — the shape A2 needs to feed a single consensus-backed store to
    /// every metadata domain. Loads existing definitions + rows on open.
    pub fn with_store(store: Arc<dyn ControlPlaneStore>) -> Result<Self, MaterializedViewError> {
        let (views, storage) = Self::load_from_store(store.as_ref())?;
        Ok(Self {
            views: Arc::new(RwLock::new(views)),
            storage: Arc::new(RwLock::new(storage)),
            store: Some(store),
        })
    }

    /// A1: rebuild the in-memory view definitions + row storage from the
    /// control-plane store. Definitions live in [`Namespace::MvDef`] keyed by id;
    /// rows in [`Namespace::MvData`] keyed by `{view_id}:{row_key}`. Secondary
    /// indexes are rebuilt from the loaded rows (not persisted separately).
    fn load_from_store(
        store: &dyn ControlPlaneStore,
    ) -> Result<LoadedViews, MaterializedViewError> {
        let mut views: HashMap<String, MaterializedView> = HashMap::new();
        let mut storage: HashMap<String, MaterializedViewStorage> = HashMap::new();

        // Load definitions.
        let defs = store
            .list(Namespace::MvDef)
            .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;
        for (_key, value) in defs {
            let view: MaterializedView = serde_json::from_slice(&value)
                .map_err(|e| MaterializedViewError::SerializationError(e.to_string()))?;
            storage
                .entry(view.id.clone())
                .or_insert_with(|| MaterializedViewStorage {
                    rows: HashMap::new(),
                    indexes: HashMap::new(),
                    delta_log: VecDeque::new(),
                });
            views.insert(view.id.clone(), view);
        }

        // Load rows and rebuild indexes. Key layout: `{view_id}:{row_key}`.
        let rows = store
            .list(Namespace::MvData)
            .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;
        for (key, value) in rows {
            let Some(sep) = key.find(':') else {
                continue;
            };
            let view_id = key[..sep].to_string();
            let row: MaterializedRow = serde_json::from_slice(&value)
                .map_err(|e| MaterializedViewError::SerializationError(e.to_string()))?;

            let view_storage = storage
                .entry(view_id)
                .or_insert_with(|| MaterializedViewStorage {
                    rows: HashMap::new(),
                    indexes: HashMap::new(),
                    delta_log: VecDeque::new(),
                });
            let row_key = row.key.clone();
            for (col_name, val) in &row.values {
                let value_str = format!("{:?}", val);
                view_storage
                    .indexes
                    .entry(col_name.clone())
                    .or_default()
                    .entry(value_str)
                    .or_default()
                    .push(row_key.clone());
            }
            view_storage.rows.insert(row_key, row);
        }

        if !views.is_empty() {
            tracing::info!(
                views = views.len(),
                "Materialized views restored from control-plane store"
            );
        }

        Ok((views, storage))
    }

    /// A1: persist an MV definition through the control-plane store. Definitions
    /// are non-replayable metadata; the store fsyncs durable writes before
    /// returning, so a definition survives a crash the instant this returns.
    fn persist_def(&self, view: &MaterializedView) -> Result<(), MaterializedViewError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let value = serde_json::to_vec(view)
            .map_err(|e| MaterializedViewError::SerializationError(e.to_string()))?;
        store
            .put(Namespace::MvDef, &view.id, &value)
            .map_err(|e| MaterializedViewError::StorageError(e.to_string()))
    }

    /// Create a new materialized view
    pub async fn create_view(
        &self,
        name: String,
        source_query: String,
        schema: Vec<ColumnDef>,
        refresh_mode: RefreshMode,
    ) -> Result<String, MaterializedViewError> {
        let id = uuid::Uuid::new_v4().to_string();

        let view = MaterializedView {
            id: id.clone(),
            name,
            source_query,
            refresh_mode,
            created_at: chrono::Utc::now(),
            last_refreshed: None,
            schema,
        };

        // Store view definition
        self.views.write().await.insert(id.clone(), view.clone());

        // Initialize storage
        self.storage.write().await.insert(
            id.clone(),
            MaterializedViewStorage {
                rows: HashMap::new(),
                indexes: HashMap::new(),
                delta_log: VecDeque::new(),
            },
        );

        // Persist definition through the control-plane store (fsync: non-replayable metadata).
        self.persist_def(&view)?;

        tracing::info!(view_id = %id, name = %view.name, "Materialized view created");
        Ok(id)
    }

    /// Insert or update a row in the materialized view
    pub async fn upsert_row(
        &self,
        view_id: &str,
        row: MaterializedRow,
    ) -> Result<(), MaterializedViewError> {
        let mut storage = self.storage.write().await;
        let view_storage = storage
            .get_mut(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        let key = row.key.clone();

        // Before overwriting the old row, clean up its existing index entries.
        // Without this step, an upsert that changes a column value leaves stale
        // index entries pointing to this key under the old value.
        if let Some(old_row) = view_storage.rows.get(&key) {
            for (col_name, old_value) in &old_row.values {
                let value_str = format!("{:?}", old_value);
                if let Some(idx) = view_storage.indexes.get_mut(col_name) {
                    if let Some(keys) = idx.get_mut(&value_str) {
                        keys.retain(|k| k != &key);
                        // Remove the bucket when it becomes empty so that the index
                        // map does not accumulate dead entries over many upserts
                        // that change column values, causing unbounded growth.
                        if keys.is_empty() {
                            idx.remove(&value_str);
                        }
                    }
                }
            }
        }

        // Update row
        view_storage.rows.insert(key.clone(), row.clone());

        // Update indexes with the new column values
        for (col_name, value) in &row.values {
            let value_str = format!("{:?}", value); // Simple serialization
            view_storage
                .indexes
                .entry(col_name.clone())
                .or_insert_with(HashMap::new)
                .entry(value_str)
                .or_insert_with(Vec::new)
                .push(key.clone());
        }

        // Persist row data through the control-plane store if available. Row
        // data is re-derivable from the SQ result stream / source query, so it
        // rides the store's normal (still-fsync'd) write path under MvData.
        if let Some(store) = &self.store {
            let store_key = format!("{}:{}", view_id, key);
            let value = serde_json::to_vec(&row)
                .map_err(|e| MaterializedViewError::SerializationError(e.to_string()))?;
            store
                .put(Namespace::MvData, &store_key, &value)
                .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;
        }

        Ok(())
    }

    /// Query materialized view by primary key
    pub async fn get_row(
        &self,
        view_id: &str,
        key: &str,
    ) -> Result<Option<MaterializedRow>, MaterializedViewError> {
        let storage = self.storage.read().await;
        let view_storage = storage
            .get(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        Ok(view_storage.rows.get(key).cloned())
    }

    /// Query materialized view by indexed column
    pub async fn query_by_column(
        &self,
        view_id: &str,
        column: &str,
        value: &PropertyValue,
    ) -> Result<Vec<MaterializedRow>, MaterializedViewError> {
        let storage = self.storage.read().await;
        let view_storage = storage
            .get(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        let value_str = format!("{:?}", value);
        let keys = view_storage
            .indexes
            .get(column)
            .and_then(|idx| idx.get(&value_str))
            .cloned()
            .unwrap_or_default();

        let rows: Vec<_> = keys
            .iter()
            .filter_map(|k| view_storage.rows.get(k).cloned())
            .collect();

        Ok(rows)
    }

    /// Get all rows from a materialized view
    pub async fn scan_view(
        &self,
        view_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<MaterializedRow>, MaterializedViewError> {
        let storage = self.storage.read().await;
        let view_storage = storage
            .get(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        let mut rows: Vec<_> = view_storage.rows.values().cloned().collect();

        if let Some(n) = limit {
            rows.truncate(n);
        }

        Ok(rows)
    }

    /// Query all rows from a materialized view (alias for scan_view with no limit)
    pub async fn query_all(
        &self,
        view_id: &str,
    ) -> Result<Vec<MaterializedRow>, MaterializedViewError> {
        self.scan_view(view_id, None).await
    }

    /// Delete a row from the materialized view
    pub async fn delete_row(&self, view_id: &str, key: &str) -> Result<(), MaterializedViewError> {
        let mut storage = self.storage.write().await;
        let view_storage = storage
            .get_mut(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        if let Some(row) = view_storage.rows.remove(key) {
            // Remove from indexes
            for (col_name, value) in &row.values {
                let value_str = format!("{:?}", value);
                if let Some(idx) = view_storage.indexes.get_mut(col_name) {
                    if let Some(keys) = idx.get_mut(&value_str) {
                        keys.retain(|k| k != key);
                    }
                }
            }

            // Remove row from the control-plane store if durable.
            if let Some(store) = &self.store {
                let db_key = format!("{}:{}", view_id, key);
                store
                    .delete(Namespace::MvData, &db_key)
                    .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// P0.3: Batch-upsert rows into a materialized view — used by refresh to
    /// replace all rows atomically.  The old rows are cleared, then every row
    /// in the provided iterator is inserted.  This avoids stale data from a
    /// previous refresh when the source query produces fewer rows on re-run.
    pub async fn replace_all(
        &self,
        view_id: &str,
        rows: Vec<MaterializedRow>,
    ) -> Result<(), MaterializedViewError> {
        let mut storage = self.storage.write().await;
        let view_storage = storage
            .get_mut(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        // Clear existing rows and indexes
        view_storage.rows.clear();
        view_storage.indexes.clear();

        let mut count = 0usize;
        for row in rows {
            let key = row.key.clone();
            // Build indexes
            for (col_name, value) in &row.values {
                let value_str = format!("{:?}", value);
                view_storage
                    .indexes
                    .entry(col_name.clone())
                    .or_default()
                    .entry(value_str)
                    .or_default()
                    .push(key.clone());
            }
            view_storage.rows.insert(key, row);
            count += 1;
        }

        // Update last_refreshed timestamp
        if let Ok(mut views) = self.views.try_write() {
            if let Some(view) = views.get_mut(view_id) {
                view.last_refreshed = Some(chrono::Utc::now());
            }
        }

        tracing::info!(view_id = %view_id, rows = count, "Materialized view replaced");
        Ok(())
    }

    /// P0.3: Append a single row to a materialized view (incremental mode).
    /// Used when a SQ match event triggers a delta update to the MV.
    pub async fn append_row(
        &self,
        view_id: &str,
        row: MaterializedRow,
    ) -> Result<(), MaterializedViewError> {
        self.upsert_row(view_id, row).await
    }

    /// Drop a materialized view
    pub async fn drop_view(&self, view_id: &str) -> Result<(), MaterializedViewError> {
        // Remove from memory
        self.views.write().await.remove(view_id);
        self.storage.write().await.remove(view_id);

        // Remove definition + all rows from the control-plane store if durable.
        if let Some(store) = &self.store {
            store
                .delete(Namespace::MvDef, view_id)
                .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;

            // Delete every row for this view. Rows are keyed `{view_id}:{row_key}`;
            // list the namespace and delete those with our view's prefix.
            let prefix = format!("{}:", view_id);
            let rows = store
                .list(Namespace::MvData)
                .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;
            for (key, _) in rows {
                if key.starts_with(&prefix) {
                    store
                        .delete(Namespace::MvData, &key)
                        .map_err(|e| MaterializedViewError::StorageError(e.to_string()))?;
                }
            }
        }

        tracing::info!(view_id = %view_id, "Materialized view dropped");
        Ok(())
    }

    /// List all materialized views
    pub async fn list_views(&self) -> Vec<MaterializedView> {
        self.views.read().await.values().cloned().collect()
    }

    /// Get view definition
    pub async fn get_view(&self, view_id: &str) -> Option<MaterializedView> {
        self.views.read().await.get(view_id).cloned()
    }

    // ============================================================
    // P1.2: Incremental Refresh Operations
    // ============================================================

    /// Apply a single delta incrementally
    pub async fn apply_delta(&self, delta: MVDelta) -> Result<(), MaterializedViewError> {
        let mut storage = self.storage.write().await;
        let view_storage = storage
            .get_mut(&delta.view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(delta.view_id.clone()))?;

        // Record in delta log (keep last 1000)
        view_storage.delta_log.push_back(delta.clone());
        if view_storage.delta_log.len() > 1000 {
            view_storage.delta_log.pop_front();
        }

        match &delta.operation {
            DeltaOperation::Insert => {
                let key = delta.row.key.clone();
                if view_storage.rows.contains_key(&key) {
                    return Err(MaterializedViewError::InvalidQuery(format!(
                        "Insert delta failed: key '{}' already exists",
                        key
                    )));
                }
                // Use internal insert logic (build indexes)
                drop(storage); // Release lock before calling upsert_row
                self.upsert_row(&delta.view_id, delta.row).await?;
            }
            DeltaOperation::Update { old_version } => {
                let key = delta.row.key.clone();
                if let Some(existing) = view_storage.rows.get(&key) {
                    if existing.version != *old_version {
                        return Err(MaterializedViewError::InvalidQuery(format!(
                            "Update delta version mismatch: expected {}, found {}",
                            old_version, existing.version
                        )));
                    }
                }
                // Upsert (update or insert)
                drop(storage);
                self.upsert_row(&delta.view_id, delta.row).await?;
            }
            DeltaOperation::Delete => {
                let key = delta.row.key.clone();
                drop(storage);
                self.delete_row(&delta.view_id, &key).await?;
            }
        }

        Ok(())
    }

    /// Refresh a view according to its refresh mode
    pub async fn refresh_view(
        &self,
        view_id: &str,
    ) -> Result<RefreshResult, MaterializedViewError> {
        let start = std::time::Instant::now();
        let view = self
            .get_view(view_id)
            .await
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        let result = match view.refresh_mode {
            RefreshMode::Incremental => {
                // Incremental mode: deltas are applied on-the-fly via apply_delta
                // Manual refresh in incremental mode is a no-op
                RefreshResult {
                    rows_inserted: 0,
                    rows_updated: 0,
                    rows_deleted: 0,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: None,
                }
            }
            RefreshMode::Manual | RefreshMode::Scheduled(_) => {
                // GAP-3: Full refresh requires executing the view's source query
                // (Cypher/SQL), which lives in the app/query layer — nexora-core
                // has no query engine. The app-layer handler
                // (`refresh_materialized_view`) performs this: it runs
                // `source_query` and calls `replace_all`. Calling this core
                // method directly for a full-refresh view is a programming error,
                // so fail loudly instead of silently returning 0 rows.
                return Err(MaterializedViewError::InvalidQuery(format!(
                    "refresh_view() cannot execute the source query for a \
                     Manual/Scheduled view ('{view_id}') — nexora-core has no query \
                     engine. Use the app-layer refresh path (executes source_query \
                     then replace_all)."
                )));
            }
        };

        // Update last_refreshed timestamp
        if let Some(v) = self.views.write().await.get_mut(view_id) {
            v.last_refreshed = Some(chrono::Utc::now());
        }

        Ok(result)
    }

    /// Get recent delta log for a view
    pub async fn get_delta_log(
        &self,
        view_id: &str,
        limit: usize,
    ) -> Result<Vec<MVDelta>, MaterializedViewError> {
        let storage = self.storage.read().await;
        let view_storage = storage
            .get(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        let deltas: Vec<_> = view_storage
            .delta_log
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect();

        Ok(deltas)
    }

    /// Set the refresh mode for a view
    pub async fn set_refresh_mode(
        &self,
        view_id: &str,
        mode: RefreshMode,
    ) -> Result<(), MaterializedViewError> {
        let mut views = self.views.write().await;
        let view = views
            .get_mut(view_id)
            .ok_or_else(|| MaterializedViewError::ViewNotFound(view_id.to_string()))?;

        view.refresh_mode = mode;
        let view = view.clone();
        drop(views);

        // A1: persist the updated definition through the control-plane store.
        self.persist_def(&view)?;

        tracing::info!(view_id = %view_id, mode = ?view.refresh_mode, "Refresh mode updated");
        Ok(())
    }

    // ============================================================
    // P0.2: PG-Wire Query Support
    // ============================================================

    /// Find a view by name and return its ID
    pub async fn find_view_by_name(&self, name: &str) -> Option<String> {
        let views = self.views.read().await;
        views
            .iter()
            .find(|(_, v)| v.name == name)
            .map(|(id, _)| id.clone())
    }

    /// Check if a view with the given name exists
    pub async fn view_exists(&self, name: &str) -> bool {
        self.find_view_by_name(name).await.is_some()
    }

    /// Get all rows from a materialized view (alias for query_all)
    /// This is used by PG-Wire to fetch MV data for SQL queries
    pub async fn get_rows(
        &self,
        view_id: &str,
    ) -> Result<Vec<MaterializedRow>, MaterializedViewError> {
        self.query_all(view_id).await
    }
}

impl Default for MaterializedViewManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MaterializedViewError {
    #[error("View not found: {0}")]
    ViewNotFound(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_query_view() {
        let manager = MaterializedViewManager::new();

        // Create view
        let view_id = manager
            .create_view(
                "high_speed_forklifts".to_string(),
                "MATCH (n:Forklift) WHERE n.speed > 100 RETURN n".to_string(),
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::String,
                    },
                    ColumnDef {
                        name: "speed".to_string(),
                        data_type: DataType::Float,
                    },
                ],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Insert row
        let mut values = HashMap::new();
        values.insert("id".to_string(), PropertyValue::String("f001".into()));
        values.insert("speed".to_string(), PropertyValue::Float(120.0));

        let row = MaterializedRow {
            key: "f001".to_string(),
            values,
            version: 1,
            updated_at: chrono::Utc::now(),
        };

        manager.upsert_row(&view_id, row).await.unwrap();

        // Query by key
        let result = manager.get_row(&view_id, "f001").await.unwrap();
        assert!(result.is_some());

        // Scan view
        let rows = manager.scan_view(&view_id, None).await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn test_indexed_query() {
        let manager = MaterializedViewManager::new();

        let view_id = manager
            .create_view(
                "test_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Insert multiple rows
        for i in 0..5 {
            let mut values = HashMap::new();
            values.insert("category".to_string(), PropertyValue::String("A".into()));
            values.insert("value".to_string(), PropertyValue::Integer(i));

            let row = MaterializedRow {
                key: format!("row{}", i),
                values,
                version: 1,
                updated_at: chrono::Utc::now(),
            };

            manager.upsert_row(&view_id, row).await.unwrap();
        }

        // Query by indexed column
        let results = manager
            .query_by_column(&view_id, "category", &PropertyValue::String("A".into()))
            .await
            .unwrap();

        assert_eq!(results.len(), 5);
    }

    // ============================================================
    // P1.2: Incremental Refresh Tests
    // ============================================================

    #[tokio::test]
    async fn test_apply_delta_insert() {
        let manager = MaterializedViewManager::new();

        let view_id = manager
            .create_view(
                "test_delta_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Apply insert delta
        let mut values = HashMap::new();
        values.insert("id".to_string(), PropertyValue::String("item1".into()));
        values.insert("value".to_string(), PropertyValue::Integer(42));

        let delta = MVDelta {
            view_id: view_id.clone(),
            operation: DeltaOperation::Insert,
            row: MaterializedRow {
                key: "item1".to_string(),
                values,
                version: 1,
                updated_at: chrono::Utc::now(),
            },
            timestamp: chrono::Utc::now(),
        };

        manager.apply_delta(delta).await.unwrap();

        // Verify row exists
        let row = manager.get_row(&view_id, "item1").await.unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().version, 1);
    }

    #[tokio::test]
    async fn test_apply_delta_update() {
        let manager = MaterializedViewManager::new();

        let view_id = manager
            .create_view(
                "test_update_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Insert initial row
        let mut values = HashMap::new();
        values.insert("id".to_string(), PropertyValue::String("item1".into()));
        values.insert("value".to_string(), PropertyValue::Integer(10));

        let row = MaterializedRow {
            key: "item1".to_string(),
            values: values.clone(),
            version: 1,
            updated_at: chrono::Utc::now(),
        };

        manager.upsert_row(&view_id, row).await.unwrap();

        // Apply update delta
        values.insert("value".to_string(), PropertyValue::Integer(20));
        let delta = MVDelta {
            view_id: view_id.clone(),
            operation: DeltaOperation::Update { old_version: 1 },
            row: MaterializedRow {
                key: "item1".to_string(),
                values,
                version: 2,
                updated_at: chrono::Utc::now(),
            },
            timestamp: chrono::Utc::now(),
        };

        manager.apply_delta(delta).await.unwrap();

        // Verify updated value
        let row = manager.get_row(&view_id, "item1").await.unwrap().unwrap();
        assert_eq!(row.version, 2);
        assert_eq!(row.values.get("value"), Some(&PropertyValue::Integer(20)));
    }

    #[tokio::test]
    async fn test_apply_delta_delete() {
        let manager = MaterializedViewManager::new();

        let view_id = manager
            .create_view(
                "test_delete_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Insert row
        let mut values = HashMap::new();
        values.insert("id".to_string(), PropertyValue::String("item1".into()));

        let row = MaterializedRow {
            key: "item1".to_string(),
            values: values.clone(),
            version: 1,
            updated_at: chrono::Utc::now(),
        };

        manager.upsert_row(&view_id, row.clone()).await.unwrap();

        // Apply delete delta
        let delta = MVDelta {
            view_id: view_id.clone(),
            operation: DeltaOperation::Delete,
            row,
            timestamp: chrono::Utc::now(),
        };

        manager.apply_delta(delta).await.unwrap();

        // Verify row deleted
        let result = manager.get_row(&view_id, "item1").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delta_log_tracking() {
        let manager = MaterializedViewManager::new();

        let view_id = manager
            .create_view(
                "test_log_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Apply multiple deltas
        for i in 0..5 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), PropertyValue::Integer(i));

            let delta = MVDelta {
                view_id: view_id.clone(),
                operation: DeltaOperation::Insert,
                row: MaterializedRow {
                    key: format!("item{}", i),
                    values,
                    version: 1,
                    updated_at: chrono::Utc::now(),
                },
                timestamp: chrono::Utc::now(),
            };

            manager.apply_delta(delta).await.unwrap();
        }

        // Check delta log
        let log = manager.get_delta_log(&view_id, 10).await.unwrap();
        assert_eq!(log.len(), 5);
    }

    #[tokio::test]
    async fn test_set_refresh_mode() {
        let manager = MaterializedViewManager::new();

        let view_id = manager
            .create_view(
                "test_mode_view".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Change to manual mode
        manager
            .set_refresh_mode(&view_id, RefreshMode::Manual)
            .await
            .unwrap();

        // Verify mode changed
        let view = manager.get_view(&view_id).await.unwrap();
        assert_eq!(view.refresh_mode, RefreshMode::Manual);
    }
}
