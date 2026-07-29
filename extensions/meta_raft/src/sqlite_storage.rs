//! SQLite-backed Raft storage for persistent Meta catalog replication.
//!
//! This module implements Raft log and state machine persistence using an
//! in-memory key-value store (placeholder for SQLite), enabling Raft-coordinated
//! catalog synchronization across Meta nodes.
//!
//! **Note**: Direct SQLite integration conflicts with RisingWave's sqlx dependency
//! on libsqlite3-sys. The actual SQLite persistence will be implemented via
//! RisingWave's existing storage layer once Phase 4 integration completes.

use crate::{Error, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// SQLite storage configuration for Raft.
#[derive(Debug, Clone)]
pub struct SqliteStorageConfig {
    /// Path to SQLite database file (for future use)
    pub db_path: PathBuf,

    /// Maximum log entries to keep before compaction
    pub max_log_entries: usize,

    /// Snapshot interval (number of applied entries)
    pub snapshot_interval: u64,
}

impl Default for SqliteStorageConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("/tmp/nexora-raft/raft.db"),
            max_log_entries: 10_000,
            snapshot_interval: 1_000,
        }
    }
}

/// Raft storage backed by in-memory key-value store.
///
/// This is a placeholder implementation that will be replaced with actual
/// SQLite persistence once RisingWave Meta integration is complete (Phase 4).
///
/// # Architecture
///
/// ```text
/// SqliteStorage (In-Memory)
/// ├─ state: RaftState         → current_term, voted_for, indices
/// ├─ log: Vec<LogEntry>       → append-only log entries
/// └─ catalog: HashMap<K,V>    → Meta catalog snapshot
/// ```
pub struct SqliteStorage {
    config: SqliteStorageConfig,
    state: Arc<RwLock<StorageState>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
    catalog: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

#[derive(Debug, Default)]
struct StorageState {
    /// Current term
    current_term: u64,

    /// Voted for in current term
    voted_for: Option<String>,

    /// Committed log index
    committed_index: u64,

    /// Applied log index
    applied_index: u64,

    /// Whether database is initialized
    db_initialized: bool,
}

impl SqliteStorage {
    /// Create a new Raft storage.
    ///
    /// Currently uses in-memory storage. Will be upgraded to SQLite in Phase 4.
    ///
    /// # Arguments
    ///
    /// - `config`: Storage configuration
    ///
    /// # Errors
    ///
    /// Returns an error if storage initialization fails.
    pub async fn new(config: SqliteStorageConfig) -> Result<Self> {
        info!("Initializing Raft storage (in-memory) at {:?}", config.db_path);

        // Create parent directory if it doesn't exist (for future SQLite use)
        if let Some(parent) = config.db_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::storage(format!("Failed to create db dir: {}", e)))?;
        }

        let mut state_data = StorageState::default();
        state_data.db_initialized = true;

        let state = Arc::new(RwLock::new(state_data));
        let log = Arc::new(RwLock::new(Vec::new()));
        let catalog = Arc::new(RwLock::new(HashMap::new()));

        info!("Raft storage initialized successfully (in-memory mode)");
        Ok(Self {
            config,
            state,
            log,
            catalog,
        })
    }

    /// Get current term.
    pub async fn current_term(&self) -> u64 {
        self.state.read().await.current_term
    }

    /// Set current term and persist to storage.
    pub async fn set_current_term(&self, term: u64) -> Result<()> {
        debug!("Setting current term to {}", term);
        self.state.write().await.current_term = term;
        Ok(())
    }

    /// Get voted for in current term.
    pub async fn voted_for(&self) -> Option<String> {
        self.state.read().await.voted_for.clone()
    }

    /// Set voted for and persist to storage.
    pub async fn set_voted_for(&self, node_id: Option<String>) -> Result<()> {
        debug!("Setting voted_for to {:?}", node_id);
        self.state.write().await.voted_for = node_id;
        Ok(())
    }

    /// Get committed index.
    pub async fn committed_index(&self) -> u64 {
        self.state.read().await.committed_index
    }

    /// Set committed index and persist to storage.
    pub async fn set_committed_index(&self, index: u64) -> Result<()> {
        debug!("Setting committed index to {}", index);
        self.state.write().await.committed_index = index;
        Ok(())
    }

    /// Get applied index.
    pub async fn applied_index(&self) -> u64 {
        self.state.read().await.applied_index
    }

    /// Set applied index and persist to storage.
    pub async fn set_applied_index(&self, index: u64) -> Result<()> {
        debug!("Setting applied index to {}", index);
        self.state.write().await.applied_index = index;
        Ok(())
    }

    /// Append a log entry to the Raft log.
    ///
    /// # Arguments
    ///
    /// - `index`: Log index
    /// - `term`: Term when entry was created
    /// - `entry_type`: Type of entry ("config", "catalog_ddl", "noop")
    /// - `data`: Entry payload (serialized operation)
    pub async fn append_log(
        &self,
        index: u64,
        term: u64,
        entry_type: &str,
        data: Option<&[u8]>,
    ) -> Result<()> {
        debug!(
            "Appending log entry: index={}, term={}, type={}",
            index, term, entry_type
        );

        let entry = LogEntry {
            index,
            term,
            entry_type: entry_type.to_string(),
            data: data.map(|d| d.to_vec()),
        };

        self.log.write().await.push(entry);
        Ok(())
    }

    /// Get log entries in range [start_index, end_index).
    pub async fn get_log_entries(
        &self,
        start_index: u64,
        end_index: u64,
    ) -> Result<Vec<LogEntry>> {
        debug!(
            "Fetching log entries: [{}, {})",
            start_index, end_index
        );

        let log = self.log.read().await;
        let entries = log
            .iter()
            .filter(|e| e.index >= start_index && e.index < end_index)
            .cloned()
            .collect();

        Ok(entries)
    }

    /// Compact log by removing entries before the given index.
    pub async fn compact_log(&self, before_index: u64) -> Result<()> {
        info!("Compacting log before index {}", before_index);

        let mut log = self.log.write().await;
        log.retain(|e| e.index >= before_index);

        Ok(())
    }

    /// Take a snapshot of the current state machine.
    ///
    /// Writes the current catalog state to the snapshot storage.
    pub async fn take_snapshot(&self) -> Result<()> {
        let applied = self.applied_index().await;
        info!("Taking snapshot at applied_index={}", applied);

        // Snapshot is already in memory, no-op for in-memory mode
        Ok(())
    }

    /// Restore from snapshot.
    ///
    /// Loads the catalog state from the snapshot storage.
    pub async fn restore_snapshot(&self) -> Result<()> {
        info!("Restoring from snapshot");

        // Snapshot is already in memory, no-op for in-memory mode
        Ok(())
    }

    /// Apply a catalog operation to the state machine.
    ///
    /// # Arguments
    ///
    /// - `key`: Catalog key
    /// - `value`: Catalog value (None = delete)
    pub async fn apply_catalog_op(&self, key: String, value: Option<Vec<u8>>) -> Result<()> {
        let mut catalog = self.catalog.write().await;

        if let Some(v) = value {
            debug!("Applying catalog op: SET {}", key);
            catalog.insert(key, v);
        } else {
            debug!("Applying catalog op: DELETE {}", key);
            catalog.remove(&key);
        }

        Ok(())
    }

    /// Get a catalog entry.
    pub async fn get_catalog_entry(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let catalog = self.catalog.read().await;
        Ok(catalog.get(key).cloned())
    }
}

/// Log entry structure.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub entry_type: String,
    pub data: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqlite_storage_lifecycle() {
        let temp_dir = std::env::temp_dir().join("nexora-raft-test");
        let config = SqliteStorageConfig {
            db_path: temp_dir.join("test.db"),
            ..Default::default()
        };

        let storage = SqliteStorage::new(config).await.unwrap();

        // Test term
        assert_eq!(storage.current_term().await, 0);
        storage.set_current_term(5).await.unwrap();
        assert_eq!(storage.current_term().await, 5);

        // Test voted_for
        assert_eq!(storage.voted_for().await, None);
        storage
            .set_voted_for(Some("node-1".to_string()))
            .await
            .unwrap();
        assert_eq!(storage.voted_for().await, Some("node-1".to_string()));
    }

    #[tokio::test]
    async fn test_sqlite_storage_indices() {
        let temp_dir = std::env::temp_dir().join("nexora-raft-test");
        let config = SqliteStorageConfig {
            db_path: temp_dir.join("test2.db"),
            ..Default::default()
        };

        let storage = SqliteStorage::new(config).await.unwrap();

        // Test committed_index
        assert_eq!(storage.committed_index().await, 0);
        storage.set_committed_index(10).await.unwrap();
        assert_eq!(storage.committed_index().await, 10);

        // Test applied_index
        assert_eq!(storage.applied_index().await, 0);
        storage.set_applied_index(8).await.unwrap();
        assert_eq!(storage.applied_index().await, 8);
    }

    #[tokio::test]
    async fn test_sqlite_storage_log_operations() {
        let temp_dir = std::env::temp_dir().join("nexora-raft-test");
        let config = SqliteStorageConfig {
            db_path: temp_dir.join("test3.db"),
            ..Default::default()
        };

        let storage = SqliteStorage::new(config).await.unwrap();

        // Test log append
        storage
            .append_log(1, 1, "catalog_ddl", Some(b"CREATE SOURCE test"))
            .await
            .unwrap();

        storage
            .append_log(2, 1, "catalog_ddl", Some(b"CREATE MV test_mv"))
            .await
            .unwrap();

        // Test log retrieval
        let entries = storage.get_log_entries(0, 10).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 2);

        // Test compaction
        storage.compact_log(2).await.unwrap();
        let entries_after = storage.get_log_entries(0, 10).await.unwrap();
        assert_eq!(entries_after.len(), 1); // Only entry with index 2 remains
    }

    #[tokio::test]
    async fn test_sqlite_storage_catalog_ops() {
        let temp_dir = std::env::temp_dir().join("nexora-raft-test");
        let config = SqliteStorageConfig {
            db_path: temp_dir.join("test4.db"),
            ..Default::default()
        };

        let storage = SqliteStorage::new(config).await.unwrap();

        // Test SET
        storage
            .apply_catalog_op("source/test".to_string(), Some(b"source_data".to_vec()))
            .await
            .unwrap();

        let value = storage.get_catalog_entry("source/test").await.unwrap();
        assert_eq!(value, Some(b"source_data".to_vec()));

        // Test DELETE
        storage
            .apply_catalog_op("source/test".to_string(), None)
            .await
            .unwrap();

        let value_after = storage.get_catalog_entry("source/test").await.unwrap();
        assert_eq!(value_after, None);
    }
}
