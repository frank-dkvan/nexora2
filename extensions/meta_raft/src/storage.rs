//! Raft storage implementation for RisingWave Meta election.
//!
//! This module provides persistent storage for Raft log entries and state.

use crate::{Error, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Storage configuration for Raft.
#[derive(Debug, Clone)]
pub struct RaftStorageConfig {
    /// Directory for Raft log and state
    pub data_dir: PathBuf,

    /// Maximum log entries to keep in memory
    pub max_log_entries: usize,

    /// Snapshot interval (number of applied entries)
    pub snapshot_interval: u64,
}

impl Default for RaftStorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/tmp/nexora-raft"),
            max_log_entries: 10_000,
            snapshot_interval: 1_000,
        }
    }
}

/// Raft storage for Meta election.
///
/// Phase 4: Simplified in-memory storage.
/// Future: Add RocksDB or other persistent storage.
///
/// # Example
///
/// ```rust,no_run
/// use extensions_meta_raft::{RaftStorage, RaftStorageConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RaftStorageConfig::default();
/// let storage = RaftStorage::new(config).await?;
/// # Ok(())
/// # }
/// ```
pub struct RaftStorage {
    #[allow(dead_code)]
    config: RaftStorageConfig,
    state: Arc<RwLock<StorageState>>,
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
}

impl RaftStorage {
    /// Create a new Raft storage.
    ///
    /// # Arguments
    ///
    /// - `config`: Storage configuration
    ///
    /// # Errors
    ///
    /// Returns an error if storage initialization fails.
    pub async fn new(config: RaftStorageConfig) -> Result<Self> {
        info!("Initializing Raft storage at {:?}", config.data_dir);

        // Phase 4: Create data directory if it doesn't exist
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .map_err(|e| Error::storage(format!("Failed to create data dir: {}", e)))?;

        let state = Arc::new(RwLock::new(StorageState::default()));

        Ok(Self { config, state })
    }

    /// Get current term.
    pub async fn current_term(&self) -> u64 {
        self.state.read().await.current_term
    }

    /// Set current term.
    pub async fn set_current_term(&self, term: u64) -> Result<()> {
        debug!("Setting current term to {}", term);
        self.state.write().await.current_term = term;
        Ok(())
    }

    /// Get voted for in current term.
    pub async fn voted_for(&self) -> Option<String> {
        self.state.read().await.voted_for.clone()
    }

    /// Set voted for in current term.
    pub async fn set_voted_for(&self, node_id: Option<String>) -> Result<()> {
        debug!("Setting voted_for to {:?}", node_id);
        self.state.write().await.voted_for = node_id;
        Ok(())
    }

    /// Get committed index.
    pub async fn committed_index(&self) -> u64 {
        self.state.read().await.committed_index
    }

    /// Set committed index.
    pub async fn set_committed_index(&self, index: u64) -> Result<()> {
        debug!("Setting committed index to {}", index);
        self.state.write().await.committed_index = index;
        Ok(())
    }

    /// Get applied index.
    pub async fn applied_index(&self) -> u64 {
        self.state.read().await.applied_index
    }

    /// Set applied index.
    pub async fn set_applied_index(&self, index: u64) -> Result<()> {
        debug!("Setting applied index to {}", index);
        self.state.write().await.applied_index = index;
        Ok(())
    }

    /// Take a snapshot.
    ///
    /// Phase 4: Placeholder implementation.
    /// Future: Implement actual snapshot logic.
    pub async fn take_snapshot(&self) -> Result<()> {
        let applied = self.applied_index().await;
        info!("Taking snapshot at applied_index={}", applied);
        // TODO: Implement snapshot logic
        Ok(())
    }

    /// Restore from snapshot.
    ///
    /// Phase 4: Placeholder implementation.
    /// Future: Implement actual restore logic.
    pub async fn restore_snapshot(&self) -> Result<()> {
        info!("Restoring from snapshot");
        // TODO: Implement restore logic
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_lifecycle() {
        let config = RaftStorageConfig::default();
        let storage = RaftStorage::new(config).await.unwrap();

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
    async fn test_storage_indices() {
        let config = RaftStorageConfig::default();
        let storage = RaftStorage::new(config).await.unwrap();

        // Test committed_index
        assert_eq!(storage.committed_index().await, 0);
        storage.set_committed_index(10).await.unwrap();
        assert_eq!(storage.committed_index().await, 10);

        // Test applied_index
        assert_eq!(storage.applied_index().await, 0);
        storage.set_applied_index(8).await.unwrap();
        assert_eq!(storage.applied_index().await, 8);
    }
}
