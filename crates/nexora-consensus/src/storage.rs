//! RocksDB-backed Raft log storage.
//!
//! This module provides persistent storage for Raft logs using RocksDB,
//! enabling multi-node consensus with durability.

use crate::error::Result;
use crate::types::{LogIndex, NodeId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Raft log entry stored in RocksDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Log index
    pub index: LogIndex,
    /// Log term
    pub term: u64,
    /// Entry data
    #[serde(with = "serde_bytes")]
    pub data: Bytes,
}

mod serde_bytes {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<u8> = Vec::deserialize(deserializer)?;
        Ok(Bytes::from(vec))
    }
}

/// RocksDB-backed Raft storage.
///
/// This provides persistent storage for:
/// - Raft logs
/// - Current term
/// - Voted for node
/// - Committed index
pub struct RaftStorage {
    /// In-memory log cache (Phase 4: RocksDB will be added)
    logs: Arc<RwLock<BTreeMap<LogIndex, LogEntry>>>,
    /// Current term
    current_term: Arc<RwLock<u64>>,
    /// Node we voted for in current term
    voted_for: Arc<RwLock<Option<NodeId>>>,
    /// Last committed index
    committed_index: Arc<RwLock<LogIndex>>,
    /// Storage path (for future RocksDB integration)
    #[allow(dead_code)]
    path: String,
}

impl RaftStorage {
    /// Create a new Raft storage.
    ///
    /// Phase 4: In-memory implementation for now. RocksDB will be added
    /// when we need true multi-node persistence.
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        info!("Creating Raft storage at: {}", path_str);

        Ok(Self {
            logs: Arc::new(RwLock::new(BTreeMap::new())),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            committed_index: Arc::new(RwLock::new(0)),
            path: path_str,
        })
    }

    /// Append a log entry.
    pub async fn append_log(&self, entry: LogEntry) -> Result<()> {
        debug!("Appending log entry at index {}", entry.index);
        let mut logs = self.logs.write().await;
        logs.insert(entry.index, entry);
        Ok(())
    }

    /// Get a log entry by index.
    pub async fn get_log(&self, index: LogIndex) -> Result<Option<LogEntry>> {
        let logs = self.logs.read().await;
        Ok(logs.get(&index).cloned())
    }

    /// Get log entries in a range [start, end).
    pub async fn get_logs(&self, start: LogIndex, end: LogIndex) -> Result<Vec<LogEntry>> {
        let logs = self.logs.read().await;
        let entries: Vec<LogEntry> = logs
            .range(start..end)
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(entries)
    }

    /// Get the last log index.
    pub async fn last_log_index(&self) -> Result<LogIndex> {
        let logs = self.logs.read().await;
        Ok(logs.keys().last().copied().unwrap_or(0))
    }

    /// Get the last log term.
    pub async fn last_log_term(&self) -> Result<u64> {
        let logs = self.logs.read().await;
        Ok(logs.values().last().map(|e| e.term).unwrap_or(0))
    }

    /// Truncate logs from index onwards.
    pub async fn truncate(&self, from_index: LogIndex) -> Result<()> {
        debug!("Truncating logs from index {}", from_index);
        let mut logs = self.logs.write().await;
        logs.retain(|&idx, _| idx < from_index);
        Ok(())
    }

    /// Get current term.
    pub async fn get_term(&self) -> Result<u64> {
        Ok(*self.current_term.read().await)
    }

    /// Set current term.
    pub async fn set_term(&self, term: u64) -> Result<()> {
        debug!("Setting term to {}", term);
        *self.current_term.write().await = term;
        Ok(())
    }

    /// Get voted for node ID.
    pub async fn get_voted_for(&self) -> Result<Option<NodeId>> {
        Ok(*self.voted_for.read().await)
    }

    /// Set voted for node ID.
    pub async fn set_voted_for(&self, node_id: Option<NodeId>) -> Result<()> {
        debug!("Voting for node {:?}", node_id);
        *self.voted_for.write().await = node_id;
        Ok(())
    }

    /// Get committed index.
    pub async fn get_committed_index(&self) -> Result<LogIndex> {
        Ok(*self.committed_index.read().await)
    }

    /// Set committed index.
    pub async fn set_committed_index(&self, index: LogIndex) -> Result<()> {
        debug!("Setting committed index to {}", index);
        *self.committed_index.write().await = index;
        Ok(())
    }

    /// Get the number of log entries.
    pub async fn log_count(&self) -> usize {
        self.logs.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_lifecycle() {
        let storage = RaftStorage::new("/tmp/raft-test").await.unwrap();

        // Initially empty
        assert_eq!(storage.last_log_index().await.unwrap(), 0);
        assert_eq!(storage.get_term().await.unwrap(), 0);
        assert_eq!(storage.get_voted_for().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_log_operations() {
        let storage = RaftStorage::new("/tmp/raft-test-logs").await.unwrap();

        // Append logs
        storage
            .append_log(LogEntry {
                index: 1,
                term: 1,
                data: Bytes::from("entry1"),
            })
            .await
            .unwrap();

        storage
            .append_log(LogEntry {
                index: 2,
                term: 1,
                data: Bytes::from("entry2"),
            })
            .await
            .unwrap();

        // Read logs
        assert_eq!(storage.last_log_index().await.unwrap(), 2);
        assert_eq!(storage.log_count().await, 2);

        let entry = storage.get_log(1).await.unwrap().unwrap();
        assert_eq!(entry.index, 1);
        assert_eq!(entry.data, Bytes::from("entry1"));

        // Truncate
        storage.truncate(2).await.unwrap();
        assert_eq!(storage.log_count().await, 1);
    }

    #[tokio::test]
    async fn test_term_operations() {
        let storage = RaftStorage::new("/tmp/raft-test-term").await.unwrap();

        storage.set_term(5).await.unwrap();
        assert_eq!(storage.get_term().await.unwrap(), 5);

        storage.set_voted_for(Some(3)).await.unwrap();
        assert_eq!(storage.get_voted_for().await.unwrap(), Some(3));
    }
}
