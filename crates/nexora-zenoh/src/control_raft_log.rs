//! Control-plane Raft log storage (A2-3).
//!
//! Implements openraft's `RaftLogStorage` over RocksDB — the same embedded KV
//! engine the control-plane store uses, so no new storage dependency. Layout in
//! a dedicated column family / DB:
//!
//! - `log/{index:020}` → JSON-serialized `Entry` (zero-padded index = key order).
//! - `meta/vote`       → the persisted `Vote`.
//! - `meta/committed`  → last committed `LogId` (optional).
//! - `meta/purged`     → last purged `LogId` (optional; entries ≤ this are gone).
//!
//! Writes use fsync (`WriteOptions::set_sync`) — the Raft log is the durability
//! anchor for consensus; losing an acked entry would violate safety. The store
//! is cloneable and internally synchronized (RocksDB handles concurrency), so it
//! serves as its own `LogReader`.

// openraft's `StorageError` is a large enum, but it is the error type the
// `RaftLogStorage` trait fixes for every method here — we cannot box it without
// unwrapping at every trait boundary. The lint is a false positive against a
// trait-constrained API.
#![allow(clippy::result_large_err)]

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{LogId, StorageError, Vote};
use rust_rocksdb as rocksdb;

use crate::control_raft::{ControlNodeId, ControlRaftTypeConfig};

type Entry = openraft::Entry<ControlRaftTypeConfig>;

const VOTE_KEY: &[u8] = b"meta/vote";
const COMMITTED_KEY: &[u8] = b"meta/committed";
const PURGED_KEY: &[u8] = b"meta/purged";
const LOG_PREFIX: &str = "log/";

/// RocksDB-backed Raft log store for the control plane.
#[derive(Clone)]
pub struct ControlLogStore {
    db: Arc<rocksdb::DB>,
}

impl ControlLogStore {
    /// Open (creating if absent) a durable log store at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, rocksdb::Error> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, path)?;
        Ok(Self { db: Arc::new(db) })
    }

    fn log_key(index: u64) -> String {
        // Zero-pad so lexicographic key order == numeric index order.
        format!("{LOG_PREFIX}{index:020}")
    }

    fn synced() -> rocksdb::WriteOptions {
        let mut w = rocksdb::WriteOptions::default();
        w.set_sync(true);
        w
    }

    fn get_meta_log_id(
        &self,
        key: &[u8],
    ) -> Result<Option<LogId<ControlNodeId>>, StorageError<ControlNodeId>> {
        match self.db.get(key).map_err(read_err)? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).map_err(read_err)?),
            None => Ok(None),
        }
    }

    /// Write a batch of entries under `log/{index}`, fsync'd. Shared by the
    /// `RaftLogStorage::append` (which wraps it with the flush callback) and
    /// tests (which can't construct openraft's private `LogFlushed`).
    fn append_entries_sync(
        &self,
        entries: impl IntoIterator<Item = Entry>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let mut batch = rocksdb::WriteBatch::default();
        for entry in entries {
            let key = Self::log_key(entry.log_id.index);
            let value = serde_json::to_vec(&entry).map_err(write_err)?;
            batch.put(key.as_bytes(), value);
        }
        self.db
            .write_opt(&batch, &Self::synced())
            .map_err(write_err)
    }
}

fn read_err(e: impl std::fmt::Display) -> StorageError<ControlNodeId> {
    openraft::StorageIOError::read_logs(openraft::AnyError::error(e)).into()
}
fn write_err(e: impl std::fmt::Display) -> StorageError<ControlNodeId> {
    openraft::StorageIOError::write_logs(openraft::AnyError::error(e)).into()
}

impl RaftLogReader<ControlRaftTypeConfig> for ControlLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<ControlNodeId>> {
        use std::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => i + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => *i + 1,
            Bound::Excluded(i) => *i,
            Bound::Unbounded => u64::MAX,
        };

        let mut out = Vec::new();
        let mut iter = self.db.raw_iterator();
        iter.seek(Self::log_key(start).as_bytes());
        while iter.valid() {
            let Some(key) = iter.key() else { break };
            if !key.starts_with(LOG_PREFIX.as_bytes()) {
                break;
            }
            let idx: u64 = std::str::from_utf8(&key[LOG_PREFIX.len()..])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| read_err("bad log key"))?;
            if idx >= end {
                break;
            }
            let value = iter.value().ok_or_else(|| read_err("missing log value"))?;
            let entry: Entry = serde_json::from_slice(value).map_err(read_err)?;
            out.push(entry);
            iter.next();
        }
        Ok(out)
    }
}

impl RaftLogStorage<ControlRaftTypeConfig> for ControlLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ControlRaftTypeConfig>, StorageError<ControlNodeId>> {
        let last_purged = self.get_meta_log_id(PURGED_KEY)?;

        // Last present log id = the entry with the greatest index, or the purged
        // id if the log is empty (openraft's contract).
        let mut iter = self.db.raw_iterator();
        iter.seek_for_prev(Self::log_key(u64::MAX).as_bytes());
        let last_log_id = if iter.valid() {
            match iter.key() {
                Some(key) if key.starts_with(LOG_PREFIX.as_bytes()) => {
                    let value = iter.value().ok_or_else(|| read_err("missing log value"))?;
                    let entry: Entry = serde_json::from_slice(value).map_err(read_err)?;
                    Some(entry.log_id)
                }
                _ => last_purged,
            }
        } else {
            last_purged
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<ControlNodeId>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let bytes = serde_json::to_vec(vote).map_err(write_err)?;
        self.db
            .put_opt(VOTE_KEY, bytes, &Self::synced())
            .map_err(write_err)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ControlNodeId>>, StorageError<ControlNodeId>> {
        match self.db.get(VOTE_KEY).map_err(read_err)? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(read_err)?)),
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<ControlNodeId>>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let bytes = serde_json::to_vec(&committed).map_err(write_err)?;
        self.db
            .put_opt(COMMITTED_KEY, bytes, &Self::synced())
            .map_err(write_err)
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<ControlNodeId>>, StorageError<ControlNodeId>> {
        self.get_meta_log_id(COMMITTED_KEY)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<ControlRaftTypeConfig>,
    ) -> Result<(), StorageError<ControlNodeId>>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        // fsync the batch, then signal completion so openraft can advance.
        match self.append_entries_sync(entries) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.log_io_completed(Err(std::io::Error::other(e.to_string())));
                Err(write_err("append batch failed"))
            }
        }
    }

    async fn truncate(
        &mut self,
        log_id: LogId<ControlNodeId>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        // Remove all entries with index >= log_id.index (conflicting suffix).
        let mut batch = rocksdb::WriteBatch::default();
        let mut iter = self.db.raw_iterator();
        iter.seek(Self::log_key(log_id.index).as_bytes());
        while iter.valid() {
            let Some(key) = iter.key() else { break };
            if !key.starts_with(LOG_PREFIX.as_bytes()) {
                break;
            }
            batch.delete(key);
            iter.next();
        }
        self.db
            .write_opt(&batch, &Self::synced())
            .map_err(write_err)
    }

    async fn purge(
        &mut self,
        log_id: LogId<ControlNodeId>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        // Remove all entries with index <= log_id.index (already applied/snapshotted),
        // and record the purge point.
        let mut batch = rocksdb::WriteBatch::default();
        let mut iter = self.db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            let Some(key) = iter.key() else { break };
            if !key.starts_with(LOG_PREFIX.as_bytes()) {
                iter.next();
                continue;
            }
            let idx: u64 = std::str::from_utf8(&key[LOG_PREFIX.len()..])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| write_err("bad log key"))?;
            if idx > log_id.index {
                break;
            }
            batch.delete(key);
            iter.next();
        }
        let purged_bytes = serde_json::to_vec(&Some(log_id)).map_err(write_err)?;
        batch.put(PURGED_KEY, purged_bytes);
        self.db
            .write_opt(&batch, &Self::synced())
            .map_err(write_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_raft::{ControlCommand, ControlResponse};
    use nexora_core::control_plane_store::Namespace;
    use openraft::{CommittedLeaderId, EntryPayload};

    fn temp_dir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("nexora-raftlog-{}", uuid::Uuid::new_v4()));
        p
    }

    fn entry(index: u64) -> Entry {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Normal(ControlCommand::put(
                Namespace::ShardMap,
                "k",
                vec![index as u8],
            )),
        }
    }

    // Tests append via the sync helper: openraft's `LogFlushed` callback is
    // private and only constructed inside the engine, so the public
    // `RaftLogStorage::append` path is exercised by the openraft test suite
    // (A2-6+); here we verify the storage/read/truncate/purge logic directly.

    #[tokio::test]
    async fn append_read_and_state() {
        let dir = temp_dir();
        {
            let mut s = ControlLogStore::open(&dir).unwrap();
            s.append_entries_sync([entry(1), entry(2), entry(3)])
                .unwrap();

            let got = s.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(got.len(), 2);
            assert_eq!(got[0].log_id.index, 1);
            assert_eq!(got[1].log_id.index, 2);

            let state = s.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 3);
            assert!(state.last_purged_log_id.is_none());
        }
        // Reopen: entries survive (durable log).
        let mut s2 = ControlLogStore::open(&dir).unwrap();
        assert_eq!(
            s2.get_log_state().await.unwrap().last_log_id.unwrap().index,
            3
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn vote_roundtrip() {
        let dir = temp_dir();
        let mut s = ControlLogStore::open(&dir).unwrap();
        assert!(s.read_vote().await.unwrap().is_none());
        let vote = Vote::new(3, 7);
        s.save_vote(&vote).await.unwrap();
        assert_eq!(s.read_vote().await.unwrap(), Some(vote));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn truncate_removes_suffix() {
        let dir = temp_dir();
        let mut s = ControlLogStore::open(&dir).unwrap();
        s.append_entries_sync([entry(1), entry(2), entry(3)])
            .unwrap();
        s.truncate(LogId::new(CommittedLeaderId::new(1, 0), 2))
            .await
            .unwrap();
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 1, "entries >=2 removed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_removes_prefix_and_records_point() {
        let dir = temp_dir();
        let mut s = ControlLogStore::open(&dir).unwrap();
        s.append_entries_sync([entry(1), entry(2), entry(3)])
            .unwrap();
        s.purge(LogId::new(CommittedLeaderId::new(1, 0), 2))
            .await
            .unwrap();
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
        assert_eq!(state.last_log_id.unwrap().index, 3);
        // Entries <=2 are gone.
        assert!(s.try_get_log_entries(1..3).await.unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Silence unused warning for ControlResponse import in this test module.
    #[allow(dead_code)]
    fn _use_response() -> ControlResponse {
        ControlResponse::default()
    }
}
