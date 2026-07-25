//! Per-shard replication log — enables *incremental* state transfer.
//!
//! HA roadmap 阶段3 (incremental). Full-snapshot catch-up ([`crate::state_transfer`])
//! re-ships an entire shard even when the recovering replica is only a few
//! writes behind. This log lets a lagging replica pull just the **delta**.
//!
//! Model: the shard owner assigns a monotonic per-shard sequence number to each
//! replicated write and stamps it into the `FencedWrite`. Every node that holds
//! the shard — the owner (on send) and each follower (on receive) — records
//! `(seq, op)` in a bounded ring buffer keyed by shard. Because the seq is
//! owner-assigned, all replicas share one seq space, so a promoted follower's
//! high-water seq is directly comparable to any other replica's.
//!
//! Catch-up then works by seq comparison:
//! - recovering node knows its high-water seq `h` for the shard (0 if empty);
//! - it asks a surviving source for "everything after `h`";
//! - if the source's ring still covers `h+1` → **incremental** (ship the tail);
//! - otherwise (`h` fell out of the retained window, or the source has no log)
//!   → **full snapshot** fallback.
//!
//! The ring is bounded (`max_entries` per shard) so the log can't grow without
//! bound; a replica that lags past the window simply takes a full snapshot.

use crate::shard_map::ShardId;
use crate::GraphOperation;
use rust_rocksdb::{Options, DB};
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Replication sequence number assigned by the shard owner
pub type ReplicationSeq = u64;

/// What a source can offer a recovering replica for a shard, given the
/// recovering node's high-water seq.
#[derive(Debug)]
pub enum CatchUp {
    /// The recovering node is already at or beyond the source's high-water —
    /// nothing to ship.
    UpToDate,
    /// The delta since the requested seq: `(seq, op)` pairs in seq order, all
    /// with seq strictly greater than the requested high-water.
    Incremental(Vec<(ReplicationSeq, GraphOperation)>),
    /// The requested seq fell outside the retained window (or the source has no
    /// log for the shard) — the caller must fall back to a full snapshot.
    TooOld,
}

/// Per-node, per-shard bounded replication log.
///
/// By default the log is purely in-memory (a bounded ring per shard). When
/// opened with a durable backend ([`ShardReplicationLog::open_durable`]), every
/// `(seq, op)` is also persisted to RocksDB and the retained window is replayed
/// on restart — so incremental catch-up survives a process restart instead of
/// falling back to a full snapshot. Keys are `{shard}:{seq:016x}` (lexicographic
/// = seq order); values are JSON-serialized ops (bincode can't encode the
/// `serde_json::Value` inside `SetProperty`). The durable backend is an internal
/// detail: all call sites use the same type and methods.
#[derive(Clone)]
pub struct ShardReplicationLog {
    inner: Arc<RwLock<HashMap<ShardId, ShardLog>>>,
    /// Max retained entries per shard. Beyond this, the oldest are evicted and a
    /// replica lagging past the window falls back to a full snapshot.
    max_entries: usize,
    /// Optional RocksDB backend for cross-restart durability. `None` = in-memory.
    db: Option<Arc<DB>>,
}

struct ShardLog {
    /// `(seq, op)` in strictly increasing seq order.
    entries: VecDeque<(ReplicationSeq, GraphOperation)>,
    /// Highest seq ever recorded for this shard (survives eviction).
    high_water: ReplicationSeq,
}

impl ShardReplicationLog {
    /// Create a log retaining up to `max_entries` ops per shard (in-memory only).
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            max_entries: max_entries.max(1),
            db: None,
        }
    }

    /// Open a *durable* log at `path`, retaining `max_entries` per shard and
    /// persisting every entry to RocksDB. Replays the retained tail of each
    /// shard's log on startup so incremental catch-up survives a restart.
    ///
    /// Synchronous: the replay map is built before the lock is created, so this
    /// can be called from a non-async constructor (e.g. `ClusterManager::new`).
    pub fn open_durable(
        path: impl AsRef<Path>,
        max_entries: usize,
    ) -> Result<Self, rust_rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, path)?);
        let max_entries = max_entries.max(1);

        // Replay: full scan grouped by shard, keep the last `max_entries` each,
        // in seq order. Build the map fully before wrapping it in the lock so no
        // async is needed.
        let mut by_shard: HashMap<ShardId, Vec<(u64, GraphOperation)>> = HashMap::new();
        for item in db.iterator(rust_rocksdb::IteratorMode::Start) {
            let (k, v) = item?;
            let key_str = String::from_utf8_lossy(&k);
            let Some((shard_str, seq_str)) = key_str.split_once(':') else {
                continue;
            };
            let (Ok(shard), Ok(seq)) = (
                shard_str.parse::<ShardId>(),
                u64::from_str_radix(seq_str, 16),
            ) else {
                continue;
            };
            let Ok(op) = serde_json::from_slice::<GraphOperation>(&v) else {
                continue;
            };
            by_shard.entry(shard).or_default().push((seq, op));
        }
        let mut initial: HashMap<ShardId, ShardLog> = HashMap::new();
        for (shard, mut entries) in by_shard {
            entries.sort_unstable_by_key(|(s, _)| *s);
            let tail = if entries.len() > max_entries {
                &entries[entries.len() - max_entries..]
            } else {
                &entries[..]
            };
            let mut sl = ShardLog {
                entries: VecDeque::new(),
                high_water: 0,
            };
            for (seq, op) in tail {
                sl.high_water = sl.high_water.max(*seq);
                sl.entries.push_back((*seq, op.clone()));
            }
            initial.insert(shard, sl);
        }

        Ok(Self {
            inner: Arc::new(RwLock::new(initial)),
            max_entries,
            db: Some(db),
        })
    }

    /// Persist a `(shard, seq, op)` entry to RocksDB if durable, and prune the
    /// on-disk window to mirror the in-memory ring's retention.
    ///
    /// Returns `Ok(())` if persistence succeeded or no DB is attached.
    /// Returns `Err` if RocksDB write failed, which should trigger degraded mode.
    fn persist(
        &self,
        shard: ShardId,
        seq: u64,
        op: &GraphOperation,
        high_water: u64,
    ) -> Result<(), String> {
        let Some(db) = &self.db else {
            return Ok(()); // No DB attached, in-memory mode is fine
        };

        let key = format!("{shard}:{seq:016x}");
        let val = serde_json::to_vec(op).unwrap_or_default();

        // Critical: handle write failure
        if let Err(e) = db.put(key.as_bytes(), &val) {
            tracing::error!(
                "Replication log persist failed for shard {} seq {}: {}. Degrading to in-memory mode.",
                shard, seq, e
            );
            return Err(format!("RocksDB write failed: {}", e));
        }

        // Prune entries older than the retained window so restart doesn't replay
        // stale ops (mirrors the in-memory pop_front eviction).
        if high_water > self.max_entries as u64 {
            let cutoff = high_water - self.max_entries as u64;
            let prefix = format!("{shard}:");
            let mut stale = Vec::new();
            for item in db.prefix_iterator(prefix.as_bytes()).take_while(|r| {
                r.as_ref()
                    .map(|(k, _)| k.starts_with(prefix.as_bytes()))
                    .unwrap_or(false)
            }) {
                let Ok((k, _)) = item else { continue };
                let ks = String::from_utf8_lossy(&k);
                if let Some((_, seq_str)) = ks.split_once(':') {
                    if let Ok(s) = u64::from_str_radix(seq_str, 16) {
                        if s <= cutoff {
                            stale.push(k.to_vec());
                        }
                    }
                }
            }
            for k in stale {
                // Prune errors are less critical, just log them
                if let Err(e) = db.delete(&k) {
                    tracing::warn!("Failed to prune old replication log entry: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Owner side: assign the next seq for `shard`, record the op, return the
    /// assigned seq. Atomic under the write lock so concurrent owner writes get
    /// distinct, increasing seqs.
    ///
    /// If persistence fails, logs an error but continues in degraded mode
    /// (in-memory only). The seq is still assigned and returned.
    pub async fn record_owner(&self, shard: ShardId, op: GraphOperation) -> u64 {
        let mut map = self.inner.write().await;
        let log = map.entry(shard).or_insert_with(|| ShardLog {
            entries: VecDeque::new(),
            high_water: 0,
        });
        let seq = log.high_water + 1;
        log.high_water = seq;
        log.entries.push_back((seq, op.clone()));
        while log.entries.len() > self.max_entries {
            log.entries.pop_front();
        }
        let hw = log.high_water;
        drop(map);

        // Try to persist, but don't fail the write if persistence fails
        if let Err(e) = self.persist(shard, seq, &op, hw) {
            tracing::error!(
                "Failed to persist replication log for shard {} seq {}: {}. Continuing in degraded mode.",
                shard, seq, e
            );
            // In a production system, you might want to:
            // 1. Set a flag to disable future persistence attempts
            // 2. Alert monitoring systems
            // 3. Consider degrading to in-memory mode permanently for this log
        }

        seq
    }

    /// Follower side: record an op the owner already assigned `seq` to. Keeps the
    /// high-water in step so this replica can later serve — or request — an
    /// incremental delta. Out-of-order or duplicate seqs are ignored beyond
    /// advancing the high-water.
    ///
    /// If persistence fails, logs an error but continues in degraded mode.
    pub async fn record_replica(&self, shard: ShardId, seq: u64, op: GraphOperation) {
        let mut map = self.inner.write().await;
        let log = map.entry(shard).or_insert_with(|| ShardLog {
            entries: VecDeque::new(),
            high_water: 0,
        });
        // Only append if it extends the tail (seq strictly greater than the last
        // recorded). This keeps `entries` monotonic; gaps advance high_water so a
        // later `since` correctly reports TooOld if we can't serve contiguously.
        if seq > log.high_water {
            log.high_water = seq;
            log.entries.push_back((seq, op.clone()));
            while log.entries.len() > self.max_entries {
                log.entries.pop_front();
            }
            let hw = log.high_water;
            drop(map);

            // Try to persist, but don't fail the replica write if persistence fails
            if let Err(e) = self.persist(shard, seq, &op, hw) {
                tracing::error!(
                    "Failed to persist replica log for shard {} seq {}: {}. Continuing in degraded mode.",
                    shard, seq, e
                );
            }
        }
    }

    /// The highest seq recorded for a shard (0 if none). This is a node's
    /// high-water mark, used as the `from_seq` it presents when catching up.
    pub async fn high_water(&self, shard: ShardId) -> u64 {
        self.inner
            .read()
            .await
            .get(&shard)
            .map(|l| l.high_water)
            .unwrap_or(0)
    }

    /// Source side: what can we offer a replica that has everything up to
    /// `from_seq`? See [`CatchUp`].
    pub async fn since(&self, shard: ShardId, from_seq: u64) -> CatchUp {
        let map = self.inner.read().await;
        let Some(log) = map.get(&shard) else {
            return CatchUp::TooOld;
        };
        if from_seq >= log.high_water {
            return CatchUp::UpToDate;
        }
        // Earliest retained seq: to serve contiguously from `from_seq`, the next
        // needed seq (`from_seq + 1`) must be present, i.e. >= earliest retained.
        let earliest = log.entries.front().map(|(s, _)| *s).unwrap_or(u64::MAX);
        if from_seq + 1 < earliest {
            return CatchUp::TooOld;
        }
        let delta: Vec<(u64, GraphOperation)> = log
            .entries
            .iter()
            .filter(|(s, _)| *s > from_seq)
            .cloned()
            .collect();
        CatchUp::Incremental(delta)
    }
}

/// Compatibility alias for anti_entropy module
pub type ReplicationLog = ShardReplicationLog;

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_id::NexoraId;

    fn op(k: &str) -> GraphOperation {
        GraphOperation::SetProperty {
            qid: NexoraId::from_bytes(b"n".to_vec()),
            key: k.into(),
            value: serde_json::json!(1),
        }
    }

    #[tokio::test]
    async fn owner_assigns_monotonic_seqs() {
        let log = ShardReplicationLog::new(100);
        assert_eq!(log.record_owner(0, op("a")).await, 1);
        assert_eq!(log.record_owner(0, op("b")).await, 2);
        assert_eq!(log.record_owner(0, op("c")).await, 3);
        assert_eq!(log.high_water(0).await, 3);
        // Distinct shard has its own seq space.
        assert_eq!(log.record_owner(1, op("x")).await, 1);
    }

    #[tokio::test]
    async fn since_returns_incremental_delta() {
        let log = ShardReplicationLog::new(100);
        for k in ["a", "b", "c", "d"] {
            log.record_owner(0, op(k)).await;
        }
        // Replica has up to seq 2 → gets seqs 3,4.
        match log.since(0, 2).await {
            CatchUp::Incremental(d) => {
                assert_eq!(d.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![3, 4]);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn since_up_to_date_when_caller_current() {
        let log = ShardReplicationLog::new(100);
        log.record_owner(0, op("a")).await;
        assert!(matches!(log.since(0, 1).await, CatchUp::UpToDate));
        assert!(matches!(log.since(0, 5).await, CatchUp::UpToDate));
    }

    #[tokio::test]
    async fn since_too_old_when_past_window() {
        // Retain only 2 entries; write 5 → earliest retained is seq 4.
        let log = ShardReplicationLog::new(2);
        for k in ["a", "b", "c", "d", "e"] {
            log.record_owner(0, op(k)).await;
        }
        // Caller at seq 1 needs seq 2, but earliest retained is 4 → TooOld.
        assert!(matches!(log.since(0, 1).await, CatchUp::TooOld));
        // Caller at seq 3 needs seq 4, which is retained → Incremental.
        assert!(matches!(log.since(0, 3).await, CatchUp::Incremental(_)));
    }

    #[tokio::test]
    async fn since_too_old_for_unknown_shard() {
        let log = ShardReplicationLog::new(100);
        assert!(matches!(log.since(9, 0).await, CatchUp::TooOld));
    }

    #[tokio::test]
    async fn replica_record_tracks_high_water() {
        let log = ShardReplicationLog::new(100);
        log.record_replica(0, 5, op("a")).await;
        log.record_replica(0, 6, op("b")).await;
        assert_eq!(log.high_water(0).await, 6);
        // A duplicate/older seq does not rewind.
        log.record_replica(0, 3, op("c")).await;
        assert_eq!(log.high_water(0).await, 6);
    }

    #[tokio::test]
    async fn durable_log_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repl.db");

        // Phase 1: write owner ops, then drop the log (simulating shutdown).
        {
            let log = ShardReplicationLog::open_durable(&path, 10).unwrap();
            assert_eq!(log.record_owner(0, op("a")).await, 1);
            assert_eq!(log.record_owner(0, op("b")).await, 2);
            assert_eq!(log.high_water(0).await, 2);
        }

        // Phase 2: reopen — high_water and incremental delta must survive.
        {
            let log = ShardReplicationLog::open_durable(&path, 10).unwrap();
            assert_eq!(
                log.high_water(0).await,
                2,
                "high_water must survive restart"
            );
            match log.since(0, 1).await {
                CatchUp::Incremental(ops) => {
                    assert_eq!(ops.len(), 1);
                    assert_eq!(ops[0].0, 2, "incremental delta available after restart");
                }
                other => panic!("expected Incremental, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn durable_log_prunes_old_entries_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repl.db");

        // Retain only 3 entries; write 5 → on-disk window pruned to last 3.
        {
            let log = ShardReplicationLog::open_durable(&path, 3).unwrap();
            for i in 0..5 {
                log.record_owner(0, op(&format!("k{i}"))).await;
            }
            assert_eq!(log.high_water(0).await, 5);
        }

        // Reopen: only seqs 3,4,5 replayed; a caller behind that window is TooOld.
        {
            let log = ShardReplicationLog::open_durable(&path, 3).unwrap();
            assert_eq!(log.high_water(0).await, 5);
            match log.since(0, 2).await {
                CatchUp::Incremental(ops) => {
                    assert_eq!(ops.len(), 3);
                    assert_eq!(ops[0].0, 3);
                    assert_eq!(ops[2].0, 5);
                }
                other => panic!("expected Incremental, got {other:?}"),
            }
            // A caller lagging before the retained window must fall back.
            assert!(matches!(log.since(0, 1).await, CatchUp::TooOld));
        }
    }

    #[tokio::test]
    async fn in_memory_log_has_no_durability() {
        // Sanity: the default (non-durable) constructor keeps nothing on restart —
        // this is the contrast case that motivates open_durable.
        let log = ShardReplicationLog::new(10);
        log.record_owner(0, op("a")).await;
        assert_eq!(log.high_water(0).await, 1);
        // A fresh in-memory log starts empty (no shared state).
        let fresh = ShardReplicationLog::new(10);
        assert_eq!(fresh.high_water(0).await, 0);
    }
}
