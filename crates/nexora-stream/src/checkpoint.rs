//! B2: Offset-aligned consistency checkpoints.
//!
//! Binds (source offsets, graph state) into an atomic snapshot pair so a crash
//! recovers to a consistent cut: graph state at epoch N + the source offsets as
//! of epoch N. Events after the checkpoint replay (at-least-once); combined with
//! idempotent apply, this yields exactly-once effect.

use crate::parallel_checkpoint::ParallelCheckpointFlusher;
use nexora_barrier::{BarrierKind, BarrierScheduler, ShardStatus};
use nexora_core::{ChecksumKind, GraphService, SnapshotKind, SnapshotManifest};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A committed checkpoint's metadata — the atomic (offset, state) pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckpointManifest {
    /// Epoch this checkpoint corresponds to (monotonic).
    pub epoch: u64,
    /// Wall-clock creation time (millis since epoch).
    pub created_at_ms: u64,
    /// Source offsets captured at this checkpoint, keyed by "{topic}:{partition}".
    /// On recovery, ingestion resumes from these offsets.
    pub offsets: HashMap<String, u64>,
    /// Number of graph shards flushed for this checkpoint.
    pub shards_flushed: usize,
    /// Total nodes flushed (diagnostic).
    pub nodes_flushed: u64,
}

/// Persists checkpoint manifests durably. The LATEST valid manifest is the
/// recovery point. Implementations must write atomically (a torn manifest must
/// not be read as valid — see the "manifest last" pattern in the roadmap).
#[async_trait::async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Persist a manifest atomically. After this returns Ok, the manifest is
    /// durable and will be the recovery point until a newer one commits.
    async fn save_manifest(&self, manifest: &CheckpointManifest) -> Result<(), String>;
    /// Load the latest committed manifest, or None if no checkpoint exists.
    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, String>;
}

/// In-memory checkpoint store for testing.
pub struct InMemoryCheckpointStore {
    manifest: tokio::sync::RwLock<Option<CheckpointManifest>>,
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        Self {
            manifest: tokio::sync::RwLock::new(None),
        }
    }
}

impl Default for InMemoryCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn save_manifest(&self, manifest: &CheckpointManifest) -> Result<(), String> {
        let mut guard = self.manifest.write().await;
        // Only keep the latest (highest epoch)
        if let Some(existing) = guard.as_ref() {
            if manifest.epoch <= existing.epoch {
                return Err(format!(
                    "manifest epoch {} not monotonic (current: {})",
                    manifest.epoch, existing.epoch
                ));
            }
        }
        *guard = Some(manifest.clone());
        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, String> {
        let guard = self.manifest.read().await;
        Ok(guard.clone())
    }
}

/// File-based checkpoint store with atomic writes.
///
/// Writes checkpoint-{epoch}.json.tmp → fsync → rename → fsync directory.
/// Load enumerates the directory and picks the highest epoch valid manifest.
///
/// File format (B1 integration):
///   {checkpoint_json}
///   ---MANIFEST---
///   {snapshot_manifest_json}
///
/// The SnapshotManifest provides cryptographic integrity (CRC32 for speed) so
/// torn writes or corruption are detected on load, not just invalid JSON.
pub struct FileCheckpointStore {
    dir: PathBuf,
}

impl FileCheckpointStore {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, String> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("failed to create checkpoint dir: {}", e))?;
        Ok(Self { dir })
    }

    /// Parse checkpoint filename to extract epoch.
    fn parse_checkpoint_file(name: &str) -> Option<u64> {
        // checkpoint-123.json → 123
        if !name.starts_with("checkpoint-") || !name.ends_with(".json") {
            return None;
        }
        let epoch_str = &name["checkpoint-".len()..name.len() - ".json".len()];
        epoch_str.parse().ok()
    }

    /// Clean up old checkpoints, keeping the latest N.
    fn cleanup_old_checkpoints(&self, keep: usize) -> Result<(), String> {
        let mut checkpoints: Vec<(u64, PathBuf)> = Vec::new();

        let entries = std::fs::read_dir(&self.dir)
            .map_err(|e| format!("failed to read checkpoint dir: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("failed to read dir entry: {}", e))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some(epoch) = Self::parse_checkpoint_file(&name_str) {
                checkpoints.push((epoch, entry.path()));
            }
        }

        // Sort by epoch descending
        checkpoints.sort_by_key(|(epoch, _)| std::cmp::Reverse(*epoch));

        // Remove all except the latest `keep`
        for (_epoch, path) in checkpoints.iter().skip(keep) {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("failed to remove old checkpoint {:?}: {}", path, e);
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn save_manifest(&self, manifest: &CheckpointManifest) -> Result<(), String> {
        let filename = format!("checkpoint-{}.json", manifest.epoch);
        let tmp_path = self.dir.join(format!("{}.tmp", filename));
        let final_path = self.dir.join(&filename);

        // Serialize checkpoint manifest
        let checkpoint_json = serde_json::to_string_pretty(manifest)
            .map_err(|e| format!("failed to serialize manifest: {}", e))?;

        // B1 integration: create snapshot manifest for integrity verification
        let snapshot_manifest = SnapshotManifest::for_payload(
            SnapshotKind::StreamCheckpoint,
            manifest.epoch,
            checkpoint_json.as_bytes(),
            ChecksumKind::Crc32, // Fast, adequate for torn-write detection
        );

        let snapshot_json = serde_json::to_string_pretty(&snapshot_manifest)
            .map_err(|e| format!("failed to serialize snapshot manifest: {}", e))?;

        // Combined format: checkpoint + separator + snapshot manifest
        let content = format!("{}\n---MANIFEST---\n{}\n", checkpoint_json, snapshot_json);

        // Write to tmp file
        tokio::fs::write(&tmp_path, content)
            .await
            .map_err(|e| format!("failed to write tmp manifest: {}", e))?;

        // Fsync the tmp file
        let file = tokio::fs::File::open(&tmp_path)
            .await
            .map_err(|e| format!("failed to open tmp for fsync: {}", e))?;
        file.sync_all()
            .await
            .map_err(|e| format!("failed to fsync tmp: {}", e))?;

        // Atomic rename
        tokio::fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|e| format!("failed to rename manifest: {}", e))?;

        // Fsync directory to persist the rename
        #[cfg(unix)]
        {
            let dir_file = tokio::fs::File::open(&self.dir)
                .await
                .map_err(|e| format!("failed to open dir for fsync: {}", e))?;
            dir_file
                .sync_all()
                .await
                .map_err(|e| format!("failed to fsync dir: {}", e))?;
        }

        // Clean up old checkpoints (keep latest 2)
        self.cleanup_old_checkpoints(2)?;

        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, String> {
        let entries = std::fs::read_dir(&self.dir)
            .map_err(|e| format!("failed to read checkpoint dir: {}", e))?;

        let mut highest_epoch: Option<u64> = None;
        let mut highest_manifest: Option<CheckpointManifest> = None;

        for entry in entries {
            let entry = entry.map_err(|e| format!("failed to read dir entry: {}", e))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some(epoch) = Self::parse_checkpoint_file(&name_str) {
                // Try to parse and verify the checkpoint
                let path = entry.path();
                match tokio::fs::read_to_string(&path).await {
                    Ok(content) => {
                        // B1 integration: check for snapshot manifest and verify integrity
                        let (checkpoint_json, verified) = if content.contains("---MANIFEST---") {
                            // New format with integrity verification
                            match Self::verify_checkpoint_with_manifest(&content) {
                                Ok(checkpoint_json) => (checkpoint_json, true),
                                Err(e) => {
                                    tracing::warn!(
                                        "checkpoint {:?} failed integrity check: {}",
                                        path,
                                        e
                                    );
                                    continue;
                                }
                            }
                        } else {
                            // Old format without snapshot manifest (backward compat)
                            (content, false)
                        };

                        match serde_json::from_str::<CheckpointManifest>(&checkpoint_json) {
                            Ok(manifest) => {
                                if manifest.epoch != epoch {
                                    tracing::warn!(
                                        "checkpoint file epoch mismatch: filename={}, manifest.epoch={}",
                                        epoch,
                                        manifest.epoch
                                    );
                                    continue;
                                }
                                if verified {
                                    tracing::debug!(
                                        "checkpoint epoch {} verified via B1 snapshot manifest",
                                        epoch
                                    );
                                }
                                if highest_epoch.map_or(true, |h| epoch > h) {
                                    highest_epoch = Some(epoch);
                                    highest_manifest = Some(manifest);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("torn/corrupt checkpoint {:?}: {}", path, e);
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("failed to read checkpoint {:?}: {}", path, e);
                        continue;
                    }
                }
            }
        }

        Ok(highest_manifest)
    }
}

impl FileCheckpointStore {
    /// Verify checkpoint file with embedded snapshot manifest (B1 integration).
    /// Returns the verified checkpoint JSON payload.
    fn verify_checkpoint_with_manifest(content: &str) -> Result<String, String> {
        let parts: Vec<&str> = content.split("---MANIFEST---").collect();
        if parts.len() != 2 {
            return Err("invalid checkpoint format: missing manifest separator".to_string());
        }

        let checkpoint_json = parts[0].trim();
        let manifest_json = parts[1].trim();

        // Deserialize the snapshot manifest
        let snapshot_manifest: SnapshotManifest = serde_json::from_str(manifest_json)
            .map_err(|e| format!("failed to parse snapshot manifest: {}", e))?;

        // Verify the checkpoint payload integrity
        snapshot_manifest.verify(checkpoint_json.as_bytes())?;

        Ok(checkpoint_json.to_string())
    }
}

/// F1.3: RocksDB-backed checkpoint store (feature `rocksdb-offsets`).
///
/// Persists each manifest under key `checkpoint:{epoch:020}` (zero-padded so
/// lexicographic order == numeric order), value = combined
/// `{checkpoint_json}\n---MANIFEST---\n{snapshot_manifest_json}` — identical
/// framing to [`FileCheckpointStore`], so the B1 SnapshotManifest provides
/// CRC32 integrity (torn/corrupt detection) on load.
///
/// Atomicity comes from RocksDB's write path (a `put` is atomic and durable
/// after a synced write); torn detection comes from the B1 manifest verify.
/// `load_latest` seeks to the last key in the checkpoint keyspace and walks
/// backward to the newest manifest that passes integrity verification, so a
/// corrupt newest entry falls back to the previous good one.
#[cfg(feature = "rocksdb-offsets")]
pub struct RocksDbCheckpointStore {
    db: std::sync::Arc<rust_rocksdb::DB>,
}

#[cfg(feature = "rocksdb-offsets")]
impl RocksDbCheckpointStore {
    const CF_CHECKPOINTS: &'static str = "checkpoints";

    /// Open or create a RocksDB at `path` with the checkpoint column family.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        use rust_rocksdb::{ColumnFamilyDescriptor, Options, DB};
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // Force fsync instead of fdatasync for stronger durability guarantees
        opts.set_use_fsync(true);
        let cf = ColumnFamilyDescriptor::new(Self::CF_CHECKPOINTS, Options::default());
        let db = DB::open_cf_descriptors(&opts, path, vec![cf])
            .map_err(|e| format!("RocksDB open checkpoint store: {e}"))?;
        Ok(Self {
            db: std::sync::Arc::new(db),
        })
    }

    fn key(epoch: u64) -> String {
        format!("checkpoint:{epoch:020}")
    }

    /// Build the combined checkpoint+manifest payload (same framing as the
    /// file store) so integrity verification is shared.
    fn encode(manifest: &CheckpointManifest) -> Result<String, String> {
        let checkpoint_json = serde_json::to_string(manifest)
            .map_err(|e| format!("failed to serialize manifest: {}", e))?;
        let snapshot_manifest = SnapshotManifest::for_payload(
            SnapshotKind::StreamCheckpoint,
            manifest.epoch,
            checkpoint_json.as_bytes(),
            ChecksumKind::Crc32,
        );
        let snapshot_json = serde_json::to_string(&snapshot_manifest)
            .map_err(|e| format!("failed to serialize snapshot manifest: {}", e))?;
        Ok(format!(
            "{}\n---MANIFEST---\n{}\n",
            checkpoint_json, snapshot_json
        ))
    }

    /// Verify + decode a stored payload, returning the manifest if intact.
    fn decode(content: &str) -> Result<CheckpointManifest, String> {
        let checkpoint_json = if content.contains("---MANIFEST---") {
            FileCheckpointStore::verify_checkpoint_with_manifest(content)?
        } else {
            content.to_string()
        };
        serde_json::from_str::<CheckpointManifest>(&checkpoint_json)
            .map_err(|e| format!("failed to parse checkpoint manifest: {}", e))
    }
}

#[cfg(feature = "rocksdb-offsets")]
#[async_trait::async_trait]
impl CheckpointStore for RocksDbCheckpointStore {
    async fn save_manifest(&self, manifest: &CheckpointManifest) -> Result<(), String> {
        // Monotonicity guard mirrors the other stores.
        if let Some(existing) = self.load_latest().await? {
            if manifest.epoch <= existing.epoch {
                return Err(format!(
                    "manifest epoch {} not monotonic (current: {})",
                    manifest.epoch, existing.epoch
                ));
            }
        }
        let cf = self
            .db
            .cf_handle(Self::CF_CHECKPOINTS)
            .ok_or_else(|| format!("CF {} not found", Self::CF_CHECKPOINTS))?;
        let content = Self::encode(manifest)?;
        self.db
            .put_cf(cf, Self::key(manifest.epoch).as_bytes(), content.as_bytes())
            .map_err(|e| format!("RocksDB put checkpoint: {e}"))?;
        // Durability: flush the WAL so the checkpoint survives a crash.
        self.db
            .flush_wal(true)
            .map_err(|e| format!("RocksDB flush_wal: {e}"))?;
        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, String> {
        let cf = self
            .db
            .cf_handle(Self::CF_CHECKPOINTS)
            .ok_or_else(|| format!("CF {} not found", Self::CF_CHECKPOINTS))?;
        // Walk from the end (highest epoch) backward, returning the newest
        // manifest that passes integrity verification.
        let iter = self.db.iterator_cf(cf, rust_rocksdb::IteratorMode::End);
        for item in iter {
            let (key_bytes, val_bytes) = item.map_err(|e| format!("RocksDB iter: {e}"))?;
            if !key_bytes.starts_with(b"checkpoint:") {
                continue;
            }
            let content = match String::from_utf8(val_bytes.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("non-UTF8 checkpoint value: {}", e);
                    continue;
                }
            };
            match Self::decode(&content) {
                Ok(manifest) => return Ok(Some(manifest)),
                Err(e) => {
                    tracing::warn!(
                        "torn/corrupt checkpoint at key {:?}: {} — trying previous",
                        String::from_utf8_lossy(&key_bytes),
                        e
                    );
                    continue;
                }
            }
        }
        Ok(None)
    }
}

/// Coordinates offset-aligned checkpoints: on trigger, it takes a barrier,
/// flushes graph state, captures current source offsets, and persists a manifest
/// binding the two. The manifest is the atomic (offset, state) recovery point.
pub struct CheckpointCoordinator {
    scheduler: Arc<BarrierScheduler>,
    graph: Arc<GraphService>,
    store: Arc<dyn CheckpointStore>,
    total_shards: usize,
}

impl CheckpointCoordinator {
    pub fn new(
        graph: Arc<GraphService>,
        store: Arc<dyn CheckpointStore>,
        total_shards: usize,
    ) -> Self {
        Self {
            scheduler: Arc::new(BarrierScheduler::new(total_shards)),
            graph,
            store,
            total_shards,
        }
    }

    /// Trigger a checkpoint: barrier → flush graph → capture offsets → persist manifest.
    ///
    /// `current_offsets` is the source offsets AT THE MOMENT OF THE BARRIER —
    /// captured by the caller (ingestion loop) so the manifest binds exactly the
    /// state that has been applied. Flushing after capturing the offsets ensures
    /// every event up to those offsets is durable in the graph before the
    /// manifest commits (so recovery from the manifest never misses applied data).
    pub async fn checkpoint(
        &self,
        current_offsets: HashMap<String, u64>,
    ) -> Result<CheckpointManifest, String> {
        // 1. Create a barrier for this epoch
        let barrier = self.scheduler.create_barrier(BarrierKind::Checkpoint).await;

        // 2+3. F1.2: real per-shard flush + real-count barrier report.
        //   Flush each shard individually so every applied event up to the
        //   captured offsets is durable, and report each shard's REAL flushed
        //   node count to the barrier. The barrier commits the epoch once all
        //   `total_shards` shards have reported (global consistency cut).
        //
        //   `total_shards` must equal the graph's shard_count so every shard is
        //   both flushed and reported; a mismatch is a config error. We flush
        //   min(total_shards, graph.shard_count()) real shards and, if the
        //   coordinator was configured with fewer logical shards than the graph
        //   (single-shard test setups where total_shards=1), fold the remaining
        //   graph shards into the last reported barrier shard so no applied data
        //   is left un-flushed.
        let graph_shards = self.graph.shard_count();

        // P1-4: 并行刷新优化 - 使用 ParallelCheckpointFlusher
        let flusher = ParallelCheckpointFlusher::new();
        let (nodes_flushed, shard_counts) = flusher
            .flush_all(Arc::clone(&self.graph), self.total_shards, graph_shards)
            .await
            .map_err(|e| format!("parallel checkpoint flush failed: {}", e))?;

        // 向 Barrier 协调器报告每个分片的状态
        for (shard_id, node_count) in shard_counts.iter().enumerate() {
            self.scheduler
                .report_shard(
                    barrier.epoch,
                    shard_id,
                    ShardStatus::Flushed {
                        node_count: *node_count,
                        // Per-event counting is not tracked at flush granularity;
                        // node_count is the real durable-unit count for this shard.
                        event_count: *node_count as u64,
                    },
                )
                .await
                .map_err(|e| format!("barrier report failed: {}", e))?;
        }
        let nodes_flushed = nodes_flushed as usize;

        // 4. Build + persist the manifest (the atomic (offset, state) pair)
        let manifest = CheckpointManifest {
            epoch: barrier.epoch.value(),
            created_at_ms: now_ms(),
            offsets: current_offsets,
            shards_flushed: self.total_shards,
            nodes_flushed: nodes_flushed as u64,
        };

        self.store.save_manifest(&manifest).await?;

        tracing::info!(
            "Checkpoint epoch {} committed: {} nodes, {} offsets",
            manifest.epoch,
            manifest.nodes_flushed,
            manifest.offsets.len()
        );

        Ok(manifest)
    }

    /// Load the latest checkpoint for recovery, if any.
    pub async fn recover(&self) -> Result<Option<CheckpointManifest>, String> {
        self.store.load_latest().await
    }

    /// F1.4: build a recovery plan from the latest committed checkpoint.
    ///
    /// Full crash recovery has two independent halves:
    ///   1. **Graph state** self-recovers from the persistence layer — WAL
    ///      replay + snapshots restore every node's state on restart. This is
    ///      already handled by `GraphService` construction/recovery; the
    ///      coordinator does NOT re-apply graph state.
    ///   2. **Offset alignment** is what this plan provides: ingestion must
    ///      resume from `manifest.offsets` (the offsets bound to the flushed
    ///      state at the checkpoint epoch) rather than from the beginning.
    ///      Events after those offsets replay (at-least-once); combined with
    ///      idempotent apply this yields exactly-once effect.
    ///
    /// Returns `None` when no checkpoint exists (cold start — ingestion begins
    /// from each source's configured start position).
    pub async fn recover_and_resume(&self) -> Result<Option<RecoveryPlan>, String> {
        match self.recover().await? {
            Some(manifest) => Ok(Some(RecoveryPlan {
                epoch: manifest.epoch,
                resume_offsets: manifest.offsets,
            })),
            None => Ok(None),
        }
    }
}

/// F1.4: the offset-alignment half of crash recovery.
///
/// Tells ingestion which offsets to resume from after a restart. Graph state
/// itself is restored independently by the persistence layer (WAL replay +
/// snapshots); this plan only re-aligns the source cursor so replay starts
/// exactly at the checkpointed cut — no gap (data loss) and, with idempotent
/// apply, no double effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPlan {
    /// The checkpoint epoch this plan recovers to.
    pub epoch: u64,
    /// Offsets to resume ingestion from, keyed by "{topic}:{partition}".
    /// Ingestion for each key continues at this offset (exclusive of already
    /// applied events up to and including it).
    pub resume_offsets: HashMap<String, u64>,
}

/// Get current time in milliseconds since epoch.
fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn in_memory_store_keeps_latest() {
        let store = InMemoryCheckpointStore::new();

        let m1 = CheckpointManifest {
            epoch: 1,
            created_at_ms: 1000,
            offsets: HashMap::from([("topic:0".to_string(), 100)]),
            shards_flushed: 1,
            nodes_flushed: 10,
        };
        store.save_manifest(&m1).await.unwrap();

        let m2 = CheckpointManifest {
            epoch: 2,
            created_at_ms: 2000,
            offsets: HashMap::from([("topic:0".to_string(), 200)]),
            shards_flushed: 1,
            nodes_flushed: 20,
        };
        store.save_manifest(&m2).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.epoch, 2);
        assert_eq!(loaded.offsets.get("topic:0"), Some(&200));
    }

    #[tokio::test]
    async fn in_memory_store_rejects_non_monotonic() {
        let store = InMemoryCheckpointStore::new();

        let m1 = CheckpointManifest {
            epoch: 2,
            created_at_ms: 1000,
            offsets: HashMap::new(),
            shards_flushed: 1,
            nodes_flushed: 0,
        };
        store.save_manifest(&m1).await.unwrap();

        let m2 = CheckpointManifest {
            epoch: 1,
            created_at_ms: 2000,
            offsets: HashMap::new(),
            shards_flushed: 1,
            nodes_flushed: 0,
        };
        assert!(store.save_manifest(&m2).await.is_err());
    }

    #[tokio::test]
    async fn file_store_atomic_roundtrip() {
        let tmp_dir = TempDir::new().unwrap();
        let store = FileCheckpointStore::new(tmp_dir.path()).unwrap();

        let manifest = CheckpointManifest {
            epoch: 42,
            created_at_ms: 5000,
            offsets: HashMap::from([("test:0".to_string(), 999)]),
            shards_flushed: 2,
            nodes_flushed: 50,
        };

        store.save_manifest(&manifest).await.unwrap();
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded, manifest);
    }

    #[tokio::test]
    async fn file_store_survives_reopen() {
        let tmp_dir = TempDir::new().unwrap();

        let manifest = CheckpointManifest {
            epoch: 10,
            created_at_ms: 3000,
            offsets: HashMap::from([("stream:1".to_string(), 500)]),
            shards_flushed: 1,
            nodes_flushed: 30,
        };

        {
            let store = FileCheckpointStore::new(tmp_dir.path()).unwrap();
            store.save_manifest(&manifest).await.unwrap();
        }

        // Reopen
        let store = FileCheckpointStore::new(tmp_dir.path()).unwrap();
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded, manifest);
    }

    #[tokio::test]
    async fn file_store_skips_torn_manifest() {
        let tmp_dir = TempDir::new().unwrap();
        let store = FileCheckpointStore::new(tmp_dir.path()).unwrap();

        // Write a valid checkpoint
        let good = CheckpointManifest {
            epoch: 5,
            created_at_ms: 1000,
            offsets: HashMap::new(),
            shards_flushed: 1,
            nodes_flushed: 0,
        };
        store.save_manifest(&good).await.unwrap();

        // Write a torn/corrupt checkpoint (higher epoch but invalid JSON)
        let corrupt_path = tmp_dir.path().join("checkpoint-10.json");
        std::fs::write(&corrupt_path, b"{ invalid json }").unwrap();

        // load_latest should return the valid one (epoch 5), skipping the torn one
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.epoch, 5);
    }

    #[tokio::test]
    async fn file_store_cleans_up_old_checkpoints() {
        let tmp_dir = TempDir::new().unwrap();
        let store = FileCheckpointStore::new(tmp_dir.path()).unwrap();

        // Write 5 checkpoints
        for epoch in 1..=5 {
            let manifest = CheckpointManifest {
                epoch,
                created_at_ms: epoch * 1000,
                offsets: HashMap::new(),
                shards_flushed: 1,
                nodes_flushed: 0,
            };
            store.save_manifest(&manifest).await.unwrap();
        }

        // Check that only the latest 2 remain
        let entries: Vec<_> = std::fs::read_dir(tmp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("checkpoint-"))
            .collect();

        assert_eq!(entries.len(), 2);

        // Should have epochs 4 and 5
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.epoch, 5);
    }

    #[tokio::test]
    async fn coordinator_checkpoint_binds_offsets_and_flushes() {
        use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
        use nexora_id::NexoraId;

        let persistor = Arc::new(InMemoryPersistor::new());
        let config = GraphServiceConfig::default();
        let graph = Arc::new(GraphService::new(config, persistor));

        // Add some data
        let qid = NexoraId::from_bytes(b"test-node".to_vec());
        graph
            .set_property(&qid, "name", "test".into())
            .await
            .unwrap();

        let store = Arc::new(InMemoryCheckpointStore::new());
        let coordinator = CheckpointCoordinator::new(graph.clone(), store.clone(), 1);

        let offsets = HashMap::from([("topic:0".to_string(), 100)]);
        let manifest = coordinator.checkpoint(offsets.clone()).await.unwrap();

        assert_eq!(manifest.epoch, 1);
        assert_eq!(manifest.offsets, offsets);
        assert!(manifest.nodes_flushed > 0);
        assert_eq!(manifest.shards_flushed, 1);

        // Verify the barrier committed
        assert_eq!(coordinator.scheduler.committed_epoch().await.value(), 1);
    }

    #[tokio::test]
    async fn coordinator_recover_returns_latest() {
        use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};

        let persistor = Arc::new(InMemoryPersistor::new());
        let config = GraphServiceConfig::default();
        let graph = Arc::new(GraphService::new(config, persistor));

        let store = Arc::new(InMemoryCheckpointStore::new());
        let coordinator = CheckpointCoordinator::new(graph, store.clone(), 1);

        // No checkpoint yet
        assert!(coordinator.recover().await.unwrap().is_none());

        // Checkpoint twice
        let offsets1 = HashMap::from([("topic:0".to_string(), 100)]);
        coordinator.checkpoint(offsets1).await.unwrap();

        let offsets2 = HashMap::from([("topic:0".to_string(), 200)]);
        coordinator.checkpoint(offsets2.clone()).await.unwrap();

        // Recover should return the latest (epoch 2)
        let recovered = coordinator.recover().await.unwrap().unwrap();
        assert_eq!(recovered.epoch, 2);
        assert_eq!(recovered.offsets, offsets2);
    }

    #[cfg(feature = "rocksdb-offsets")]
    #[tokio::test]
    async fn rocksdb_store_atomic_roundtrip_and_latest() {
        let tmp_dir = TempDir::new().unwrap();
        let store = RocksDbCheckpointStore::open(tmp_dir.path()).unwrap();

        assert!(store.load_latest().await.unwrap().is_none());

        let m1 = CheckpointManifest {
            epoch: 1,
            created_at_ms: 1000,
            offsets: HashMap::from([("topic:0".to_string(), 100)]),
            shards_flushed: 4,
            nodes_flushed: 10,
        };
        store.save_manifest(&m1).await.unwrap();

        let m2 = CheckpointManifest {
            epoch: 2,
            created_at_ms: 2000,
            offsets: HashMap::from([("topic:0".to_string(), 200)]),
            shards_flushed: 4,
            nodes_flushed: 20,
        };
        store.save_manifest(&m2).await.unwrap();

        // Latest is epoch 2.
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded, m2);

        // Non-monotonic save rejected.
        assert!(store.save_manifest(&m1).await.is_err());
    }

    #[cfg(feature = "rocksdb-offsets")]
    #[tokio::test]
    async fn rocksdb_store_survives_reopen() {
        let tmp_dir = TempDir::new().unwrap();
        let manifest = CheckpointManifest {
            epoch: 7,
            created_at_ms: 3000,
            offsets: HashMap::from([("stream:1".to_string(), 500)]),
            shards_flushed: 2,
            nodes_flushed: 30,
        };
        {
            let store = RocksDbCheckpointStore::open(tmp_dir.path()).unwrap();
            store.save_manifest(&manifest).await.unwrap();
        }
        // Reopen same path — checkpoint persists.
        let store = RocksDbCheckpointStore::open(tmp_dir.path()).unwrap();
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded, manifest);
    }

    #[cfg(feature = "rocksdb-offsets")]
    #[tokio::test]
    async fn rocksdb_store_falls_back_on_corrupt_latest() {
        use rust_rocksdb::{ColumnFamilyDescriptor, Options, DB};
        let tmp_dir = TempDir::new().unwrap();
        {
            let store = RocksDbCheckpointStore::open(tmp_dir.path()).unwrap();
            let good = CheckpointManifest {
                epoch: 5,
                created_at_ms: 1000,
                offsets: HashMap::from([("t:0".to_string(), 50)]),
                shards_flushed: 1,
                nodes_flushed: 5,
            };
            store.save_manifest(&good).await.unwrap();
        }
        // Corrupt the newest entry by writing a torn value under a higher epoch
        // key directly, bypassing the encode path.
        {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cf = ColumnFamilyDescriptor::new(
                RocksDbCheckpointStore::CF_CHECKPOINTS,
                Options::default(),
            );
            let db = DB::open_cf_descriptors(&opts, tmp_dir.path(), vec![cf]).unwrap();
            let handle = db
                .cf_handle(RocksDbCheckpointStore::CF_CHECKPOINTS)
                .unwrap();
            db.put_cf(
                handle,
                RocksDbCheckpointStore::key(10).as_bytes(),
                b"{ torn json }\n---MANIFEST---\n{ also torn }",
            )
            .unwrap();
            db.flush_wal(true).unwrap();
        }
        // load_latest should skip corrupt epoch 10 and return valid epoch 5.
        let store = RocksDbCheckpointStore::open(tmp_dir.path()).unwrap();
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.epoch, 5);
    }

    #[tokio::test]
    async fn coordinator_per_shard_flush_reports_real_counts() {
        use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
        use nexora_id::NexoraId;

        let persistor = Arc::new(InMemoryPersistor::new());
        let config = GraphServiceConfig {
            num_shards: 4,
            ..Default::default()
        };
        let graph = Arc::new(GraphService::new(config, persistor));

        // Write several nodes so multiple shards have resident nodes.
        for i in 0..20 {
            let qid = NexoraId::from_bytes(format!("n-{}", i).into_bytes());
            graph
                .set_property(&qid, "v", (i as i64).into())
                .await
                .unwrap();
        }
        let active_before = graph.active_node_count().await;
        assert!(active_before > 0);

        let store = Arc::new(InMemoryCheckpointStore::new());
        // total_shards matches the graph shard count → true per-shard flush.
        let coordinator = CheckpointCoordinator::new(graph.clone(), store, 4);

        let offsets = HashMap::from([("topic:0".to_string(), 20)]);
        let manifest = coordinator.checkpoint(offsets).await.unwrap();

        // Real count: every active node was flushed and reported.
        assert_eq!(
            manifest.nodes_flushed, active_before as u64,
            "per-shard flush should report the real total flushed node count"
        );
        assert_eq!(manifest.shards_flushed, 4);
        // Barrier committed once all 4 shards reported.
        assert_eq!(coordinator.scheduler.committed_epoch().await.value(), 1);
    }

    #[tokio::test]
    async fn file_store_detects_corruption_via_b1_manifest() {
        let tmp_dir = TempDir::new().unwrap();
        let store = FileCheckpointStore::new(tmp_dir.path()).unwrap();

        // Write a valid checkpoint with B1 snapshot manifest
        let manifest = CheckpointManifest {
            epoch: 100,
            created_at_ms: 5000,
            offsets: HashMap::from([("stream:0".to_string(), 1000)]),
            shards_flushed: 1,
            nodes_flushed: 50,
        };
        store.save_manifest(&manifest).await.unwrap();

        // Verify it loads correctly
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.epoch, 100);

        // Now corrupt the checkpoint payload in the file
        let checkpoint_path = tmp_dir.path().join("checkpoint-100.json");
        let mut content = tokio::fs::read_to_string(&checkpoint_path).await.unwrap();

        // Find and corrupt a byte in the checkpoint JSON (before the manifest separator)
        if let Some(separator_pos) = content.find("---MANIFEST---") {
            let mut bytes = content.as_bytes().to_vec();
            // Corrupt a byte in the checkpoint JSON section
            if separator_pos > 50 {
                bytes[50] = bytes[50].wrapping_add(1);
            }
            content = String::from_utf8(bytes).unwrap();
            tokio::fs::write(&checkpoint_path, content).await.unwrap();
        }

        // Write another valid checkpoint with lower epoch
        let manifest2 = CheckpointManifest {
            epoch: 90,
            created_at_ms: 4000,
            offsets: HashMap::from([("stream:0".to_string(), 900)]),
            shards_flushed: 1,
            nodes_flushed: 40,
        };
        store.save_manifest(&manifest2).await.unwrap();

        // load_latest should skip the corrupted epoch 100 and return epoch 90
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(
            loaded.epoch, 90,
            "B1 manifest should detect corruption and fall back to valid checkpoint"
        );
    }
}
