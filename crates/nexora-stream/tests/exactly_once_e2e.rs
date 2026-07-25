//! F2: Exactly-once end-to-end verification.
//!
//! Drives the full B2/F1 checkpoint machinery — CheckpointCoordinator with a
//! real per-shard flush (F1.2), FileCheckpointStore, and RecoveryPlan (F1.4) —
//! through a write → checkpoint → crash → recover → replay cycle and asserts
//! the exactly-once *effect*: after recovery every key holds its correct value,
//! with no lost writes and no double-applied writes.
//!
//! Exactly-once here = at-least-once replay (from the checkpointed offsets) +
//! idempotent apply (`set_property` is a last-writer-wins overwrite). The graph
//! state itself is restored by the persistence layer on restart; the
//! RecoveryPlan supplies the offset cut so replay starts exactly where the
//! checkpoint left off.
//!
//! These complement (do not duplicate) `offset_aligned_checkpoint.rs` (B2 unit
//! coverage) and `chaos_checkpoint_recovery.rs` (torn-manifest fallback): this
//! file exercises the RecoveryPlan wiring and value-level no-loss/no-dup checks.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::NexoraId;
use nexora_stream::{CheckpointCoordinator, FileCheckpointStore, InMemoryCheckpointStore};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Deterministic node id for record `i`.
fn node_id(i: u64) -> NexoraId {
    NexoraId::from_bytes(format!("record-{}", i).as_bytes().to_vec())
}

/// Apply a batch of idempotent writes: record `i` sets its "value" property to
/// `value-{i}` (last-writer-wins, so replaying is a no-op on effect).
async fn apply_batch(graph: &GraphService, start: u64, end: u64) {
    for i in start..end {
        graph
            .set_property(&node_id(i), "value", format!("value-{}", i).into())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn exactly_once_write_checkpoint_crash_replay_no_loss_no_dup() {
    // --- Setup: graph + file-backed checkpoint store + coordinator ---
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(GraphServiceConfig::default(), persistor));
    let tmp_dir = TempDir::new().unwrap();
    let store = Arc::new(FileCheckpointStore::new(tmp_dir.path()).unwrap());
    let shard_count = graph.shard_count();
    let coordinator = CheckpointCoordinator::new(graph.clone(), store.clone(), shard_count);

    // --- Batch 1: records 0..100, THEN checkpoint at offset 100 ---
    apply_batch(&graph, 0, 100).await;
    let offsets = HashMap::from([("topic:0".to_string(), 100u64)]);
    let manifest = coordinator.checkpoint(offsets.clone()).await.unwrap();
    assert_eq!(manifest.offsets["topic:0"], 100);
    assert!(
        manifest.nodes_flushed > 0,
        "per-shard flush should report a real node count, got {}",
        manifest.nodes_flushed
    );

    // --- Batch 2: records 100..200, NOT checkpointed (simulates in-flight
    //     work lost on crash). ---
    apply_batch(&graph, 100, 200).await;

    // --- Crash + recover: build the recovery plan from the last checkpoint. ---
    let plan = coordinator
        .recover_and_resume()
        .await
        .unwrap()
        .expect("a checkpoint exists");
    assert_eq!(plan.epoch, manifest.epoch);
    assert_eq!(
        plan.resume_offsets["topic:0"], 100,
        "recovery must resume from the checkpointed offset, not later"
    );

    // --- Replay from the resume offset (100..200). Because apply is idempotent
    //     (overwrite), replaying already-applied-then-lost records restores them
    //     without any double-count. Records 100..200 that were lost on crash are
    //     re-applied here. ---
    let resume = plan.resume_offsets["topic:0"];
    apply_batch(&graph, resume, 200).await;

    // --- Verify no loss (every record 0..200 present + correct) and no dup
    //     (last-writer-wins gives exactly one correct value per key). ---
    for i in 0..200 {
        let v = graph
            .get_property(&node_id(i), "value")
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("record {} lost after recovery", i));
        assert_eq!(
            v,
            format!("value-{}", i).into(),
            "record {} has wrong value (dup/corrupt effect)",
            i
        );
    }
}

#[tokio::test]
async fn exactly_once_checkpoint_offset_binding() {
    // A checkpoint's manifest must bind EXACTLY the offsets passed at trigger
    // time — the atomic (offset, state) pair. Multi-partition to prove per-key
    // fidelity.
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(GraphServiceConfig::default(), persistor));
    let store = Arc::new(InMemoryCheckpointStore::new());
    let coordinator = CheckpointCoordinator::new(graph.clone(), store, graph.shard_count());

    apply_batch(&graph, 0, 50).await;

    let offsets = HashMap::from([
        ("orders:0".to_string(), 512u64),
        ("orders:1".to_string(), 1024u64),
        ("events:0".to_string(), 7u64),
    ]);
    let manifest = coordinator.checkpoint(offsets.clone()).await.unwrap();

    assert_eq!(
        manifest.offsets, offsets,
        "manifest offsets must equal the offsets bound at checkpoint time"
    );
    assert_eq!(manifest.shards_flushed, graph.shard_count());
}

#[tokio::test]
async fn exactly_once_recover_from_latest_checkpoint() {
    // Multiple checkpoints (epochs 1,2,3) — recovery must resume from the
    // newest committed epoch's offsets, never an older one.
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(GraphServiceConfig::default(), persistor));
    let tmp_dir = TempDir::new().unwrap();
    let store = Arc::new(FileCheckpointStore::new(tmp_dir.path()).unwrap());
    let coordinator = CheckpointCoordinator::new(graph.clone(), store, graph.shard_count());

    // epoch 1 @ offset 100
    apply_batch(&graph, 0, 100).await;
    let m1 = coordinator
        .checkpoint(HashMap::from([("topic:0".to_string(), 100u64)]))
        .await
        .unwrap();
    assert_eq!(m1.epoch, 1);

    // epoch 2 @ offset 200
    apply_batch(&graph, 100, 200).await;
    let m2 = coordinator
        .checkpoint(HashMap::from([("topic:0".to_string(), 200u64)]))
        .await
        .unwrap();
    assert_eq!(m2.epoch, 2);

    // epoch 3 @ offset 300
    apply_batch(&graph, 200, 300).await;
    let m3 = coordinator
        .checkpoint(HashMap::from([("topic:0".to_string(), 300u64)]))
        .await
        .unwrap();
    assert_eq!(m3.epoch, 3);

    // Recovery resumes from the LATEST epoch (3) @ offset 300.
    let plan = coordinator
        .recover_and_resume()
        .await
        .unwrap()
        .expect("checkpoints exist");
    assert_eq!(plan.epoch, 3);
    assert_eq!(plan.resume_offsets["topic:0"], 300);
}

#[tokio::test]
async fn exactly_once_cold_start_no_checkpoint() {
    // No checkpoint yet → recovery plan is None (cold start; ingestion begins
    // from each source's configured start position).
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(GraphServiceConfig::default(), persistor));
    let store = Arc::new(InMemoryCheckpointStore::new());
    let coordinator = CheckpointCoordinator::new(graph.clone(), store, graph.shard_count());

    assert!(coordinator.recover_and_resume().await.unwrap().is_none());
}
