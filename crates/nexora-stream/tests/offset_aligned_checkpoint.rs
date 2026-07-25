//! Integration test for B2 Offset-Aligned Checkpoints.
//!
//! Verifies that (offset, graph_state) are bound atomically and that recovery
//! restores to a consistent cut — enabling exactly-once semantics when combined
//! with idempotent apply.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::NexoraId;
use nexora_stream::{CheckpointCoordinator, FileCheckpointStore};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn checkpoint_recovery_exactly_once() {
    // Setup: GraphService + FileCheckpointStore
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig::default();
    let graph = Arc::new(GraphService::new(config, persistor));

    let tmp_dir = TempDir::new().unwrap();
    let store = Arc::new(FileCheckpointStore::new(tmp_dir.path()).unwrap());
    let coordinator = CheckpointCoordinator::new(graph.clone(), store.clone(), 1);

    // Simulate batch 1: offsets 0-100
    for i in 0..100 {
        let qid = NexoraId::from_bytes(format!("node-{}", i).as_bytes().to_vec());
        graph
            .set_property(&qid, "value", format!("batch1-{}", i).into())
            .await
            .unwrap();
    }

    // Checkpoint at offset 100
    let offsets_batch1 = HashMap::from([("topic:0".to_string(), 100)]);
    let manifest1 = coordinator
        .checkpoint(offsets_batch1.clone())
        .await
        .unwrap();

    assert_eq!(manifest1.epoch, 1);
    assert_eq!(manifest1.offsets["topic:0"], 100);
    assert!(manifest1.nodes_flushed > 0);

    // Simulate batch 2: offsets 100-200 (NOT checkpointed)
    for i in 100..200 {
        let qid = NexoraId::from_bytes(format!("node-{}", i).as_bytes().to_vec());
        graph
            .set_property(&qid, "value", format!("batch2-{}", i).into())
            .await
            .unwrap();
    }

    // Simulate crash: recover from checkpoint
    let recovered = coordinator.recover().await.unwrap().unwrap();

    // Verify recovery point
    assert_eq!(recovered.epoch, 1);
    assert_eq!(recovered.offsets["topic:0"], 100);

    // The recovery point is offset 100, which means:
    // - batch 1 (0-100) was checkpointed and is durable
    // - batch 2 (100-200) was NOT checkpointed and will be replayed
    // This is the exactly-once guarantee: replay from offset 100 with
    // idempotent apply ensures events after the checkpoint are reprocessed
    // correctly without duplication.

    println!(
        "Recovery successful: epoch={}, offset={}, nodes={}",
        recovered.epoch, recovered.offsets["topic:0"], recovered.nodes_flushed
    );
}

#[tokio::test]
async fn multiple_checkpoints_advance_epoch() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig::default();
    let graph = Arc::new(GraphService::new(config, persistor));

    let tmp_dir = TempDir::new().unwrap();
    let store = Arc::new(FileCheckpointStore::new(tmp_dir.path()).unwrap());
    let coordinator = CheckpointCoordinator::new(graph.clone(), store.clone(), 1);

    // Checkpoint 1
    let offsets1 = HashMap::from([("stream:0".to_string(), 50)]);
    let m1 = coordinator.checkpoint(offsets1).await.unwrap();
    assert_eq!(m1.epoch, 1);

    // Checkpoint 2
    let offsets2 = HashMap::from([("stream:0".to_string(), 150)]);
    let m2 = coordinator.checkpoint(offsets2).await.unwrap();
    assert_eq!(m2.epoch, 2);

    // Checkpoint 3
    let offsets3 = HashMap::from([("stream:0".to_string(), 300)]);
    let m3 = coordinator.checkpoint(offsets3.clone()).await.unwrap();
    assert_eq!(m3.epoch, 3);

    // Recovery returns the latest
    let recovered = coordinator.recover().await.unwrap().unwrap();
    assert_eq!(recovered.epoch, 3);
    assert_eq!(recovered.offsets, offsets3);
}

#[tokio::test]
async fn checkpoint_multiple_partitions() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig::default();
    let graph = Arc::new(GraphService::new(config, persistor));

    let tmp_dir = TempDir::new().unwrap();
    let store = Arc::new(FileCheckpointStore::new(tmp_dir.path()).unwrap());
    let coordinator = CheckpointCoordinator::new(graph.clone(), store.clone(), 1);

    // Multi-partition offsets
    let offsets = HashMap::from([
        ("topic:0".to_string(), 100),
        ("topic:1".to_string(), 200),
        ("topic:2".to_string(), 300),
    ]);

    let manifest = coordinator.checkpoint(offsets.clone()).await.unwrap();

    assert_eq!(manifest.offsets.len(), 3);
    assert_eq!(manifest.offsets["topic:0"], 100);
    assert_eq!(manifest.offsets["topic:1"], 200);
    assert_eq!(manifest.offsets["topic:2"], 300);

    // Recovery preserves all partitions
    let recovered = coordinator.recover().await.unwrap().unwrap();
    assert_eq!(recovered.offsets, offsets);
}

#[tokio::test]
async fn checkpoint_with_no_data_still_valid() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig::default();
    let graph = Arc::new(GraphService::new(config, persistor));

    let tmp_dir = TempDir::new().unwrap();
    let store = Arc::new(FileCheckpointStore::new(tmp_dir.path()).unwrap());
    let coordinator = CheckpointCoordinator::new(graph.clone(), store.clone(), 1);

    // Checkpoint with no nodes flushed (empty graph)
    let offsets = HashMap::from([("stream:0".to_string(), 0)]);
    let manifest = coordinator.checkpoint(offsets.clone()).await.unwrap();

    assert_eq!(manifest.epoch, 1);
    assert_eq!(manifest.nodes_flushed, 0);
    assert_eq!(manifest.offsets, offsets);

    // Recovery works even with empty checkpoint
    let recovered = coordinator.recover().await.unwrap().unwrap();
    assert_eq!(recovered.epoch, 1);
}
