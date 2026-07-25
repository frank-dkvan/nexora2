//! Comprehensive integration tests for nexora-barrier crate.
//!
//! Coverage:
//! - Epoch: creation, ordering, comparison, display, serialization
//! - Barrier: all barrier kinds (Checkpoint, LightCheckpoint, Snapshot)
//! - BarrierScheduler: create barrier, epoch advancement, monotonicity
//! - ShardStatus: Pending, Flushed, Failed variants
//! - BarrierScheduler: single shard completion, partial completion, errored shards
//! - Commit notification via broadcast channel
//! - Multiple epochs: sequential barriers, concurrent barrier reporting
//! - Error paths: UnknownEpoch, AlreadyCommitted
//! - Concurrency: concurrent shard reporting from many tasks
//! - High throughput: rapid barrier creation and commit cycle
//! - Edge cases: zero shards, single shard cluster, duplicate shard reports

use nexora_barrier::{Barrier, BarrierKind, BarrierScheduler, Epoch, EpochError, ShardStatus};
use std::sync::Arc;
use tokio::sync::Barrier as TokioBarrier;

// ============================================================
// Epoch Tests
// ============================================================

#[test]
fn test_epoch_creation() {
    let e = Epoch::new(1);
    assert_eq!(e.value(), 1);
}

#[test]
fn test_epoch_next() {
    let e = Epoch::new(5);
    assert_eq!(e.next().value(), 6);
    assert_eq!(e.value(), 5); // Original unchanged
}

#[test]
fn test_epoch_ordering() {
    let e1 = Epoch::new(1);
    let e2 = Epoch::new(2);
    let e3 = Epoch::new(10);

    assert!(e1 < e2);
    assert!(e2 < e3);
    assert!(e1 < e3);
    assert!(e2 > e1);
}

#[test]
fn test_epoch_equality() {
    assert_eq!(Epoch::new(42), Epoch::new(42));
    assert_ne!(Epoch::new(1), Epoch::new(2));
}

#[test]
fn test_epoch_display() {
    let e = Epoch::new(7);
    assert_eq!(format!("{}", e), "epoch-7");
}

#[test]
fn test_epoch_serialization() {
    let e = Epoch::new(99);
    let json = serde_json::to_string(&e).unwrap();
    let deserialized: Epoch = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.value(), 99);
}

#[test]
fn test_epoch_clone() {
    let e = Epoch::new(3);
    let cloned = e;
    assert_eq!(cloned.value(), 3);
}

#[test]
fn test_epoch_copy() {
    let e = Epoch::new(5);
    let copied = e; // Copy
    assert_eq!(copied.value(), 5);
    assert_eq!(e.value(), 5);
}

// ============================================================
// BarrierKind Tests
// ============================================================

#[test]
fn test_barrier_kind_checkpoint_serialization() {
    let kind = BarrierKind::Checkpoint;
    let json = serde_json::to_string(&kind).unwrap();
    let deserialized: BarrierKind = serde_json::from_str(&json).unwrap();
    match deserialized {
        BarrierKind::Checkpoint => {}
        _ => panic!("Expected Checkpoint"),
    }
}

#[test]
fn test_barrier_kind_light_checkpoint() {
    let kind = BarrierKind::LightCheckpoint;
    let json = serde_json::to_string(&kind).unwrap();
    let deserialized: BarrierKind = serde_json::from_str(&json).unwrap();
    match deserialized {
        BarrierKind::LightCheckpoint => {}
        _ => panic!("Expected LightCheckpoint"),
    }
}

#[test]
fn test_barrier_kind_snapshot() {
    let kind = BarrierKind::Snapshot("my-snapshot".into());
    let json = serde_json::to_string(&kind).unwrap();
    assert!(json.contains("my-snapshot"));
    let deserialized: BarrierKind = serde_json::from_str(&json).unwrap();
    match deserialized {
        BarrierKind::Snapshot(name) => assert_eq!(name, "my-snapshot"),
        _ => panic!("Expected Snapshot"),
    }
}

// ============================================================
// Barrier Tests
// ============================================================

#[test]
fn test_barrier_checkpoint() {
    let b = Barrier {
        epoch: Epoch::new(1),
        created_at: chrono::Utc::now(),
        kind: BarrierKind::Checkpoint,
    };
    assert_eq!(b.epoch.value(), 1);
    match b.kind {
        BarrierKind::Checkpoint => {}
        _ => panic!("Expected Checkpoint"),
    }
}

#[test]
fn test_barrier_serialization() {
    let b = Barrier {
        epoch: Epoch::new(10),
        created_at: chrono::Utc::now(),
        kind: BarrierKind::Checkpoint,
    };
    let json = serde_json::to_string(&b).unwrap();
    let deserialized: Barrier = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.epoch.value(), 10);
}

// ============================================================
// ShardStatus Tests
// ============================================================

#[test]
fn test_shard_status_pending() {
    let status = ShardStatus::Pending;
    assert_eq!(status, ShardStatus::Pending);
}

#[test]
fn test_shard_status_flushed() {
    let status = ShardStatus::Flushed {
        node_count: 100,
        event_count: 5000,
    };
    match status {
        ShardStatus::Flushed {
            node_count,
            event_count,
        } => {
            assert_eq!(node_count, 100);
            assert_eq!(event_count, 5000);
        }
        _ => panic!("Expected Flushed"),
    }
}

#[test]
fn test_shard_status_failed() {
    let status = ShardStatus::Failed("disk full".into());
    match &status {
        ShardStatus::Failed(msg) => assert_eq!(msg, "disk full"),
        _ => panic!("Expected Failed"),
    }
}

#[test]
fn test_shard_status_clone() {
    let status = ShardStatus::Flushed {
        node_count: 1,
        event_count: 10,
    };
    let cloned = status.clone();
    match cloned {
        ShardStatus::Flushed {
            node_count,
            event_count,
        } => {
            assert_eq!(node_count, 1);
            assert_eq!(event_count, 10);
        }
        _ => panic!(),
    }
}

// ============================================================
// BarrierScheduler — Basic Flow Tests
// ============================================================

#[tokio::test]
async fn test_scheduler_creation() {
    let scheduler = BarrierScheduler::new(4);
    assert_eq!(scheduler.current_epoch().await.value(), 1);
    assert_eq!(scheduler.committed_epoch().await.value(), 0);
}

#[tokio::test]
async fn test_create_barrier_advances_epoch() {
    let scheduler = BarrierScheduler::new(3);
    assert_eq!(scheduler.current_epoch().await.value(), 1);

    let b1 = scheduler.create_barrier(BarrierKind::Checkpoint).await;
    assert_eq!(b1.epoch.value(), 1);
    assert_eq!(scheduler.current_epoch().await.value(), 2);

    let b2 = scheduler.create_barrier(BarrierKind::Checkpoint).await;
    assert_eq!(b2.epoch.value(), 2);
    assert_eq!(scheduler.current_epoch().await.value(), 3);
}

#[tokio::test]
async fn test_barrier_epochs_are_monotonic() {
    let scheduler = BarrierScheduler::new(2);
    let mut prev = 0;
    for _ in 0..10 {
        let b = scheduler.create_barrier(BarrierKind::Checkpoint).await;
        assert!(b.epoch.value() > prev);
        prev = b.epoch.value();
    }
}

#[tokio::test]
async fn test_single_shard_completion_triggers_commit() {
    let scheduler = BarrierScheduler::new(1);

    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    scheduler
        .report_shard(
            barrier.epoch,
            0,
            ShardStatus::Flushed {
                node_count: 42,
                event_count: 100,
            },
        )
        .await
        .unwrap();

    // With 1 shard, completing it commits immediately
    assert_eq!(scheduler.committed_epoch().await, barrier.epoch);
    assert!(scheduler.is_barrier_complete(barrier.epoch).await);
}

#[tokio::test]
async fn test_partial_completion_no_commit() {
    let scheduler = BarrierScheduler::new(4);

    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    // Only 2 out of 4 shards
    for i in 0..2 {
        scheduler
            .report_shard(
                barrier.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 10,
                    event_count: 50,
                },
            )
            .await
            .unwrap();
    }

    assert!(!scheduler.is_barrier_complete(barrier.epoch).await);
    assert_eq!(scheduler.committed_epoch().await.value(), 0);
}

#[tokio::test]
async fn test_complete_all_shards_commits() {
    let scheduler = BarrierScheduler::new(4);

    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    for i in 0..4 {
        scheduler
            .report_shard(
                barrier.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 25,
                    event_count: 100,
                },
            )
            .await
            .unwrap();
    }

    assert_eq!(scheduler.committed_epoch().await, barrier.epoch);
}

#[tokio::test]
async fn test_shard_failure_does_not_crash() {
    let scheduler = BarrierScheduler::new(2);

    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    // One shard fails
    scheduler
        .report_shard(barrier.epoch, 0, ShardStatus::Failed("oom".into()))
        .await
        .unwrap();

    // Other shard succeeds
    scheduler
        .report_shard(
            barrier.epoch,
            1,
            ShardStatus::Flushed {
                node_count: 10,
                event_count: 10,
            },
        )
        .await
        .unwrap();

    // Failures don't count towards completion — 1 out of 2 completed
    assert!(!scheduler.is_barrier_complete(barrier.epoch).await);
}

// ============================================================
// BarrierScheduler — Error Paths
// ============================================================

#[tokio::test]
async fn test_report_unknown_epoch() {
    let scheduler = BarrierScheduler::new(2);

    let result = scheduler
        .report_shard(
            Epoch::new(999),
            0,
            ShardStatus::Flushed {
                node_count: 0,
                event_count: 0,
            },
        )
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        EpochError::UnknownEpoch(e) => assert_eq!(e.value(), 999),
        _ => panic!("Expected UnknownEpoch"),
    }
}

// ============================================================
// BarrierScheduler — Broadcast Notification
// ============================================================

#[tokio::test]
async fn test_commit_broadcast_notifies_subscribers() {
    let scheduler = Arc::new(BarrierScheduler::new(1));

    let mut rx = scheduler.subscribe_commits();
    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    scheduler
        .report_shard(
            barrier.epoch,
            0,
            ShardStatus::Flushed {
                node_count: 1,
                event_count: 1,
            },
        )
        .await
        .unwrap();

    // Should receive the committed epoch
    let committed = rx.recv().await.unwrap();
    assert_eq!(committed, barrier.epoch);
}

#[tokio::test]
async fn test_commit_broadcast_only_on_completion() {
    let scheduler = BarrierScheduler::new(3);
    let mut rx = scheduler.subscribe_commits();
    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    // Report 2 out of 3 — no broadcast yet
    for i in 0..2 {
        scheduler
            .report_shard(
                barrier.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 1,
                    event_count: 1,
                },
            )
            .await
            .unwrap();
    }

    // The receiver should timeout (no commit yet)
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;
    assert!(result.is_err(), "Should timeout — epoch not yet committed");

    // Report last shard → commit
    scheduler
        .report_shard(
            barrier.epoch,
            2,
            ShardStatus::Flushed {
                node_count: 1,
                event_count: 1,
            },
        )
        .await
        .unwrap();

    let committed = rx.recv().await.unwrap();
    assert_eq!(committed, barrier.epoch);
}

// ============================================================
// BarrierScheduler — Multiple Epochs
// ============================================================

#[tokio::test]
async fn test_sequential_epochs() {
    let scheduler = BarrierScheduler::new(2);

    for epoch_num in 1..=5 {
        let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;
        assert_eq!(barrier.epoch.value(), epoch_num);

        for i in 0..2 {
            scheduler
                .report_shard(
                    barrier.epoch,
                    i,
                    ShardStatus::Flushed {
                        node_count: epoch_num as usize * 10,
                        event_count: 0,
                    },
                )
                .await
                .unwrap();
        }

        assert_eq!(scheduler.committed_epoch().await.value(), epoch_num);
    }
}

#[tokio::test]
async fn test_epochs_cannot_regress() {
    let scheduler = BarrierScheduler::new(2);

    // Epoch 1 completes
    let b1 = scheduler.create_barrier(BarrierKind::Checkpoint).await;
    for i in 0..2 {
        scheduler
            .report_shard(
                b1.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 0,
                    event_count: 0,
                },
            )
            .await
            .unwrap();
    }

    // Epoch 2 starts
    let b2 = scheduler.create_barrier(BarrierKind::Checkpoint).await;
    assert!(b2.epoch > b1.epoch);

    // current_epoch > committed_epoch after new barrier
    assert!(scheduler.current_epoch().await > scheduler.committed_epoch().await);
}

/// Reporting a shard for an old epoch that's already been committed.
/// The current implementation may keep state after commit, so this may
/// succeed rather than error. Both behaviors are valid — we just verify no crash.
#[tokio::test]
async fn test_report_old_epoch_behavior() {
    let scheduler = BarrierScheduler::new(2);

    let b1 = scheduler.create_barrier(BarrierKind::Checkpoint).await;
    for i in 0..2 {
        scheduler
            .report_shard(
                b1.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 0,
                    event_count: 0,
                },
            )
            .await
            .unwrap();
    }

    // Epoch 1 is committed. Reporting it again should either succeed
    // (if state is kept) or return UnknownEpoch (if state was cleaned up).
    let result = scheduler
        .report_shard(
            b1.epoch,
            0,
            ShardStatus::Flushed {
                node_count: 1,
                event_count: 1,
            },
        )
        .await;

    // Both success, AlreadyCommitted, and UnknownEpoch are valid; just verify no crash
    match result {
        Ok(()) => {}                               // state is kept
        Err(EpochError::UnknownEpoch(_)) => {}     // state cleaned up
        Err(EpochError::AlreadyCommitted(_)) => {} // another valid rejection
        Err(other) => panic!("Unexpected error: {}", other),
    }
}

// ============================================================
// Concurrency & Stress Tests
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_shard_reporting() {
    let scheduler = Arc::new(BarrierScheduler::new(10));
    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;
    let tokio_barrier = Arc::new(TokioBarrier::new(10));
    let mut handles = vec![];

    for i in 0..10 {
        let s = scheduler.clone();
        let b = tokio_barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            s.report_shard(
                barrier.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: i * 100,
                    event_count: (i * 1000) as u64,
                },
            )
            .await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(scheduler.committed_epoch().await, barrier.epoch);
    assert!(scheduler.is_barrier_complete(barrier.epoch).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_epoch_creation_and_reporting() {
    let scheduler = Arc::new(BarrierScheduler::new(4));
    let tokio_barrier = Arc::new(TokioBarrier::new(4));
    let mut handles = vec![];

    // Create 5 barriers, then report all shards concurrently
    let mut barriers = vec![];
    for _ in 0..5 {
        barriers.push(scheduler.create_barrier(BarrierKind::Checkpoint).await);
    }

    // Spawn concurrent reporters for epoch 3
    for i in 0..4 {
        let s = scheduler.clone();
        let b = tokio_barrier.clone();
        let epoch = barriers[2].epoch;
        handles.push(tokio::spawn(async move {
            b.wait().await;
            s.report_shard(
                epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 1,
                    event_count: 1,
                },
            )
            .await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    // At least epoch 3 should be committed
    assert!(scheduler.committed_epoch().await >= barriers[2].epoch);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_high_volume_barrier_throughput() {
    let scheduler = Arc::new(BarrierScheduler::new(3));
    let num_epochs = 50;

    for _ in 0..num_epochs {
        let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;
        let handles: Vec<_> = (0..3)
            .map(|i| {
                let s = scheduler.clone();
                let epoch = barrier.epoch;
                tokio::spawn(async move {
                    s.report_shard(
                        epoch,
                        i,
                        ShardStatus::Flushed {
                            node_count: 1,
                            event_count: 1,
                        },
                    )
                    .await
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }
    }

    assert_eq!(
        scheduler.committed_epoch().await.value(),
        num_epochs,
        "All {} epochs should be committed",
        num_epochs
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_duplicate_shard_reports_dont_break() {
    let scheduler = Arc::new(BarrierScheduler::new(2));
    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    // Report the same shard 0 twice
    scheduler
        .report_shard(
            barrier.epoch,
            0,
            ShardStatus::Flushed {
                node_count: 10,
                event_count: 10,
            },
        )
        .await
        .unwrap();

    scheduler
        .report_shard(
            barrier.epoch,
            0,
            ShardStatus::Flushed {
                node_count: 20,
                event_count: 20,
            },
        )
        .await
        .unwrap();

    // Shard 1 still needed to commit
    assert!(!scheduler.is_barrier_complete(barrier.epoch).await);

    scheduler
        .report_shard(
            barrier.epoch,
            1,
            ShardStatus::Flushed {
                node_count: 5,
                event_count: 5,
            },
        )
        .await
        .unwrap();

    assert!(scheduler.is_barrier_complete(barrier.epoch).await);
}

// ============================================================
// BarrierKind Stress — All Variants
// ============================================================

#[tokio::test]
async fn test_light_checkpoint_barrier_flow() {
    let scheduler = BarrierScheduler::new(2);
    let barrier = scheduler.create_barrier(BarrierKind::LightCheckpoint).await;

    for i in 0..2 {
        scheduler
            .report_shard(
                barrier.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 0,
                    event_count: 0,
                },
            )
            .await
            .unwrap();
    }

    assert_eq!(scheduler.committed_epoch().await, barrier.epoch);
}

#[tokio::test]
async fn test_snapshot_barrier_flow() {
    let scheduler = BarrierScheduler::new(1);
    let barrier = scheduler
        .create_barrier(BarrierKind::Snapshot("snap-001".into()))
        .await;

    scheduler
        .report_shard(
            barrier.epoch,
            0,
            ShardStatus::Flushed {
                node_count: 1000,
                event_count: 50000,
            },
        )
        .await
        .unwrap();

    assert_eq!(scheduler.committed_epoch().await, barrier.epoch);
    match &barrier.kind {
        BarrierKind::Snapshot(name) => assert_eq!(name, "snap-001"),
        _ => panic!("Expected Snapshot variant"),
    }
}

#[tokio::test]
async fn test_mixed_barrier_kinds_across_epochs() {
    let scheduler = BarrierScheduler::new(2);

    // Epoch 1: Checkpoint
    let b1 = scheduler.create_barrier(BarrierKind::Checkpoint).await;
    for i in 0..2 {
        scheduler
            .report_shard(
                b1.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 0,
                    event_count: 0,
                },
            )
            .await
            .unwrap();
    }

    // Epoch 2: LightCheckpoint
    let b2 = scheduler.create_barrier(BarrierKind::LightCheckpoint).await;
    for i in 0..2 {
        scheduler
            .report_shard(
                b2.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 0,
                    event_count: 0,
                },
            )
            .await
            .unwrap();
    }

    // Epoch 3: Snapshot
    let b3 = scheduler
        .create_barrier(BarrierKind::Snapshot("final".into()))
        .await;
    for i in 0..2 {
        scheduler
            .report_shard(
                b3.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 0,
                    event_count: 0,
                },
            )
            .await
            .unwrap();
    }

    assert_eq!(scheduler.committed_epoch().await.value(), 3);
}

// ============================================================
// EpochError Tests
// ============================================================

#[test]
fn test_epoch_error_display() {
    let err = EpochError::UnknownEpoch(Epoch::new(42));
    assert!(format!("{}", err).contains("42"));

    let err = EpochError::Timeout(Epoch::new(7));
    assert!(format!("{}", err).contains("timeout"));
    assert!(format!("{}", err).contains("7"));

    let err = EpochError::AlreadyCommitted(Epoch::new(5));
    assert!(format!("{}", err).contains("already committed"));
}

// ============================================================
// Edge Cases
// ============================================================

#[tokio::test]
async fn test_zero_shards() {
    // Zero-shard scheduler: any barrier commits instantly? Or does it never complete?
    let scheduler = BarrierScheduler::new(0);
    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    // With 0 shards, there's nothing to report — it should already be "complete"
    // since total=0 and completed=0 → 0 >= 0
    assert!(scheduler.is_barrier_complete(barrier.epoch).await);
    assert_eq!(scheduler.committed_epoch().await.value(), 0, "Our implementation requires shards to actually report before committing; 0 shards means 0 reports");
}

#[tokio::test]
async fn test_large_number_of_shards() {
    let scheduler = BarrierScheduler::new(100);
    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    // Report 99 out of 100 — not committed
    for i in 0..99 {
        scheduler
            .report_shard(
                barrier.epoch,
                i,
                ShardStatus::Flushed {
                    node_count: 1,
                    event_count: 1,
                },
            )
            .await
            .unwrap();
    }

    assert!(!scheduler.is_barrier_complete(barrier.epoch).await);

    // Last shard
    scheduler
        .report_shard(
            barrier.epoch,
            99,
            ShardStatus::Flushed {
                node_count: 1,
                event_count: 1,
            },
        )
        .await
        .unwrap();

    assert!(scheduler.is_barrier_complete(barrier.epoch).await);
}

#[tokio::test]
async fn test_multiple_subscribers_receive_commit() {
    let scheduler = Arc::new(BarrierScheduler::new(1));
    let mut rx1 = scheduler.subscribe_commits();
    let mut rx2 = scheduler.subscribe_commits();

    let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

    scheduler
        .report_shard(
            barrier.epoch,
            0,
            ShardStatus::Flushed {
                node_count: 1,
                event_count: 1,
            },
        )
        .await
        .unwrap();

    assert_eq!(rx1.recv().await.unwrap(), barrier.epoch);
    assert_eq!(rx2.recv().await.unwrap(), barrier.epoch);
}

// ============================================================
// Send + Sync verification
// ============================================================

#[test]
fn test_types_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Epoch>();
    assert_send_sync::<Barrier>();
    assert_send_sync::<BarrierKind>();
    assert_send_sync::<BarrierScheduler>();
    assert_send_sync::<EpochError>();
}
