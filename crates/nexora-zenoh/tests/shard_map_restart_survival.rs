//! A0: the committed shard map — runtime failover/rebalance ownership and
//! *bumped epochs* — must survive a restart.
//!
//! Before A0, `ClusterManager::new` always rebuilt the initial map from
//! membership (version 1, all epochs 1), so a failover that changed a shard's
//! owner and bumped its epoch was lost on restart. The reset epoch could no
//! longer fence the deposed owner's stale writes. These tests drive the
//! `ControlPlane` mutation path against a durable `ShardMapStore`, then reopen
//! the store (simulating a restart) and assert the committed owner + epoch come
//! back.

use nexora_zenoh::control::ControlPlane;
use nexora_zenoh::shard_map::ShardMap;
use nexora_zenoh::shard_map_store::ShardMapStore;

fn temp_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("nexora-cp-shardmap-{}", uuid::Uuid::new_v4()));
    p
}

#[tokio::test]
async fn failover_owner_and_epoch_survive_restart() {
    let dir = temp_dir();

    let (shard, expected_owner, expected_epoch, expected_version) = {
        // RF=3 over 3 data nodes; dedicated always-alive voters keep quorum
        // healthy so we reach the promotion logic (mirrors control.rs tests).
        let data_nodes = vec![
            "node-0".to_string(),
            "node-1".to_string(),
            "node-2".to_string(),
        ];
        let voters = vec!["v0".to_string(), "v1".to_string(), "v2".to_string()];
        let map = ShardMap::new_distributed_rf(3, &data_nodes, "node-0".into(), 3);
        let cp =
            ControlPlane::with_shard_map(map, voters.clone()).with_store(ShardMapStore::open(&dir));
        for v in &voters {
            cp.mark_node_alive(v).await;
        }
        for n in &data_nodes {
            cp.mark_node_alive(n).await;
        }

        // Find a shard owned by node-0, kill node-0, and auto-failover.
        let before = cp.get_shard_map().await;
        let shard = (0..3)
            .find(|s| before.get(*s).unwrap().owner == "node-0")
            .unwrap();
        cp.mark_node_failed("node-0").await;
        let token = cp
            .failover_shard_auto(shard)
            .await
            .unwrap()
            .expect("a live replica must be promoted");

        let after = cp.get_shard_map().await;
        let asg = after.get(shard).unwrap();
        (shard, asg.owner.clone(), token.epoch.value(), after.version)
    }; // control plane dropped — simulates process shutdown

    // The snapshot on disk must carry the failed-over state.
    let store2 = ShardMapStore::open(&dir);
    let loaded = store2.load().expect("snapshot must exist after failover");

    assert_eq!(
        loaded.version, expected_version,
        "committed version must survive"
    );
    let asg = loaded.get(shard).unwrap();
    assert_eq!(
        asg.owner, expected_owner,
        "failed-over owner must survive restart"
    );
    assert_ne!(asg.owner, "node-0", "deposed owner must not be restored");
    assert_eq!(
        asg.epoch.value(),
        expected_epoch,
        "bumped epoch must survive so the deposed owner stays fenced"
    );
    assert!(
        expected_epoch >= 2,
        "failover must have bumped the epoch past the initial 1"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn no_store_means_no_snapshot() {
    // Without a store attached, failover still works but nothing is persisted.
    let data_nodes = vec!["node-0".to_string(), "node-1".to_string()];
    let voters = vec!["v0".to_string(), "v1".to_string(), "v2".to_string()];
    let map = ShardMap::new_distributed_rf(2, &data_nodes, "node-0".into(), 2);
    let cp = ControlPlane::with_shard_map(map, voters.clone()); // no with_store
    for v in &voters {
        cp.mark_node_alive(v).await;
    }
    for n in &data_nodes {
        cp.mark_node_alive(n).await;
    }
    let before = cp.get_shard_map().await;
    let shard = (0..2)
        .find(|s| before.get(*s).unwrap().owner == "node-0")
        .unwrap();
    cp.mark_node_failed("node-0").await;
    // Should not panic even though no store is attached (persist_map is a no-op).
    let _ = cp.failover_shard_auto(shard).await.unwrap();
}
