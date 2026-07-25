//! A0 durability: registered Standing Query definitions must survive a restart.
//!
//! Before this was wired, `main.rs` never called `restore`/`persist`, so SQ
//! definitions lived only in memory and were lost on every process restart —
//! violating the "nexora-built metadata must not be lost" constraint. These
//! tests exercise the production wiring: definition changes are written through
//! to the store, and a fresh manager over the same store recovers them.
//!
//! A restart is simulated by dropping one `StandingQueryManager` and building a
//! new one over the *same* `Arc<dyn ControlPlaneStore>` — the store outlives the
//! manager exactly as RocksDB outlives the process across a restart.
//!
//! A1: SQ state now persists through the unified `ControlPlaneStore` (shared with
//! the shard map and MV definitions) rather than the graph persistence layer.

use std::collections::HashMap;
use std::sync::Arc;

use nexora_core::control_plane_store::{ControlPlaneStore, InMemoryControlPlaneStore};
use nexora_standing_query::pattern::{FilterCondition, StandingQueryPattern};
use nexora_standing_query::StandingQueryManager;

fn shared_store() -> Arc<dyn ControlPlaneStore> {
    Arc::new(InMemoryControlPlaneStore::new())
}

#[tokio::test]
async fn registered_sqs_survive_restart() {
    let store = shared_store();

    // --- First "process": register two SQs. ---
    let id1;
    let id2;
    {
        let mgr = StandingQueryManager::new(128);
        mgr.set_store(store.clone()).await;
        // Nothing persisted yet → restore is a no-op returning 0.
        assert_eq!(mgr.restore().await.unwrap(), 0);

        id1 = mgr
            .register(
                "high-speed",
                StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
            )
            .await;
        id2 = mgr
            .register(
                "low-temp",
                StandingQueryPattern::property("temp", FilterCondition::LessThan(0.0)),
            )
            .await;
        // mgr dropped here — simulates process exit. Persistor survives (Arc).
    }

    // --- Second "process": fresh manager over the same persistor. ---
    let mgr2 = StandingQueryManager::new(128);
    mgr2.set_store(store.clone()).await;
    let restored = mgr2.restore().await.unwrap();
    assert_eq!(restored, 2, "both SQ definitions should be restored");

    let listed = mgr2.list().await;
    assert_eq!(listed.len(), 2);
    let names: Vec<String> = {
        let mut n: Vec<String> = listed.iter().map(|sq| sq.name.clone()).collect();
        n.sort();
        n
    };
    assert_eq!(
        names,
        vec!["high-speed".to_string(), "low-temp".to_string()]
    );

    // IDs are preserved (not regenerated) so downstream references stay valid.
    let ids: std::collections::HashSet<_> = listed.iter().map(|sq| sq.id).collect();
    assert!(ids.contains(&id1));
    assert!(ids.contains(&id2));

    // A restored SQ still functions: feeding a matching property fires it.
    let mut props = HashMap::new();
    props.insert("speed".to_string(), nexora_id::PropertyValue::Float(150.0));
    let qid = nexora_id::NexoraId::from_bytes(b"forklift-1".to_vec());
    mgr2.on_property_change(
        &qid,
        "speed",
        &nexora_id::PropertyValue::Float(150.0),
        &props,
    )
    .await;
    assert_eq!(
        mgr2.match_count(id1).await,
        1,
        "restored high-speed SQ should match speed=150"
    );
}

#[tokio::test]
async fn removed_sq_does_not_come_back_after_restart() {
    let store = shared_store();

    let id_keep;
    {
        let mgr = StandingQueryManager::new(128);
        mgr.set_store(store.clone()).await;
        id_keep = mgr
            .register(
                "keep",
                StandingQueryPattern::property("a", FilterCondition::GreaterThan(1.0)),
            )
            .await;
        let id_drop = mgr
            .register(
                "drop",
                StandingQueryPattern::property("b", FilterCondition::GreaterThan(1.0)),
            )
            .await;
        // Remove one — the write-through persist must reflect the deletion.
        assert!(mgr.remove(id_drop).await);
    }

    let mgr2 = StandingQueryManager::new(128);
    mgr2.set_store(store.clone()).await;
    let restored = mgr2.restore().await.unwrap();
    assert_eq!(restored, 1, "only the surviving SQ should be restored");

    let listed = mgr2.list().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id_keep);
    assert_eq!(listed[0].name, "keep");
}

#[tokio::test]
async fn updated_sq_version_and_metadata_survive_restart() {
    let store = shared_store();

    let id;
    {
        let mgr = StandingQueryManager::new(128);
        mgr.set_store(store.clone()).await;
        let mut meta = HashMap::new();
        meta.insert("domain".to_string(), "logistics".to_string());
        id = mgr
            .register_with_metadata(
                "v1",
                StandingQueryPattern::property("x", FilterCondition::GreaterThan(1.0)),
                meta,
            )
            .await;
        // Update bumps version to 2.
        assert!(
            mgr.update(
                id,
                "v2",
                StandingQueryPattern::property("x", FilterCondition::GreaterThan(2.0)),
            )
            .await
        );
    }

    let mgr2 = StandingQueryManager::new(128);
    mgr2.set_store(store.clone()).await;
    assert_eq!(mgr2.restore().await.unwrap(), 1);
    let listed = mgr2.list().await;
    assert_eq!(listed.len(), 1);
    let sq = &listed[0];
    assert_eq!(sq.name, "v2");
    assert_eq!(sq.version, 2, "updated version must survive restart");
    assert_eq!(
        sq.metadata.get("domain"),
        Some(&"logistics".to_string()),
        "metadata must survive restart"
    );
}
