//! 完整集成测试：RocksDB + WAL + GraphService + Standing Query
//!
//! INT-001 ~ INT-005

use nexora_core::{GraphService, GraphServiceConfig};
use nexora_id::{NexoraId, PropertyValue};
use nexora_persistor_rocksdb::RocksDbPersistor;
use nexora_standing_query::{
    pattern::{FilterCondition, StandingQueryPattern},
    StandingQueryManager,
};
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;

// INT-001: RocksDB + WAL 完整栈
#[tokio::test]
async fn test_full_stack_rocksdb_wal() {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };

    let persistor = Arc::new(RocksDbPersistor::open(temp_dir.path()).unwrap());
    let qid = NexoraId::from_bytes(b"full-stack".to_vec());

    // Phase 1: Write data
    {
        let graph =
            GraphService::new_with_wal(config.clone(), persistor.clone(), wal_dir.clone(), None)
                .unwrap();
        graph.replay_all_wals().await.unwrap();

        graph
            .set_property(&qid, "name", PropertyValue::String("Test".into()))
            .await
            .unwrap();
        graph
            .set_property(&qid, "value", PropertyValue::Integer(42))
            .await
            .unwrap();

        graph.shutdown().await.unwrap();
    }

    // Phase 2: Restart and verify
    {
        let graph = GraphService::new_with_wal(config, persistor, wal_dir, None).unwrap();
        // Should replay from WAL or load from RocksDB
        let _replayed = graph.replay_all_wals().await.unwrap();

        let name = graph.get_property(&qid, "name").await.unwrap();
        assert_eq!(name, Some(PropertyValue::String("Test".into())));

        let value = graph.get_property(&qid, "value").await.unwrap();
        assert_eq!(value, Some(PropertyValue::Integer(42)));
    }
}

// INT-002: GraphService + StandingQuery 完整流程
#[tokio::test]
async fn test_graph_with_standing_query() {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let persistor = Arc::new(nexora_core::InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(config, persistor));
    let sqm = Arc::new(StandingQueryManager::new(100));

    // Register SQ
    let sq_id = sqm
        .register(
            "high-value",
            StandingQueryPattern::property("value", FilterCondition::GreaterThan(50.0)),
        )
        .await;

    // Create nodes with varying values
    for i in 0..100 {
        let qid = NexoraId::from_bytes(format!("node-{i:03}").into_bytes());
        graph
            .set_property(&qid, "value", PropertyValue::Integer(i))
            .await
            .unwrap();

        // Manually trigger SQ (in production, this would be via callback)
        let props = graph.get_all_properties(&qid).await.unwrap();
        let props_map: std::collections::HashMap<String, PropertyValue> =
            props.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        if let Some(value) = props_map.get("value") {
            sqm.on_property_change(&qid, "value", value, &props_map)
                .await;
        }
    }

    // Verify matches
    let matches = sqm.match_count(sq_id).await;
    assert_eq!(matches, 49, "Should match nodes 51..99 (49 total)");
}

// INT-003: Multi-hop graph traversal
#[tokio::test]
async fn test_multi_hop_traversal() {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let persistor = Arc::new(nexora_core::InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create chain: A → B → C → D
    let a = NexoraId::from_bytes(b"A".to_vec());
    let b = NexoraId::from_bytes(b"B".to_vec());
    let c = NexoraId::from_bytes(b"C".to_vec());
    let d = NexoraId::from_bytes(b"D".to_vec());

    graph
        .add_edge(&a, HalfEdge::out(Symbol::new("NEXT"), b.clone()))
        .await
        .unwrap();
    graph
        .add_edge(&b, HalfEdge::out(Symbol::new("NEXT"), c.clone()))
        .await
        .unwrap();
    graph
        .add_edge(&c, HalfEdge::out(Symbol::new("NEXT"), d.clone()))
        .await
        .unwrap();

    // Verify edges
    let edges_a = graph.get_edges(&a).await.unwrap();
    assert_eq!(edges_a.len(), 1);

    let edges_b = graph.get_edges(&b).await.unwrap();
    assert_eq!(edges_b.len(), 1);

    let edges_c = graph.get_edges(&c).await.unwrap();
    assert_eq!(edges_c.len(), 1);
}

// INT-004: LRU eviction under memory pressure
#[tokio::test]
async fn test_lru_eviction() {
    let config = GraphServiceConfig {
        num_shards: 2,
        max_nodes_per_shard: 5, // Very low limit to force eviction
        node_channel_size: 16,
    };
    let persistor = Arc::new(nexora_core::InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create 20 nodes (10 per shard on average)
    for i in 0..20 {
        let qid = NexoraId::from_bytes(format!("evict-{i:02}").into_bytes());
        graph
            .set_property(&qid, "id", PropertyValue::Integer(i))
            .await
            .unwrap();
    }

    // All nodes should still be accessible (LRU eviction should persist them)
    for i in 0..20 {
        let qid = NexoraId::from_bytes(format!("evict-{i:02}").into_bytes());
        let val = graph.get_property(&qid, "id").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Integer(i)));
    }
}

// INT-005: Crash recovery with partial WAL
#[tokio::test]
async fn test_partial_wal_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    let config = GraphServiceConfig {
        num_shards: 2,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };

    let persistor = Arc::new(RocksDbPersistor::open(temp_dir.path()).unwrap());
    let qid1 = NexoraId::from_bytes(b"partial-1".to_vec());
    let qid2 = NexoraId::from_bytes(b"partial-2".to_vec());

    // Phase 1: Write first node, flush, write second node, crash (no flush)
    {
        let graph =
            GraphService::new_with_wal(config.clone(), persistor.clone(), wal_dir.clone(), None)
                .unwrap();

        graph
            .set_property(&qid1, "flushed", PropertyValue::Boolean(true))
            .await
            .unwrap();
        graph.sleep_node(&qid1).await.unwrap(); // Flush to RocksDB

        graph
            .set_property(&qid2, "unflushed", PropertyValue::Boolean(true))
            .await
            .unwrap();
        // No sleep — simulates crash
    }

    // Phase 2: Restart — should recover qid2 from WAL
    {
        let graph = GraphService::new_with_wal(config, persistor, wal_dir, None).unwrap();
        graph.replay_all_wals().await.unwrap();

        // Both nodes should be recoverable
        let val1 = graph.get_property(&qid1, "flushed").await.unwrap();
        assert_eq!(val1, Some(PropertyValue::Boolean(true)));

        let val2 = graph.get_property(&qid2, "unflushed").await.unwrap();
        assert_eq!(val2, Some(PropertyValue::Boolean(true)));
    }
}
