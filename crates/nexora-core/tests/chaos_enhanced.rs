//! Enhanced chaos engineering tests — including index stress testing.
//!
//! New tests: CHAOS-007 through CHAOS-015
//! Focus: Index resilience, query optimizer, concurrent access

use nexora_core::{
    FilterPredicate, GraphService, GraphServiceConfig, InMemoryPersistor, IndexStatistics,
    LabelIndex, PropertyIndex, QueryOptimizer,
};
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

fn make_service() -> Arc<GraphService> {
    Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 16,
            max_nodes_per_shard: 10_000,
            node_channel_size: 256,
        },
        Arc::new(InMemoryPersistor::new()),
    ))
}

// CHAOS-007: Concurrent index updates (1000 threads × 100 ops)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_index_updates() {
    let property_index = Arc::new(PropertyIndex::new());
    let label_index = Arc::new(LabelIndex::new());

    let mut handles = Vec::new();

    // Spawn 1000 tasks, each doing 100 index operations
    for thread_id in 0..1000 {
        let prop_idx = property_index.clone();
        let lbl_idx = label_index.clone();

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let node = NexoraId::from_bytes(format!("node-{}-{}", thread_id, i).into_bytes());

                // Property index
                prop_idx
                    .insert(
                        "thread_id",
                        PropertyValue::Integer(thread_id as i64),
                        node.clone(),
                    )
                    .await
                    .unwrap();

                // Label index
                lbl_idx.add_label("TestNode", node.clone()).await;

                // Query immediately
                let results = prop_idx
                    .query("thread_id", &PropertyValue::Integer(thread_id as i64))
                    .await
                    .unwrap();

                assert!(!results.is_empty());
            }
        });

        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final state
    let stats = property_index.stats().await;
    assert!(stats.writes >= 100_000); // 1000 threads × 100 ops

    let label_stats = label_index.stats().await;
    assert_eq!(label_stats.total_nodes, 100_000);
}

// CHAOS-008: Index thrashing (rapid add/remove cycle)
#[tokio::test]
async fn test_index_thrashing() {
    let property_index = PropertyIndex::new();
    let node = NexoraId::from_bytes(b"thrash-node".to_vec());

    for i in 0..10_000 {
        // Add
        property_index
            .insert("value", PropertyValue::Integer(i), node.clone())
            .await
            .unwrap();

        // Query
        let results = property_index
            .query("value", &PropertyValue::Integer(i))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        // Remove and re-add
        if i % 100 == 0 {
            property_index.remove_node(&node).await.unwrap();
        }
    }

    // Index should still be functional
    property_index
        .insert("final", PropertyValue::Integer(999), node.clone())
        .await
        .unwrap();
    let results = property_index
        .query("final", &PropertyValue::Integer(999))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
}

// CHAOS-009: Query optimizer under extreme cardinality
#[tokio::test]
async fn test_optimizer_extreme_cardinality() {
    let property_index = PropertyIndex::new();
    let label_index = LabelIndex::new();

    // Create skewed distribution:
    // - 1 node with label "Rare" (0.01% selectivity)
    // - 10,000 nodes with label "Common" (100% selectivity)
    for i in 0..10_000 {
        let node = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
        label_index.add_label("Common", node.clone()).await;

        if i == 0 {
            label_index.add_label("Rare", node.clone()).await;
        }

        property_index
            .insert("id", PropertyValue::Integer(i), node.clone())
            .await
            .unwrap();
    }

    // Build statistics
    let mut stats = IndexStatistics::new();
    stats.total_nodes = 10_000;
    stats.label_cardinality.insert("Common".into(), 10_000);
    stats.label_cardinality.insert("Rare".into(), 1);

    let optimizer = QueryOptimizer::with_indexes(stats, property_index, label_index);

    // Optimizer should choose "Rare" as starting point (lowest cardinality)
    let predicates = vec![
        FilterPredicate::HasLabel("Common".into()),
        FilterPredicate::HasLabel("Rare".into()),
    ];

    let plan = optimizer.optimize(predicates);

    // Should start with Rare (1 node) not Common (10K nodes)
    let start_label = match &plan.start_with {
        FilterPredicate::HasLabel(label) => label.clone(),
        _ => panic!("Expected HasLabel predicate"),
    };

    assert_eq!(
        start_label, "Rare",
        "Optimizer should choose lowest cardinality"
    );

    // Execute and verify
    let results = optimizer.execute(plan).await.unwrap();
    assert_eq!(results.len(), 1); // Only 1 node matches both labels
}

// CHAOS-010: Memory pressure simulation (large index)
#[tokio::test]
#[ignore] // Run with: cargo test --test chaos_enhanced -- --ignored
async fn test_large_index_memory_pressure() {
    let property_index = PropertyIndex::new();
    let label_index = LabelIndex::new();

    // Insert 1 million nodes
    for i in 0..1_000_000 {
        let node = NexoraId::from_bytes(format!("node-{:07}", i).into_bytes());

        label_index.add_label("Person", node.clone()).await;

        property_index
            .insert("id", PropertyValue::Integer(i), node.clone())
            .await
            .unwrap();

        // Progress indicator
        if i % 100_000 == 0 {
            println!("Inserted {} nodes", i);
        }
    }

    println!("✅ Inserted 1M nodes");

    // Query performance should still be acceptable
    let start = std::time::Instant::now();
    let results = label_index.query("Person").await;
    let duration = start.elapsed();

    assert_eq!(results.len(), 1_000_000);
    assert!(
        duration.as_millis() < 100,
        "Query should be fast even with 1M nodes"
    );

    println!("✅ Query completed in {:?}", duration);
}

// CHAOS-011: Interleaved index and graph operations
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_interleaved_index_and_graph_ops() {
    let graph = make_service();
    let property_index = Arc::new(PropertyIndex::new());
    let label_index = Arc::new(LabelIndex::new());

    let mut handles = Vec::new();

    for worker_id in 0..100 {
        let g = graph.clone();
        let p_idx = property_index.clone();
        let l_idx = label_index.clone();

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let node = NexoraId::from_bytes(format!("w{}-n{}", worker_id, i).into_bytes());

                // Graph operation
                g.set_property(&node, "worker_id", PropertyValue::Integer(worker_id))
                    .await
                    .unwrap();

                // Index operation
                p_idx
                    .insert("worker_id", PropertyValue::Integer(worker_id), node.clone())
                    .await
                    .unwrap();

                l_idx.add_label("Worker", node.clone()).await;

                // Interleaved read
                let graph_val = g.get_property(&node, "worker_id").await.unwrap();
                assert_eq!(graph_val, Some(PropertyValue::Integer(worker_id)));

                let index_results = p_idx
                    .query("worker_id", &PropertyValue::Integer(worker_id))
                    .await
                    .unwrap();
                assert!(!index_results.is_empty());
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify consistency
    let label_stats = label_index.stats().await;
    assert_eq!(label_stats.total_nodes, 10_000); // 100 workers × 100 nodes
}

// CHAOS-012: Cache eviction stress test
//
// With in-memory only index (L1+L2), evicted entries are no longer queryable.
// This test verifies that:
//   1. Cache eviction keeps sizes within configured bounds
//   2. Entries exceeding capacity are gracefully evicted (no panic/corruption)
//   3. Recently inserted entries ARE retrievable
#[tokio::test]
async fn test_cache_eviction_stress() {
    use nexora_core::IndexConfig;

    let config = IndexConfig {
        l1_max_entries: 10,
        l2_max_entries: 100,
        ..Default::default()
    };

    let index = PropertyIndex::new_with_config(config);

    // Insert 10,000 entries — far beyond L2 capacity (100)
    for i in 0..10_000 {
        let node = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
        index
            .insert("id", PropertyValue::Integer(i), node)
            .await
            .unwrap();
    }

    // Cache sizes must stay within configured bounds
    let (l1_size, l2_size) = index.cache_sizes().await;
    assert!(l1_size <= 10, "L1 must not exceed max_entries={}", 10);
    assert!(l2_size <= 100, "L2 must not exceed max_entries={}", 100);

    // Recent entries (inserted last) should be retrievable from L2
    let mut found = 0;
    for i in 9_900..10_000 {
        let results = index.query("id", &PropertyValue::Integer(i)).await.unwrap();
        if !results.is_empty() {
            found += 1;
        }
    }
    assert!(found > 0, "Recently inserted entries should be in L2 cache");

    let stats = index.stats().await;
    println!(
        "L1 hits: {}  L2 hits: {}  Misses: {}  Cache({}/{})",
        stats.l1_hits, stats.l2_hits, stats.misses, l1_size, l2_size
    );
    println!("Recent 100 entries found: {}/100", found);

    // Write path must not have panicked — total writes should be recorded
    assert_eq!(stats.writes, 10_000);
}

// CHAOS-013: Label intersection explosion (combinatorial)
#[tokio::test]
async fn test_label_intersection_explosion() {
    let label_index = LabelIndex::new();

    // Create 10 labels, each with 1000 nodes
    for label_id in 0..10 {
        for node_id in 0..1000 {
            let node = NexoraId::from_bytes(format!("l{}-n{}", label_id, node_id).into_bytes());
            label_index
                .add_label(format!("Label{}", label_id), node)
                .await;
        }
    }

    // Query intersection of all 10 labels (should return 0)
    let labels: Vec<&str> = (0..10)
        .map(|i| {
            // Leak to get 'static
            Box::leak(format!("Label{}", i).into_boxed_str()) as &str
        })
        .collect();

    let results = label_index.query_all(&labels).await;
    assert_eq!(results.len(), 0); // No node has all 10 labels

    // Query single label (should return 1000)
    let single_results = label_index.query("Label0").await;
    assert_eq!(single_results.len(), 1000);
}

// CHAOS-014: Query optimizer with empty results
#[tokio::test]
async fn test_optimizer_empty_results() {
    let property_index = PropertyIndex::new();
    let label_index = LabelIndex::new();

    // Empty indexes
    let stats = IndexStatistics::new();
    let optimizer = QueryOptimizer::with_indexes(stats, property_index, label_index);

    let predicates = vec![
        FilterPredicate::HasLabel("NonExistent".into()),
        FilterPredicate::PropertyEquals("missing".into(), PropertyValue::Integer(999)),
    ];

    let plan = optimizer.optimize(predicates);
    let results = optimizer.execute(plan).await.unwrap();

    assert_eq!(results.len(), 0); // Should handle empty gracefully
}

// CHAOS-015: Rapid query optimizer switching
#[tokio::test]
async fn test_rapid_optimizer_switching() {
    let property_index = PropertyIndex::new();
    let label_index = LabelIndex::new();

    // Populate
    for i in 0..1000 {
        let node = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
        label_index.add_label("Test", node.clone()).await;
        property_index
            .insert("id", PropertyValue::Integer(i), node)
            .await
            .unwrap();
    }

    let mut stats = IndexStatistics::new();
    stats.total_nodes = 1000;
    stats.label_cardinality.insert("Test".into(), 1000);

    let optimizer = QueryOptimizer::with_indexes(stats.clone(), property_index, label_index);

    // Rapidly create and execute 1000 different plans
    for i in 0..1000 {
        let predicates = vec![
            FilterPredicate::HasLabel("Test".into()),
            FilterPredicate::PropertyEquals("id".into(), PropertyValue::Integer(i)),
        ];

        let plan = optimizer.optimize(predicates);
        let results = optimizer.execute(plan).await.unwrap();

        assert_eq!(results.len(), 1);
    }
}

// CHAOS-016: Race condition detection (property + label)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_race_condition_detection() {
    let property_index = Arc::new(PropertyIndex::new());
    let label_index = Arc::new(LabelIndex::new());
    let node = Arc::new(NexoraId::from_bytes(b"race-node".to_vec()));

    let mut handles = Vec::new();

    // 100 tasks all updating the same node
    for thread_id in 0..100 {
        let p_idx = property_index.clone();
        let l_idx = label_index.clone();
        let n = node.clone();

        let handle = tokio::spawn(async move {
            for _ in 0..100 {
                p_idx
                    .insert("counter", PropertyValue::Integer(thread_id), (*n).clone())
                    .await
                    .unwrap();

                l_idx
                    .add_label(format!("Thread{}", thread_id), (*n).clone())
                    .await;

                // Small delay to increase race probability
                sleep(Duration::from_micros(1)).await;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final state is consistent (no panics/corruptions)
    let results = property_index
        .query("counter", &PropertyValue::Integer(99))
        .await
        .unwrap();
    assert!(!results.is_empty());

    let label_results = label_index.query("Thread99").await;
    assert!(!label_results.is_empty());

    println!("✅ No race conditions detected");
}
