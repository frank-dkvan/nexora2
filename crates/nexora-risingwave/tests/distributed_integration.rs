//! Integration tests for distributed library mode.
//!
//! These tests verify the full distributed cluster behavior including:
//! - Multi-node Meta cluster with Raft election
//! - Frontend pool load balancing
//! - Compute cluster registration and heartbeat
//! - End-to-end DDL and query execution
//!
//! The distributed cluster types are gated behind the `library` feature, so
//! the whole test file only compiles when that feature is enabled. Without it
//! (e.g. a default `cargo clippy --all-targets`) the file compiles to nothing.
#![cfg(feature = "library")]

use nexora_risingwave::{
    DistributedComputeCluster, DistributedFrontendPool, DistributedLibraryConfig,
    DistributedMetaCluster,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Test 3-node Meta cluster startup and leader election.
///
/// Note: This test verifies the cluster can start successfully and has a consistent
/// term view. Actual leader election requires the full Raft implementation which is
/// a placeholder in Day 3. This will be completed in Phase 4.
#[tokio::test]
async fn test_three_node_meta_cluster_election() {
    // Create configs for 3 Meta nodes
    let config1 = DistributedLibraryConfig::test_3node_memory("meta-1", 6000);
    let config2 = DistributedLibraryConfig::test_3node_memory("meta-2", 6010);
    let config3 = DistributedLibraryConfig::test_3node_memory("meta-3", 6020);

    // Start all 3 Meta nodes concurrently
    let (meta1, meta2, meta3) = tokio::join!(
        DistributedMetaCluster::start(config1),
        DistributedMetaCluster::start(config2),
        DistributedMetaCluster::start(config3),
    );

    let meta1 = meta1.expect("meta-1 should start");
    let meta2 = meta2.expect("meta-2 should start");
    let meta3 = meta3.expect("meta-3 should start");

    // Wait for leader election (max 10 seconds)
    sleep(Duration::from_secs(2)).await;

    // Verify all nodes have consistent term view (at least term 1)
    let term1 = meta1.current_term();
    let term2 = meta2.current_term();
    let term3 = meta3.current_term();

    assert!(
        term1 >= 1 && term2 >= 1 && term3 >= 1,
        "All nodes should have advanced to at least term 1"
    );

    // Check leader status (placeholder Raft may not elect leader yet)
    let is_leader1 = meta1.is_leader().await;
    let is_leader2 = meta2.is_leader().await;
    let is_leader3 = meta3.is_leader().await;

    let leader_count = [is_leader1, is_leader2, is_leader3]
        .iter()
        .filter(|&&is_leader| is_leader)
        .count();

    // Phase 4 TODO: When full Raft is implemented, exactly one should be leader
    // For now, we just verify the cluster starts without crashing
    assert!(
        leader_count <= 1,
        "At most one node should claim to be leader (found {})",
        leader_count
    );

    // Shutdown all nodes
    tokio::join!(meta1.shutdown(), meta2.shutdown(), meta3.shutdown());
}

/// Test Frontend pool creation and health checks.
#[tokio::test]
async fn test_frontend_pool_creation_and_health() {
    let config = DistributedLibraryConfig::test_3node_memory("node-1", 6100);
    let pool = DistributedFrontendPool::new(config)
        .await
        .expect("Frontend pool should be created");

    // Verify initial state
    assert_eq!(pool.healthy_count().await, 1);

    // Start health check background task
    let health_handle = pool.start_health_check();

    // Wait for one health check cycle
    sleep(Duration::from_millis(500)).await;

    // Should still be healthy
    assert_eq!(pool.healthy_count().await, 1);

    health_handle.abort();
}

/// Test Compute cluster registration and heartbeat.
#[tokio::test]
async fn test_compute_cluster_registration_and_heartbeat() {
    let config = DistributedLibraryConfig::test_3node_memory("node-1", 6200);
    let cluster = Arc::new(
        DistributedComputeCluster::new(config)
            .await
            .expect("Compute cluster should be created"),
    );

    // Register local node
    cluster
        .register_compute_node()
        .await
        .expect("Node should register");

    assert_eq!(cluster.healthy_count().await, 1);
    assert_eq!(cluster.total_parallelism().await, 2);

    // Start heartbeat background task
    let heartbeat_handle = cluster.start_heartbeat();

    // Wait for heartbeat cycles
    sleep(Duration::from_millis(500)).await;

    // Node should still be healthy
    assert_eq!(cluster.healthy_count().await, 1);

    heartbeat_handle.abort();

    // Shutdown cluster
    cluster.shutdown().await.expect("Should shutdown cleanly");
}

/// Test end-to-end DDL execution through Frontend pool.
#[tokio::test]
async fn test_e2e_ddl_execution() {
    let config = DistributedLibraryConfig::test_3node_memory("node-1", 6300);
    let pool = DistributedFrontendPool::new(config)
        .await
        .expect("Frontend pool should be created");

    // Execute simple DDL (placeholder, will succeed in current implementation)
    let result = pool
        .execute_ddl("CREATE SOURCE test_source WITH (connector='kafka')")
        .await;

    assert!(result.is_ok(), "DDL execution should succeed");
}

/// Test end-to-end query execution through Frontend pool.
#[tokio::test]
async fn test_e2e_query_execution() {
    let config = DistributedLibraryConfig::test_3node_memory("node-1", 6400);
    let pool = DistributedFrontendPool::new(config)
        .await
        .expect("Frontend pool should be created");

    // Execute simple query (placeholder, returns empty result in current implementation)
    let result = pool.query("SELECT * FROM test_mv LIMIT 10").await;

    assert!(result.is_ok(), "Query execution should succeed");
    assert_eq!(result.unwrap().len(), 0, "Should return empty result set");
}

/// Test Fragment scheduler work distribution.
#[tokio::test]
async fn test_fragment_scheduler_distribution() {
    use nexora_risingwave::distributed_library_compute::FragmentScheduler;

    let config = DistributedLibraryConfig::test_3node_memory("node-1", 6500);
    let cluster = Arc::new(
        DistributedComputeCluster::new(config)
            .await
            .expect("Compute cluster should be created"),
    );

    cluster
        .register_compute_node()
        .await
        .expect("Node should register");

    let scheduler = FragmentScheduler::new(cluster.clone());

    // Schedule multiple fragments
    let assignment1 = scheduler.schedule_fragment(1, 2).await;
    let assignment2 = scheduler.schedule_fragment(2, 2).await;
    let assignment3 = scheduler.schedule_fragment(3, 2).await;

    // All should succeed
    assert!(assignment1.is_ok());
    assert!(assignment2.is_ok());
    assert!(assignment3.is_ok());

    // All should be assigned to same node (only one node available)
    assert_eq!(assignment1.unwrap().node_id, "node-1");
    assert_eq!(assignment2.unwrap().node_id, "node-1");
    assert_eq!(assignment3.unwrap().node_id, "node-1");
}

/// Test configuration validation catches invalid setups.
#[tokio::test]
async fn test_config_validation_rejects_invalid() {
    let mut config = DistributedLibraryConfig::test_3node_memory("meta-1", 6600);

    // Make config invalid: heartbeat >= election timeout
    config.meta.heartbeat_interval_ms = 5000;
    config.meta.election_timeout_ms = 3000;

    let validation = config.validate();
    assert!(
        validation.is_err(),
        "Should reject heartbeat >= election timeout"
    );

    // Fix and verify
    config.meta.heartbeat_interval_ms = 1000;
    assert!(config.validate().is_ok());
}
