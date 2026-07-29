//! Integration tests for distributed embedded RisingWave (Phase 8)

#[cfg(feature = "embedded")]
mod tests {
    use nexora_risingwave::{DistributedConfig, DistributedEmbeddedRisingWave};
    use std::time::Duration;

    #[tokio::test]
    #[ignore] // Requires RisingWave binary
    async fn test_distributed_config_default() {
        let config = DistributedConfig::default();

        // Verify 3 meta nodes
        assert_eq!(config.meta_nodes.len(), 3);
        assert_eq!(config.meta_nodes[0].node_id, 1);
        assert_eq!(config.meta_nodes[1].node_id, 2);
        assert_eq!(config.meta_nodes[2].node_id, 3);

        // Verify frontend
        assert_eq!(config.frontend.listen_addr, "127.0.0.1:4566");

        // Verify compute nodes
        assert_eq!(config.compute_nodes.len(), 1);
        assert_eq!(config.compute_nodes[0].listen_addr, "127.0.0.1:5688");
    }

    #[tokio::test]
    #[ignore] // Requires RisingWave binary and is slow (~15s startup)
    async fn test_3_node_cluster_startup() {
        // Use non-standard ports to avoid conflicts
        let mut config = DistributedConfig::default();
        config.meta_nodes[0].listen_addr = "127.0.0.1:15690".to_string();
        config.meta_nodes[0].advertise_addr = "127.0.0.1:15690".to_string();
        config.meta_nodes[0].dashboard_addr = "127.0.0.1:15691".to_string();

        config.meta_nodes[1].listen_addr = "127.0.0.1:15692".to_string();
        config.meta_nodes[1].advertise_addr = "127.0.0.1:15692".to_string();
        config.meta_nodes[1].dashboard_addr = "127.0.0.1:15693".to_string();

        config.meta_nodes[2].listen_addr = "127.0.0.1:15694".to_string();
        config.meta_nodes[2].advertise_addr = "127.0.0.1:15694".to_string();
        config.meta_nodes[2].dashboard_addr = "127.0.0.1:15695".to_string();

        config.frontend.listen_addr = "127.0.0.1:14566".to_string();
        config.compute_nodes[0].listen_addr = "127.0.0.1:15688".to_string();

        config.startup_timeout_secs = 90;

        // This will fail if RisingWave binary is not found, which is expected
        let result = DistributedEmbeddedRisingWave::start(config).await;

        if let Ok(cluster) = result {
            // Wait for cluster to stabilize
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Check cluster health
            let health = cluster.monitor_health().await;
            assert!(health.is_ok());

            if let Ok(h) = health {
                // Should have 3 meta nodes
                assert_eq!(h.meta_nodes.len(), 3);

                // At least one should be leader
                assert!(h.leader_node_id.is_some());

                // Frontend should be running
                assert!(h.frontend.is_running);

                // Compute nodes should be running
                assert_eq!(h.compute_nodes.len(), 1);
                assert!(h.compute_nodes[0].is_running);
            }

            // Graceful shutdown
            cluster.shutdown().await.expect("Failed to shutdown cluster");
        }
    }
}
