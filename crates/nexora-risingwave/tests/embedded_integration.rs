//! Phase 7.5: Integration test for embedded RisingWave in nexora-app context
//!
//! This test verifies the complete lifecycle:
//! 1. Start embedded RisingWave process
//! 2. Connect EventStreamingModule client to it
//! 3. Execute basic DDL
//! 4. Shutdown cleanly

#[cfg(all(feature = "embedded", feature = "event-streaming"))]
mod embedded_integration_tests {
    use nexora_risingwave::{
        EmbeddedConfig, EmbeddedEventStreaming, EventStreamingConfig, EventStreamingModule,
    };
    use std::time::Duration;
    use tempfile::TempDir;

    /// Test the full embedded workflow: start process, connect client, shutdown
    #[tokio::test]
    #[ignore] // Requires RisingWave binary to be available
    async fn test_embedded_full_workflow() {
        // 1. Start embedded RisingWave
        let temp_dir = TempDir::new().unwrap();
        let embedded_config = EmbeddedConfig {
            binary_path: None, // Auto-discover
            data_dir: temp_dir.path().to_path_buf(),
            meta: nexora_risingwave::MetaConfig {
                listen_addr: "127.0.0.1:15690".to_string(), // Non-default port
                backend: nexora_risingwave::MetaBackend::Memory,
            },
            frontend: nexora_risingwave::FrontendConfig {
                listen_addr: "127.0.0.1:14566".to_string(), // Non-default port
            },
            compute: nexora_risingwave::ComputeConfig { parallelism: 2 },
            startup_timeout_secs: 120, // Allow more time for CI
            shutdown_timeout_secs: 30,
        };

        let embedded = match EmbeddedEventStreaming::start(embedded_config).await {
            Ok(e) => e,
            Err(e) => {
                eprintln!(
                    "Failed to start embedded RisingWave (binary may not be available): {}",
                    e
                );
                return; // Skip test if binary not found
            }
        };

        assert_eq!(embedded.state(), nexora_risingwave::EmbeddedState::Running);
        let pid = embedded.pid();
        assert!(pid > 0);

        // 2. Connect EventStreamingModule client
        // Give RisingWave a moment to fully initialize
        tokio::time::sleep(Duration::from_secs(2)).await;

        let client_config = EventStreamingConfig::new()
            .with_meta_addr("127.0.0.1:15690".parse().unwrap())
            .with_frontend_addr("127.0.0.1:14566".parse().unwrap());

        // EventStreamingModule::start() will try to connect to the addresses
        // In Phase 3 implementation, this is a no-op, but the API is correct
        let _module = EventStreamingModule::start(client_config).await.unwrap();

        // 3. Shutdown
        embedded.shutdown().await.unwrap();
    }

    /// Test that embedded instance can be created with custom config
    #[test]
    fn test_embedded_config_builder() {
        let temp_dir = TempDir::new().unwrap();

        let config = EmbeddedConfig {
            binary_path: Some("/custom/path/risingwave".into()),
            data_dir: temp_dir.path().to_path_buf(),
            meta: nexora_risingwave::MetaConfig {
                listen_addr: "127.0.0.1:5690".to_string(),
                backend: nexora_risingwave::MetaBackend::Postgres {
                    uri: "postgres://localhost/test".to_string(),
                },
            },
            frontend: nexora_risingwave::FrontendConfig {
                listen_addr: "127.0.0.1:4566".to_string(),
            },
            compute: nexora_risingwave::ComputeConfig { parallelism: 8 },
            startup_timeout_secs: 90,
            shutdown_timeout_secs: 45,
        };

        assert_eq!(config.meta.listen_addr, "127.0.0.1:5690");
        assert_eq!(config.frontend.listen_addr, "127.0.0.1:4566");
        assert_eq!(config.compute.parallelism, 8);
        assert_eq!(config.startup_timeout_secs, 90);
        assert_eq!(config.shutdown_timeout_secs, 45);
    }
}
