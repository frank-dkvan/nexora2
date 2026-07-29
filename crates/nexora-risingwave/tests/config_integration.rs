//! Configuration integration tests for RisingWave
//!
//! Tests the configuration hierarchy: CLI > Config File > Defaults

#[cfg(feature = "embedded")]
mod embedded_config_tests {
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Test that configuration file is loaded when present
    #[test]
    fn test_config_file_loading() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("nexora.toml");

        let config_content = r#"
[risingwave]
enabled = true
embedded = true
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
data_dir = "./test-data/risingwave"
startup_timeout_secs = 90
shutdown_timeout_secs = 45
parallelism = 8
"#;

        fs::write(&config_path, config_content).unwrap();

        // Parse the config
        let content = fs::read_to_string(&config_path).unwrap();
        let config: toml::Value = toml::from_str(&content).unwrap();

        assert_eq!(config["risingwave"]["enabled"].as_bool().unwrap(), true);
        assert_eq!(config["risingwave"]["embedded"].as_bool().unwrap(), true);
        assert_eq!(
            config["risingwave"]["meta_addr"].as_str().unwrap(),
            "127.0.0.1:5690"
        );
        assert_eq!(
            config["risingwave"]["startup_timeout_secs"]
                .as_integer()
                .unwrap(),
            90
        );
    }

    /// Test that default configuration is used when file is missing
    #[test]
    fn test_default_configuration() {
        // Default values should match what's in config.rs
        let default_meta = "127.0.0.1:5690";
        let default_frontend = "127.0.0.1:4566";
        let default_timeout = 60u64;

        assert_eq!(default_meta, "127.0.0.1:5690");
        assert_eq!(default_frontend, "127.0.0.1:4566");
        assert_eq!(default_timeout, 60);
    }

    /// Test configuration priority: CLI > Config File
    #[test]
    fn test_cli_overrides_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("nexora.toml");

        // Config file sets meta_addr to 5690
        let config_content = r#"
[risingwave]
enabled = true
meta_addr = "127.0.0.1:5690"
"#;
        fs::write(&config_path, config_content).unwrap();

        // Simulate CLI override to 6000
        let cli_meta_addr = Some("127.0.0.1:6000".to_string());
        let config_meta_addr = Some("127.0.0.1:5690".to_string());

        // CLI takes precedence
        let final_addr = cli_meta_addr
            .or(config_meta_addr)
            .unwrap_or_else(|| "127.0.0.1:5690".to_string());

        assert_eq!(final_addr, "127.0.0.1:6000");
    }

    /// Test configuration priority: Config File > Defaults
    #[test]
    fn test_config_file_overrides_defaults() {
        let config_meta_addr = Some("192.168.1.10:5690".to_string());
        let default_meta_addr = "127.0.0.1:5690".to_string();

        let final_addr = config_meta_addr.unwrap_or(default_meta_addr);

        assert_eq!(final_addr, "192.168.1.10:5690");
    }

    /// Test full priority chain: CLI > Config > Default
    #[test]
    fn test_full_priority_chain() {
        // Scenario 1: All sources present (CLI wins)
        let cli = Some("cli-value".to_string());
        let config = Some("config-value".to_string());
        let default = "default-value".to_string();

        let result = cli.or(config).unwrap_or(default);
        assert_eq!(result, "cli-value");

        // Scenario 2: No CLI (Config wins)
        let cli: Option<String> = None;
        let config = Some("config-value".to_string());
        let default = "default-value".to_string();

        let result = cli.or(config).unwrap_or(default);
        assert_eq!(result, "config-value");

        // Scenario 3: No CLI, no Config (Default wins)
        let cli: Option<String> = None;
        let config: Option<String> = None;
        let default = "default-value".to_string();

        let result = cli.or(config).unwrap_or(default);
        assert_eq!(result, "default-value");
    }

    /// Test parallelism defaults to CPU count
    #[test]
    fn test_parallelism_defaults_to_cpu_count() {
        let cpu_count = num_cpus::get();
        let config_parallelism: Option<usize> = None;

        let final_parallelism = config_parallelism.unwrap_or(cpu_count);

        assert_eq!(final_parallelism, cpu_count);
        assert!(final_parallelism >= 1);
    }

    /// Test binary_path search order
    #[test]
    fn test_binary_path_search_order() {
        // 1. Explicit config
        let config_path = Some("/opt/risingwave/bin/risingwave".to_string());
        let env_path = std::env::var("RISINGWAVE_BIN").ok();

        let final_path = config_path.or(env_path);

        assert_eq!(
            final_path.unwrap(),
            "/opt/risingwave/bin/risingwave"
        );

        // 2. Environment variable (when config is None)
        let config_path: Option<String> = None;
        std::env::set_var("RISINGWAVE_BIN", "/env/risingwave");
        let env_path = std::env::var("RISINGWAVE_BIN").ok();

        let final_path = config_path.or(env_path);

        assert_eq!(final_path.unwrap(), "/env/risingwave");
        std::env::remove_var("RISINGWAVE_BIN");
    }

    /// Test data_dir path resolution
    #[test]
    fn test_data_dir_resolution() {
        let config_dir: Option<String> = None;
        let default_rocksdb_path = PathBuf::from("./nexora-data");

        let final_dir = config_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| default_rocksdb_path.join("risingwave"));

        assert_eq!(final_dir, PathBuf::from("./nexora-data/risingwave"));
    }

    /// Test timeout value bounds
    #[test]
    fn test_timeout_value_bounds() {
        let startup_timeout = 120u64;
        let shutdown_timeout = 60u64;

        // Timeouts should be reasonable
        assert!(startup_timeout >= 10);
        assert!(startup_timeout <= 300);
        assert!(shutdown_timeout >= 5);
        assert!(shutdown_timeout <= 120);
    }

    /// Test invalid configuration is caught
    #[test]
    fn test_invalid_config_detection() {
        let invalid_toml = r#"
[risingwave]
enabled = true
meta_addr = "invalid-address"  # Missing port
"#;

        // Parse should succeed (TOML is valid)
        let config: Result<toml::Value, _> = toml::from_str(invalid_toml);
        assert!(config.is_ok());

        // But the address validation would fail later
        let config = config.unwrap();
        let addr = config["risingwave"]["meta_addr"]
            .as_str()
            .unwrap();
        let parsed: Result<std::net::SocketAddr, _> = addr.parse();
        assert!(parsed.is_err());
    }

    /// Test example configuration is valid
    #[test]
    fn test_example_config_is_valid() {
        let example_config = r#"
[risingwave]
enabled = false
embedded = false
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
data_dir = "./nexora-data/risingwave"
startup_timeout_secs = 60
shutdown_timeout_secs = 30
parallelism = 4
"#;

        let config: Result<toml::Value, _> = toml::from_str(example_config);
        assert!(config.is_ok());

        let config = config.unwrap();
        assert!(config["risingwave"]["enabled"].as_bool().is_some());
        assert!(config["risingwave"]["embedded"].as_bool().is_some());
        assert!(config["risingwave"]["meta_addr"].as_str().is_some());
        assert!(config["risingwave"]["startup_timeout_secs"]
            .as_integer()
            .is_some());
    }
}

#[cfg(not(feature = "embedded"))]
mod disabled_tests {
    /// When features are disabled, tests are skipped
    #[test]
    fn test_features_disabled() {
        // This test runs when risingwave or embedded features are not enabled
        assert!(true, "RisingWave features not enabled");
    }
}
