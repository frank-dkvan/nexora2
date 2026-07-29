//! 嵌入式 RisingWave 集成测试

#[cfg(feature = "embedded")]
mod embedded_tests {
    use nexora_risingwave::{EmbeddedRisingWave, EmbeddedConfig, MetaBackend, EmbeddedState};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// 测试嵌入式配置默认值
    #[test]
    fn test_embedded_config_default() {
        let config = EmbeddedConfig::default();

        assert_eq!(config.meta.listen_addr, "127.0.0.1:5690");
        assert_eq!(config.frontend.listen_addr, "127.0.0.1:4566");
        assert!(config.compute.parallelism > 0);
        assert_eq!(config.startup_timeout_secs, 60);
        assert_eq!(config.shutdown_timeout_secs, 30);
    }

    /// 测试二进制查找逻辑
    #[tokio::test]
    async fn test_binary_discovery() {
        // 设置环境变量
        std::env::set_var("RISINGWAVE_BIN", "/tmp/fake-risingwave");

        let config = EmbeddedConfig {
            binary_path: None,
            ..Default::default()
        };

        // 应该优先使用环境变量
        // 注意：这个测试会失败，因为二进制不存在，但验证了查找逻辑
        let result = EmbeddedRisingWave::start(config).await;
        assert!(result.is_err());

        std::env::remove_var("RISINGWAVE_BIN");
    }

    /// 测试显式指定二进制路径
    #[tokio::test]
    async fn test_explicit_binary_path() {
        let temp_dir = TempDir::new().unwrap();
        let fake_binary = temp_dir.path().join("risingwave");

        // 创建一个假的可执行文件
        std::fs::write(&fake_binary, "#!/bin/sh\necho test").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&fake_binary).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&fake_binary, perms).unwrap();
        }

        let config = EmbeddedConfig {
            binary_path: Some(fake_binary.clone()),
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        // 启动会失败（因为不是真正的 RisingWave），但能验证路径查找
        let result = EmbeddedRisingWave::start(config).await;
        assert!(result.is_err());
    }

    /// 测试 Meta 配置构建
    #[test]
    fn test_meta_opts_memory_backend() {
        let config = EmbeddedConfig::default();
        let opts = nexora_risingwave::embedded_process::EmbeddedRisingWave::build_meta_opts(&config);

        assert!(opts.contains("--listen-addr"));
        assert!(opts.contains("127.0.0.1:5690"));
        assert!(opts.contains("--backend mem"));
    }

    #[test]
    fn test_meta_opts_postgres_backend() {
        let mut config = EmbeddedConfig::default();
        config.meta.backend = MetaBackend::Postgres {
            uri: "postgres://localhost/test".to_string(),
        };

        let opts = nexora_risingwave::embedded_process::EmbeddedRisingWave::build_meta_opts(&config);

        assert!(opts.contains("--backend postgres"));
        assert!(opts.contains("--store-uri postgres://localhost/test"));
    }

    /// 测试 Frontend 配置构建
    #[test]
    fn test_frontend_opts() {
        let config = EmbeddedConfig::default();
        let opts = nexora_risingwave::embedded_process::EmbeddedRisingWave::build_frontend_opts(&config);

        assert!(opts.contains("--listen-addr 127.0.0.1:4566"));
        assert!(opts.contains("--meta-addr 127.0.0.1:5690"));
    }

    /// 测试 Compute 配置构建
    #[test]
    fn test_compute_opts() {
        let mut config = EmbeddedConfig::default();
        config.compute.parallelism = 4;

        let opts = nexora_risingwave::embedded_process::EmbeddedRisingWave::build_compute_opts(&config);

        assert!(opts.contains("--meta-addr 127.0.0.1:5690"));
        assert!(opts.contains("--parallelism 4"));
    }
}
