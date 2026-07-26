//! Configuration hot reload via SIGHUP signal.
//!
//! Allows reloading configuration from nexora.toml without restarting the service.
//! Thread-safe configuration updates with atomic swap semantics.

use crate::config::{AppTomlConfig, load_toml_config};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Thread-safe configuration holder with hot reload support.
#[derive(Clone)]
pub struct ReloadableConfig {
    inner: Arc<RwLock<AppTomlConfig>>,
    config_path: Option<PathBuf>,
}

impl ReloadableConfig {
    /// Create a new reloadable config holder.
    pub fn new(initial_config: AppTomlConfig, config_path: Option<PathBuf>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial_config)),
            config_path,
        }
    }

    /// Get a read-only snapshot of the current configuration.
    pub async fn get(&self) -> AppTomlConfig {
        self.inner.read().await.clone()
    }

    /// Attempt to reload configuration from disk.
    /// Returns Ok(true) if reloaded successfully, Ok(false) if no changes or file not found.
    pub async fn reload(&self) -> Result<bool, String> {
        let path = match &self.config_path {
            Some(p) => p.as_path(),
            None => std::path::Path::new("nexora.toml"),
        };

        if !path.exists() {
            return Err(format!("Config file not found: {}", path.display()));
        }

        match load_toml_config(Some(path)) {
            Some(new_config) => {
                let mut guard = self.inner.write().await;
                *guard = new_config;
                tracing::info!("Configuration reloaded from {}", path.display());
                Ok(true)
            }
            None => Err(format!("Failed to parse config file: {}", path.display())),
        }
    }
}

/// Install SIGHUP handler for configuration hot reload.
///
/// On Unix systems, sends SIGHUP to the process to trigger a configuration reload.
/// Example: kill -HUP <pid>
#[cfg(unix)]
pub fn spawn_sighup_handler(reloadable_config: ReloadableConfig) {
    use tokio::signal::unix::{signal, SignalKind};

    tokio::spawn(async move {
        let mut sighup = signal(SignalKind::hangup())
            .expect("failed to install SIGHUP handler");

        loop {
            sighup.recv().await;
            tracing::info!("Received SIGHUP - reloading configuration...");

            match reloadable_config.reload().await {
                Ok(true) => {
                    tracing::info!("✅ Configuration reloaded successfully");
                }
                Ok(false) => {
                    tracing::warn!("Configuration file unchanged or not found");
                }
                Err(e) => {
                    tracing::error!("❌ Failed to reload configuration: {}", e);
                }
            }
        }
    });

    tracing::info!("SIGHUP handler installed - send SIGHUP to reload configuration");
}

/// No-op for non-Unix platforms (Windows doesn't support SIGHUP).
#[cfg(not(unix))]
pub fn spawn_sighup_handler(_reloadable_config: ReloadableConfig) {
    tracing::warn!("Configuration hot reload (SIGHUP) is not supported on this platform");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ServerConfig, GraphTomlConfig, LoggingConfig, MetricsConfig};

    fn make_test_config() -> AppTomlConfig {
        AppTomlConfig {
            server: ServerConfig {
                host: "127.0.0.1".into(),
                port: 8080,
                request_body_limit_mb: 16,
            },
            graph: GraphTomlConfig {
                num_shards: 256,
                max_nodes_per_shard: 10000,
                node_channel_size: 64,
            },
            storage: None,
            blob: None,
            cluster: None,
            ingest: None,
            logging: LoggingConfig {
                level: "info".into(),
                format: "json".into(),
            },
            metrics: MetricsConfig { enabled: true },
        }
    }

    #[tokio::test]
    async fn test_reloadable_config_get() {
        let config = make_test_config();
        let reloadable = ReloadableConfig::new(config.clone(), None);

        let retrieved = reloadable.get().await;
        assert_eq!(retrieved.server.port, 8080);
        assert_eq!(retrieved.graph.num_shards, 256);
    }

    #[tokio::test]
    async fn test_reloadable_config_clone() {
        let config = make_test_config();
        let reloadable = ReloadableConfig::new(config, None);
        let cloned = reloadable.clone();

        let original_value = reloadable.get().await;
        let cloned_value = cloned.get().await;

        assert_eq!(original_value.server.port, cloned_value.server.port);
    }

    #[tokio::test]
    async fn test_reload_nonexistent_file() {
        let config = make_test_config();
        let reloadable = ReloadableConfig::new(
            config,
            Some(PathBuf::from("/nonexistent/nexora.toml")),
        );

        let result = reloadable.reload().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }
}
