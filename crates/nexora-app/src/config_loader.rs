//! Configuration file loading utilities
//!
//! Loads configuration from nexora.toml with fallback to defaults.

use crate::config::AppTomlConfig;
use anyhow::{Context, Result};
use std::path::Path;

/// Load configuration from TOML file
///
/// Search order:
/// 1. Explicit path if provided
/// 2. ./nexora.toml
/// 3. Return default config
pub fn load_config(config_path: Option<&Path>) -> Result<AppTomlConfig> {
    // If explicit path provided, require it to exist
    if let Some(path) = config_path {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: AppTomlConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        tracing::info!("Loaded configuration from: {}", path.display());
        return Ok(config);
    }

    // Try default location
    let default_path = Path::new("nexora.toml");
    if default_path.exists() {
        let content =
            std::fs::read_to_string(default_path).context("Failed to read nexora.toml")?;

        let config: AppTomlConfig =
            toml::from_str(&content).context("Failed to parse nexora.toml")?;

        tracing::info!("Loaded configuration from: nexora.toml");
        return Ok(config);
    }

    // No config file found, use defaults
    tracing::debug!("No configuration file found, using defaults");
    Ok(AppTomlConfig::default())
}

impl Default for AppTomlConfig {
    fn default() -> Self {
        Self {
            server: Default::default(),
            graph: Default::default(),
            storage: None,
            blob: None,
            cluster: None,
            ingest: None,
            logging: Default::default(),
            metrics: Default::default(),
            #[cfg(feature = "event-streaming")]
            event_streaming: None,
        }
    }
}
