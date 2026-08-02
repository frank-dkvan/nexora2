//! Configuration types for GraphStreaming

use serde::{Deserialize, Serialize};

/// GraphStreaming configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStreamingConfig {
    /// Enable graph streaming projection
    pub enabled: bool,

    /// Directory containing projection rule YAML files
    pub rules_dir: String,

    /// Maximum concurrent projections
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_projections: usize,

    /// Event buffer size per projection
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Batch size for graph updates (0 = no batching)
    #[serde(default)]
    pub batch_size: usize,

    /// Batch timeout in milliseconds (0 = no timeout)
    #[serde(default)]
    pub batch_timeout_ms: u64,
}

impl Default for GraphStreamingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rules_dir: "/etc/nexora/projections".to_string(),
            max_concurrent_projections: 10,
            buffer_size: 1000,
            batch_size: 0,
            batch_timeout_ms: 0,
        }
    }
}

fn default_max_concurrent() -> usize {
    10
}

fn default_buffer_size() -> usize {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GraphStreamingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_concurrent_projections, 10);
        assert_eq!(config.buffer_size, 1000);
    }

    #[test]
    fn test_config_serde() {
        let config = GraphStreamingConfig {
            enabled: true,
            rules_dir: "/custom/path".to_string(),
            max_concurrent_projections: 5,
            buffer_size: 500,
            batch_size: 100,
            batch_timeout_ms: 1000,
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        let restored: GraphStreamingConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(restored.enabled, config.enabled);
        assert_eq!(restored.rules_dir, config.rules_dir);
        assert_eq!(restored.max_concurrent_projections, config.max_concurrent_projections);
    }
}
