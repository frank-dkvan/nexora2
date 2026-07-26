//! RisingWave configuration.

use std::net::SocketAddr;
use serde::{Deserialize, Serialize};

/// Configuration for RisingWave module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RisingWaveConfig {
    /// Meta node address.
    pub meta_addr: SocketAddr,

    /// Frontend node address.
    pub frontend_addr: SocketAddr,

    /// Optional compute node address.
    /// If None, compute will be embedded with frontend.
    pub compute_addr: Option<SocketAddr>,

    /// Enable high availability with Raft consensus.
    pub enable_ha: bool,

    /// Raft peers for Meta HA (only used if enable_ha = true).
    pub raft_peers: Vec<(u64, String)>,
}

impl RisingWaveConfig {
    /// Create a new configuration with default values.
    ///
    /// # Example
    ///
    /// ```rust
    /// use nexora_risingwave::RisingWaveConfig;
    ///
    /// let config = RisingWaveConfig::new()
    ///     .with_meta_addr("127.0.0.1:5690".parse().unwrap())
    ///     .with_frontend_addr("127.0.0.1:4566".parse().unwrap());
    /// ```
    pub fn new() -> Self {
        Self {
            meta_addr: "127.0.0.1:5690".parse().unwrap(),
            frontend_addr: "127.0.0.1:4566".parse().unwrap(),
            compute_addr: None,
            enable_ha: false,
            raft_peers: vec![],
        }
    }

    /// Set the Meta node address.
    pub fn with_meta_addr(mut self, addr: SocketAddr) -> Self {
        self.meta_addr = addr;
        self
    }

    /// Set the Frontend node address.
    pub fn with_frontend_addr(mut self, addr: SocketAddr) -> Self {
        self.frontend_addr = addr;
        self
    }

    /// Set the optional Compute node address.
    pub fn with_compute_addr(mut self, addr: SocketAddr) -> Self {
        self.compute_addr = Some(addr);
        self
    }

    /// Enable high availability with Raft consensus.
    pub fn with_ha(mut self, enabled: bool) -> Self {
        self.enable_ha = enabled;
        self
    }

    /// Set Raft peers for Meta HA.
    pub fn with_raft_peers(mut self, peers: Vec<(u64, String)>) -> Self {
        self.raft_peers = peers;
        self
    }
}

impl Default for RisingWaveConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RisingWaveConfig::default();
        assert_eq!(config.meta_addr.port(), 5690);
        assert_eq!(config.frontend_addr.port(), 4566);
        assert!(config.compute_addr.is_none());
        assert!(!config.enable_ha);
    }

    #[test]
    fn test_builder_pattern() {
        let config = RisingWaveConfig::new()
            .with_meta_addr("0.0.0.0:5690".parse().unwrap())
            .with_frontend_addr("0.0.0.0:4566".parse().unwrap())
            .with_compute_addr("0.0.0.0:5688".parse().unwrap())
            .with_ha(true)
            .with_raft_peers(vec![(1, "node1:5690".to_string())]);

        assert_eq!(config.meta_addr.port(), 5690);
        assert_eq!(config.frontend_addr.port(), 4566);
        assert_eq!(config.compute_addr.unwrap().port(), 5688);
        assert!(config.enable_ha);
        assert_eq!(config.raft_peers.len(), 1);
    }
}
