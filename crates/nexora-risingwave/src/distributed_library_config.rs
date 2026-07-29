//! Configuration for distributed library mode.
//!
//! Distributed library mode runs a multi-node RisingWave cluster where each
//! Nexora process embeds Meta + Frontend + Compute nodes. This provides high
//! availability and horizontal scaling while maintaining the zero-external-binary
//! deployment model.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Configuration for distributed library mode deployment.
///
/// # Example
///
/// ```rust
/// use nexora_risingwave::DistributedLibraryConfig;
/// use nexora_risingwave::distributed_library_config::{MetaNodeConfig, MetaBackend, FrontendNodeConfig, ComputeNodeConfig};
/// use std::path::PathBuf;
///
/// let config = DistributedLibraryConfig {
///     node_id: "meta-1".to_string(),
///     meta: MetaNodeConfig {
///         listen_addr: "0.0.0.0:5690".parse().unwrap(),
///         advertise_addr: "node1.local:5690".to_string(),
///         raft_peers: vec![
///             "meta-2@node2.local:5690".to_string(),
///             "meta-3@node3.local:5690".to_string(),
///         ],
///         backend: MetaBackend::Etcd {
///             endpoints: vec!["http://etcd:2379".to_string()],
///         },
///         election_timeout_ms: 3000,
///         heartbeat_interval_ms: 1000,
///     },
///     frontend: FrontendNodeConfig {
///         listen_addr: "0.0.0.0:4566".parse().unwrap(),
///     },
///     compute: ComputeNodeConfig {
///         listen_addr: "0.0.0.0:5688".parse().unwrap(),
///         parallelism: None, // Auto-detect from CPU cores
///         internal_rpc_addr: None, // Use listen_addr
///     },
///     data_dir: PathBuf::from("./nexora-data/event-streaming-distributed"),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedLibraryConfig {
    /// Unique identifier for this node in the cluster.
    ///
    /// Must be unique across all nodes. Typically follows the pattern
    /// "meta-1", "meta-2", "meta-3" for a 3-node cluster.
    pub node_id: String,

    /// Meta node configuration (catalog and coordination).
    pub meta: MetaNodeConfig,

    /// Frontend node configuration (SQL query processing).
    pub frontend: FrontendNodeConfig,

    /// Compute node configuration (stream processing execution).
    pub compute: ComputeNodeConfig,

    /// Base directory for persistent data (Hummock storage metadata).
    ///
    /// Actual data is stored in S3/MinIO, but local metadata cache is kept here.
    pub data_dir: PathBuf,
}

/// Meta node configuration.
///
/// Meta nodes form a Raft cluster for catalog consistency and coordination.
/// Minimum 3 nodes recommended for production (tolerates 1 node failure).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaNodeConfig {
    /// Address to bind Meta service (0.0.0.0:5690 for all interfaces).
    pub listen_addr: SocketAddr,

    /// Address other nodes use to reach this Meta node.
    ///
    /// Should be a reachable hostname or IP. Used for Raft communication.
    /// Example: "node1.local:5690" or "10.0.1.10:5690"
    pub advertise_addr: String,

    /// Peer Meta nodes for Raft cluster formation.
    ///
    /// Format: "node_id@advertise_addr"
    /// Example: ["meta-2@node2.local:5690", "meta-3@node3.local:5690"]
    ///
    /// Do not include this node's own ID here.
    pub raft_peers: Vec<String>,

    /// Meta backend storage (catalog persistence).
    pub backend: MetaBackend,

    /// Raft election timeout in milliseconds (default: 3000).
    ///
    /// If no heartbeat from leader within this time, trigger election.
    /// Higher values = more stable but slower failover.
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,

    /// Raft heartbeat interval in milliseconds (default: 1000).
    ///
    /// How often leader sends heartbeats to followers.
    /// Should be << election_timeout_ms (typically 1/3).
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,
}

/// Frontend node configuration.
///
/// Frontend nodes accept PostgreSQL wire protocol connections and execute
/// SQL queries. They are stateless and can be load balanced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontendNodeConfig {
    /// Address to bind PostgreSQL wire protocol listener.
    pub listen_addr: SocketAddr,
}

/// Compute node configuration.
///
/// Compute nodes execute stream processing operators (fragments).
/// They pull data from sources and write to sinks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeNodeConfig {
    /// Address to bind Compute service.
    pub listen_addr: SocketAddr,

    /// Number of parallel workers (default: number of CPU cores).
    ///
    /// Controls how many stream processing tasks can run concurrently.
    /// `None` means auto-detect from `num_cpus::get()`.
    pub parallelism: Option<usize>,

    /// Internal RPC address for inter-compute communication.
    ///
    /// If `None`, uses `listen_addr`.
    pub internal_rpc_addr: Option<SocketAddr>,
}

/// Meta backend storage options.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MetaBackend {
    /// Etcd backend (recommended for production).
    ///
    /// Provides shared catalog storage across Meta nodes.
    /// Requires external etcd cluster (3-node recommended).
    Etcd {
        /// Etcd endpoints (e.g., ["http://etcd1:2379", "http://etcd2:2379"]).
        endpoints: Vec<String>,
    },

    /// SQLite backend (single-node or testing only).
    ///
    /// Uses local SQLite file. NOT suitable for multi-node production
    /// (no shared state). Useful for development and single-node deployments.
    Sqlite {
        /// Path to SQLite database file.
        path: PathBuf,
    },

    /// In-memory backend (testing only).
    ///
    /// All catalog state is lost on restart. Only for integration tests.
    Memory,
}

fn default_election_timeout() -> u64 {
    3000
}

fn default_heartbeat_interval() -> u64 {
    1000
}

impl DistributedLibraryConfig {
    /// Parse a peer string into (node_id, advertise_addr).
    ///
    /// Format: "node_id@advertise_addr"
    /// Example: "meta-2@node2.local:5690" -> ("meta-2", "node2.local:5690")
    pub fn parse_peer(peer: &str) -> Result<(String, String), String> {
        let parts: Vec<&str> = peer.split('@').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid peer format '{}'. Expected: node_id@advertise_addr",
                peer
            ));
        }
        Ok((parts[0].to_string(), parts[1].to_string()))
    }

    /// Validate configuration consistency.
    pub fn validate(&self) -> Result<(), String> {
        // Node ID must not be empty
        if self.node_id.is_empty() {
            return Err("node_id cannot be empty".to_string());
        }

        // For Raft cluster, need at least 2 peers (self + 2 peers = 3 nodes minimum)
        if self.meta.raft_peers.len() < 2 {
            return Err(format!(
                "Raft cluster requires at least 3 nodes total. \
                 Found 1 (self) + {} peers = {} nodes. \
                 Add more peers to meta.raft_peers.",
                self.meta.raft_peers.len(),
                1 + self.meta.raft_peers.len()
            ));
        }

        // Validate peer format
        for peer in &self.meta.raft_peers {
            if let Err(e) = Self::parse_peer(peer) {
                return Err(format!("Invalid peer '{}': {}", peer, e));
            }
        }

        // Peer list must not contain self
        for peer in &self.meta.raft_peers {
            if let Ok((peer_id, _)) = Self::parse_peer(peer) {
                if peer_id == self.node_id {
                    return Err(format!(
                        "meta.raft_peers must not include this node's ID ({}). \
                         Remove it from the peers list.",
                        self.node_id
                    ));
                }
            }
        }

        // Etcd backend required for production multi-node
        if matches!(self.meta.backend, MetaBackend::Memory) {
            return Err(
                "Memory backend not supported for distributed mode. \
                 Use 'etcd' or 'sqlite' backend."
                    .to_string(),
            );
        }

        // Heartbeat interval should be < election timeout
        if self.meta.heartbeat_interval_ms >= self.meta.election_timeout_ms {
            return Err(format!(
                "heartbeat_interval_ms ({}) must be < election_timeout_ms ({})",
                self.meta.heartbeat_interval_ms, self.meta.election_timeout_ms
            ));
        }

        Ok(())
    }

    /// Create a default configuration for testing (3-node cluster, in-memory).
    ///
    /// **NOT for production use.**
    pub fn test_3node_memory(node_id: &str, base_port: u16) -> Self {
        let meta_port = base_port;
        let frontend_port = base_port + 1;
        let compute_port = base_port + 2;

        let peers = match node_id {
            "meta-1" => vec![
                format!("meta-2@127.0.0.1:{}", meta_port + 10),
                format!("meta-3@127.0.0.1:{}", meta_port + 20),
            ],
            "meta-2" => vec![
                format!("meta-1@127.0.0.1:{}", meta_port - 10),
                format!("meta-3@127.0.0.1:{}", meta_port + 10),
            ],
            "meta-3" => vec![
                format!("meta-1@127.0.0.1:{}", meta_port - 20),
                format!("meta-2@127.0.0.1:{}", meta_port - 10),
            ],
            // For single-node tests, use empty peer list
            _ => vec![],
        };

        DistributedLibraryConfig {
            node_id: node_id.to_string(),
            meta: MetaNodeConfig {
                listen_addr: format!("127.0.0.1:{}", meta_port).parse().unwrap(),
                advertise_addr: format!("127.0.0.1:{}", meta_port),
                raft_peers: peers,
                backend: MetaBackend::Sqlite {
                    path: PathBuf::from(format!("/tmp/nexora-test-{}.db", node_id)),
                },
                election_timeout_ms: 3000,
                heartbeat_interval_ms: 1000,
            },
            frontend: FrontendNodeConfig {
                listen_addr: format!("127.0.0.1:{}", frontend_port).parse().unwrap(),
            },
            compute: ComputeNodeConfig {
                listen_addr: format!("127.0.0.1:{}", compute_port).parse().unwrap(),
                parallelism: Some(2),
                internal_rpc_addr: None,
            },
            data_dir: PathBuf::from(format!("/tmp/nexora-test-{}", node_id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_peer() {
        let (id, addr) = DistributedLibraryConfig::parse_peer("meta-2@node2.local:5690").unwrap();
        assert_eq!(id, "meta-2");
        assert_eq!(addr, "node2.local:5690");
    }

    #[test]
    fn test_parse_peer_invalid() {
        assert!(DistributedLibraryConfig::parse_peer("invalid").is_err());
        assert!(DistributedLibraryConfig::parse_peer("too@many@parts").is_err());
    }

    #[test]
    fn test_validate_min_nodes() {
        let mut config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
        config.meta.raft_peers = vec!["meta-2@localhost:5700".to_string()]; // Only 2 nodes total
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_self_in_peers() {
        let mut config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
        config.meta.raft_peers.push("meta-1@localhost:5690".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_heartbeat_vs_election() {
        let mut config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
        config.meta.heartbeat_interval_ms = 5000;
        config.meta.election_timeout_ms = 3000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_memory_backend() {
        let mut config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
        config.meta.backend = MetaBackend::Memory;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_success() {
        let config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
        assert!(config.validate().is_ok());
    }
}
