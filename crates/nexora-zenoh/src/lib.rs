//! Distributed graph routing layer — Eclipse Zenoh P2P communication.
//!
//! Provides two transport backends:
//! - **TCP** (default): custom length-prefixed JSON protocol, zero external deps
//! - **Zenoh** (`zenoh` feature): real Eclipse Zenoh integration with P2P routing,
//!   automatic discovery, liveliness, and multi-transport support

pub mod anti_entropy;
pub mod checkpoint;
pub mod exactly_once;
pub mod load_balancer;
pub mod metrics;
pub mod replication_metrics;
pub mod standing_query;
pub mod watermark;

use nexora_id::NexoraId;
use serde::{Deserialize, Serialize};

/// A graph operation that can be routed locally or remotely.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GraphOperation {
    GetProperty {
        qid: NexoraId,
        key: String,
    },
    SetProperty {
        qid: NexoraId,
        key: String,
        value: serde_json::Value,
    },
    AddEdge {
        source: NexoraId,
        edge_type: String,
        target: NexoraId,
        direction: String,
    },
    GetEdges {
        qid: NexoraId,
        edge_type: Option<String>,
    },
    GetAllProperties {
        qid: NexoraId,
    },
    ExecuteCypher {
        query: String,
    },
    /// Health check ping — used by the health monitor to check node liveness.
    Ping,
    /// A write replicated from a shard owner to a follower, stamped with the
    /// owner's epoch for fencing. The follower admits it only if `epoch` is at
    /// least the highest epoch it has seen for `shard_id` (see [`fencing`]); a
    /// lower epoch means it came from an owner deposed by failover and is
    /// rejected. `inner` is the actual mutation (`SetProperty`/`AddEdge`).
    ///
    /// `seq` is the owner-assigned per-shard replication sequence number (see
    /// [`replication_log`]); followers record `(seq, inner)` so a later
    /// incremental catch-up can ship only the delta. `seq == 0` means "no seq
    /// assigned" (legacy / unlogged path) and is not recorded.
    FencedWrite {
        shard_id: usize,
        epoch: crate::shard_map::OwnerEpoch,
        #[serde(default)]
        seq: u64,
        inner: Box<GraphOperation>,
    },
    /// Ask a node to export every node+edge it holds for a cluster shard, so a
    /// recovering owner/replica can catch up (state transfer, see
    /// [`state_transfer`]). The cluster shard of a key is
    /// `qid.shard_key() % total_shards`, so both fields are needed to select the
    /// right keys. The response is a JSON-serialized
    /// [`migration::ShardSnapshot`] wrapped in [`GraphResult::Property`].
    ExportShard {
        shard_id: usize,
        total_shards: usize,
    },
    /// Ask a node for the incremental replication delta for `shard_id` since the
    /// caller's high-water `from_seq`. The response is a JSON-serialized
    /// [`state_transfer::DeltaResponse`] wrapped in [`GraphResult::Property`]:
    /// either the tail of `(seq, op)` pairs, an up-to-date marker, or a
    /// too-old marker telling the caller to fall back to a full snapshot.
    ExportDelta {
        shard_id: usize,
        from_seq: u64,
    },
    /// Ask a node for the Merkle digest of its replication log for `shard_id`,
    /// so anti-entropy can compare it against the local digest and decide whether
    /// the replicas have diverged before pulling any ops (see [`anti_entropy`]).
    /// The response is a JSON-serialized [`anti_entropy::ShardDigest`] wrapped in
    /// [`GraphResult::Property`].
    ExportDigest {
        shard_id: usize,
    },
    /// Ask a node to scan its local event table `table` and return the rows as
    /// base64-encoded Arrow IPC stream bytes, wrapped in [`GraphResult::Property`]
    /// (a JSON string). Event tables are per-node (each node has its own Iceberg
    /// catalog), so a cross-node event query fans this out to every node and the
    /// coordinator unions the decoded batches. An empty string means the node has
    /// no such table or no rows. Read-only.
    ScanEventTable {
        table: String,
    },
    /// Broadcast an ontology (domain package) to a peer so it registers the
    /// schema locally: creates the event tables and updates its topic router so
    /// the topic double-writes and its event tables are queryable there too.
    /// `pkg_json` is the JSON-serialized `DomainPackage`. The receiving node
    /// applies it locally WITHOUT re-broadcasting (the originating node fans it
    /// out to every peer). Used so a cross-node event query works no matter which
    /// node the ontology was first created on. Not read-only (mutates state).
    ApplyOntology {
        pkg_json: String,
    },
    /// Broadcast an ontology removal to a peer so it drops the domain's routing
    /// rules locally (symmetric with [`ApplyOntology`]). The receiving node
    /// removes it WITHOUT re-broadcasting. The event *tables* are intentionally
    /// NOT dropped — they are an append-only source of truth — so removal only
    /// stops new double-writes and updates the router. Not read-only.
    RemoveOntology {
        domain: String,
    },
}

impl GraphOperation {
    /// Returns true if this operation is read-only and safe to retry on followers.
    pub fn is_read_only(&self) -> bool {
        match self {
            GraphOperation::GetProperty { .. }
            | GraphOperation::GetEdges { .. }
            | GraphOperation::GetAllProperties { .. }
            | GraphOperation::ExecuteCypher { .. }
            | GraphOperation::ExportShard { .. }
            | GraphOperation::ExportDelta { .. }
            | GraphOperation::ExportDigest { .. }
            | GraphOperation::ScanEventTable { .. }
            | GraphOperation::Ping => true,
            GraphOperation::SetProperty { .. }
            | GraphOperation::AddEdge { .. }
            | GraphOperation::ApplyOntology { .. }
            | GraphOperation::RemoveOntology { .. }
            | GraphOperation::FencedWrite { .. } => false,
        }
    }
}

/// Result of a graph operation executed locally or remotely.
///
/// This enum captures the different shapes of responses a [`GraphOperation`]
/// can produce, from simple property lookups to full Cypher result sets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GraphResult {
    /// A single property value (or `None` if the property does not exist).
    Property(Option<serde_json::Value>),
    /// A status acknowledgment with `ok` flag and human-readable `message`.
    Status { ok: bool, message: String },
    /// Row-oriented result set from a Cypher query execution.
    CypherRows {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
}

/// Trait for remote graph operation execution.
///
/// Implementations of this trait send [`GraphOperation`]s to a remote node
/// and return the resulting [`GraphResult`]. The future-based design allows
/// different transport backends (TCP, Zenoh) to plug in transparently.
///
/// # Example
///
/// ```rust,no_run
/// # use nexora_zenoh::{RemoteGraphClient, GraphOperation, GraphResult, RouterError};
/// struct MyClient;
/// impl RemoteGraphClient for MyClient {
///     fn execute<'a>(
///         &'a self,
///         target_node: &'a str,
///         op: GraphOperation,
///     ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>> {
///         Box::pin(async move {
///             // send op to target_node over the wire
///             Ok(GraphResult::Status { ok: true, message: "done".into() })
///         })
///     }
/// }
/// ```
pub trait RemoteGraphClient: Send + Sync {
    /// Execute a graph operation on a remote node.
    ///
    /// # Arguments
    ///
    /// * `target_node` - The logical node ID to route the operation to.
    /// * `op` - The [`GraphOperation`] to execute remotely.
    ///
    /// # Returns
    ///
    /// A future resolving to the [`GraphResult`] or a [`RouterError`].
    fn execute<'a>(
        &'a self,
        target_node: &'a str,
        op: GraphOperation,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
    >;
}

/// Bridge for scanning a node's local event tables, implemented in the app layer
/// (which owns the `nexora-eventlog` store). `nexora-zenoh` sits below
/// `nexora-eventlog` in the dependency graph, so the concrete store cannot be
/// referenced here directly — the app injects an implementation into the graph
/// handler instead. Returns Arrow IPC stream bytes for the whole table, or an
/// empty vec when the node has no such table / no rows.
#[async_trait::async_trait]
pub trait EventTableScanner: Send + Sync {
    async fn scan_table_ipc(&self, table: &str) -> Result<Vec<u8>, String>;
}

/// Bridge for applying a broadcast ontology (domain package) to this node's
/// local schema state, implemented in the app layer (which owns the ontology
/// manager + event store + topic router). Same layering rationale as
/// [`EventTableScanner`]: `nexora-zenoh` sits below those crates, so the app
/// injects an implementation into the graph handler. `pkg_json` is the
/// JSON-serialized `DomainPackage`. The implementation registers the ontology
/// and activates its event tables/routing WITHOUT re-broadcasting.
#[async_trait::async_trait]
pub trait OntologyApplier: Send + Sync {
    async fn apply_ontology(&self, pkg_json: &str) -> Result<(), String>;
    /// Remove an ontology locally (drop routing rules; keep event tables).
    async fn remove_ontology(&self, domain: &str) -> Result<(), String>;
}

/// Errors that can occur when routing graph operations between nodes.
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    /// The target node could not be found in the cluster.
    #[error("node not found: {0}")]
    NodeNotFound(String),
    /// An error occurred during remote execution on the target node.
    #[error("remote execution failed: {0}")]
    Remote(String),
    /// The operation timed out waiting for a response.
    #[error("timeout")]
    Timeout,
    /// Failed to serialize or deserialize the operation or result.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Quorum write failed to reach required acks.
    #[error("quorum write failed: acked={acked}, required={required}")]
    QuorumFailed { acked: usize, required: usize },
}

/// Catch-up write barrier: blocks writes to a shard while it reconciles.
pub mod catch_up_barrier;
/// Cluster formation and membership management.
pub mod cluster;
/// Cluster status queries and monitoring API.
pub mod cluster_status;
/// Control messages: health checks, leadership elections, node commands.
pub mod control;
/// A2: in-process openraft control-plane consensus type config + commands.
pub mod control_raft;
/// A2-3: RocksDB-backed Raft log storage for the control plane.
pub mod control_raft_log;
/// A2-4: openraft RaftNetwork over the existing TCP transport (byte-shuttle).
pub mod control_raft_network;
/// A2-2: Raft state machine over the unified ControlPlaneStore.
pub mod control_raft_sm;
/// A2-6: combined Raft log + state machine storage adapter.
pub mod control_raft_storage;
/// A2-5: control-plane voter/learner topology resolution + validation.
pub mod control_raft_topology;
/// Peer discovery service — finds and tracks other nodes in the cluster.
pub mod discovery;
/// Distributed CREATE execution with RF>1 quorum replication support.
pub mod distributed_create_with_replication;
/// Distributed Cypher read planner: fan provably-mergeable reads out to shard owners.
pub mod distributed_query;
/// Distributed write execution with RF>1 quorum replication support.
pub mod distributed_query_with_replication;
/// Owner failover: read from followers when owner is down.
pub mod failover;
/// E3: failover alerting hooks — push failover events to ops (webhook/log).
pub mod failover_alert;
/// Per-shard epoch fencing: rejects stale replicated writes after failover.
pub mod fencing;
/// Adapter that wraps a local [`nexora_core::GraphService`] for remote execution.
pub mod graph_service_adapter;
/// Health monitoring and heartbeat system for cluster nodes.
pub mod health_monitor;
/// Idempotency tracking for distributed writes to prevent duplicates.
pub mod idempotency;
/// Local graph client that calls operations in-process (no network).
pub mod local_client;
/// Shard migration protocol: safely transfer shard ownership between nodes.
pub mod migration;
/// Quorum read implementation for strong consistency.
pub mod quorum_read;
/// Writes replicated data from remote nodes into the local graph store.
pub mod replica_writer;
/// Replication protocol: log shipping, consistency guarantees, catch-up.
pub mod replication;
/// Per-shard replication log enabling incremental (delta) state transfer.
/// Optionally RocksDB-backed for cross-restart incremental catch-up.
pub mod replication_log;
/// Replication progress tracking for each shard.
pub mod replication_progress;
/// Top-level router that decides whether to execute locally or remotely.
pub mod router;
/// Consistent hash-based shard map for distributing graph data across nodes.
pub mod shard_map;
/// A0: durable fsync'd snapshot of the committed ShardMap (survives restart).
pub mod shard_map_store;
/// State transfer: a recovering owner/replica catches up a shard's data from a
/// surviving node (operation-based, not WAL shipping).
pub mod state_transfer;
/// Task registry for tracking and gracefully shutting down spawned tokio tasks.
pub mod task_registry;
/// TCP transport: length-prefixed JSON protocol over raw TCP sockets.
pub mod tcp_transport;
/// Cross-shard distributed transactions: two-phase commit (2PC) protocol.
pub mod transaction;

/// Zenoh-based cluster manager (available with `zenoh` feature).
#[cfg(feature = "zenoh")]
pub mod zenoh_cluster;
/// Zenoh-based peer discovery (available with `zenoh` feature).
#[cfg(feature = "zenoh")]
pub mod zenoh_discovery;
/// Zenoh transport layer (available with `zenoh` feature).
#[cfg(feature = "zenoh")]
pub mod zenoh_transport;

// Re-exports from cluster module.
/// Configuration for a cluster peer node.
pub use cluster::ClusterConfig;
/// Manages cluster membership: join, leave, health monitoring.
pub use cluster::ClusterManager;
/// Statistics about the current cluster state.
pub use cluster::ClusterStats;
/// Network-level configuration for an individual peer.
pub use cluster::PeerConfig;
// Re-export from graph_service_adapter.
/// Bridges a local [`nexora_core::GraphService`] so it can receive remote operations.
pub use graph_service_adapter::GraphServiceAdapter;
// Re-export from replica_writer.
/// Applies replicated log entries to the local graph store.
pub use replica_writer::ReplicaWriter;
/// Write concern configuration for replica writes.
pub use replica_writer::WriteConcern;
/// B2: observable replication counters + snapshot.
pub use replica_writer::{ReplicationMetrics, ReplicationMetricsSnapshot};
// C1: session read-after-write.
/// Per-shard replication progress + follower-read caught-up guard.
pub use replication_progress::{ReplicationProgress, SessionReadTracker, ShardProgress};
// C2: read concern for bounded-staleness reads.
/// Read consistency level (Local/Majority/Linearizable) + concern-aware read.
pub use failover::{read_with_concern, ReadConcern};
// Re-export from state_transfer.
/// Operation-based shard catch-up for failover recovery.
pub use state_transfer::{CatchUpResult, StateTransfer, StateTransferError};
// Re-export from shard_map.
/// Monotonically increasing epoch number for shard ownership tracking.
pub use shard_map::OwnerEpoch;
// Re-exports from tcp_transport.
/// Handles an individual TCP connection for the graph protocol.
pub use tcp_transport::GraphHandler;
/// TCP server that listens for incoming graph operation requests.
pub use tcp_transport::TcpGraphServer;
/// Remote client that sends graph operations over a TCP connection.
pub use tcp_transport::TcpRemoteClient;

// Re-exports from migration module.
/// Coordinates shard migrations between nodes.
pub use migration::MigrationManager;
/// State of a shard migration.
pub use migration::MigrationState;
/// A snapshot of a shard's data for transfer.
pub use migration::ShardSnapshot;

// Re-exports from transaction module.
/// Transaction coordinator — runs the 2PC protocol.
pub use transaction::TransactionCoordinator;
/// Transaction participant — handles prepare/commit/abort.
pub use transaction::TransactionParticipant;
/// Result of a distributed transaction.
pub use transaction::TransactionResult;
/// State of a distributed transaction.
pub use transaction::TransactionState;

// Zenoh feature-gated re-exports.
/// Configuration for the Zenoh-based cluster manager.
#[cfg(feature = "zenoh")]
pub use zenoh_cluster::ZenohClusterConfig;
/// Cluster manager backed by Eclipse Zenoh for P2P communication.
#[cfg(feature = "zenoh")]
pub use zenoh_cluster::ZenohClusterManager;
/// Statistics for the Zenoh-based cluster.
#[cfg(feature = "zenoh")]
pub use zenoh_cluster::ZenohClusterStats;
/// Graph operation server that listens over Zenoh.
#[cfg(feature = "zenoh")]
pub use zenoh_transport::ZenohGraphServer;
/// Remote client that sends graph operations over Zenoh.
#[cfg(feature = "zenoh")]
pub use zenoh_transport::ZenohRemoteClient;
