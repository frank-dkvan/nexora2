//! Cluster manager — wires together discovery, control plane, and routing.
//!
//! Design doc §6.2-6.5: Zenoh routing, fixed logical shards,
//! distributed Cypher, and fault recovery.
//!
//! The ClusterManager is the main entry point for distributed mode:
//! - Starts TcpGraphServer to accept incoming graph operations
//! - Maintains TcpRemoteClient for outgoing operations
//! - Runs periodic heartbeat protocol
//! - Detects node failures and triggers failover
//! - Propagates ShardMap updates across the cluster

use crate::control::ControlPlane;
use crate::discovery::{ClusterRegistry, NodeInfo};
use crate::router::HybridRouter;
use crate::shard_map::ShardMap;
use crate::tcp_transport::{GraphHandler, TcpGraphServer, TcpRemoteClient};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// YAML schema for cluster configuration file loading.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClusterConfigYaml {
    cluster: ClusterMeta,
    node: NodeConfig,
    #[serde(default)]
    peers: Vec<PeerConfigYaml>,
    health: HealthConfig,
    replication: ReplicationConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClusterMeta {
    name: String,
    total_shards: usize,
    replication_factor: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NodeConfig {
    id: String,
    listen_addr: String,
    heartbeat_addr: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PeerConfigYaml {
    node_id: String,
    graph_addr: String,
    heartbeat_addr: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HealthConfig {
    heartbeat_interval_secs: u64,
    failure_timeout_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReplicationConfig {
    log_dir: Option<std::path::PathBuf>,
    #[serde(default = "default_write_timeout")]
    write_timeout_secs: u64,
    /// Optional directory for the durable shard-map snapshot. Opt-in: when
    /// absent the shard map stays in-memory only.
    #[serde(default)]
    shard_map_dir: Option<std::path::PathBuf>,
}

fn default_write_timeout() -> u64 {
    10
}

/// Configuration for cluster mode.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// This node's unique ID (e.g., "node-1")
    pub node_id: String,
    /// Address to listen on for graph operations (e.g., "127.0.0.1:7000")
    pub listen_addr: String,
    /// Address to listen on for heartbeat (e.g., "127.0.0.1:7001")
    pub heartbeat_addr: String,
    /// Total number of logical shards
    pub total_shards: usize,
    /// Peer nodes to bootstrap with (node_id, graph_addr, heartbeat_addr)
    pub peers: Vec<PeerConfig>,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Node failure timeout (missed heartbeats)
    pub failure_timeout: Duration,
    /// Replication factor: 1 owner + (rf-1) followers per shard. 1 = no
    /// replication (owner only). Clamped to the number of nodes.
    pub replication_factor: usize,
    /// Optional directory for the durable replication log. When set, the
    /// per-shard replication log is RocksDB-backed so incremental catch-up
    /// survives a restart; when `None`, the log is in-memory only (a restart
    /// forces a full-snapshot catch-up).
    pub replication_log_dir: Option<std::path::PathBuf>,
    /// Optional directory for the durable shard-map snapshot. When set, every
    /// committed shard-map change (failover, rebalance, add/remove node) is
    /// snapshotted to disk, and cold start seeds the map from the last snapshot
    /// instead of the membership-derived initial map — so runtime ownership and
    /// epoch changes survive a restart. When `None`, the map is in-memory only
    /// (a restart reverts to the cold initial assignment with reset epochs).
    pub shard_map_dir: Option<std::path::PathBuf>,
    /// Optional background Merkle anti-entropy interval. When `Some(d)`, a
    /// background task every `d` compares this node's per-shard replication-log
    /// digest against each peer replica's and pulls+applies any divergent ops
    /// (see [`crate::anti_entropy`]). `None` (default) disables it: divergence is
    /// still healed on failover catch-up and read-repair, so anti-entropy is
    /// opt-in defense-in-depth, matching the codebase's other opt-in cluster
    /// features. Requires a replication log (`replication_log_dir`) to be useful.
    pub anti_entropy_interval: Option<Duration>,
}

impl ClusterConfig {
    /// Load cluster configuration from a YAML file.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::ConfigLoad`] if the file cannot be read or
    /// parsed as valid YAML matching the expected schema.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, ClusterError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| {
            ClusterError::ConfigLoad(format!("failed to read {}: {}", path.display(), e))
        })?;

        let yaml: ClusterConfigYaml = serde_yaml::from_str(&content)
            .map_err(|e| ClusterError::ConfigLoad(format!("failed to parse YAML: {}", e)))?;

        let peers = yaml
            .peers
            .into_iter()
            .map(|p| PeerConfig {
                node_id: p.node_id,
                graph_addr: p.graph_addr,
                heartbeat_addr: p.heartbeat_addr,
            })
            .collect();

        Ok(ClusterConfig {
            node_id: yaml.node.id,
            listen_addr: yaml.node.listen_addr,
            heartbeat_addr: yaml.node.heartbeat_addr,
            total_shards: yaml.cluster.total_shards,
            peers,
            heartbeat_interval: Duration::from_secs(yaml.health.heartbeat_interval_secs),
            failure_timeout: Duration::from_secs(yaml.health.failure_timeout_secs),
            replication_factor: yaml.cluster.replication_factor,
            replication_log_dir: yaml.replication.log_dir,
            shard_map_dir: yaml.replication.shard_map_dir,
            // Opt-in; defaults off. Enabled via config once operators choose to
            // run background anti-entropy (divergence is otherwise healed on
            // failover catch-up + read-repair).
            anti_entropy_interval: None,
        })
    }
}

/// Configuration for a peer node.
#[derive(Clone, Debug)]
pub struct PeerConfig {
    pub node_id: String,
    pub graph_addr: String,
    pub heartbeat_addr: String,
}

/// Cluster manager — orchestrates all distributed components.
pub struct ClusterManager {
    config: ClusterConfig,
    registry: Arc<ClusterRegistry>,
    control_plane: Arc<ControlPlane>,
    router: Arc<HybridRouter>,
    remote_client: Arc<TcpRemoteClient>,
    /// Quorum write replicator, shared with the app layer so the write path can
    /// replicate owner writes to followers. Populated with per-shard replica
    /// sets derived from the initial ShardMap.
    replica_writer: Arc<crate::replica_writer::ReplicaWriter>,
    /// Per-shard epoch fence, shared with the graph handler. Advanced on every
    /// shard-map change (notably failover, which bumps a shard's epoch) so the
    /// handler rejects `FencedWrite`s from a deposed owner.
    fence: crate::fencing::ShardFence,
    /// Per-shard replication log, shared with the writer (owner seqs) and the
    /// adapter (follower seqs). Enables incremental state-transfer catch-up.
    replication_log: crate::replication_log::ShardReplicationLog,
    /// Per-shard catch-up write barrier: blocks owner writes to a shard while it
    /// reconciles after a failover promotion. Shared with the app write path.
    catch_up_barrier: crate::catch_up_barrier::CatchUpBarrier,
    /// C2: per-shard replication progress. Tracks each replica's applied seq and
    /// the quorum commit_index per shard. The writer records acks here; the
    /// distributed read path gates `ReadConcern::Majority` on replicas caught up
    /// to commit_index. Shared with ReplicaWriter and the PG-wire server.
    replication_progress: Arc<crate::replication_progress::ReplicationProgress>,
    /// A2-6: openraft control-plane consensus node. None when the control plane
    /// runs in fallback (hand-rolled quorum) mode, Some when openraft is enabled.
    /// Created lazily in start() since Raft::new is async.
    raft: Option<Arc<openraft::Raft<crate::control_raft::ControlRaftTypeConfig>>>,
    /// A2-6: Raft storage (log + state machine), built in new() and consumed by
    /// start() to create the Raft node. None after start() is called.
    raft_storage: Option<crate::control_raft_storage::ControlStorage>,
    /// A2-6: resolved voter/learner topology for the cluster.
    voter_topology: Option<crate::control_raft_topology::VoterTopology>,
    graph_server: Option<TcpGraphServer>,
    heartbeat_server: Option<TcpHeartbeatServer>,
    /// Node addresses: node_id -> (graph_addr, heartbeat_addr)
    node_addrs: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
    /// Background loop handles (heartbeat sender, failure detector). Held so
    /// `shutdown()` can abort them — otherwise a "killed" node keeps sending
    /// heartbeats and peers never fail it over.
    background_tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Start time for uptime calculation
    start_time: Instant,
    /// Receiver for committed ontology (`DomainDef`) changes from the Raft state
    /// machine, taken by the app layer to drive activation. `Some` only when the
    /// Raft control-plane store was assembled (Raft enabled); `None` in fallback
    /// mode. Taken once via `take_ontology_activation_rx()`.
    ontology_activation_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::control_raft_sm::OntologyActivation>>,
    /// Clone of the Raft state machine's control-plane store, for startup
    /// reconciliation (a follower caught up by snapshot install has the committed
    /// `DomainDef` entries in the store but never saw the per-entry activation
    /// events). `Some` only when Raft is enabled.
    control_plane_store:
        Option<Arc<dyn nexora_core::control_plane_store::ControlPlaneStore>>,
}
impl ClusterManager {
    /// Create a new cluster manager (does not start yet).
    pub fn new(config: ClusterConfig) -> Self {
        let registry = Arc::new(ClusterRegistry::new());
        // Voters = ALL cluster members, including self. Excluding self would
        // undercount the quorum: in a 3-node cluster each node would see only 2
        // voters (its peers), so losing one owner drops alive voters to 1 < 2 and
        // failover would be wrongly refused as NoQuorum even though 2 of 3 nodes
        // survive. The failure detector marks self alive every tick, so self
        // counts toward the majority on the surviving side.
        let voters: Vec<String> = {
            let mut v: Vec<String> = config.peers.iter().map(|p| p.node_id.clone()).collect();
            v.push(config.node_id.clone());
            v
        };
        let remote_client = Arc::new(TcpRemoteClient::new());

        // Build the initial ShardMap by distributing shards across all known
        // members (this node + configured peers), round-robin and deterministic
        // so every node derives the same assignment from the same membership.
        // Followers are assigned per the replication factor. Failover later
        // reassigns ownership via the control plane.
        let mut members: Vec<String> = config.peers.iter().map(|p| p.node_id.clone()).collect();
        members.push(config.node_id.clone());
        let initial_map = ShardMap::new_distributed_rf(
            config.total_shards,
            &members,
            config.node_id.clone(),
            config.replication_factor,
        );

        // A0: open the durable shard-map store (opt-in via `shard_map_dir`) and
        // prefer a persisted snapshot over the cold membership-derived map. The
        // snapshot carries runtime failover/rebalance history — the owners and
        // *bumped epochs* that the initial map resets to 1. Recovering them keeps
        // a deposed owner fenced across a restart. A snapshot from a node whose
        // `local_node` no longer matches (config renamed) is ignored to avoid
        // routing as the wrong node.
        let store = match &config.shard_map_dir {
            Some(dir) => crate::shard_map_store::ShardMapStore::open(dir),
            None => crate::shard_map_store::ShardMapStore::in_memory(),
        };
        let shard_map = match store.load() {
            Some(persisted) if persisted.local_node == config.node_id => {
                tracing::info!(
                    "Restored shard map from snapshot (version {}, {} assignments)",
                    persisted.version,
                    persisted.assignments.len()
                );
                persisted
            }
            Some(persisted) => {
                tracing::warn!(
                    "Ignoring shard-map snapshot: local_node '{}' != configured node_id '{}'; \
                     using membership-derived initial map",
                    persisted.local_node,
                    config.node_id
                );
                initial_map
            }
            None => initial_map,
        };

        // Build the replica writer FIRST, seeding it with the replica set for
        // every shard this node owns, so owner writes on this node fan out to
        // followers. Built synchronously (no async lock) from the initial map.
        // It is attached to the router below so the distributed write path
        // (execute_write → execute_write_with_replication) actually replicates.
        let local_replica_sets: Vec<crate::replication::ReplicaSet> = {
            use crate::replication::ReplicaSet;
            let local = &config.node_id;
            shard_map
                .assignments
                .iter()
                .filter(|(_, asg)| &asg.owner == local && !asg.replicas.is_empty())
                .map(|(shard_id, asg)| {
                    ReplicaSet::new(*shard_id, asg.owner.clone(), asg.replicas.clone())
                })
                .collect()
        };
        // Shared per-shard replication log: the writer records owner-assigned
        // seqs on send; the adapter records them on receive. Both share this one
        // instance so incremental catch-up can compare high-water marks across
        // owner and followers. Retains a bounded tail per shard (past which a
        // lagging replica falls back to a full snapshot).
        //
        // Durable when `replication_log_dir` is set — the retained window then
        // survives a restart, so a recovering node can catch up incrementally
        // instead of taking a full snapshot. Falls back to in-memory on open
        // failure (durability is best-effort; correctness is unaffected).
        let replication_log = match &config.replication_log_dir {
            Some(dir) => {
                let path = dir.join(format!("replog-{}", config.node_id));
                match crate::replication_log::ShardReplicationLog::open_durable(&path, 4096) {
                    Ok(log) => {
                        tracing::info!("Durable replication log at {}", path.display());
                        log
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to open durable replication log at {} ({e}); \
                             falling back to in-memory",
                            path.display()
                        );
                        crate::replication_log::ShardReplicationLog::new(4096)
                    }
                }
            }
            None => crate::replication_log::ShardReplicationLog::new(4096),
        };

        // C2: per-shard replication progress tracker. The writer records owner write
        // seqs and follower acks here; the distributed read path (via PG-wire server)
        // checks whether a replica has caught up to the quorum commit_index before
        // failing over to it under ReadConcern::Majority. Shared with ReplicaWriter
        // and exposed to the app layer so PG-wire can pass it to distributed_query.
        let replication_progress =
            Arc::new(crate::replication_progress::ReplicationProgress::new());

        let replica_writer = Arc::new(
            crate::replica_writer::ReplicaWriter::with_replica_sets(
                remote_client.clone(),
                local_replica_sets,
            )
            .with_replication_log(replication_log.clone())
            .with_progress(replication_progress.clone()),
        );

        // Use the no-local-channel router: local shards route through the
        // remote client to this node's own graph server (TCP-to-self), rather
        // than the dead in-process `local_tx` channel. The app read/write path
        // never uses this for local shards (it hits `state.graph` directly), but
        // scatter-gather distributed queries DO route every shard — including
        // local ones — through the client, so a working local route is required.
        // Self is registered in `remote_client` in `start()` once its graph
        // server address is bound.
        //
        // B2: attach the replica writer so the distributed write path replicates
        // owner writes to followers (best-effort, non-blocking). Without this the
        // router's `replica_writer()` is None and RF>1 silently degrades to
        // owner-only writes even when replicas are configured.
        let router = Arc::new(
            HybridRouter::new_clustered_no_local(shard_map.clone(), remote_client.clone())
                .with_replica_writer(replica_writer.clone()),
        );

        // The control plane shares the SAME initial map as the router, so the
        // failover path (which pushes the control plane's map to the router)
        // can't clobber routing with a stale/local-only assignment. Attach the
        // store so every committed shard-map change is snapshotted going forward.
        // A2-7: control_plane is initially built without Raft; the handle is
        // injected in start() once the Raft node is created. Router is attached
        // so every Raft-committed ShardMap mutation propagates to it.
        let control_plane = Arc::new(
            ControlPlane::with_shard_map(shard_map.clone(), voters)
                .with_store(store)
                .with_router(router.clone()),
        );

        // Epoch fence seeded from the initial map. `start()` shares this with the
        // graph handler so replicated writes are checked against it.
        let fence = crate::fencing::ShardFence::new();

        // Catch-up write barrier: empty at start (nothing reconciling yet).
        let catch_up_barrier = crate::catch_up_barrier::CatchUpBarrier::new();

        let mut node_addrs = std::collections::HashMap::new();
        node_addrs.insert(
            config.node_id.clone(),
            (config.listen_addr.clone(), config.heartbeat_addr.clone()),
        );

        // A2-6: resolve voter topology and assemble openraft node.
        // Membership = self + peers. Voters are auto-selected or taken from
        // an explicit list (not yet wired to config; A2-6 accepts None for now
        // so the adaptive policy runs).
        let members: Vec<String> = {
            let mut m = vec![config.node_id.clone()];
            m.extend(config.peers.iter().map(|p| p.node_id.clone()));
            m
        };
        let voter_topology =
            match crate::control_raft_topology::VoterTopology::resolve(&members, None) {
                Ok(topo) => {
                    tracing::info!(
                        "Control plane: {} voters, {} learners (ft={})",
                        topo.voters.len(),
                        topo.learners.len(),
                        topo.fault_tolerance()
                    );
                    Some(topo)
                }
                Err(e) => {
                    tracing::warn!(
                    "Voter topology resolution failed ({e}); control plane runs in fallback mode"
                );
                    None
                }
            };

        // A2-6: assemble openraft storage if topology resolved. The Raft node itself
        // is created lazily in start() since Raft::new is async. We prepare the
        // storage (log + state machine) here and consume it in start().
        let mut ontology_activation_rx = None;
        let mut control_plane_store_clone = None;
        let raft_storage = if let Some(ref _topo) = voter_topology {
            match &config.replication_log_dir {
                Some(dir) => {
                    // Open the unified control-plane store (A1) for the Raft state machine.
                    let cp_store_path = dir.join(format!("control-plane-{}", config.node_id));
                    let cp_store: Arc<dyn nexora_core::control_plane_store::ControlPlaneStore> =
                        match nexora_core::control_plane_store::RocksDbControlPlaneStore::open(
                            &cp_store_path,
                        ) {
                            Ok(store) => {
                                tracing::info!(
                                    "Control-plane store at {}",
                                    cp_store_path.display()
                                );
                                Arc::new(store)
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to open control-plane store at {} ({e}); Raft disabled",
                                    cp_store_path.display()
                                );
                                Arc::new(nexora_core::control_plane_store::InMemoryControlPlaneStore::new())
                            }
                        };

                    // Open the Raft log store (adjacent to the control-plane store).
                    let raft_log_path = dir.join(format!("raft-log-{}", config.node_id));
                    let log_store =
                        match crate::control_raft_log::ControlLogStore::open(&raft_log_path) {
                            Ok(log) => {
                                tracing::info!(
                                    "Control-plane Raft log at {}",
                                    raft_log_path.display()
                                );
                                Some(log)
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to open Raft log at {} ({e}); control plane disabled",
                                    raft_log_path.display()
                                );
                                None
                            }
                        };

                    if let Some(log_store) = log_store {
                        // Build the state machine over the unified control-plane store,
                        // wiring an ontology-activation channel so committed DomainDef
                        // changes emit to the app layer for table creation + routing.
                        let (ontology_tx, ontology_rx) =
                            tokio::sync::mpsc::unbounded_channel();
                        let sm = crate::control_raft_sm::ControlStateMachine::new(cp_store.clone())
                            .with_ontology_activation(ontology_tx);
                        ontology_activation_rx = Some(ontology_rx);
                        control_plane_store_clone = Some(cp_store);

                        // openraft requires a combined `RaftLogStorage + RaftStateMachine`.
                        let storage =
                            crate::control_raft_storage::ControlStorage::new(log_store, sm);
                        Some(storage)
                    } else {
                        None
                    }
                }
                None => {
                    tracing::warn!("No replication_log_dir configured; Raft disabled (A2 requires durable storage)");
                    None
                }
            }
        } else {
            None
        };

        Self {
            config,
            registry,
            control_plane,
            router,
            remote_client,
            replica_writer,
            fence,
            replication_log,
            catch_up_barrier,
            replication_progress,
            raft: None,
            raft_storage,
            voter_topology,
            graph_server: None,
            heartbeat_server: None,
            node_addrs: Arc::new(RwLock::new(node_addrs)),
            background_tasks: Vec::new(),
            start_time: Instant::now(),
            ontology_activation_rx,
            control_plane_store: control_plane_store_clone,
        }
    }

    /// The per-shard epoch fence for this node. Share it with the graph handler
    /// ([`GraphServiceAdapter::with_fence`]) so replicated writes are checked
    /// against the current epoch, and stale writes from a deposed owner rejected.
    pub fn fence(&self) -> crate::fencing::ShardFence {
        self.fence.clone()
    }

    /// The per-shard replication log for this node. Share it with the graph
    /// handler ([`GraphServiceAdapter::with_fence_and_log`]) so admitted writes
    /// are recorded by seq, enabling incremental catch-up.
    pub fn replication_log(&self) -> crate::replication_log::ShardReplicationLog {
        self.replication_log.clone()
    }

    /// The per-shard catch-up write barrier for this node. Share it with the app
    /// write path so owner writes to a shard mid-catch-up are rejected until
    /// reconciliation completes (fence → catch-up → reopen).
    pub fn catch_up_barrier(&self) -> crate::catch_up_barrier::CatchUpBarrier {
        self.catch_up_barrier.clone()
    }

    /// Start the cluster manager: bind servers, register peers, begin heartbeats.
    pub async fn start(&mut self, handler: Arc<dyn GraphHandler>) -> Result<(), ClusterError> {
        // 0. Seed the epoch fence from the initial shard map so followers start
        //    with the correct per-shard high-water mark before any write arrives.
        self.fence
            .observe_map(&self.router.shard_map_snapshot().await)
            .await;

        // A2-6: Create the Raft node if storage was prepared in new().
        // openraft::Raft::new takes log_store and state_machine separately, so we
        // split the combined storage adapter back into its parts.
        if let Some(storage) = self.raft_storage.take() {
            let network = crate::control_raft_network::ControlRaftNetworkFactory::new(
                self.remote_client.clone(),
            );
            let raft_config = Arc::new(openraft::Config::default());
            let node_id = crate::control_raft::node_id_to_raft(&self.config.node_id);

            // Extract the log and SM from the combined storage. We clone the storage
            // to get both parts (it's cheap: they're both Arc/Clone internally).
            let log_store = storage.clone();
            let state_machine = storage;

            match openraft::Raft::new(node_id, raft_config, network, log_store, state_machine).await
            {
                Ok(raft_node) => {
                    tracing::info!("Control-plane Raft node created (id={})", node_id);
                    let raft_arc = Arc::new(raft_node);
                    self.raft = Some(raft_arc.clone());

                    // A2-7: Inject the Raft handle into the control plane so its write
                    // paths (propose_shard_map_update, failover_shard, failover_shard_auto)
                    // go through consensus rather than hand-rolled quorum.
                    self.control_plane.set_raft(raft_arc).await;
                }
                Err(e) => {
                    tracing::error!("Failed to create Raft node: {e}");
                }
            }
        }

        // 1. Start graph operation server. If Raft is enabled, attach the Raft RPC
        //    handler so the same TCP server serves both data-plane and control-plane
        //    traffic (A2-4).
        let mut graph_server = TcpGraphServer::new(handler, self.config.listen_addr.clone());
        if let Some(ref raft) = self.raft {
            let raft_handler = Arc::new(crate::control_raft_network::ControlRaftRpcServer::new(
                raft.clone(),
            ));
            graph_server = graph_server.with_raft_handler(raft_handler);
            tracing::info!("Node {} Raft RPC handler registered", self.config.node_id);
        }
        let graph_addr = graph_server.start().await?;
        tracing::info!(
            "Node {} graph server on {}",
            self.config.node_id,
            graph_addr
        );
        self.graph_server = Some(graph_server);

        // 1b. Register SELF in the remote client so the router (no-local-channel
        //     mode) can route local-shard operations back to this node's own
        //     graph server over TCP. Required for scatter-gather over local shards.
        self.remote_client
            .register_node(&self.config.node_id, &graph_addr)
            .await;

        // 2. Start heartbeat server
        let hb_server = TcpHeartbeatServer::new(
            self.config.node_id.clone(),
            self.config.heartbeat_addr.clone(),
            self.registry.clone(),
            self.node_addrs.clone(),
        );
        let hb_addr = hb_server.start().await?;
        tracing::info!(
            "Node {} heartbeat server on {}",
            self.config.node_id,
            hb_addr
        );
        self.heartbeat_server = Some(hb_server);

        // 3. Register self in the registry
        self.registry
            .register(NodeInfo {
                id: self.config.node_id.clone(),
                address: graph_addr.clone(),
                roles: vec!["compute".into(), "voter".into()],
                last_heartbeat_ms: current_millis(),
                alive: true,
            })
            .await;

        // 4. Register peers and their addresses
        for peer in &self.config.peers {
            self.node_addrs.write().await.insert(
                peer.node_id.clone(),
                (peer.graph_addr.clone(), peer.heartbeat_addr.clone()),
            );
            self.remote_client
                .register_node(&peer.node_id, &peer.graph_addr)
                .await;

            // Pre-register peer in registry (will be marked alive on first heartbeat)
            self.registry
                .register(NodeInfo {
                    id: peer.node_id.clone(),
                    address: peer.graph_addr.clone(),
                    roles: vec!["compute".into(), "voter".into()],
                    last_heartbeat_ms: 0,
                    alive: false,
                })
                .await;
        }

        // 5. Initial shard distribution is already fixed in `new()` (deterministic
        //    `new_distributed_rf` over self + configured peers), and is shared with
        //    the control plane. We must NOT re-distribute here: at cold start only
        //    this node has sent a heartbeat, so `alive_nodes` = [self], and
        //    rebalancing would pull every shard back to local and drop replicas.
        //    Rebalancing belongs to failover / membership-change events, not boot.

        // A2-6/A2-9: Bootstrap the Raft cluster if enabled. Exactly one node (the
        // first voter by sorted node id) calls `initialize` with the FULL voter
        // set; it wins the initial election and replicates the membership + log to
        // the other voters, forming a real multi-voter Raft. Every other node
        // stays pristine and receives its membership from the leader. This is the
        // cold-start path; a node rejoining an existing cluster (Raft log
        // non-empty) skips this and lets openraft resume from its log.
        if let Some(ref raft) = self.raft {
            if let Some(ref topo) = self.voter_topology {
                // Check if this node's Raft log is empty (first boot).
                let metrics = raft.metrics().borrow().clone();
                let is_first_boot = metrics.last_log_index.is_none();

                if is_first_boot {
                    // Determine if we are the bootstrap node (first by sorted voter id).
                    let mut sorted_voters = topo.voters.clone();
                    sorted_voters.sort();
                    let bootstrap_node = sorted_voters.first().expect("voters non-empty");

                    if bootstrap_node == &self.config.node_id {
                        // We are the bootstrap node: initialize the cluster with the
                        // FULL voter set, not just ourselves. openraft requires exactly
                        // one pristine node to call `initialize`; it becomes leader and
                        // replicates the membership (and the initial log) to the other
                        // voters via append-entries once they're reachable. Seeding all
                        // voters here is what forms a real multi-voter Raft — the prior
                        // single-node `initialize` left every peer permanently
                        // uninitialized (a 1-voter cluster that always self-quorums),
                        // which is why split-brain could never actually be exercised.
                        //
                        // Each voter's `BasicNode.addr` carries its string node_id; the
                        // network layer (ControlRaftNetworkFactory) resolves that to a
                        // real graph address through the same TcpRemoteClient the peers
                        // were registered in (step 4 above / register_node).
                        let mut members = std::collections::BTreeMap::new();
                        for voter in &topo.voters {
                            members.insert(
                                crate::control_raft::node_id_to_raft(voter),
                                openraft::BasicNode {
                                    addr: voter.clone(),
                                },
                            );
                        }
                        let voter_count = members.len();
                        match raft.initialize(members).await {
                            Ok(()) => tracing::info!(
                                "Raft cluster bootstrapped ({voter_count}-voter: {:?})",
                                topo.voters
                            ),
                            // NotAllowed means the cluster is already formed (e.g. this
                            // node restarted with a non-empty log that metrics hadn't
                            // reflected yet). The openraft docs say this is safe to
                            // ignore — the goal (a running cluster) is already met.
                            Err(openraft::error::RaftError::APIError(
                                openraft::error::InitializeError::NotAllowed(_),
                            )) => tracing::info!("Raft already initialized; skipping bootstrap"),
                            Err(e) => tracing::error!("Raft bootstrap failed: {e}"),
                        }
                    } else {
                        // We are not the bootstrap node: stay pristine. The bootstrap
                        // node's `initialize` above includes us in the voter set, so the
                        // elected leader will replicate our membership and log to us via
                        // append-entries. We must NOT call `initialize` ourselves —
                        // openraft requires exactly one initializer.
                        tracing::info!(
                            "Raft node pristine; awaiting membership from bootstrap node '{}'",
                            bootstrap_node
                        );
                    }
                } else {
                    tracing::info!("Raft node rejoining cluster (log non-empty)");
                }
            }
        }

        // 6. Start heartbeat sender and failure detector. Keep their handles so
        //    shutdown() can abort them — a killed node must stop heartbeating.
        let hb = self.start_heartbeat_sender();
        let fd = self.start_failure_detector();
        self.background_tasks.push(hb);
        self.background_tasks.push(fd);

        // 7. Opt-in background Merkle anti-entropy. Defaults off; when an interval
        //    is configured, periodically reconcile each shard this node holds
        //    against its peer replicas (digest compare → pull+apply divergent
        //    ops). Divergence is otherwise healed on failover catch-up and
        //    read-repair, so this is defense-in-depth. Applies go through the
        //    shared catch-up barrier so a repair can't clobber a concurrent write.
        if let Some(interval) = self.config.anti_entropy_interval {
            use crate::anti_entropy::AntiEntropyRepairer;
            let repairer = Arc::new(
                AntiEntropyRepairer::new(
                    Arc::new(RwLock::new(self.replication_log.clone())),
                    interval,
                )
                .with_remote_repair(
                    self.remote_client.clone(),
                    self.control_plane.shard_map_handle(),
                    self.config.node_id.clone(),
                    Some(self.catch_up_barrier.clone()),
                ),
            );
            let ae = repairer.start_background_repair();
            self.background_tasks.push(ae);
            tracing::info!(
                interval_secs = interval.as_secs(),
                "background Merkle anti-entropy enabled"
            );
        }

        Ok(())
    }

    /// Re-distribute shards across all currently-alive nodes (round-robin).
    ///
    /// NOT called at cold start (initial distribution is fixed deterministically
    /// in `new()`). Reserved for membership-change / rebalance events in the
    /// failover work line, where the alive set is meaningfully populated.
    #[allow(dead_code)]
    async fn distribute_shards(&self) {
        let alive_nodes: Vec<String> = self
            .registry
            .alive_nodes()
            .await
            .into_iter()
            .map(|n| n.id)
            .collect();

        if alive_nodes.is_empty() {
            return;
        }

        // Use ControlPlane's rebalance to redistribute shards
        if let Some(new_map) = self.control_plane.rebalance_shards(&alive_nodes).await {
            tracing::info!(
                "Distributing {} shards across {} nodes (version {})",
                new_map.total_shards,
                alive_nodes.len(),
                new_map.version
            );
            self.router.update_shard_map(new_map).await;
        }
    }

    /// Start the heartbeat sender loop. Returns the task handle so `shutdown()`
    /// can abort it (a killed node must stop announcing liveness).
    fn start_heartbeat_sender(&self) -> tokio::task::JoinHandle<()> {
        let node_id = self.config.node_id.clone();
        let graph_addr = self.config.listen_addr.clone();
        let hb_addr = self.config.heartbeat_addr.clone();
        let interval = self.config.heartbeat_interval;
        let node_addrs = self.node_addrs.clone();
        let registry = self.registry.clone();
        // Captured for dynamic membership: when gossip reveals a NEW node, learn
        // its address, register it in the remote client, and rebalance so the new
        // member takes on its share of shards (data migrates to it).
        let remote_client = self.remote_client.clone();
        let control_plane = self.control_plane.clone();
        let replication_log = self.replication_log.clone();
        let catch_up_barrier = self.catch_up_barrier.clone();
        let fence = self.fence.clone();
        let router = self.router.clone();
        let rf = self.config.replication_factor;
        let total_shards = self.config.total_shards;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip first immediate tick

            // FIXED P1-2: Limit gossip learned nodes per tick to prevent memory exhaustion
            const MAX_LEARNED_NODES_PER_TICK: usize = 100;

            // Membership we last rebalanced for. Rebalancing is driven by a
            // *change* in known membership (from any source — gossip replies OR
            // heartbeats we received), not by the per-tick gossip diff, so both
            // the sending and receiving side converge.
            let mut last_members: std::collections::BTreeSet<String> = {
                let mut m: std::collections::BTreeSet<String> =
                    node_addrs.read().await.keys().cloned().collect();
                m.insert(node_id.clone());
                m
            };
            loop {
                ticker.tick().await;
                let addrs = node_addrs.read().await.clone();
                // Nodes learned from gossip replies this tick (id → (graph, hb)).
                let mut learned: std::collections::HashMap<String, (String, String)> =
                    std::collections::HashMap::new();
                for (peer_id, (_, peer_hb_addr)) in &addrs {
                    if peer_id == &node_id {
                        continue;
                    }
                    // Send heartbeat via TCP; the reply is the peer's gossip list.
                    if let Ok(stream) = tokio::net::TcpStream::connect(peer_hb_addr).await {
                        if let Ok(known) =
                            send_heartbeat(stream, &node_id, &graph_addr, &hb_addr).await
                        {
                            for k in known {
                                // FIXED P1-2: Enforce limit on learned nodes to prevent DoS
                                if learned.len() >= MAX_LEARNED_NODES_PER_TICK {
                                    tracing::warn!(
                                        "Gossip learned node limit reached ({MAX_LEARNED_NODES_PER_TICK}), \
                                         dropping excess nodes from peer {peer_id}"
                                    );
                                    break;
                                }
                                if k.node_id != node_id && !addrs.contains_key(&k.node_id) {
                                    learned.insert(
                                        k.node_id.clone(),
                                        (k.graph_addr, k.heartbeat_addr),
                                    );
                                }
                            }
                        }
                    }
                }
                // Update own heartbeat
                registry.heartbeat(&node_id, current_millis()).await;

                // Register nodes learned from gossip replies (the receiving side,
                // `handle_heartbeat`, already registered senders it heard from).
                for (id, (graph, hb)) in &learned {
                    node_addrs
                        .write()
                        .await
                        .insert(id.clone(), (graph.clone(), hb.clone()));
                    remote_client.register_node(id, graph).await;
                    registry
                        .register(NodeInfo {
                            id: id.clone(),
                            address: graph.clone(),
                            roles: vec!["compute".into(), "voter".into()],
                            last_heartbeat_ms: current_millis(),
                            alive: true,
                        })
                        .await;
                    tracing::info!("Dynamic membership: learned node {id} via gossip");
                }

                // Dynamic membership: rebalance whenever the known membership set
                // changed (a node joined via gossip on either side). Ensures peers
                // registered by `handle_heartbeat` — not just gossip replies — also
                // trigger a rebalance.
                let current_members: std::collections::BTreeSet<String> = {
                    let mut m: std::collections::BTreeSet<String> =
                        node_addrs.read().await.keys().cloned().collect();
                    m.insert(node_id.clone());
                    m
                };
                if current_members != last_members {
                    let members: Vec<String> = current_members.iter().cloned().collect();
                    let migrated = rebalance_impl(
                        &control_plane,
                        &remote_client,
                        &replication_log,
                        &catch_up_barrier,
                        &fence,
                        &router,
                        &node_id,
                        rf,
                        total_shards,
                        &members,
                    )
                    .await;
                    tracing::info!(
                        "Dynamic membership: membership changed to {} nodes; {migrated} shards migrated to {node_id}",
                        members.len()
                    );
                    last_members = current_members;
                }
            }
        })
    }

    /// Start the failure detector loop. Returns the task handle so `shutdown()`
    /// can abort it.
    fn start_failure_detector(&self) -> tokio::task::JoinHandle<()> {
        let registry = self.registry.clone();
        let control_plane = self.control_plane.clone();
        let router = self.router.clone();
        let fence = self.fence.clone();
        let remote_client = self.remote_client.clone();
        let replication_log = self.replication_log.clone();
        let catch_up_barrier = self.catch_up_barrier.clone();
        let total_shards = self.config.total_shards;
        let timeout = self.config.failure_timeout;
        let node_id = self.config.node_id.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(timeout);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let now = current_millis();
                let nodes = registry.alive_nodes().await;
                // This node is a live voter; record its own liveness so the
                // control plane can recognize a healthy quorum. Failover and
                // ShardMap proposals now fail closed unless quorum_healthy().
                control_plane.mark_node_alive(&node_id).await;
                for node in &nodes {
                    if node.id == node_id {
                        continue;
                    }
                    // P1-2 fix: Add grace period to prevent false positives from network jitter
                    let grace_period = timeout.mul_f64(1.5);
                    let stale = now.saturating_sub(node.last_heartbeat_ms)
                        > grace_period.as_millis() as u64;
                    if !stale {
                        // Peer heartbeat is fresh — record its liveness so a
                        // healthy majority is reflected in quorum_healthy().
                        control_plane.mark_node_alive(&node.id).await;
                        continue;
                    }

                    tracing::warn!("Node {} missed heartbeat, marking failed", node.id);
                    let failed_shards = control_plane.mark_node_failed(&node.id).await;

                    // Snapshot the pre-failover replica sets so we know, for each
                    // shard promoted to THIS node, which other replica still holds
                    // the data and can serve a catch-up.
                    let pre_map = control_plane.get_shard_map().await;

                    // Fail each orphaned shard over to a SURVIVING REPLICA (the
                    // node that actually holds the quorum-replicated data), not
                    // to self. Fails closed (NoQuorum) on a partition minority.
                    // A shard with no live replica is marked non-writable rather
                    // than handed to an empty owner.
                    //
                    // Collect the shards this node was promoted to own, plus a
                    // surviving source replica for each, so we can auto-catch-up
                    // after the map is published.
                    let mut catch_up_targets: Vec<(usize, String)> = Vec::new();
                    for shard_id in &failed_shards {
                        match control_plane.failover_shard_auto(*shard_id).await {
                            Ok(Some(token)) => {
                                tracing::info!(
                                    "Shard {} failed over to a replica (epoch {})",
                                    shard_id,
                                    token.epoch.value()
                                );
                                // If WE are the promoted owner, pick another
                                // pre-failover replica (still alive, not us, not
                                // the dead node) as the catch-up source. The
                                // promoted node already holds quorum-replicated
                                // data, but a lagging replica may have missed the
                                // most recent writes — catch-up reconciles it.
                                if control_plane.owner_of_shard(*shard_id).await.as_deref()
                                    == Some(node_id.as_str())
                                {
                                    if let Some(pre) = pre_map.get(*shard_id) {
                                        let source = pre
                                            .replicas
                                            .iter()
                                            .find(|r| {
                                                r.as_str() != node_id
                                                    && r.as_str() != node.id.as_str()
                                            })
                                            .cloned();
                                        if let Some(src) = source {
                                            catch_up_targets.push((*shard_id, src));
                                        }
                                    }
                                }
                            }
                            Ok(None) => tracing::error!(
                                "Shard {} has no surviving replica; left non-writable",
                                shard_id
                            ),
                            Err(e) => tracing::warn!("Shard {} failover refused: {e}", shard_id),
                        }
                    }

                    // Update router with new shard map
                    if !failed_shards.is_empty() {
                        let map = control_plane.get_shard_map().await;
                        // Advance the fence first: failover bumped the epoch of
                        // each reassigned shard, so the fence must reject the old
                        // owner's stragglers even before the new map reaches the
                        // router (and any follower learns it authoritatively).
                        fence.observe_map(&map).await;
                        router.update_shard_map(map).await;
                    }

                    // Auto-trigger state transfer: reconcile each shard we were
                    // promoted to own from a surviving replica, so the new owner
                    // serves reconciled data rather than possibly-stale local
                    // state. Best-effort — a failed catch-up is logged; the shard
                    // still has the promoted node's quorum-replicated data.
                    // Prefer an incremental delta (ship only the ops we're behind
                    // by); fall back to a full snapshot if we lag past the log
                    // window. Our high-water for the shard is the seq we last
                    // recorded as a follower.
                    //
                    // Each catch-up runs inside the write barrier: the shard is
                    // blocked for owner writes (fence) → catch-up → reopen, so a
                    // client write can't interleave with the replay and get
                    // clobbered by a stale delta op.
                    for (shard_id, source) in catch_up_targets {
                        let st = crate::state_transfer::StateTransfer::new(remote_client.clone());
                        let from_seq = replication_log.high_water(shard_id).await;
                        let barrier = catch_up_barrier.clone();
                        let result = barrier
                            .guard(
                                shard_id,
                                st.catch_up_incremental(
                                    &source,
                                    &node_id,
                                    shard_id,
                                    total_shards,
                                    from_seq,
                                ),
                            )
                            .await;
                        match result {
                            Ok(r) if r.incremental => tracing::info!(
                                "Shard {shard_id} caught up from {source} incrementally: {} ops (from_seq={from_seq})",
                                r.ops_applied
                            ),
                            Ok(r) => tracing::info!(
                                "Shard {shard_id} caught up from {source} via snapshot: {} nodes, {} edges",
                                r.nodes_applied,
                                r.edges_applied
                            ),
                            Err(e) => tracing::warn!(
                                "Shard {shard_id} catch-up from {source} failed (non-fatal): {e}"
                            ),
                        }
                    }
                }
            }
        })
    }

    /// Get a reference to the hybrid router.
    pub fn router(&self) -> &HybridRouter {
        &self.router
    }

    /// Get a cloneable handle to the hybrid router.
    ///
    /// Used by the app layer to inject the router into `AppState` so the HTTP
    /// handlers can route cross-node operations to shard owners.
    pub fn router_arc(&self) -> Arc<HybridRouter> {
        self.router.clone()
    }

    /// Get a cloneable handle to the quorum write replicator.
    ///
    /// Injected into `AppState` so the write path can replicate owner writes to
    /// followers and wait for quorum before acknowledging the client.
    pub fn replica_writer_arc(&self) -> Arc<crate::replica_writer::ReplicaWriter> {
        self.replica_writer.clone()
    }

    /// C2: shared handle to the replication progress tracker (per-shard commit
    /// index and per-replica applied seqs). The distributed read path checks
    /// this to gate `ReadConcern::Majority` on replicas caught up to quorum.
    pub fn replication_progress(&self) -> Arc<crate::replication_progress::ReplicationProgress> {
        self.replication_progress.clone()
    }

    /// B2: shared handle to the replication metrics, for monitoring endpoints.
    /// Reflects best-effort follower replication health (attempts, quorum
    /// failures, follower nacks) — the only signal that a cluster is silently
    /// degrading toward effective RF=1.
    pub fn replication_metrics(&self) -> Arc<crate::replica_writer::ReplicationMetrics> {
        self.replica_writer.metrics()
    }

    /// Get the remote client for manual operations.
    pub fn remote_client(&self) -> &TcpRemoteClient {
        &self.remote_client
    }

    /// Get a reference to the control plane for testing and manual operations.
    pub fn control_plane(&self) -> &ControlPlane {
        &self.control_plane
    }

    /// Take ownership of the ontology-activation receiver (one-shot, drains the
    /// `Option`). The app layer spawns a task consuming this to activate committed
    /// domain definitions (create event tables, update router, schedule views).
    /// `None` when Raft is disabled (fallback mode).
    pub fn take_ontology_activation_rx(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<crate::control_raft_sm::OntologyActivation>>
    {
        self.ontology_activation_rx.take()
    }

    /// List every committed ontology (`Namespace::DomainDef`) in the Raft state
    /// machine's control-plane store, for startup reconciliation. A follower
    /// caught up via snapshot install has these entries in the store but never
    /// saw per-entry activation events, so the app layer calls this on startup
    /// to re-activate them. Returns empty when Raft is disabled.
    pub fn committed_ontologies(
        &self,
    ) -> Vec<(String, Vec<u8>)> {
        let Some(ref store) = self.control_plane_store else {
            return Vec::new();
        };
        store
            .list(nexora_core::control_plane_store::Namespace::DomainDef)
            .unwrap_or_default()
    }

    /// Propose an ontology package creation/update via Raft consensus. Returns
    /// `Ok(true)` when committed (every node's apply callback has persisted it and
    /// will activate via the drain task), `Ok(false)` when Raft is disabled
    /// (caller falls back to best-effort broadcast).
    pub async fn propose_ontology_put(
        &self,
        domain: &str,
        pkg_json: Vec<u8>,
    ) -> Result<bool, crate::control::ControlError> {
        self.control_plane.propose_domain_put(domain, pkg_json).await
    }

    /// Propose an ontology removal via Raft. Returns `Ok(false)` when Raft is
    /// disabled.
    pub async fn propose_ontology_delete(
        &self,
        domain: &str,
    ) -> Result<bool, crate::control::ControlError> {
        self.control_plane.propose_domain_delete(domain).await
    }

    /// Check whether the Raft control plane is enabled (vs fallback mode).
    pub fn is_raft_enabled(&self) -> bool {
        self.control_plane_store.is_some()
    }

    /// Get a snapshot of the current shard map from the router.
    pub async fn shard_map_snapshot(&self) -> ShardMap {
        self.router.shard_map_snapshot().await
    }

    /// Recover a shard's data into this node's graph by pulling it from a
    /// surviving source (owner or a live replica). Used after a failover
    /// promotion or when a restarted replica must catch up before serving.
    ///
    /// The recovering node applies the fetched operations *to itself*, routing
    /// through the remote client's self-registration (TCP-to-self, established
    /// in `start()`), so the data lands in its local graph. `source_node` must
    /// be a node id known to the remote client that currently holds the shard.
    pub async fn catch_up_shard(
        &self,
        source_node: &str,
        shard_id: usize,
    ) -> Result<crate::state_transfer::CatchUpResult, crate::state_transfer::StateTransferError>
    {
        let total_shards = self.config.total_shards;
        let st = crate::state_transfer::StateTransfer::new(self.remote_client.clone());
        st.catch_up_shard(source_node, &self.config.node_id, shard_id, total_shards)
            .await
    }

    /// Rebalance shards across the given membership set (scale-out / scale-in),
    /// migrating data for shards this node newly owns before publishing the map.
    ///
    /// Steps, RF-aware and deterministic:
    /// 1. Recompute the target assignment for `members` (control plane).
    /// 2. For each shard whose ownership moves *to this node*, pull its data from
    ///    the previous owner via state transfer — under the catch-up barrier, so
    ///    writes don't race the migration — before the shard is served here.
    /// 3. Publish the new map to the router (and advance the fence).
    ///
    /// `members` must be the full cluster membership (all nodes, not just alive)
    /// so every node derives the identical target. Returns the number of shards
    /// migrated to this node. A no-op (returns 0) if nothing moved.
    pub async fn rebalance(&self, members: &[String]) -> usize {
        rebalance_impl(
            &self.control_plane,
            &self.remote_client,
            &self.replication_log,
            &self.catch_up_barrier,
            &self.fence,
            &self.router,
            &self.config.node_id,
            self.config.replication_factor,
            self.config.total_shards,
            members,
        )
        .await
    }

    /// Get cluster stats.
    pub async fn stats(&self) -> ClusterStats {
        let alive = self.registry.alive_count().await;
        let total = self.registry.total_count().await;
        let uptime_secs = self.start_time.elapsed().as_secs();
        let shard_map = self.router.shard_map_snapshot().await;

        ClusterStats {
            node_id: self.config.node_id.clone(),
            alive_nodes: alive,
            total_known_nodes: total,
            uptime_secs,
            shard_map_version: shard_map.version,
            total_shards: shard_map.total_shards,
            local_shards: shard_map.local_shard_count(),
        }
    }

    /// Dynamically add a node to the cluster membership and rebalance shards.
    ///
    /// The new node must already be running and reachable (heartbeat and graph
    /// listeners up) so data migration can succeed. Returns the list of shards
    /// reassigned by the rebalance: `(shard_id, old_owner, new_owner)`.
    ///
    /// Steps:
    /// 1. Add node to the control plane's membership roster
    /// 2. Register in local discovery registry
    /// 3. Trigger RF-aware rebalance across the new membership
    /// 4. Migrate data for reassigned shards (via catch-up)
    /// 5. Publish the new ShardMap to the router
    pub async fn add_node(
        &self,
        node_id: String,
        graph_addr: String,
    ) -> Result<Vec<(usize, String, String)>, ClusterError> {
        // Register in control plane (updates internal membership list)
        let moves = self
            .control_plane
            .add_node(node_id.clone())
            .await
            .map_err(|e| ClusterError::Rebalance(format!("add_node failed: {e}")))?;

        // Update local registry so heartbeat and discovery know about the new peer
        self.registry
            .register(NodeInfo {
                id: node_id.clone(),
                address: graph_addr.clone(),
                roles: vec!["compute".into()],
                last_heartbeat_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                alive: true,
            })
            .await;

        // Teach the remote client the new node's address BEFORE rebalancing.
        // Otherwise a shard reassigned to this node by the rebalance would route
        // to an unknown node id and the catch-up/data path would fail. The
        // heartbeat address is unknown here (only the graph address is passed);
        // record the graph address for both so node_addrs stays consistent — the
        // heartbeat loop will refresh the real heartbeat address once the new
        // node starts gossiping.
        self.remote_client
            .register_node(&node_id, &graph_addr)
            .await;
        self.node_addrs
            .write()
            .await
            .insert(node_id.clone(), (graph_addr.clone(), graph_addr.clone()));

        // Trigger rebalance with the updated membership
        let members = self.control_plane.get_members().await;
        self.rebalance(&members).await;

        tracing::info!(
            node_id = %node_id,
            reassignments = moves.len(),
            "node added to cluster and rebalance complete"
        );

        Ok(moves)
    }

    /// Dynamically remove a node from the cluster membership and rebalance shards.
    ///
    /// Shards owned by the removed node are reassigned to remaining members and
    /// data is migrated. Returns the list of reassignments: `(shard_id, old_owner, new_owner)`.
    ///
    /// The removed node should be stopped afterward (it will continue serving
    /// requests until its processes are killed). If it stays up and keeps sending
    /// heartbeats, it may be re-added by the discovery layer.
    pub async fn remove_node(
        &self,
        node_id: &str,
    ) -> Result<Vec<(usize, String, String)>, ClusterError> {
        // Remove from control plane membership
        let moves = self
            .control_plane
            .remove_node(node_id)
            .await
            .map_err(|e| ClusterError::Rebalance(format!("remove_node failed: {e}")))?;

        // Unregister from local registry
        self.registry.unregister(node_id).await;

        // Trigger rebalance with the reduced membership
        let members = self.control_plane.get_members().await;
        self.rebalance(&members).await;

        tracing::info!(
            node_id = %node_id,
            reassignments = moves.len(),
            "node removed from cluster and rebalance complete"
        );

        Ok(moves)
    }

    /// Shutdown the cluster manager: stop the TCP servers and abort the
    /// background heartbeat/failure-detector loops. Aborting the heartbeat sender
    /// is what makes a "killed" node stop announcing liveness, so peers actually
    /// detect the failure and fail its shards over.
    ///
    /// NOTE: this does NOT stop the openraft node. A node shut down this way keeps
    /// participating in control-plane consensus over its still-open outbound
    /// clients, which is fine for data-plane-only teardown but does NOT simulate a
    /// partition of the control plane. To fully sever a node from Raft (e.g. to
    /// test failover / split-brain), use [`shutdown_async`], which also shuts the
    /// Raft node down.
    pub fn shutdown(&self) {
        if let Some(ref server) = self.graph_server {
            server.shutdown();
        }
        if let Some(ref server) = self.heartbeat_server {
            server.shutdown();
        }
        for task in &self.background_tasks {
            task.abort();
        }
    }

    /// Full shutdown including the control-plane Raft node.
    ///
    /// Beyond [`shutdown`](Self::shutdown)'s data-plane teardown, this shuts down
    /// the openraft node so it stops issuing votes and append-entries. To the rest
    /// of the cluster the node then looks partitioned away: it can no longer hold
    /// leadership or contribute to quorum, which is the precondition for
    /// exercising real leader failover and the no-split-brain guarantee.
    pub async fn shutdown_async(&self) {
        if let Some(ref raft) = self.raft {
            // Best-effort: a node whose RaftCore already stopped returns an error
            // we don't care about here.
            let _ = raft.shutdown().await;
        }
        self.shutdown();
    }
}

/// Cluster statistics.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ClusterStats {
    pub node_id: String,
    pub alive_nodes: usize,
    pub total_known_nodes: usize,
    pub uptime_secs: u64,
    pub shard_map_version: u64,
    pub total_shards: usize,
    pub local_shards: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error("config load error: {0}")]
    ConfigLoad(String),
    #[error("rebalance error: {0}")]
    Rebalance(String),
}

// ============================================================
// Heartbeat protocol
// ============================================================

/// TCP heartbeat server — accepts heartbeat connections and updates the registry.
pub struct TcpHeartbeatServer {
    _node_id: String,
    listen_addr: String,
    registry: Arc<ClusterRegistry>,
    node_addrs: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl TcpHeartbeatServer {
    pub fn new(
        node_id: String,
        listen_addr: String,
        registry: Arc<ClusterRegistry>,
        node_addrs: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
    ) -> Self {
        Self {
            _node_id: node_id,
            listen_addr,
            registry,
            node_addrs,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub async fn start(&self) -> Result<String, std::io::Error> {
        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        let addr = listener.local_addr()?.to_string();
        tracing::debug!("Heartbeat server on {addr}");

        let registry = self.registry.clone();
        let node_addrs = self.node_addrs.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, _peer)) => {
                                let r = registry.clone();
                                let na = node_addrs.clone();
                                tokio::spawn(handle_heartbeat(stream, r, na));
                            }
                            Err(e) => {
                                tracing::error!("Heartbeat accept error: {e}");
                                break;
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        tracing::debug!("Heartbeat server shutting down");
                        break;
                    }
                }
            }
        });

        Ok(addr)
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

/// Handle a heartbeat connection: read heartbeat, update registry, respond with known nodes.
async fn handle_heartbeat(
    mut stream: tokio::net::TcpStream,
    registry: Arc<ClusterRegistry>,
    node_addrs: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Read heartbeat: 4B BE length + JSON {node_id, graph_addr, heartbeat_addr}
    let len = match stream.read_u32().await {
        Ok(n) => n as usize,
        Err(_) => return,
    };
    if len > 65536 {
        return;
    }
    let mut buf = vec![0u8; len];
    if stream.read_exact(&mut buf).await.is_err() {
        return;
    }

    let hb: HeartbeatMessage = match serde_json::from_slice(&buf) {
        Ok(h) => h,
        Err(_) => return,
    };

    // Register/update the heartbeat sender
    registry
        .register(NodeInfo {
            id: hb.node_id.clone(),
            address: hb.graph_addr.clone(),
            roles: vec!["compute".into(), "voter".into()],
            last_heartbeat_ms: current_millis(),
            alive: true,
        })
        .await;

    // Learn about the sender's addresses
    node_addrs
        .write()
        .await
        .insert(hb.node_id.clone(), (hb.graph_addr, hb.heartbeat_addr));

    // Respond with known alive nodes (gossip)
    let known_nodes: Vec<HeartbeatMessage> = {
        let nodes = registry.alive_nodes().await;
        let addrs = node_addrs.read().await;
        nodes
            .into_iter()
            .map(|n| {
                let (graph, hb) = addrs
                    .get(&n.id)
                    .cloned()
                    .unwrap_or((n.address.clone(), n.address));
                HeartbeatMessage {
                    node_id: n.id,
                    graph_addr: graph,
                    heartbeat_addr: hb,
                }
            })
            .collect()
    };

    let resp_bytes = serde_json::to_vec(&known_nodes).unwrap_or_default();
    let _ = stream.write_u32(resp_bytes.len() as u32).await;
    let _ = stream.write_all(&resp_bytes).await;

    // Process gossip: learn about new nodes
    // (already done above via the registry update)
}

/// Send a heartbeat to a peer and return the peer's gossip list (the nodes it
/// knows about, so the caller can learn new members for dynamic membership).
async fn send_heartbeat(
    mut stream: tokio::net::TcpStream,
    node_id: &str,
    graph_addr: &str,
    heartbeat_addr: &str,
) -> Result<Vec<HeartbeatMessage>, std::io::Error> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let hb = HeartbeatMessage {
        node_id: node_id.to_string(),
        graph_addr: graph_addr.to_string(),
        heartbeat_addr: heartbeat_addr.to_string(),
    };

    let payload = serde_json::to_vec(&hb).unwrap_or_default();
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    // Read response (list of known nodes for gossip)
    let resp_len = stream.read_u32().await? as usize;
    if resp_len > 65536 {
        return Ok(Vec::new());
    }
    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await?;

    // Parse gossip response — the caller learns any new nodes from it.
    let known: Vec<HeartbeatMessage> = serde_json::from_slice(&resp_buf).unwrap_or_default();
    Ok(known)
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct HeartbeatMessage {
    node_id: String,
    graph_addr: String,
    heartbeat_addr: String,
}

/// Shared rebalance implementation, callable from `ClusterManager::rebalance`
/// and the heartbeat loop (dynamic membership). Recomputes the target map for
/// `members`, migrates data for shards moving to this node (under the catch-up
/// barrier), then publishes the new map. Returns shards migrated to this node.
#[allow(clippy::too_many_arguments)]
async fn rebalance_impl(
    control_plane: &Arc<ControlPlane>,
    remote_client: &Arc<TcpRemoteClient>,
    replication_log: &crate::replication_log::ShardReplicationLog,
    catch_up_barrier: &crate::catch_up_barrier::CatchUpBarrier,
    fence: &crate::fencing::ShardFence,
    router: &Arc<HybridRouter>,
    node_id: &str,
    replication_factor: usize,
    total_shards: usize,
    members: &[String],
) -> usize {
    let Some((new_map, moved)) = control_plane
        .rebalance_for_members(members, replication_factor)
        .await
    else {
        return 0;
    };

    let mut migrated = 0usize;
    for (shard_id, old_owner, new_owner) in &moved {
        // Only migrate shards moving TO this node; other moves are handled by
        // their own new owners (each node runs the same deterministic plan).
        if new_owner != node_id || old_owner == node_id {
            continue;
        }
        let st = crate::state_transfer::StateTransfer::new(remote_client.clone());
        let from_seq = replication_log.high_water(*shard_id).await;
        let result = catch_up_barrier
            .guard(
                *shard_id,
                st.catch_up_incremental(old_owner, node_id, *shard_id, total_shards, from_seq),
            )
            .await;
        match result {
            Ok(_) => {
                migrated += 1;
                tracing::info!(
                    "Rebalance: shard {shard_id} migrated to {node_id} from {old_owner}"
                );
            }
            Err(e) => tracing::warn!(
                "Rebalance: shard {shard_id} migration from {old_owner} failed (non-fatal): {e}"
            ),
        }
    }

    // Publish: advance the fence (owner epochs bumped) then update routing.
    fence.observe_map(&new_map).await;
    router.update_shard_map(new_map).await;
    migrated
}

/// Get current time in milliseconds.
fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Extension for ClusterRegistry to get total count.
impl ClusterRegistry {
    pub async fn total_count(&self) -> usize {
        self.alive_count().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config_creation() {
        let config = ClusterConfig {
            node_id: "node-1".into(),
            listen_addr: "127.0.0.1:0".into(),
            heartbeat_addr: "127.0.0.1:0".into(),
            total_shards: 4,
            peers: vec![],
            heartbeat_interval: Duration::from_secs(1),
            failure_timeout: Duration::from_secs(5),
            replication_factor: 1,
            replication_log_dir: None,
            shard_map_dir: None,
            anti_entropy_interval: None,
        };
        assert_eq!(config.node_id, "node-1");
        assert_eq!(config.total_shards, 4);
    }

    #[test]
    fn test_cluster_manager_creation() {
        let config = ClusterConfig {
            node_id: "node-1".into(),
            listen_addr: "127.0.0.1:0".into(),
            heartbeat_addr: "127.0.0.1:0".into(),
            total_shards: 4,
            peers: vec![],
            heartbeat_interval: Duration::from_secs(1),
            failure_timeout: Duration::from_secs(5),
            replication_factor: 1,
            replication_log_dir: None,
            shard_map_dir: None,
            anti_entropy_interval: None,
        };
        let manager = ClusterManager::new(config);
        assert!(manager.graph_server.is_none());
        assert!(manager.heartbeat_server.is_none());
    }

    #[tokio::test]
    async fn test_cluster_manager_start_single_node() {
        let handler = Arc::new(TestHandler);
        let config = ClusterConfig {
            node_id: "node-1".into(),
            listen_addr: "127.0.0.1:0".into(),
            heartbeat_addr: "127.0.0.1:0".into(),
            total_shards: 4,
            peers: vec![],
            heartbeat_interval: Duration::from_secs(1),
            failure_timeout: Duration::from_secs(5),
            replication_factor: 1,
            replication_log_dir: None,
            shard_map_dir: None,
            anti_entropy_interval: None,
        };
        let mut manager = ClusterManager::new(config);
        manager.start(handler).await.unwrap();

        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let stats = manager.stats().await;
        assert_eq!(stats.node_id, "node-1");
        assert!(stats.alive_nodes >= 1); // at least self

        manager.shutdown();
    }

    #[tokio::test]
    async fn test_shard_map_local_shard_count() {
        let map = ShardMap::new_local(8);
        assert_eq!(map.local_shard_count(), 8);

        let mut map = ShardMap::new_local(4);
        map.local_node = "other-node".to_string();
        for a in map.assignments.values_mut() {
            a.owner = "other-node".to_string();
        }
        assert_eq!(map.local_shard_count(), 4);
    }

    /// Regression: a multi-peer ClusterManager must NOT own every shard locally.
    /// A prior bug re-ran `distribute_shards()` at cold start with alive_nodes =
    /// [self] (peers hadn't heartbeated yet), pulling all shards back to local
    /// and dropping replicas — silently defeating sharding and replication.
    #[tokio::test]
    async fn test_new_distributes_shards_across_peers() {
        let config = ClusterConfig {
            node_id: "node-1".into(),
            listen_addr: "127.0.0.1:0".into(),
            heartbeat_addr: "127.0.0.1:0".into(),
            total_shards: 9,
            peers: vec![
                PeerConfig {
                    node_id: "node-2".into(),
                    graph_addr: "127.0.0.1:1".into(),
                    heartbeat_addr: "127.0.0.1:2".into(),
                },
                PeerConfig {
                    node_id: "node-3".into(),
                    graph_addr: "127.0.0.1:3".into(),
                    heartbeat_addr: "127.0.0.1:4".into(),
                },
            ],
            heartbeat_interval: Duration::from_secs(1),
            failure_timeout: Duration::from_secs(5),
            replication_factor: 3,
            replication_log_dir: None,
            shard_map_dir: None,
            anti_entropy_interval: None,
        };
        let manager = ClusterManager::new(config);
        let router = manager.router();

        // 3 nodes, 9 shards → this node owns ~3, not all 9.
        let local = router.local_shard_count().await;
        assert_eq!(local, 3, "node-1 should own 1/3 of shards, not all");
        assert!(!router.all_shards_local().await, "not all shards are local");

        // Replicas must be present (RF=3 → each shard has 2 followers).
        let map = router.shard_map_snapshot().await;
        for shard in 0..9 {
            assert_eq!(
                map.get(shard).unwrap().replicas.len(),
                2,
                "shard {shard} must carry 2 replicas after new()"
            );
        }
    }

    /// Simple test handler.
    struct TestHandler;

    #[async_trait::async_trait]
    impl GraphHandler for TestHandler {
        async fn handle(
            &self,
            _target: &str,
            _op: crate::GraphOperation,
        ) -> Result<crate::GraphResult, crate::RouterError> {
            Ok(crate::GraphResult::Status {
                ok: true,
                message: "test".into(),
            })
        }
    }
}
