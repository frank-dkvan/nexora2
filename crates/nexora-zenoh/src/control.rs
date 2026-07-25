//! Control plane — consensus-based ShardMap management.
//!
//! Design doc §6.3: At least 3 control plane voters.
//! Uses majority quorum for ShardMap changes and owner failover.

use crate::replication::{FencingToken, ReplicaSet};
use crate::shard_map::{OwnerEpoch, ShardId, ShardMap};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A2-7: Optional Raft handle for consensus-based control-plane writes.
type RaftHandle = openraft::Raft<crate::control_raft::ControlRaftTypeConfig>;

/// Control plane state.
pub struct ControlPlane {
    /// Current ShardMap (versioned, consensus-backed)
    shard_map: Arc<RwLock<ShardMap>>,
    /// Known voter nodes
    voters: Vec<String>,
    /// Replica sets per shard
    replica_sets: RwLock<HashMap<ShardId, ReplicaSet>>,
    /// Node health status
    node_health: RwLock<HashMap<String, NodeHealth>>,
    /// Current cluster membership (data nodes)
    members: RwLock<Vec<String>>,
    /// Replication factor
    replication_factor: usize,
    /// A0: durable snapshot of the committed shard map. Every mutation that
    /// bumps the map version (proposal, failover, rebalance) is snapshotted so
    /// owners + epochs survive a restart and a deposed owner stays fenced.
    /// In-memory no-op by default; made durable via [`Self::with_store`].
    store: crate::shard_map_store::ShardMapStore,
    /// A2-7: Optional Raft handle. When present, control-plane writes go
    /// through consensus rather than the hand-rolled quorum path. Wrapped in
    /// RwLock to allow late injection after Raft node creation in start().
    raft: RwLock<Option<Arc<RaftHandle>>>,
    /// A2-7: Optional Router reference. When present, every committed ShardMap
    /// mutation (via Raft or fallback quorum) propagates to the router so
    /// PG-wire queries route to the correct owner post-failover.
    router: Option<Arc<crate::router::HybridRouter>>,
}

#[derive(Clone, Debug)]
pub struct NodeHealth {
    pub alive: bool,
    pub last_heartbeat_ms: u64,
    pub failure_count: u32,
    pub role: NodeRole,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRole {
    Voter,
    Compute,
    Compactor,
    Observer,
}

impl ControlPlane {
    /// Create a control plane with voter set and local node ID.
    pub fn new(total_shards: usize, voters: Vec<String>) -> Self {
        let map = ShardMap::new_local(total_shards);
        let local_node = map.local_node.clone();
        Self {
            shard_map: Arc::new(RwLock::new(map)),
            voters,
            replica_sets: RwLock::new(HashMap::new()),
            node_health: RwLock::new(HashMap::new()),
            members: RwLock::new(vec![local_node]),
            replication_factor: 1,
            store: crate::shard_map_store::ShardMapStore::in_memory(),
            raft: RwLock::new(None),
            router: None,
        }
    }

    /// Create a control plane with a specific local node ID.
    pub fn with_local_node(total_shards: usize, voters: Vec<String>, local_node: String) -> Self {
        let mut map = ShardMap::new_local(total_shards);
        map.local_node = local_node.clone();
        for a in map.assignments.values_mut() {
            a.owner = map.local_node.clone();
        }
        Self {
            shard_map: Arc::new(RwLock::new(map)),
            voters,
            replica_sets: RwLock::new(HashMap::new()),
            node_health: RwLock::new(HashMap::new()),
            members: RwLock::new(vec![local_node]),
            replication_factor: 1,
            store: crate::shard_map_store::ShardMapStore::in_memory(),
            raft: RwLock::new(None),
            router: None,
        }
    }

    /// Create a control plane seeded with a pre-built ShardMap.
    ///
    /// Used so the control plane and the router share the *same* initial
    /// (distributed, replica-aware) assignment from cold start — otherwise the
    /// control plane's map (used to drive failover) would disagree with the
    /// router's map (used to route traffic), and a failover would overwrite the
    /// router with a stale/local-only map.
    pub fn with_shard_map(shard_map: ShardMap, voters: Vec<String>) -> Self {
        let members: Vec<String> = shard_map
            .assignments
            .values()
            .flat_map(|a| {
                let mut nodes = vec![a.owner.clone()];
                nodes.extend(a.replicas.iter().cloned());
                nodes
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        Self {
            shard_map: Arc::new(RwLock::new(shard_map)),
            voters,
            replica_sets: RwLock::new(HashMap::new()),
            node_health: RwLock::new(HashMap::new()),
            members: RwLock::new(members),
            replication_factor: 1,
            store: crate::shard_map_store::ShardMapStore::in_memory(),
            raft: RwLock::new(None),
            router: None,
        }
    }

    /// Create a control plane with explicit member list and RF.
    pub fn with_members(
        shard_map: ShardMap,
        voters: Vec<String>,
        members: Vec<String>,
        replication_factor: usize,
    ) -> Self {
        Self {
            shard_map: Arc::new(RwLock::new(shard_map)),
            voters,
            replica_sets: RwLock::new(HashMap::new()),
            node_health: RwLock::new(HashMap::new()),
            members: RwLock::new(members),
            replication_factor,
            store: crate::shard_map_store::ShardMapStore::in_memory(),
            raft: RwLock::new(None),
            router: None,
        }
    }

    /// Attach a durable shard-map store to an existing control plane.
    ///
    /// A0: when set, every committed shard-map change (proposal, failover,
    /// rebalance, add/remove node) is snapshotted to disk so runtime ownership
    /// and epoch changes survive a restart. The caller is responsible for
    /// seeding the initial map from `store.load()` before constructing the
    /// control plane (see `ClusterManager::new`), so this only wires the
    /// write-through path.
    pub fn with_store(mut self, store: crate::shard_map_store::ShardMapStore) -> Self {
        self.store = store;
        self
    }

    /// A2-7: Attach a Router reference for ShardMap propagation.
    /// When set, every committed ShardMap mutation (via Raft or fallback quorum)
    /// propagates to the router so PG-wire queries route to the correct owner.
    pub fn with_router(mut self, router: Arc<crate::router::HybridRouter>) -> Self {
        self.router = Some(router);
        self
    }

    /// A2-7: Attach a Raft handle for consensus-based control-plane writes.
    /// When set, `propose_shard_map_update`, `failover_shard`, and
    /// `failover_shard_auto` go through Raft rather than the hand-rolled quorum.
    pub async fn set_raft(&self, raft: Arc<RaftHandle>) {
        *self.raft.write().await = Some(raft);
    }

    /// Persist the current in-memory map through the store. Called after every
    /// committed mutation while still holding the map write lock, so the
    /// snapshot never lags a committed change. Failures are logged, not
    /// propagated: the in-memory map is authoritative for the running process
    /// and a snapshot write error must not fail the originating operation.
    fn persist_map(&self, map: &ShardMap) {
        if let Err(e) = self.store.save(map) {
            tracing::warn!("Failed to persist shard map snapshot: {e}");
        }
    }

    /// Propose a ShardMap update (requires quorum acceptance).
    ///
    /// A2-7: When Raft is enabled, submits the proposal as a Raft command and
    /// waits for commit. Falls back to hand-rolled quorum when Raft is None.
    ///
    /// Fails closed: a ShardMap change is only applied when a majority of voters
    /// is currently reachable (`quorum_healthy`). Accepting a proposal on the
    /// minority side of a partition would let two partitions commit contradictory
    /// maps at the same version, so we refuse rather than diverge.
    pub async fn propose_shard_map_update(&self, proposed: ShardMap) -> Result<bool, ControlError> {
        if let Some(ref raft) = *self.raft.read().await {
            // A2-7: Raft path — submit as a command and wait for apply.
            let proposed_bytes = serde_json::to_vec(&proposed)
                .map_err(|e| ControlError::RaftError(format!("serialize map: {}", e)))?;
            let cmd = crate::control_raft::ControlCommand::ProposeShardMap {
                proposed: proposed_bytes,
            };

            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    crate::control_raft::ControlResponse::MapAccepted { version } => {
                        // Update local cache to match the committed map.
                        let new_map = {
                            let mut map = self.shard_map.write().await;
                            if version > map.version {
                                *map = proposed.clone();
                            }
                            map.clone()
                        };
                        // Propagate to router so PG-wire queries route correctly.
                        if let Some(ref router) = self.router {
                            router.update_shard_map(new_map).await;
                        }
                        Ok(true)
                    }
                    crate::control_raft::ControlResponse::Error { message } => {
                        Err(ControlError::RaftError(message))
                    }
                    _ => Err(ControlError::RaftError("unexpected response".into())),
                },
                Err(e) => Err(ControlError::RaftError(format!("client_write: {}", e))),
            }
        } else {
            // Fallback: hand-rolled quorum path (pre-A2-7).
            let voters_count = self.voters.len();
            let required_votes = voters_count / 2 + 1;

            if !self.quorum_healthy().await {
                let health = self.node_health.read().await;
                let alive_voters = self
                    .voters
                    .iter()
                    .filter(|v| health.get(v.as_str()).is_some_and(|h| h.alive))
                    .count();
                return Err(ControlError::NoQuorum {
                    voters: alive_voters,
                    required: required_votes,
                });
            }

            let new_map = {
                let mut map = self.shard_map.write().await;
                if proposed.version <= map.version {
                    return Err(ControlError::StaleProposal {
                        proposed: proposed.version,
                        current: map.version,
                    });
                }
                *map = proposed;
                tracing::info!("ShardMap committed at version {}", map.version);
                self.persist_map(&map);
                map.clone()
            };
            // Propagate to router so PG-wire queries route correctly.
            if let Some(ref router) = self.router {
                router.update_shard_map(new_map).await;
            }
            Ok(true)
        }
    }

    /// Propose an ontology package under `Namespace::DomainDef` via Raft. On
    /// commit the state machine persists it and emits an activation event on
    /// every node. Returns `Ok(true)` when committed, `Ok(false)` when Raft is
    /// not enabled (caller falls back to best-effort broadcast).
    pub async fn propose_domain_put(
        &self,
        domain: &str,
        pkg_json: Vec<u8>,
    ) -> Result<bool, ControlError> {
        let Some(raft) = self.raft.read().await.clone() else {
            return Ok(false);
        };
        let cmd = crate::control_raft::ControlCommand::put(
            nexora_core::control_plane_store::Namespace::DomainDef,
            domain.to_string(),
            pkg_json,
        );
        match raft.client_write(cmd).await {
            Ok(resp) => match resp.data {
                crate::control_raft::ControlResponse::Applied => Ok(true),
                crate::control_raft::ControlResponse::Error { message } => {
                    Err(ControlError::RaftError(message))
                }
                _ => Err(ControlError::RaftError("unexpected response".into())),
            },
            Err(e) => Err(ControlError::RaftError(format!("client_write: {}", e))),
        }
    }

    /// Propose an ontology removal (delete the `DomainDef` key) via Raft.
    /// Returns `Ok(false)` when Raft is not enabled.
    pub async fn propose_domain_delete(&self, domain: &str) -> Result<bool, ControlError> {
        let Some(raft) = self.raft.read().await.clone() else {
            return Ok(false);
        };
        let cmd = crate::control_raft::ControlCommand::delete(
            nexora_core::control_plane_store::Namespace::DomainDef,
            domain.to_string(),
        );
        match raft.client_write(cmd).await {
            Ok(resp) => match resp.data {
                crate::control_raft::ControlResponse::Applied => Ok(true),
                crate::control_raft::ControlResponse::Error { message } => {
                    Err(ControlError::RaftError(message))
                }
                _ => Err(ControlError::RaftError("unexpected response".into())),
            },
            Err(e) => Err(ControlError::RaftError(format!("client_write: {}", e))),
        }
    }

    /// Failover a shard to a new owner.
    ///
    /// A2-7: When Raft is enabled, submits the failover as a Raft command.
    /// Falls back to hand-rolled quorum when Raft is None.
    ///
    /// Fails closed on the minority side of a partition. Each node runs its own
    /// failure detector, so without this guard two partitions would each mark the
    /// other failed and seize the same shards at the same new epoch — split-brain
    /// with two writable owners. Requiring a healthy voter quorum ensures only the
    /// majority side performs failover; the minority side returns `NoQuorum` and
    /// keeps its shards read-only pending reconciliation.
    pub async fn failover_shard(
        &self,
        shard_id: ShardId,
        new_owner: String,
    ) -> Result<FencingToken, ControlError> {
        if let Some(ref raft) = *self.raft.read().await {
            // A2-7: Raft path.
            let cmd = crate::control_raft::ControlCommand::FailoverShard {
                shard_id: shard_id as u32,
                new_owner: new_owner.clone(),
            };

            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    crate::control_raft::ControlResponse::FailoverToken { shard_id: _, epoch } => {
                        // Update local cache to match committed state.
                        let new_map = {
                            let mut map = self.shard_map.write().await;
                            if let Some(assignment) = map.assignments.get_mut(&shard_id) {
                                assignment.owner = new_owner;
                                assignment.epoch = OwnerEpoch::from_value(epoch);
                                assignment.writable = true;
                                map.version += 1;
                            }
                            map.clone()
                        };
                        // Propagate to router so PG-wire queries route to the new owner.
                        if let Some(ref router) = self.router {
                            router.update_shard_map(new_map).await;
                        }
                        Ok(FencingToken::new(shard_id, OwnerEpoch::from_value(epoch)))
                    }
                    crate::control_raft::ControlResponse::Error { message } => {
                        Err(ControlError::RaftError(message))
                    }
                    _ => Err(ControlError::RaftError("unexpected response".into())),
                },
                Err(e) => Err(ControlError::RaftError(format!("client_write: {}", e))),
            }
        } else {
            // Fallback: hand-rolled quorum path.
            if !self.quorum_healthy().await {
                let health = self.node_health.read().await;
                let alive_voters = self
                    .voters
                    .iter()
                    .filter(|v| health.get(v.as_str()).is_some_and(|h| h.alive))
                    .count();
                return Err(ControlError::NoQuorum {
                    voters: alive_voters,
                    required: self.voters.len() / 2 + 1,
                });
            }

            let (new_epoch, new_map) = {
                let mut map = self.shard_map.write().await;
                let new_epoch = {
                    let Some(assignment) = map.assignments.get_mut(&shard_id) else {
                        return Err(ControlError::ShardNotFound(shard_id));
                    };
                    let new_epoch = assignment.epoch.next();
                    assignment.owner = new_owner.clone();
                    assignment.epoch = new_epoch;
                    assignment.writable = true;
                    new_epoch
                };
                map.version += 1;
                tracing::warn!(
                    "Shard {} failed over to {} (epoch {})",
                    shard_id,
                    new_owner,
                    new_epoch.value()
                );
                self.persist_map(&map);
                (new_epoch, map.clone())
            };
            // Propagate to router so PG-wire queries route to the new owner.
            if let Some(ref router) = self.router {
                router.update_shard_map(new_map).await;
            }
            Ok(FencingToken::new(shard_id, new_epoch))
        }
    }

    /// Fail a shard over to one of its *surviving replicas* — the node that
    /// actually holds the data.
    ///
    /// A2-7: When Raft is enabled, submits as a Raft command after filtering
    /// replicas by health. Falls back to hand-rolled quorum when Raft is None.
    ///
    /// This is the correct failover: the old design promoted an arbitrary node
    /// (often the detector itself, `failover_shard(shard, self)`), handing over
    /// an empty shard. Here we promote the first replica that is still alive per
    /// the health map, so the new owner has the quorum-replicated data.
    ///
    /// Returns:
    /// - `Ok(Some(token))` — promoted a live replica; `token` carries the new epoch.
    /// - `Ok(None)` — no surviving replica (RF=1, or all replicas also down):
    ///   the shard is marked non-writable and left ownerless-in-effect, so writes
    ///   fail loudly instead of landing on a node without the data.
    /// - `Err(NoQuorum)` — minority side of a partition; refuse (anti-split-brain).
    pub async fn failover_shard_auto(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<FencingToken>, ControlError> {
        if let Some(ref raft) = *self.raft.read().await {
            // A2-7: Raft path. First filter surviving replicas, then submit.
            if !self.quorum_healthy().await {
                let health = self.node_health.read().await;
                let alive_voters = self
                    .voters
                    .iter()
                    .filter(|v| health.get(v.as_str()).is_some_and(|h| h.alive))
                    .count();
                return Err(ControlError::NoQuorum {
                    voters: alive_voters,
                    required: self.voters.len() / 2 + 1,
                });
            }

            let cmd = crate::control_raft::ControlCommand::FailoverShardAuto {
                shard_id: shard_id as u32,
            };

            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    crate::control_raft::ControlResponse::FailoverPromoted {
                        shard_id: _,
                        epoch,
                    } => {
                        // Update local cache.
                        let new_map = {
                            let mut map = self.shard_map.write().await;
                            if let Some(assignment) = map.assignments.get_mut(&shard_id) {
                                assignment.epoch = OwnerEpoch::from_value(epoch);
                                assignment.writable = true;
                                map.version += 1;
                            }
                            map.clone()
                        };
                        // Propagate to router.
                        if let Some(ref router) = self.router {
                            router.update_shard_map(new_map).await;
                        }
                        Ok(Some(FencingToken::new(
                            shard_id,
                            OwnerEpoch::from_value(epoch),
                        )))
                    }
                    crate::control_raft::ControlResponse::FailoverNoReplica { .. } => {
                        // Update local cache to mark unavailable.
                        let new_map = {
                            let mut map = self.shard_map.write().await;
                            if let Some(assignment) = map.assignments.get_mut(&shard_id) {
                                assignment.writable = false;
                                map.version += 1;
                            }
                            map.clone()
                        };
                        // Propagate to router.
                        if let Some(ref router) = self.router {
                            router.update_shard_map(new_map).await;
                        }
                        Ok(None)
                    }
                    crate::control_raft::ControlResponse::Error { message } => {
                        Err(ControlError::RaftError(message))
                    }
                    _ => Err(ControlError::RaftError("unexpected response".into())),
                },
                Err(e) => Err(ControlError::RaftError(format!("client_write: {}", e))),
            }
        } else {
            // Fallback: hand-rolled quorum path (pre-A2-7).
            if !self.quorum_healthy().await {
                let health = self.node_health.read().await;
                let alive_voters = self
                    .voters
                    .iter()
                    .filter(|v| health.get(v.as_str()).is_some_and(|h| h.alive))
                    .count();
                return Err(ControlError::NoQuorum {
                    voters: alive_voters,
                    required: self.voters.len() / 2 + 1,
                });
            }

            let mut map = self.shard_map.write().await;
            let health = self.node_health.read().await;

            let Some(assignment) = map.assignments.get_mut(&shard_id) else {
                return Err(ControlError::ShardNotFound(shard_id));
            };
            let new_owner = assignment
                .replicas
                .iter()
                .find(|r| health.get(r.as_str()).is_some_and(|h| h.alive))
                .cloned();

            let result = match new_owner {
                Some(owner) => {
                    let new_epoch = assignment.epoch.next();
                    assignment.replicas.retain(|r| r != &owner);
                    assignment.owner = owner.clone();
                    assignment.epoch = new_epoch;
                    assignment.writable = true;
                    map.version += 1;
                    tracing::warn!(
                        "Shard {shard_id} failed over to replica {owner} (epoch {})",
                        new_epoch.value()
                    );
                    Ok(Some(FencingToken::new(shard_id, new_epoch)))
                }
                None => {
                    assignment.writable = false;
                    map.version += 1;
                    tracing::error!(
                        "Shard {shard_id} has no surviving replica; marked non-writable (data unavailable until a replica returns)"
                    );
                    Ok(None)
                }
            };
            self.persist_map(&map);
            let new_map = map.clone();
            // Release the map write lock before touching the router.
            drop(health);
            drop(map);
            // Propagate to router so PG-wire queries route to the new owner.
            if let Some(ref router) = self.router {
                router.update_shard_map(new_map).await;
            }
            result
        }
    }

    /// Mark a node as alive (records/refreshes a heartbeat).
    ///
    /// Voter liveness drives `quorum_healthy`, which now gates failover and
    /// ShardMap proposals; callers must record voter heartbeats here so the
    /// majority side is recognized as healthy.
    pub async fn mark_node_alive(&self, node_id: &str) {
        let mut health = self.node_health.write().await;
        let entry = health.entry(node_id.to_string()).or_insert(NodeHealth {
            alive: true,
            last_heartbeat_ms: 0,
            failure_count: 0,
            role: NodeRole::Voter,
        });
        entry.alive = true;
        entry.last_heartbeat_ms = current_millis_or_zero();
    }

    /// Assign replicas to a shard.
    pub async fn assign_replicas(&self, shard_id: ShardId, owner: String, followers: Vec<String>) {
        let replicas = ReplicaSet::new(shard_id, owner, followers);
        self.replica_sets.write().await.insert(shard_id, replicas);
    }

    /// Mark a node as failed (triggers failover for its shards).
    pub async fn mark_node_failed(&self, node_id: &str) -> Vec<ShardId> {
        {
            let mut health = self.node_health.write().await;
            if let Some(h) = health.get_mut(node_id) {
                h.alive = false;
                h.failure_count += 1;
            } else {
                health.insert(
                    node_id.to_string(),
                    NodeHealth {
                        alive: false,
                        last_heartbeat_ms: 0,
                        failure_count: 1,
                        role: NodeRole::Compute,
                    },
                );
            }
        }
        // Find shards owned by this node
        let map = self.shard_map.read().await;
        let mut shard_ids = Vec::new();
        for (s, a) in &map.assignments {
            if a.owner == node_id {
                shard_ids.push(*s);
            }
        }
        shard_ids
    }

    /// Get current quorum status.
    pub async fn quorum_healthy(&self) -> bool {
        let voters = self.voters.len();
        if voters == 0 {
            return true;
        }
        let required = voters / 2 + 1;
        let health = self.node_health.read().await;
        let alive_voters = self
            .voters
            .iter()
            .filter(|v| health.get(v.as_str()).is_some_and(|h| h.alive))
            .count();
        alive_voters >= required
    }

    /// The Raft node id this control plane currently believes is leader, if any.
    ///
    /// Returns `None` when Raft isn't assembled (fallback mode) or the node is in
    /// an election / has no known leader. Reads openraft metrics, so it reflects
    /// real consensus state — a minority-partitioned node reports `None` once it
    /// steps down, which is what the split-brain test asserts.
    pub async fn raft_leader(&self) -> Option<crate::control_raft::ControlNodeId> {
        let guard = self.raft.read().await;
        let raft = guard.as_ref()?;
        raft.metrics().borrow().current_leader
    }

    /// Whether this node currently believes itself to be the Raft leader.
    ///
    /// `false` in fallback mode (no Raft assembled). Used by tests to assert that
    /// exactly one leader exists cluster-wide and that a minority partition has
    /// none.
    pub async fn is_raft_leader(&self) -> bool {
        let guard = self.raft.read().await;
        let Some(raft) = guard.as_ref() else {
            return false;
        };
        let m = raft.metrics().borrow().clone();
        m.current_leader == Some(m.id)
    }

    /// Get the current shard map (read-only snapshot).
    pub async fn get_shard_map(&self) -> ShardMap {
        self.shard_map.read().await.clone()
    }

    /// Share the live shard-map handle. Background readers (e.g. anti-entropy)
    /// hold this to observe ownership/replica changes as the control plane
    /// commits them, rather than polling a point-in-time `get_shard_map` copy.
    pub fn shard_map_handle(&self) -> Arc<RwLock<ShardMap>> {
        self.shard_map.clone()
    }

    /// Current owner of a shard, if the shard is assigned. Used post-failover to
    /// check whether this node was the one promoted (and thus should catch up).
    pub async fn owner_of_shard(&self, shard_id: ShardId) -> Option<String> {
        self.shard_map
            .read()
            .await
            .assignments
            .get(&shard_id)
            .map(|a| a.owner.clone())
    }

    /// Get the local node ID from the shard map.
    pub async fn local_node(&self) -> String {
        self.shard_map.read().await.local_node.clone()
    }

    /// Rebalance shards across alive nodes (round-robin).
    pub async fn rebalance_shards(&self, alive_nodes: &[String]) -> Option<ShardMap> {
        if alive_nodes.is_empty() {
            return None;
        }
        let mut map = self.shard_map.write().await;
        let local_node = map.local_node.clone();
        let total_shards = map.total_shards;

        for (shard_id, assignment) in map.assignments.iter_mut() {
            let new_owner = &alive_nodes[shard_id % alive_nodes.len()];
            if &assignment.owner != new_owner {
                assignment.owner = new_owner.clone();
                assignment.epoch = assignment.epoch.next();
            }
        }
        map.version += 1;
        map.local_node = local_node;
        map.total_shards = total_shards;
        self.persist_map(&map);
        Some(map.clone())
    }

    /// Rebalance for a new membership set, RF-aware and deterministic.
    ///
    /// Recomputes owner + followers for every shard exactly as
    /// `ShardMap::new_distributed_rf` would for `members` (sorted, round-robin
    /// on the ring), so every node derives the identical assignment. Where a
    /// shard's owner changes, its epoch is bumped (fences the old owner). Used
    /// on scale-out (a node joins) and scale-in (a node leaves) — a superset of
    /// the alive-only `rebalance_shards`, which ignores replicas.
    ///
    /// Returns the diff of shards whose owner changed as `(shard, old, new)`,
    /// plus the new map, so the caller can migrate data for reassigned shards
    /// before publishing. Returns `None` if `members` is empty or nothing moved.
    pub async fn rebalance_for_members(
        &self,
        members: &[String],
        replication_factor: usize,
    ) -> Option<(ShardMap, Vec<(ShardId, String, String)>)> {
        if members.is_empty() {
            return None;
        }
        let mut map = self.shard_map.write().await;
        let local_node = map.local_node.clone();
        let total_shards = map.total_shards;

        // Target assignment: identical to what every node computes from the same
        // membership, so the cluster converges without coordination.
        let target = ShardMap::new_distributed_rf(
            total_shards,
            members,
            local_node.clone(),
            replication_factor,
        );

        let mut moved: Vec<(ShardId, String, String)> = Vec::new();
        for (shard_id, target_asg) in &target.assignments {
            let entry = map
                .assignments
                .entry(*shard_id)
                .or_insert_with(|| target_asg.clone());
            let old_owner = entry.owner.clone();
            // Always adopt the target replica set; bump epoch only on owner change.
            entry.replicas = target_asg.replicas.clone();
            entry.writable = true;
            if old_owner != target_asg.owner {
                entry.owner = target_asg.owner.clone();
                entry.epoch = entry.epoch.next();
                moved.push((*shard_id, old_owner, target_asg.owner.clone()));
            }
        }

        if moved.is_empty() {
            return None;
        }
        map.version += 1;
        map.local_node = local_node;
        map.total_shards = total_shards;
        self.persist_map(&map);
        Some((map.clone(), moved))
    }

    /// Add a node to the cluster and trigger rebalance.
    ///
    /// Returns the list of shards that moved as `(shard_id, old_owner, new_owner)`.
    pub async fn add_node(
        &self,
        node_id: String,
    ) -> Result<Vec<(ShardId, String, String)>, ControlError> {
        if !self.quorum_healthy().await {
            let health = self.node_health.read().await;
            let alive_voters = self
                .voters
                .iter()
                .filter(|v| health.get(v.as_str()).is_some_and(|h| h.alive))
                .count();
            return Err(ControlError::NoQuorum {
                voters: alive_voters,
                required: self.voters.len() / 2 + 1,
            });
        }

        let mut members = self.members.write().await;
        if members.contains(&node_id) {
            return Err(ControlError::NodeAlreadyExists(node_id));
        }
        members.push(node_id.clone());
        members.sort();

        // Rebalance with the new member list
        drop(members);
        let result = self
            .rebalance_for_members(&self.members.read().await, self.replication_factor)
            .await;

        match result {
            Some((_new_map, moved)) => {
                tracing::info!("Node {} added, {} shards moved", node_id, moved.len());
                Ok(moved)
            }
            None => {
                tracing::info!("Node {} added, no shards moved", node_id);
                Ok(vec![])
            }
        }
    }

    /// Remove a node from the cluster and trigger rebalance.
    ///
    /// Returns the list of shards that moved as `(shard_id, old_owner, new_owner)`.
    pub async fn remove_node(
        &self,
        node_id: &str,
    ) -> Result<Vec<(ShardId, String, String)>, ControlError> {
        if !self.quorum_healthy().await {
            let health = self.node_health.read().await;
            let alive_voters = self
                .voters
                .iter()
                .filter(|v| health.get(v.as_str()).is_some_and(|h| h.alive))
                .count();
            return Err(ControlError::NoQuorum {
                voters: alive_voters,
                required: self.voters.len() / 2 + 1,
            });
        }

        let mut members = self.members.write().await;
        if !members.contains(&node_id.to_string()) {
            return Err(ControlError::NodeNotFound(node_id.to_string()));
        }
        members.retain(|n| n != node_id);

        if members.is_empty() {
            return Err(ControlError::CannotRemoveLastNode);
        }

        // Rebalance with the remaining members
        drop(members);
        let result = self
            .rebalance_for_members(&self.members.read().await, self.replication_factor)
            .await;

        match result {
            Some((_new_map, moved)) => {
                tracing::info!("Node {} removed, {} shards moved", node_id, moved.len());
                Ok(moved)
            }
            None => {
                tracing::warn!("Node {} removed, but no shards moved", node_id);
                Ok(vec![])
            }
        }
    }

    /// Get current cluster members.
    pub async fn get_members(&self) -> Vec<String> {
        self.members.read().await.clone()
    }
}

/// Current wall-clock time in Unix millis, or 0 if the clock is before the
/// epoch. Used only for heartbeat freshness bookkeeping.
fn current_millis_or_zero() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("no quorum: {voters} voters, need {required}")]
    NoQuorum { voters: usize, required: usize },
    #[error("shard {0} not found")]
    ShardNotFound(ShardId),
    #[error("proposal version {proposed} is not newer than current {current}")]
    StaleProposal { proposed: u64, current: u64 },
    #[error("node {0} not found")]
    NodeNotFound(String),
    #[error("node {0} already exists")]
    NodeAlreadyExists(String),
    #[error("cannot remove the last node from cluster")]
    CannotRemoveLastNode,
    #[error("raft error: {0}")]
    RaftError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum_health_single_node() {
        // Single node mode: no voters needed
        let cp = ControlPlane::new(4, vec![]);
        // No voters = always healthy
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert!(cp.quorum_healthy().await);
        });
    }

    #[test]
    fn test_failover_increments_epoch() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let cp = ControlPlane::new(4, vec!["local-node".into()]);
            // Failover requires a healthy voter quorum. Mark the sole voter alive.
            cp.mark_node_alive("local-node").await;
            let token = cp.failover_shard(0, "new-node".into()).await.unwrap();
            assert_eq!(token.epoch.value(), 2); // Epoch incremented from 1
            assert_eq!(token.shard_id, 0);
        });
    }

    #[test]
    fn test_failover_refused_without_quorum() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Two other voters, neither reachable → local node is a minority and
            // must NOT be able to seize shards (split-brain prevention).
            let cp = ControlPlane::new(4, vec!["voter-a".into(), "voter-b".into()]);
            let result = cp.failover_shard(0, "local".into()).await;
            assert!(matches!(result, Err(ControlError::NoQuorum { .. })));
        });
    }

    /// Build a ControlPlane over an RF=3 distributed map for auto-failover tests.
    ///
    /// NOTE: `quorum_healthy` is driven by the *voter* set, while failover target
    /// selection is driven by the shard's *data replicas*. These are separate
    /// concerns, so the tests use a dedicated always-alive voter set (kept as a
    /// majority) distinct from the data nodes being killed — otherwise killing
    /// data nodes would also break voter quorum and (correctly) trip the
    /// anti-split-brain NoQuorum guard before we reach the promotion logic.
    async fn cp_rf3() -> ControlPlane {
        let data_nodes = vec![
            "node-0".to_string(),
            "node-1".to_string(),
            "node-2".to_string(),
        ];
        let voters = vec!["v0".to_string(), "v1".to_string(), "v2".to_string()];
        let map = ShardMap::new_distributed_rf(3, &data_nodes, "node-0".into(), 3);
        let cp = ControlPlane::with_shard_map(map, voters.clone());
        // Keep a healthy voter majority alive throughout.
        for v in &voters {
            cp.mark_node_alive(v).await;
        }
        // Data replicas start alive so failover can consider them.
        for n in &data_nodes {
            cp.mark_node_alive(n).await;
        }
        cp
    }

    #[tokio::test]
    async fn test_failover_auto_promotes_live_replica() {
        let cp = cp_rf3().await;
        // shard 0: owner node-0, replicas [node-1, node-2].
        let asg = cp.get_shard_map().await;
        let owner = asg.get(0).unwrap().owner.clone();
        assert_eq!(owner, "node-0");

        // node-0 fails → auto-failover must promote a LIVE replica (node-1/2).
        cp.mark_node_failed("node-0").await;
        let token = cp.failover_shard_auto(0).await.unwrap();
        assert!(token.is_some(), "a live replica must be promoted");

        let after = cp.get_shard_map().await;
        let new_owner = &after.get(0).unwrap().owner;
        assert!(
            new_owner == "node-1" || new_owner == "node-2",
            "new owner must be a surviving replica, got {new_owner}"
        );
        assert!(
            after.get(0).unwrap().writable,
            "shard stays writable after failover"
        );
        // Epoch bumped for fencing.
        assert_eq!(after.get(0).unwrap().epoch.value(), 2);
    }

    #[tokio::test]
    async fn test_failover_auto_no_replica_marks_non_writable() {
        // RF=1: no followers. Owner failure leaves nothing to promote.
        // Dedicated always-alive voters keep quorum healthy (see cp_rf3 note),
        // so we reach the promotion logic rather than tripping NoQuorum.
        let data_nodes = vec!["node-0".to_string(), "node-1".to_string()];
        let voters = vec!["v0".to_string(), "v1".to_string(), "v2".to_string()];
        let map = ShardMap::new_distributed_rf(2, &data_nodes, "node-0".into(), 1);
        let cp = ControlPlane::with_shard_map(map, voters.clone());
        for v in &voters {
            cp.mark_node_alive(v).await;
        }
        for n in &data_nodes {
            cp.mark_node_alive(n).await;
        }
        // pick a shard owned by node-0
        let m = cp.get_shard_map().await;
        let shard = (0..2)
            .find(|s| m.get(*s).unwrap().owner == "node-0")
            .unwrap();

        cp.mark_node_failed("node-0").await;
        let result = cp.failover_shard_auto(shard).await.unwrap();
        assert!(result.is_none(), "no replica → nothing to promote");
        assert!(
            !cp.get_shard_map().await.get(shard).unwrap().writable,
            "shard with no surviving replica must be non-writable, not handed to an empty owner"
        );
    }

    #[tokio::test]
    async fn test_failover_auto_skips_dead_replica() {
        let cp = cp_rf3().await;
        // Both owner and one replica dead; only one replica survives.
        let asg = cp.get_shard_map().await;
        let replicas = asg.get(0).unwrap().replicas.clone(); // [node-1, node-2] (some order)
        cp.mark_node_failed("node-0").await;
        cp.mark_node_failed(&replicas[0]).await;

        let token = cp.failover_shard_auto(0).await.unwrap();
        assert!(token.is_some());
        let new_owner = cp.get_shard_map().await.get(0).unwrap().owner.clone();
        assert_eq!(
            new_owner, replicas[1],
            "must promote the one SURVIVING replica"
        );
    }

    #[tokio::test]
    async fn test_rebalance_for_members_scale_out_moves_shards() {
        // Start with a 2-node cluster, RF=1: node-0 owns even shards, node-1 odd.
        let two = vec!["node-0".to_string(), "node-1".to_string()];
        let map = ShardMap::new_distributed_rf(4, &two, "node-0".into(), 1);
        let cp = ControlPlane::with_shard_map(map, two.clone());

        // Scale out to 3 nodes. Some shards must move to node-2.
        let three = vec![
            "node-0".to_string(),
            "node-1".to_string(),
            "node-2".to_string(),
        ];
        let (new_map, moved) = cp
            .rebalance_for_members(&three, 1)
            .await
            .expect("rebalance must produce a diff on scale-out");

        // At least one shard moved, and every move's new owner matches the
        // deterministic target for the 3-node set.
        assert!(!moved.is_empty(), "scale-out must move some shards");
        let target = ShardMap::new_distributed_rf(4, &three, "node-0".into(), 1);
        for (shard, _old, new) in &moved {
            assert_eq!(
                new,
                &target.get(*shard).unwrap().owner,
                "moved shard {shard} must match the deterministic target owner"
            );
            // Epoch bumped on the moved shard (fences the old owner).
            assert!(new_map.get(*shard).unwrap().epoch.value() >= 2);
        }
        // node-2 must now own at least one shard.
        assert!(
            new_map.assignments.values().any(|a| a.owner == "node-2"),
            "node-2 must own shards after scale-out"
        );
    }

    #[tokio::test]
    async fn test_rebalance_for_members_noop_when_unchanged() {
        let three = vec![
            "node-0".to_string(),
            "node-1".to_string(),
            "node-2".to_string(),
        ];
        let map = ShardMap::new_distributed_rf(6, &three, "node-0".into(), 2);
        let cp = ControlPlane::with_shard_map(map, three.clone());
        // Rebalancing to the SAME membership must be a no-op (nothing moved).
        assert!(
            cp.rebalance_for_members(&three, 2).await.is_none(),
            "rebalance to identical membership must not move anything"
        );
    }

    #[tokio::test]
    async fn test_add_node_triggers_rebalance() {
        let two = vec!["node-0".to_string(), "node-1".to_string()];
        let voters = vec!["v0".to_string(), "v1".to_string(), "v2".to_string()];
        let map = ShardMap::new_distributed_rf(4, &two, "node-0".into(), 1);
        let cp = ControlPlane::with_members(map, voters.clone(), two.clone(), 1);

        // Mark voters alive for quorum
        for v in &voters {
            cp.mark_node_alive(v).await;
        }

        // Add node-2
        let moved = cp.add_node("node-2".to_string()).await.unwrap();

        // Should rebalance some shards to node-2
        assert!(!moved.is_empty(), "adding a node should trigger rebalance");

        let members = cp.get_members().await;
        assert_eq!(members.len(), 3);
        assert!(members.contains(&"node-2".to_string()));

        // Verify node-2 owns some shards now
        let shard_map = cp.get_shard_map().await;
        let node2_shards = shard_map
            .assignments
            .values()
            .filter(|a| a.owner == "node-2")
            .count();
        assert!(node2_shards > 0, "node-2 should own at least one shard");
    }

    #[tokio::test]
    async fn test_remove_node_redistributes_shards() {
        let three = vec![
            "node-0".to_string(),
            "node-1".to_string(),
            "node-2".to_string(),
        ];
        let voters = vec!["v0".to_string(), "v1".to_string(), "v2".to_string()];
        let map = ShardMap::new_distributed_rf(6, &three, "node-0".into(), 1);
        let cp = ControlPlane::with_members(map, voters.clone(), three.clone(), 1);

        // Mark voters alive for quorum
        for v in &voters {
            cp.mark_node_alive(v).await;
        }

        // Remove node-2
        let _moved = cp.remove_node("node-2").await.unwrap();

        // Should redistribute node-2's shards
        let members = cp.get_members().await;
        assert_eq!(members.len(), 2);
        assert!(!members.contains(&"node-2".to_string()));

        // Verify no shard is owned by node-2
        let shard_map = cp.get_shard_map().await;
        for assignment in shard_map.assignments.values() {
            assert_ne!(
                assignment.owner, "node-2",
                "node-2 should not own any shards"
            );
        }
    }

    #[tokio::test]
    async fn test_add_node_duplicate_fails() {
        let two = vec!["node-0".to_string(), "node-1".to_string()];
        let voters = vec!["v0".to_string(), "v1".to_string()];
        let map = ShardMap::new_distributed_rf(4, &two, "node-0".into(), 1);
        let cp = ControlPlane::with_members(map, voters.clone(), two.clone(), 1);

        for v in &voters {
            cp.mark_node_alive(v).await;
        }

        // Try to add existing node
        let result = cp.add_node("node-0".to_string()).await;
        assert!(matches!(result, Err(ControlError::NodeAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_remove_last_node_fails() {
        let one = vec!["node-0".to_string()];
        let voters = vec!["v0".to_string()];
        let map = ShardMap::new_distributed_rf(2, &one, "node-0".into(), 1);
        let cp = ControlPlane::with_members(map, voters.clone(), one.clone(), 1);

        cp.mark_node_alive("v0").await;

        // Try to remove the last node
        let result = cp.remove_node("node-0").await;
        assert!(matches!(result, Err(ControlError::CannotRemoveLastNode)));
    }

    #[tokio::test]
    async fn test_add_remove_without_quorum_fails() {
        let two = vec!["node-0".to_string(), "node-1".to_string()];
        let voters = vec!["v0".to_string(), "v1".to_string(), "v2".to_string()];
        let map = ShardMap::new_distributed_rf(4, &two, "node-0".into(), 1);
        let cp = ControlPlane::with_members(map, voters.clone(), two.clone(), 1);

        // Don't mark any voters alive - no quorum

        let add_result = cp.add_node("node-2".to_string()).await;
        assert!(matches!(add_result, Err(ControlError::NoQuorum { .. })));

        let remove_result = cp.remove_node("node-1").await;
        assert!(matches!(remove_result, Err(ControlError::NoQuorum { .. })));
    }
}
