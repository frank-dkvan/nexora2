# Phase 5.4 Implementation Roadmap

## Current Status: Test Framework Created ✅

Phase 5.4 test framework has been created with 7 test cases covering:
1. 3-node cluster startup
2. DDL execution and catalog sync
3. Leader failover
4. Query execution during failover
5. Cluster status endpoint
6. DDL performance
7. Query performance

All tests are marked `#[ignore]` pending implementation of distributed library cluster infrastructure.

## Implementation Blockers

### Blocker 1: Distributed Library Cluster Not Created in risingwave_init.rs

**Current State**:
```rust
// crates/nexora-app/src/risingwave_init.rs:233
#[cfg(feature = "library")]
async fn init_distributed_node(
    cli: &crate::Cli,
    config: Option<&crate::config::EventStreamingConfig>,
) -> Result<Option<Arc<nexora_risingwave::EventStreamingModule>>> {
    // TODO: Implement distributed library mode
    anyhow::bail!("Distributed library mode not yet implemented. Use --features embedded for multi-node clusters.")
}
```

**What's Needed**:
1. Create `MetaCluster` with Raft election
2. Create `FrontendPool` for query routing
3. Create `ComputeCluster` for stream processing
4. Return both `EventStreamingModule` and cluster components

**Implementation**:
```rust
#[cfg(feature = "library")]
async fn init_distributed_node(
    cli: &crate::Cli,
    config: Option<&crate::config::EventStreamingConfig>,
) -> Result<(
    Option<Arc<EventStreamingModule>>,
    Option<Arc<(Arc<MetaCluster>, Arc<FrontendPool>, Arc<ComputeCluster>)>>,
)> {
    let dist_config = config
        .and_then(|c| c.distributed.as_ref())
        .ok_or_else(|| anyhow::anyhow!("Distributed config required"))?;

    // 1. Create Raft consensus client
    let raft_config = nexora_consensus::RaftConfig {
        node_id: dist_config.raft_node_id,
        peers: dist_config.meta.peers.iter().map(|p| {
            nexora_consensus::PeerConfig {
                node_id: p.node_id,
                addr: p.addr.clone(),
            }
        }).collect(),
        data_dir: dist_config.consensus.data_dir.clone().into(),
        heartbeat_interval: Duration::from_secs(dist_config.consensus.heartbeat_interval_secs),
        election_timeout: Duration::from_secs(dist_config.consensus.election_timeout_secs),
    };
    
    let consensus_client = nexora_consensus::RaftConsensusClient::new(raft_config).await?;
    let election_client = nexora_consensus::RaftElectionClient::new(consensus_client.clone());
    let adapter = Arc::new(RaftElectionAdapter::new(election_client));

    // 2. Create Meta cluster with election
    let meta_cluster = nexora_risingwave::meta_cluster::MetaCluster::with_election(
        dist_config.raft_node_id as u32,
        dist_config.meta.listen_addr.clone(),
        adapter.clone(),
    ).await?;

    // 3. Create Frontend pool
    let frontend_pool = nexora_risingwave::frontend_pool::FrontendPool::new(
        vec![dist_config.frontend_addr.clone()],
    ).await?;

    // 4. Create Compute cluster
    let compute_cluster = nexora_risingwave::compute_cluster::ComputeCluster::new(
        dist_config.compute_nodes.clone(),
    ).await?;

    // 5. Create EventStreamingModule
    let module = EventStreamingModule::with_distributed_cluster(
        meta_cluster.clone(),
        frontend_pool.clone(),
        compute_cluster.clone(),
    )?;

    let cluster_tuple = Arc::new((meta_cluster, frontend_pool, compute_cluster));

    Ok((Some(Arc::new(module)), Some(cluster_tuple)))
}
```

**Estimated Effort**: 4-6 hours
**Dependencies**: 
- `nexora-risingwave::meta_cluster::MetaCluster`
- `nexora-risingwave::frontend_pool::FrontendPool`
- `nexora-risingwave::compute_cluster::ComputeCluster`

---

### Blocker 2: AppState Missing distributed_library_cluster Field

**Current State**:
```rust
// crates/nexora-app/src/main.rs
pub struct AppState {
    pub graph_service: Arc<GraphService>,
    pub event_store: Option<Arc<IcebergEventLogStore>>,
    pub event_streaming: Option<Arc<EventStreamingModule>>,
    // Missing: distributed_library_cluster
}
```

**What's Needed**:
Add field to store distributed cluster components for status endpoint.

**Implementation**:
```rust
pub struct AppState {
    pub graph_service: Arc<GraphService>,
    pub event_store: Option<Arc<IcebergEventLogStore>>,
    pub event_streaming: Option<Arc<EventStreamingModule>>,
    
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    pub distributed_library_cluster: Option<Arc<(
        Arc<nexora_risingwave::meta_cluster::MetaCluster>,
        Arc<nexora_risingwave::frontend_pool::FrontendPool>,
        Arc<nexora_risingwave::compute_cluster::ComputeCluster>,
    )>>,
}
```

**Changes in main()**:
```rust
// In main initialization
#[cfg(all(feature = "event-streaming", feature = "library"))]
let (event_streaming_module, distributed_library_cluster) = 
    risingwave_init::init_event_streaming(&cli, &config).await?;

#[cfg(not(all(feature = "event-streaming", feature = "library")))]
let (event_streaming_module, distributed_library_cluster) = 
    (risingwave_init::init_event_streaming(&cli, &config).await?, None);

let state = AppState {
    graph_service,
    event_store,
    event_streaming: event_streaming_module,
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    distributed_library_cluster,
};
```

**Estimated Effort**: 1-2 hours

---

### Blocker 3: nexora-risingwave Missing Cluster Types

**Current State**:
`nexora-risingwave` has `EventStreamingModule` but not separate cluster types.

**What's Needed**:
Three new public types in `nexora-risingwave`:

1. **MetaCluster**:
```rust
// crates/nexora-risingwave/src/meta_cluster.rs
pub struct MetaCluster {
    node_id: u32,
    election_client: Option<Arc<dyn ElectionClientTrait>>,
}

impl MetaCluster {
    pub async fn with_election(
        node_id: u32,
        listen_addr: String,
        election_client: Arc<dyn ElectionClientTrait>,
    ) -> Result<Arc<Self>>;
    
    pub async fn get_state(&self) -> MetaState;
    pub async fn is_leader(&self) -> bool;
}

pub struct MetaState {
    pub leader_id: Option<u64>,
    pub raft_state: RaftState,
    pub peer_count: usize,
}
```

2. **FrontendPool**:
```rust
// crates/nexora-risingwave/src/frontend_pool.rs
pub struct FrontendPool {
    connections: Vec<PostgresConnection>,
}

impl FrontendPool {
    pub async fn new(addrs: Vec<String>) -> Result<Arc<Self>>;
    pub async fn health_check(&self) -> FrontendHealth;
}

pub struct FrontendHealth {
    pub active_count: usize,
    pub total_count: usize,
    pub is_healthy: bool,
}
```

3. **ComputeCluster**:
```rust
// crates/nexora-risingwave/src/compute_cluster.rs
pub struct ComputeCluster {
    nodes: Vec<ComputeNode>,
}

impl ComputeCluster {
    pub async fn new(node_configs: Vec<ComputeNodeConfig>) -> Result<Arc<Self>>;
    pub async fn health_check(&self) -> ComputeHealth;
}

pub struct ComputeHealth {
    pub active_count: usize,
    pub total_count: usize,
    pub is_healthy: bool,
    pub total_parallelism: usize,
}
```

**Estimated Effort**: 8-12 hours (requires deep RisingWave integration)

---

### Blocker 4: Test Harness for Multi-Instance Cluster

**Current State**:
Test helpers are stubs.

**What's Needed**:
Implement helper functions to spawn and manage 3-node test cluster.

**Implementation Options**:

**Option A: In-Process Axum Servers** (Recommended for Phase 5.4)
```rust
async fn start_test_cluster(node_count: usize) -> Vec<TestClusterNode> {
    let mut nodes = vec![];
    
    for i in 0..node_count {
        let node_id = (i + 1) as u64;
        let http_port = 8080 + i as u16;
        let raft_port = 5690 + i as u16;
        
        // Create config
        let config = create_test_config(node_id, http_port, raft_port, &all_peers);
        
        // Spawn server in background task
        let handle = tokio::spawn(async move {
            // Start nexora-app with config
            nexora_app::run_with_config(config).await
        });
        
        nodes.push(TestClusterNode {
            node_id,
            http_addr: format!("http://127.0.0.1:{}", http_port),
            raft_addr: format!("127.0.0.1:{}", raft_port),
            handle,
        });
    }
    
    // Wait for all nodes to be healthy
    for node in &nodes {
        wait_for_health(&node.http_addr).await;
    }
    
    nodes
}
```

**Option B: Subprocess Spawning**
```rust
async fn start_test_cluster(node_count: usize) -> Vec<TestClusterNode> {
    let mut nodes = vec![];
    
    for i in 0..node_count {
        // Write temporary config file
        let config_path = format!("/tmp/nexora-test-node{}.toml", i + 1);
        std::fs::write(&config_path, generate_config(i))?;
        
        // Spawn subprocess
        let mut child = Command::new(cargo_bin("nexora"))
            .arg("--config").arg(&config_path)
            .arg("--enable-event-streaming")
            .arg("--event-streaming-mode=distributed")
            .spawn()?;
        
        nodes.push(TestClusterNode {
            node_id: (i + 1) as u64,
            process: child,
            ...
        });
    }
    
    nodes
}
```

**Recommendation**: Start with **Option A** for faster iteration. Switch to Option B for true isolation testing later.

**Estimated Effort**: 4-6 hours

---

## Implementation Timeline

| Day | Tasks | Deliverables |
|-----|-------|--------------|
| 1 | Blocker 3: Create MetaCluster, FrontendPool, ComputeCluster in nexora-risingwave | 3 new modules, ~500 LOC |
| 2 | Blocker 1: Implement init_distributed_node() | Functional distributed init |
| 2 | Blocker 2: Add AppState field and wire initialization | Cluster status endpoint working |
| 3 | Blocker 4: Implement test harness (Option A) | start_test_cluster() functional |
| 3 | Implement test_three_node_cluster_startup | First E2E test passing |
| 4 | Implement test_ddl_execution_and_sync | Catalog sync verified |
| 4 | Implement test_cluster_status_endpoint | Status endpoint verified |
| 5 | Implement test_leader_failover | Failover working |
| 5 | Implement test_query_during_failover | HA validated |
| 6 | Implement test_ddl_performance + test_query_performance | Performance baselines |
| 6 | Documentation and cleanup | PHASE5.4_COMPLETE.md |

**Total Estimated Effort**: 6 days (48 hours)

---

## Alternative: Fast-Track Path for Phase 5.4

If full distributed library implementation is too complex, we can take a **fast-track approach**:

### Fast-Track: Stub Distributed Cluster with Mock Responses

**Goal**: Get Phase 5.4 tests passing with mock/stub implementations, defer real distributed cluster to Phase 6.

**Changes**:
1. Create stub `MetaCluster`, `FrontendPool`, `ComputeCluster` that return hardcoded health
2. `init_distributed_node()` creates stubs instead of real Raft cluster
3. Tests validate API shape but not actual distributed behavior
4. Mark tests as "validation-only, not integration"

**Benefits**:
- ✅ Unblocks Phase 5.4 completion (2 days vs 6 days)
- ✅ Validates API surface and test harness
- ✅ Provides foundation for Phase 6 real implementation

**Drawbacks**:
- ❌ Not true E2E testing
- ❌ Doesn't validate Raft HA
- ❌ Needs rework in Phase 6

**Recommendation**: 
- If timeline is critical → Fast-track
- If quality is critical → Full implementation

---

## Next Actions

**Decision Point**: Which path for Phase 5.4?

1. **Full Implementation** (6 days): Real distributed library cluster, true E2E testing
2. **Fast-Track** (2 days): Stub implementations, API validation only

Please advise on which path to take, and I'll proceed accordingly.

---

**Last Updated**: 2026-08-02  
**Document Version**: 1.0  
**Status**: Awaiting Direction
