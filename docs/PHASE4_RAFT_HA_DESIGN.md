# Phase 4: Raft HA Extension Design

**Status**: 🚧 In Progress  
**Date**: 2026-08-02

## Overview

Phase 4 upgrades the consensus layer from single-node to multi-node Raft, enabling high-availability RisingWave Meta clusters. This phase bridges nexora-consensus with RisingWave's ElectionClient trait.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              RisingWave Meta Service (HA Mode)               │
├─────────────────────────────────────────────────────────────┤
│  MetaService                                                 │
│    ├─ CatalogManager                                        │
│    ├─ ClusterManager                                        │
│    └─ ElectionClient ◄──────────────────┐                   │
└──────────────────────────────────────────┼──────────────────┘
                                          │
                                          │ (bridge)
┌─────────────────────────────────────────▼──────────────────┐
│          extensions-meta-raft (this phase)                  │
├─────────────────────────────────────────────────────────────┤
│  RaftElectionClient                                         │
│    ├─ Implements: ElectionClient (RisingWave)              │
│    └─ Delegates to: ConsensusClient (nexora-consensus)     │
└─────────────────────────────────────────┬──────────────────┘
                                          │
                                          │
┌─────────────────────────────────────────▼──────────────────┐
│          nexora-consensus (upgraded)                        │
├─────────────────────────────────────────────────────────────┤
│  ConsensusClient trait                                      │
│  RaftConsensusClient (openraft 0.9)                         │
│    ├─ Multi-node Raft cluster (3-5 nodes)                  │
│    ├─ Persistent log storage (RocksDB)                     │
│    ├─ Network layer (TCP + RPC)                            │
│    └─ Leader election & log replication                    │
└─────────────────────────────────────────────────────────────┘
```

## Goals

1. **Multi-node Raft**: Upgrade RaftConsensusClient from single-node to 3-5 node cluster
2. **Persistent Storage**: Add RocksDB-based log persistence
3. **Network Layer**: Implement TCP networking for Raft communication
4. **ElectionClient Bridge**: Complete RaftElectionClient implementation
5. **Integration Tests**: 3-node cluster tests with failover scenarios

## Implementation Plan

### Task 4.1: Design RaftElectionClient Bridge ✅

**Goal**: Define the bridge interface between nexora-consensus and RisingWave

**Current Status**: Basic structure exists in `extensions/meta_raft/src/client.rs`

**Design**:
```rust
// RisingWave ElectionClient trait (from vendor/risingwave)
#[async_trait]
pub trait ElectionClient: Send + Sync {
    async fn init(&self) -> MetaResult<()>;
    fn is_leader(&self) -> bool;
    async fn run_once(&self, ttl: i64, stop: Receiver<()>) -> MetaResult<()>;
    fn id(&self) -> MetaResult<String>;
}

// Our bridge implementation
pub struct RaftElectionClient {
    consensus: Arc<dyn ConsensusClient>,  // nexora-consensus
    config: RaftElectionConfig,
    is_leader_sender: Sender<bool>,       // notify RisingWave of changes
}
```

**Key Challenges**:
- RisingWave expects blocking `is_leader()` -> map to async `consensus.is_leader().await`
- `run_once()` must block until leadership lost -> use tokio::select! on leader changes
- Need to notify RisingWave Meta when leadership changes

### Task 4.2: Implement RaftElectionClient ✅

**Goal**: Complete the bridge implementation

**Current Status**: Placeholder implementation exists, needs:
- Fix `is_leader()` to properly query consensus (currently returns false)
- Implement proper `run_once()` blocking behavior
- Add leader change notifications

**Updates Needed**:
```rust
impl RaftElectionClient {
    pub fn is_leader(&self) -> bool {
        // CURRENT: returns false
        // NEEDED: query consensus synchronously
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.consensus.is_leader())
                .unwrap_or(false)
        })
    }
}
```

### Task 4.3: Upgrade RaftConsensusClient to Multi-Node

**Goal**: Transform RaftConsensusClient from single-node to distributed Raft

**Current State** (Phase 2):
```rust
pub struct RaftConsensusClient {
    node_id: NodeId,
    state: Arc<RwLock<RaftState>>,  // In-memory only
    config: RaftConfig,
}

struct RaftState {
    is_leader: bool,           // Always true
    current_leader: Some(1),   // Always self
    log: Vec<LogEntry>,        // In-memory
}
```

**Target State** (Phase 4):
```rust
use openraft::{Raft, Config as RaftOptions};

pub struct RaftConsensusClient {
    node_id: NodeId,
    raft: Arc<Raft<TypeConfig>>,     // openraft Raft instance
    storage: Arc<RaftStorage>,        // RocksDB-backed
    network: Arc<RaftNetwork>,        // TCP network layer
    config: RaftConfig,
}

// openraft TypeConfig
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = LogEntry;              // Log entry data
    type R = ();                    // Response type
    type Node = NodeId;             // Node identifier
    type Entry = Entry<Self>;       // Log entry
    type SnapshotData = Cursor<Vec<u8>>;
}
```

**Required Components**:

1. **RaftStorage** (RocksDB backend):
```rust
pub struct RaftStorage {
    db: Arc<DB>,  // RocksDB instance
}

#[async_trait]
impl RaftLogReader<TypeConfig> for RaftStorage {
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>>;
    async fn try_get_log_entries(&mut self, range: Range<u64>) 
        -> Result<Vec<Entry<TypeConfig>>>;
}

#[async_trait]
impl RaftLogWriter<TypeConfig> for RaftStorage {
    async fn append<I>(&mut self, entries: I) -> Result<()>;
    async fn truncate(&mut self, index: u64) -> Result<()>;
    async fn purge(&mut self, upto: u64) -> Result<()>;
}
```

2. **RaftNetwork** (TCP/RPC communication):
```rust
pub struct RaftNetwork {
    peers: HashMap<NodeId, RpcClient>,
    rpc_server: Arc<dyn RpcServer>,
}

#[async_trait]
impl RaftNetwork<TypeConfig> for RaftNetwork {
    async fn send_append_entries(
        &mut self,
        target: NodeId,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<TypeConfig>>;
    
    async fn send_vote(
        &mut self,
        target: NodeId,
        req: VoteRequest<TypeConfig>,
    ) -> Result<VoteResponse<TypeConfig>>;
}
```

3. **Configuration**:
```rust
pub struct RaftConfig {
    // Existing fields
    pub node_id: NodeId,
    pub listen_addr: SocketAddr,
    pub peers: Vec<(NodeId, SocketAddr)>,
    
    // New Phase 4 fields
    pub data_dir: PathBuf,           // RocksDB directory
    pub snapshot_interval: u64,      // Snapshot every N entries
    pub max_in_flight: usize,        // Max concurrent RPCs
}
```

**Implementation Steps**:

1. Add RocksDB storage implementation
2. Add TCP network layer using nexora-rpc
3. Wire openraft Raft instance with storage + network
4. Update ConsensusClient methods to delegate to openraft
5. Add configuration for data directory and tuning parameters

### Task 4.4: Test Multi-Node Raft Cluster

**Goal**: Validate 3-node Raft cluster with failover scenarios

**Test Scenarios**:

1. **Basic Cluster Formation**:
```rust
#[tokio::test]
async fn test_three_node_cluster_election() {
    let configs = vec![
        RaftConfig::new(1, "127.0.0.1:5690".parse()?),
        RaftConfig::new(2, "127.0.0.1:5691".parse()?),
        RaftConfig::new(3, "127.0.0.1:5692".parse()?),
    ];
    
    let nodes = start_cluster(configs).await?;
    
    // One node should be elected leader
    let leaders: Vec<_> = nodes.iter()
        .filter(|n| n.is_leader().await.unwrap())
        .collect();
    assert_eq!(leaders.len(), 1);
}
```

2. **Leader Failover**:
```rust
#[tokio::test]
async fn test_leader_failover() {
    let nodes = start_cluster(3).await?;
    let leader = find_leader(&nodes).await?;
    
    // Kill leader
    nodes[leader].shutdown().await?;
    
    // Wait for new election
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // New leader should be elected
    let remaining = &nodes[..leader];
    let leaders: Vec<_> = remaining.iter()
        .filter(|n| n.is_leader().await.unwrap())
        .collect();
    assert_eq!(leaders.len(), 1);
}
```

3. **Log Replication**:
```rust
#[tokio::test]
async fn test_log_replication() {
    let nodes = start_cluster(3).await?;
    let leader_idx = find_leader(&nodes).await?;
    let leader = &nodes[leader_idx];
    
    // Commit 10 entries
    for i in 0..10 {
        let data = format!("entry-{}", i).into_bytes();
        leader.commit(data.into()).await?;
    }
    
    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // All nodes should have same log length
    for node in &nodes {
        assert_eq!(node.log_len().await?, 10);
    }
}
```

4. **Network Partition Tolerance**:
```rust
#[tokio::test]
async fn test_network_partition() {
    let nodes = start_cluster(5).await?;
    
    // Partition: [1,2] vs [3,4,5]
    partition_network(&nodes, vec![0, 1], vec![2, 3, 4]).await?;
    
    // Majority partition (3 nodes) should elect leader
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    let majority_leaders: Vec<_> = nodes[2..]
        .iter()
        .filter(|n| n.is_leader().await.unwrap())
        .collect();
    assert_eq!(majority_leaders.len(), 1);
    
    // Minority partition should have no leader
    let minority_leaders: Vec<_> = nodes[..2]
        .iter()
        .filter(|n| n.is_leader().await.unwrap())
        .collect();
    assert_eq!(minority_leaders.len(), 0);
}
```

### Task 4.5: Integrate with RisingWave Meta HA

**Goal**: Wire RaftElectionClient into RisingWave Meta for HA mode

**Current Integration Point**:
RisingWave Meta uses `ElectionClient` in `risingwave_meta::manager::election`:

```rust
// vendor/risingwave/src/meta/src/manager/election.rs
pub enum ElectionBackend {
    Etcd { endpoints: Vec<String> },
    Sql { endpoint: String },
    // Phase 4: Add external Raft
    #[cfg(feature = "raft-ha")]
    External { client: Box<dyn ElectionClient> },
}
```

**Integration Steps**:

1. **Patch RisingWave** (if needed):
```diff
diff --git a/vendor/risingwave/src/meta/src/manager/election.rs
+++ b/vendor/risingwave/src/meta/src/manager/election.rs
@@ -15,6 +15,11 @@ pub enum ElectionBackend {
     Sql {
         endpoint: String,
     },
+    /// External election provider (e.g., embedded Raft)
+    #[cfg(feature = "raft-ha")]
+    External {
+        client: Arc<dyn ElectionClient>,
+    },
 }
```

2. **Wire into nexora-risingwave**:
```rust
// crates/nexora-risingwave/src/meta_wrapper.rs (updated)
use extensions_meta_raft::{RaftElectionClient, RaftElectionConfig};

impl MetaNode {
    pub async fn start_with_raft_ha(
        consensus_config: RaftElectionConfig,
        meta_addr: SocketAddr,
    ) -> Result<Self> {
        // Create Raft election client
        let election = RaftElectionClient::new(consensus_config).await?;
        election.init().await?;
        
        // Start RisingWave Meta with Raft election
        let meta_opts = MetaNodeOpts {
            listen_addr: meta_addr,
            election_backend: ElectionBackend::External {
                client: Arc::new(election),
            },
            ..Default::default()
        };
        
        let meta = MetaService::start(meta_opts).await?;
        
        Ok(Self { meta, addr: meta_addr })
    }
}
```

3. **Configuration**:
```rust
// nexora.toml
[event_streaming]
enabled = true
mode = "distributed"  # vs "single-node"

[event_streaming.meta]
node_id = 1
listen_addr = "127.0.0.1:5690"
peers = ["127.0.0.1:5691", "127.0.0.1:5692"]

[event_streaming.consensus]
data_dir = "/data/nexora/raft"
heartbeat_interval_secs = 1
election_timeout_secs = 5
```

4. **End-to-End Test**:
```rust
#[tokio::test]
async fn test_risingwave_meta_ha_cluster() {
    // Start 3 Meta nodes with Raft
    let configs = vec![
        create_meta_config(1, 5690, vec![5691, 5692]),
        create_meta_config(2, 5691, vec![5690, 5692]),
        create_meta_config(3, 5692, vec![5690, 5691]),
    ];
    
    let metas: Vec<_> = configs.into_iter()
        .map(|c| MetaNode::start_with_raft_ha(c.raft, c.meta).await)
        .collect::<Result<_>>()?;
    
    // Find leader
    let leader = metas.iter().find(|m| m.is_leader().await).unwrap();
    
    // Execute DDL on leader
    leader.execute_ddl("CREATE SOURCE test_source WITH (...)").await?;
    
    // Kill leader
    leader.shutdown().await?;
    
    // Wait for new election
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // New leader should be elected and catalog should be preserved
    let new_leader = metas[1..].iter()
        .find(|m| m.is_leader().await)
        .unwrap();
    
    let sources = new_leader.list_sources().await?;
    assert!(sources.iter().any(|s| s.name == "test_source"));
}
```

## Performance Targets

### Multi-Node Raft (Phase 4)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Leader election time | <5s | Time from leader failure to new leader elected |
| Log replication latency | <5ms | P99 latency for quorum (2/3 nodes) |
| Commit throughput | >1000 ops/s | Commits per second on leader |
| Network bandwidth | <10 MB/s | Raft traffic per node |
| Storage overhead | <2x data size | RocksDB space amplification |

### RisingWave Meta HA

| Metric | Target | Measurement |
|--------|--------|-------------|
| Failover time | <10s | DDL unavailable window during leader change |
| Catalog sync latency | <100ms | Time for catalog update to reach all Meta nodes |
| DDL throughput | >100 ops/s | CREATE/DROP operations per second |

## Testing Strategy

### Unit Tests
- ✅ RaftElectionClient lifecycle (already exists)
- ⏳ RaftStorage read/write operations
- ⏳ RaftNetwork RPC send/receive

### Integration Tests
- ⏳ 3-node Raft cluster formation
- ⏳ Leader election and failover
- ⏳ Log replication across nodes
- ⏳ Network partition tolerance (5-node cluster)

### End-to-End Tests
- ⏳ RisingWave Meta HA cluster (3 nodes)
- ⏳ DDL execution with leader failover
- ⏳ Catalog consistency after network partition

## Dependencies

### New Dependencies
```toml
# nexora-consensus (upgraded)
[dependencies]
openraft = { version = "0.9", features = ["serde"] }
rocksdb = { package = "rust-rocksdb", version = "0.50" }
serde = { workspace = true }

# extensions-meta-raft (no changes needed)
```

### Patches
No patches to RisingWave are required if we use the "external election" feature. If that feature doesn't exist, we'll need:
- `patches/001-enable-external-election.patch` (add ElectionBackend::External)

## Migration Path

### From Phase 2 (Single-Node) to Phase 4 (Multi-Node)

**Backward Compatibility**: Phase 4 must support both modes:

```rust
pub enum RaftMode {
    SingleNode,   // Phase 2 behavior (in-memory, always leader)
    MultiNode,    // Phase 4 behavior (distributed, persistent)
}

impl RaftConsensusClient {
    pub async fn new(config: RaftConfig) -> Result<Self> {
        match config.mode {
            RaftMode::SingleNode => Self::new_single_node(config).await,
            RaftMode::MultiNode => Self::new_multi_node(config).await,
        }
    }
}
```

**Configuration Migration**:
```toml
# Phase 2 (single-node)
[consensus]
node_id = 1
mode = "single"

# Phase 4 (multi-node)
[consensus]
node_id = 1
mode = "multi"
peers = [2, 3]
data_dir = "/data/raft"
```

## Known Issues & Limitations

### Phase 4 Limitations

1. **No dynamic membership**: Cluster membership is static (configured at startup)
   - Future: Add/remove nodes without restart (openraft supports this)

2. **No snapshot compaction**: Log grows unbounded
   - Future: Implement periodic snapshots to compact log

3. **TCP-only network**: No TLS encryption
   - Future: Add TLS support via nexora-rpc

4. **Single Raft group**: All Meta nodes in one Raft cluster
   - Future: Sharded Raft for horizontal scaling

### Testing Gaps

- No chaos testing (random node failures, network delays)
- No performance benchmarks established
- No load testing for high-frequency commits

## Next Steps (Phase 5)

With Phase 4 complete, Phase 5 will integrate the full stack into nexora-app:

1. Add `--event-streaming-mode=distributed` CLI flag
2. Start RisingWave Meta cluster with Raft HA
3. Add HTTP endpoints for RisingWave operations
4. Test end-to-end event pipeline with HA

---

**Last Updated**: 2026-08-02  
**Document Version**: 1.0  
**Status**: Phase 4 In Progress
