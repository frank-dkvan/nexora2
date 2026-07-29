# RisingWave Integration Plan for Nexora 2

## Executive Summary

Integrate RisingWave as an **optional advanced stream processing module** into the existing Nexora 2 platform, preserving all current functionality while adding powerful SQL-based materialized views and complex event processing capabilities.

## Current Architecture (Preserved)

```
Nexora 2.0 (Event-First Architecture)
├── nexora-eventlog       ✓ Apache Iceberg + DataFusion (event storage)
├── nexora-stream         ✓ Kafka/Kinesis/Pulsar/MQTT connectors
├── nexora-core           ✓ Graph engine (Cypher queries)
├── nexora-cypher         ✓ Cypher parser & executor
├── nexora-sql            ✓ SQL → Cypher translator
└── nexora-app            ✓ HTTP API server (1590+ tests)
```

**Status**: Production-ready for single-node, experimental for multi-node

## Integration Architecture (New)

```
Nexora 2.0 + RisingWave (Hybrid)
├── TIER 1: Simple Event Ingestion (Current)
│   ├── nexora-stream → nexora-eventlog → nexora-core
│   └── Use case: Direct event-to-graph ingestion
│
├── TIER 2: Advanced Stream Processing (NEW - Optional)
│   ├── vendor/risingwave/              # Git Subtree
│   ├── crates/nexora-risingwave/       # RisingWave wrapper
│   ├── crates/nexora-consensus/        # Raft abstraction (shared)
│   ├── crates/nexora-rpc/             # gRPC abstraction (shared)
│   ├── extensions/meta_raft/          # RisingWave Raft HA
│   └── Use case: Complex SQL transformations, temporal joins, aggregations
│
└── Feature Flag: --features event-streaming
```

## Integration Strategy

### Principle: Additive, Not Replacement

1. **Keep all existing functionality**
   - nexora-eventlog remains the primary event store
   - nexora-stream continues to handle direct ingestion
   - All 1590+ tests must continue to pass

2. **Add the Event Streaming engine as an optional enhancement**
   - Enabled via `--features event-streaming`
   - Provides SQL materialized views over event streams
   - Outputs enriched events back to nexora-eventlog

3. **Shared infrastructure**
   - Raft consensus for both RisingWave Meta and Nexora Graph
   - Unified gRPC communication layer
   - Common storage abstractions

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Nexora 2 Platform                             │
├─────────────────────────────────────────────────────────────────┤
│  API Layer (nexora-app)                                          │
│    └── /api/query/cypher, /api/query/sql, /api/ingest/*        │
├─────────────────────────────────────────────────────────────────┤
│  Query Layer                                                     │
│    ├── nexora-cypher (Cypher → Graph)                           │
│    ├── nexora-sql (SQL → Cypher/DataFusion)                     │
│    └── nexora-risingwave (SQL → Materialized Views) [OPTIONAL]  │
├─────────────────────────────────────────────────────────────────┤
│  Graph Layer                                                     │
│    └── nexora-core (RocksDB-backed graph storage)               │
├─────────────────────────────────────────────────────────────────┤
│  Event Processing Layer (two parallel paths)                     │
│    ├── Path A (Simple):   nexora-stream → nexora-eventlog        │
│    └── Path B (Advanced): Event Streaming engine (own CREATE     │
│          SOURCE + SQL/MV) → nexora-eventlog [OPTIONAL]           │
├─────────────────────────────────────────────────────────────────┤
│  Event Storage Layer                                             │
│    └── nexora-eventlog (Apache Iceberg tables)                  │
├─────────────────────────────────────────────────────────────────┤
│  Consensus Layer (NEW - Shared)                                 │
│    ├── nexora-consensus (Raft trait abstraction)                │
│    ├── RaftConsensusClient (openraft implementation)            │
│    ├── Used by: RisingWave Meta HA                              │
│    └── Used by: Nexora Graph Cluster (future)                   │
└─────────────────────────────────────────────────────────────────┘

External Sources (Kafka/Kinesis/Pulsar/MQTT)
    │
    ├─► Path A: nexora-stream ──────────────► nexora-eventlog
    │           (direct event-to-graph ingestion)
    │
    └─► Path B: Event Streaming engine [OPTIONAL]
                │   (connects to sources directly — no nexora-stream)
                ├─► CREATE SOURCE (Kafka/…)
                ├─► CREATE MATERIALIZED VIEW (SQL transforms)
                └─► Output enriched events ─► nexora-eventlog

Both paths converge at nexora-eventlog, then flow to nexora-core.
```

## Implementation Phases

### Phase 1: Repository Setup (Week 1)

**Goal**: Add RisingWave as Git Subtree

**Tasks**:
1. Create `scripts/init-risingwave.sh`
   ```bash
   git remote add risingwave-upstream https://github.com/risingwavelabs/risingwave.git
   git subtree add --prefix=vendor/risingwave risingwave-upstream v3.0.2 --squash
   ```

2. Update root `Cargo.toml`
   ```toml
   [workspace]
   members = [
       # ... existing crates ...
       "crates/nexora-risingwave",
       "crates/nexora-consensus",
       "crates/nexora-rpc",
       "extensions/meta_raft",
   ]
   
   [features]
   risingwave = [
       "nexora-app/risingwave",
       "nexora-risingwave",
       "extensions-meta-raft",
   ]
   ```

3. Create `.gitignore` additions
   ```
   /vendor/risingwave/target/
   /vendor/risingwave/.idea/
   ```

**Deliverables**:
- `scripts/init-risingwave.sh`
- `scripts/sync-risingwave.sh`
- Updated `Cargo.toml`

### Phase 2: Shared Infrastructure (Week 2)

**Goal**: Develop reusable consensus and RPC layers

**2.1 Consensus Abstraction** (`crates/nexora-consensus/`)

```rust
// crates/nexora-consensus/src/lib.rs
#[async_trait::async_trait]
pub trait ConsensusClient: Send + Sync + 'static {
    async fn init(&self, peers: Vec<String>) -> Result<()>;
    fn is_leader(&self) -> bool;
    async fn commit(&self, data: Bytes) -> Result<LogIndex>;
    fn subscribe_leader_change(&self) -> Receiver<LeaderChange>;
}

pub struct RaftConsensusClient {
    raft: Arc<Raft<...>>,
}

impl ConsensusClient for RaftConsensusClient { ... }
```

**2.2 RPC Abstraction** (`crates/nexora-rpc/`)

```rust
// crates/nexora-rpc/src/lib.rs
#[async_trait::async_trait]
pub trait RpcServer: Send + Sync {
    async fn start(&self, addr: SocketAddr) -> Result<()>;
}

pub struct TonicRpcServer { ... }
```

**Dependencies**:
```toml
openraft = "0.9"
tonic = "0.11"
tokio = { workspace = true }
```

**Deliverables**:
- `crates/nexora-consensus/`
- `crates/nexora-rpc/`
- Integration tests

### Phase 3: RisingWave Wrapper (Week 3)

**Goal**: Create nexora-risingwave crate to encapsulate RisingWave

**3.1 Module Structure**

```
crates/nexora-risingwave/
├── src/
│   ├── lib.rs              # Public API
│   ├── meta_wrapper.rs     # RisingWave Meta node wrapper
│   ├── frontend_wrapper.rs # RisingWave Frontend wrapper
│   ├── compute_wrapper.rs  # RisingWave Compute node wrapper
│   ├── materialized_view.rs # MV management
│   └── config.rs           # RisingWave configuration
└── Cargo.toml
```

**3.2 Core API**

```rust
// crates/nexora-risingwave/src/lib.rs
pub struct RisingWaveModule {
    meta: Arc<MetaNode>,
    frontend: Arc<FrontendNode>,
    compute: Option<Arc<ComputeNode>>,
}

impl RisingWaveModule {
    /// Start embedded RisingWave cluster (Meta + Frontend + Compute)
    pub async fn start(config: RisingWaveConfig) -> Result<Self>;
    
    /// Execute SQL DDL (CREATE SOURCE, CREATE MV)
    pub async fn execute_ddl(&self, sql: &str) -> Result<()>;
    
    /// Query materialized view
    pub async fn query_mv(&self, sql: &str) -> Result<Vec<Row>>;
    
    /// Subscribe to MV changes (CDC-like)
    pub async fn subscribe_mv(&self, mv_name: &str) -> Result<Receiver<Change>>;
}
```

**3.3 Dependencies**

```toml
[dependencies]
nexora-consensus = { path = "../nexora-consensus" }
nexora-rpc = { path = "../nexora-rpc" }
risingwave_meta = { path = "../../vendor/risingwave/src/meta" }
risingwave_frontend = { path = "../../vendor/risingwave/src/frontend" }
risingwave_stream = { path = "../../vendor/risingwave/src/stream" }
```

**Deliverables**:
- `crates/nexora-risingwave/`
- Unit tests for wrapper API

### Phase 4: Raft HA Extension (Week 4)

**Goal**: Replace RisingWave's PostgreSQL-based leader election with embedded Raft

**4.1 Extension Structure**

```
extensions/meta_raft/
├── src/
│   ├── lib.rs              # Public exports
│   ├── client.rs           # RaftElectionClient impl
│   ├── storage.rs          # Raft log storage
│   └── network.rs          # Raft network layer
└── Cargo.toml
```

**4.2 Implementation**

```rust
// extensions/meta_raft/src/client.rs
use nexora_consensus::ConsensusClient;
use risingwave_meta::manager::election::ElectionClient;

pub struct RaftElectionClient {
    consensus: Arc<dyn ConsensusClient>,
}

#[async_trait::async_trait]
impl ElectionClient for RaftElectionClient {
    async fn init(&self) -> MetaResult<()> {
        self.consensus.init(self.peers.clone()).await?;
        Ok(())
    }
    
    fn is_leader(&self) -> bool {
        self.consensus.is_leader()
    }
    
    async fn run_once(&self, ttl: i64, stop: Receiver<()>) -> MetaResult<()> {
        // Delegate to nexora-consensus
        let mut leader_rx = self.consensus.subscribe_leader_change();
        // ... implementation
    }
}
```

**4.3 Patch Files**

Create minimal patches to enable external election:

```
patches/
├── 001-enable-external-election.patch
└── 002-expose-election-trait.patch
```

**Patch Example**:
```diff
diff --git a/vendor/risingwave/src/meta/src/manager/election.rs
+++ b/vendor/risingwave/src/meta/src/manager/election.rs
@@ -10,6 +10,10 @@ pub enum ElectionBackend {
     Etcd { endpoints: Vec<String> },
     Sql { endpoint: String },
+    #[cfg(feature = "raft-ha")]
+    External {
+        plugin: Box<dyn ElectionClient>,
+    },
 }
```

**Deliverables**:
- `extensions/meta_raft/`
- `patches/001-enable-external-election.patch`
- 3-node cluster integration test

### Phase 5: Integration with nexora-app (Week 5)

**Goal**: Wire RisingWave into nexora-app with feature gate

**5.1 Update nexora-app**

```rust
// crates/nexora-app/src/main.rs
#[cfg(feature = "risingwave")]
use nexora_risingwave::RisingWaveModule;

#[tokio::main]
async fn main() -> Result<()> {
    // ... existing initialization ...
    
    #[cfg(feature = "risingwave")]
    let risingwave = if cli.enable_risingwave {
        let rw = RisingWaveModule::start(config.risingwave).await?;
        Some(Arc::new(rw))
    } else {
        None
    };
    
    let state = AppState {
        graph: graph_service,
        event_store,
        #[cfg(feature = "risingwave")]
        risingwave,
        // ... other fields ...
    };
    
    // ... rest of setup ...
}
```

**5.2 Add HTTP Endpoints**

```rust
// crates/nexora-app/src/handlers/risingwave.rs
#[cfg(feature = "risingwave")]
pub async fn execute_rw_ddl(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RisingWaveDdlRequest>,
) -> Result<Json<RisingWaveDdlResponse>, ApiError> {
    let rw = state.risingwave.as_ref()
        .ok_or(ApiError::FeatureNotEnabled("risingwave"))?;
    
    rw.execute_ddl(&req.sql).await?;
    Ok(Json(RisingWaveDdlResponse { success: true }))
}
```

**5.3 New API Endpoints**

```
POST /api/risingwave/ddl
  - Execute RisingWave SQL DDL

POST /api/risingwave/query
  - Query RisingWave materialized views

GET /api/risingwave/sources
  - List RisingWave sources

GET /api/risingwave/materialized_views
  - List materialized views
```

**Deliverables**:
- Updated `nexora-app` with RisingWave integration
- New API endpoints
- Feature-gated tests

### Phase 6: Event Pipeline Integration (Week 6)

**Goal**: Connect RisingWave output back to nexora-eventlog

**6.1 RisingWave → EventLog Bridge**

```rust
// crates/nexora-risingwave/src/event_sink.rs
pub struct EventLogSink {
    event_store: Arc<EventLogStore>,
}

impl EventLogSink {
    /// Subscribe to MV changes and write to event log
    pub async fn start_sync(&self, mv_name: &str, topic: &str) -> Result<()> {
        let mut rx = self.risingwave.subscribe_mv(mv_name).await?;
        
        while let Some(change) = rx.recv().await {
            match change {
                Change::Insert(row) => {
                    let event = self.row_to_event(row)?;
                    self.event_store.append(topic, event).await?;
                }
                // Handle updates and deletes
            }
        }
        Ok(())
    }
}
```

**6.2 Example Use Case**

```sql
-- Create Kafka source in RisingWave
CREATE SOURCE raw_cargo_events WITH (
    connector = 'kafka',
    topic = 'logistics.raw_events',
    properties.bootstrap.server = 'kafka:9092'
) FORMAT PLAIN ENCODE JSON;

-- Create materialized view with enrichment
CREATE MATERIALIZED VIEW enriched_cargo_events AS
SELECT 
    c.cargo_id,
    c.status,
    c.location,
    l.city,
    l.country,
    c.event_time
FROM raw_cargo_events c
LEFT JOIN location_lookup l ON c.location = l.code;

-- Nexora subscribes to the MV and writes enriched events back
```

**Deliverables**:
- `EventLogSink` implementation
- End-to-end test: Kafka → RisingWave → EventLog → Graph

## Configuration

### nexora.toml (Extended)

```toml
[server]
host = "127.0.0.1"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/graph"

[event_store]
backend = "rest"
rest_uri = "http://localhost:8181/catalog"
rest_warehouse = "nexora"
s3_endpoint = "http://localhost:9000"
s3_bucket = "nexora-events"

# NEW: Event Streaming engine configuration (optional)
# Requires: --features event-streaming
[event_streaming]
enabled = false
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
compute_nodes = 1

[event_streaming.raft]
node_id = 1
peers = ["node1:5690", "node2:5690", "node3:5690"]
data_dir = "/data/nexora/raft"

[event_streaming.storage]
state_store = "hummock+s3://nexora-rw-state"
data_directory = "/data/nexora/rw-data"
```

## Testing Strategy

### Unit Tests
```bash
# Test shared libraries
cargo test -p nexora-consensus
cargo test -p nexora-rpc

# Test Event Streaming wrapper
cargo test -p nexora-risingwave --features event-streaming
```

### Integration Tests
```bash
# Test Raft HA (3-node cluster)
cargo test -p extensions-meta-raft -- --test-threads=1

# Test event pipeline
cargo test test_kafka_event_streaming_eventlog --features event-streaming
```

### End-to-End Test
```bash
# Start full stack
./scripts/start-nexora-full.sh

# Inject test events
./scripts/test-risingwave-pipeline.sh

# Verify graph nodes created
curl http://localhost:8080/api/query/cypher \
  -d '{"query": "MATCH (c:Cargo) RETURN count(c)"}'
```

## Build Commands

```bash
# Default build (no Event Streaming engine)
cargo build --release

# With Event Streaming engine
cargo build --release --features event-streaming

# With event-first + Event Streaming engine
cargo build --release --features event-first,event-streaming
```

## Migration Path

### For Existing Users

1. **No changes required** if you don't enable the Event Streaming engine
2. All existing APIs and features continue to work
3. Opt-in by recompiling with `--features event-streaming`

### Upgrade Steps

```bash
# 1. Pull latest code
git pull origin main

# 2. Optional: Enable Event Streaming engine
cargo build --release --features event-streaming

# 3. Update config (add [event_streaming] section if using)
vim nexora.toml

# 4. Restart
./nexora --config nexora.toml
```

## Performance Considerations

### Memory Overhead
- **Without RisingWave**: Current baseline (~500MB for graph + events)
- **With RisingWave**: +2GB for Meta/Frontend/Compute nodes

### When to Use RisingWave

**Use RisingWave when**:
- Complex SQL transformations needed (joins, aggregations)
- Temporal joins across multiple event streams
- Real-time data enrichment before graph ingestion
- Need for SQL-based materialized views

**Use Direct Path when**:
- Simple event-to-graph ingestion
- Low latency requirements (<10ms)
- Minimal memory footprint needed

## Risk Mitigation

### Risk 1: RisingWave Binary Size
**Impact**: Large vendor/ directory (~500MB source code)
**Mitigation**:
- Git Subtree with `--squash` flag
- Selective compilation (only Meta + Frontend + minimal Compute)
- Exclude test files and examples

### Risk 2: Dependency Conflicts
**Impact**: RisingWave dependencies might conflict with Nexora
**Mitigation**:
- Feature gates isolate RisingWave deps
- Shared abstractions (consensus, RPC) avoid duplication
- Careful dependency version pinning

### Risk 3: Maintenance Burden
**Impact**: Need to track RisingWave upstream changes
**Mitigation**:
- Minimal patches (<100 lines total)
- Monthly sync with upstream
- Automated patch application script

## Success Criteria

### Phase 1-2 Success
- [ ] RisingWave v3.0.2 added as Git Subtree
- [ ] `nexora-consensus` and `nexora-rpc` crates functional
- [ ] All existing tests still pass

### Phase 3-4 Success
- [ ] `nexora-risingwave` wrapper can start Meta+Frontend
- [ ] 3-node Raft HA cluster works without external dependencies
- [ ] Can execute basic DDL (CREATE SOURCE, CREATE MV)

### Phase 5-6 Success
- [ ] RisingWave integrated into `nexora-app` via feature flag
- [ ] Events flow: Kafka → RisingWave → EventLog → Graph
- [ ] End-to-end test passes

### Overall Success
- [ ] Build without `--features event-streaming`: All existing features work
- [ ] Build with `--features event-streaming`: Advanced SQL processing available
- [ ] Performance: <10% overhead for non-RisingWave code paths
- [ ] Documentation: README updated with RisingWave usage examples

## Timeline

| Week | Phase | Deliverables |
|------|-------|-------------|
| 1 | Repository Setup | Git Subtree, scripts, Cargo config |
| 2 | Shared Infrastructure | nexora-consensus, nexora-rpc |
| 3 | RisingWave Wrapper | nexora-risingwave crate |
| 4 | Raft HA Extension | extensions/meta_raft, patches |
| 5 | App Integration | HTTP endpoints, feature gate |
| 6 | Event Pipeline | EventLogSink, E2E test |

## Next Steps

1. Review this plan with stakeholders
2. Create feature branch: `feat/risingwave-integration`
3. Start Phase 1: Repository setup
4. Weekly reviews to track progress

---

**Document Version**: 1.0  
**Date**: 2026-07-26  
**Author**: Nexora Development Team
