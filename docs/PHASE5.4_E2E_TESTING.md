# Phase 5.4: End-to-End HA Cluster Testing

**Status**: 🚧 In Progress  
**Date**: 2026-08-02

## Overview

Phase 5.4 validates the complete distributed RisingWave integration with end-to-end testing of a 3-node Raft HA cluster. This phase ensures DDL execution, query functionality, leader election, and failover all work correctly under real workload conditions.

## Goals

1. **3-Node Cluster Startup**: Verify three nexora-app instances form a Raft cluster
2. **DDL Execution & Sync**: Validate catalog replication across all nodes
3. **Leader Election**: Test automatic leader election and re-election
4. **Failover**: Verify system continues operating after leader failure
5. **Performance**: Measure DDL latency, query throughput, failover time

## Test Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  End-to-End Test Framework                   │
└────────────────────┬────────────────────────────────────────┘
                     │
         ┌───────────┴───────────┬─────────────┐
         ▼                       ▼             ▼
    ┌────────┐             ┌────────┐    ┌────────┐
    │ Node 1 │◄───Raft────►│ Node 2 │◄───│ Node 3 │
    │  :8080 │             │  :8081 │    │  :8082 │
    │ Leader │             │Follower│    │Follower│
    └────┬───┘             └────┬───┘    └────┬───┘
         │                      │             │
         └──────────┬───────────┴─────────────┘
                    │
         ┌──────────▼──────────┐
         │  Shared MinIO (S3)  │
         │  Shared Catalog     │
         └─────────────────────┘
```

## Test Cases

### 1. Three-Node Cluster Startup

**Test**: `test_three_node_cluster_startup`

**Steps**:
1. Start three nexora-app instances with library mode
2. Each node configured with different ports and data directories
3. Wait for Raft cluster formation (max 15s)
4. Verify exactly one leader elected
5. Verify all nodes can communicate

**Configuration** (per node):
```toml
[event_streaming]
enabled = true
mode = "distributed"

[event_streaming.distributed]
node_id = "meta-{1,2,3}"
raft_node_id = {1,2,3}
data_dir = "./test-data/node{1,2,3}/event-streaming"

[event_streaming.distributed.meta]
listen_addr = "127.0.0.1:{5690,5691,5692}"

[[event_streaming.distributed.meta.peers]]
node_id = {other nodes}
addr = "127.0.0.1:{peer_ports}"

[event_streaming.distributed.consensus]
data_dir = "./test-data/node{1,2,3}/raft"
heartbeat_interval_secs = 1
election_timeout_secs = 5
```

**Success Criteria**:
- ✅ All 3 nodes start successfully
- ✅ Raft cluster forms within 10s
- ✅ Exactly 1 leader elected
- ✅ Leader responds to health checks
- ✅ Followers report correct leader_id

**Expected Metrics**:
- Cluster formation time: <10s
- Leader election time: <5s

---

### 2. DDL Execution and Catalog Sync

**Test**: `test_ddl_execution_and_sync`

**Steps**:
1. Start 3-node cluster
2. Execute DDL on leader: `CREATE SOURCE test_source WITH (connector = 'datagen') FORMAT PLAIN ENCODE JSON`
3. Wait for Raft replication (1-2s)
4. Query all nodes: `GET /api/event-streaming/sources`
5. Verify all nodes return `test_source`
6. Create materialized view on leader
7. Verify MV appears on all nodes

**API Calls**:
```bash
# Execute DDL on leader (Node 1)
curl -X POST http://localhost:8080/api/event-streaming/ddl \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE SOURCE test_source WITH (connector = '\''datagen'\'') FORMAT PLAIN ENCODE JSON"}'

# Check Node 2
curl http://localhost:8081/api/event-streaming/sources

# Check Node 3
curl http://localhost:8082/api/event-streaming/sources
```

**Success Criteria**:
- ✅ DDL succeeds on leader
- ✅ Source appears on all nodes within 2s
- ✅ MV creation succeeds
- ✅ MV appears on all nodes within 2s

**Expected Metrics**:
- DDL execution latency: <100ms (P99)
- Catalog sync latency: <500ms (P99)

---

### 3. Leader Failover

**Test**: `test_leader_failover`

**Steps**:
1. Start 3-node cluster
2. Identify current leader via `/api/event-streaming/cluster/distributed`
3. Execute DDL to create source and MV
4. Simulate leader crash (shutdown process)
5. Wait for election timeout (5s)
6. Verify new leader elected
7. Execute DDL on new leader
8. Query new leader - verify previous DDL still present

**Expected Behavior**:
```
Time  | Event
------|----------------------------------------------
T+0s  | Node 1 is leader, execute DDL
T+5s  | Shutdown Node 1
T+6s  | Heartbeat timeout detected by Node 2/3
T+8s  | Election starts, Node 2 wins
T+10s | Node 2 becomes new leader
T+12s | Execute DDL on Node 2 - succeeds
T+14s | Query Node 2 - shows both old and new DDL
```

**Success Criteria**:
- ✅ New leader elected within 10s of crash
- ✅ No data loss (old DDL preserved)
- ✅ New DDL executes successfully
- ✅ Remaining nodes continue operating

**Expected Metrics**:
- Leader failover time: <10s
- DDL availability during failover: 0 (expected)
- DDL availability after failover: 100%

---

### 4. Query Execution During Failover

**Test**: `test_query_during_failover`

**Steps**:
1. Start 3-node cluster with data (source + MV)
2. Start continuous query workload (10 QPS)
3. Trigger leader failure mid-workload
4. Track query success/failure during election
5. After new leader elected, verify queries succeed
6. Verify query results consistent

**Query Workload**:
```sql
SELECT COUNT(*) FROM test_mv
```

**Expected Behavior**:
```
Queries during leader crash → Fail or timeout
Queries during election (5-10s) → Fail or retry
Queries after new leader → Succeed
```

**Success Criteria**:
- ✅ Queries fail gracefully during election
- ✅ Queries succeed after new leader elected
- ✅ No data corruption
- ✅ Query results match pre-failover state

**Expected Metrics**:
- Query availability during failover: 0-50%
- Query availability after failover: 100%
- Query latency increase during failover: <2x normal

---

### 5. Cluster Status Endpoint

**Test**: `test_cluster_status_endpoint`

**Steps**:
1. Start 3-node cluster
2. Query each node: `GET /api/event-streaming/cluster/distributed`
3. Verify response structure
4. Verify leader reports `is_leader: true`
5. Verify followers report correct `leader_id`
6. Trigger failover
7. Query status again, verify new leader

**Expected Response** (Leader):
```json
{
  "mode": "distributed_library",
  "meta": {
    "is_leader": true,
    "leader_id": 1,
    "raft_state": "Leader",
    "node_count": 3
  },
  "frontend": {
    "active_nodes": 1,
    "total_nodes": 1,
    "healthy": true
  },
  "compute": {
    "active_nodes": 1,
    "total_nodes": 1,
    "healthy": true,
    "total_parallelism": 8
  }
}
```

**Expected Response** (Follower):
```json
{
  "mode": "distributed_library",
  "meta": {
    "is_leader": false,
    "leader_id": 1,
    "raft_state": "Follower",
    "node_count": 3
  },
  ...
}
```

**Success Criteria**:
- ✅ Status endpoint works on all nodes
- ✅ Leader correctly reports `is_leader: true`
- ✅ Followers report correct `leader_id`
- ✅ After failover, status updates correctly

---

### 6. DDL Performance

**Test**: `test_ddl_performance`

**Metrics**:
- DDL execution latency (P50, P95, P99)
- DDL throughput (operations/sec)
- Catalog replication latency

**Workload**:
```sql
CREATE SOURCE source_1 WITH (connector = 'datagen') FORMAT PLAIN ENCODE JSON;
CREATE SOURCE source_2 WITH (connector = 'datagen') FORMAT PLAIN ENCODE JSON;
...
CREATE SOURCE source_100 WITH (connector = 'datagen') FORMAT PLAIN ENCODE JSON;
```

**Target Performance**:
- P50 latency: <50ms
- P95 latency: <80ms
- P99 latency: <100ms
- Throughput: >10 DDL ops/sec

---

### 7. Query Performance

**Test**: `test_query_performance`

**Metrics**:
- Query execution latency (P50, P95, P99)
- Query throughput (queries/sec)
- Impact of Raft on read latency

**Workload**:
```sql
SELECT COUNT(*) FROM test_mv;
SELECT * FROM test_mv LIMIT 100;
SELECT user_id, COUNT(*) FROM test_mv GROUP BY user_id LIMIT 10;
```

**Target Performance**:
- P50 latency: <20ms
- P95 latency: <50ms
- P99 latency: <100ms
- Throughput: >100 queries/sec

---

## Implementation Plan

### Step 1: Test Infrastructure (1 day)

**Files**:
- `crates/nexora-app/tests/phase5_4_e2e_test.rs` (created)

**Tasks**:
- [ ] Implement `start_test_cluster(node_count)` helper
- [ ] Implement `wait_for_leader_election()` helper
- [ ] Implement `execute_ddl_on_node()` helper
- [ ] Implement `query_node()` helper
- [ ] Implement `shutdown_node()` helper

**Challenges**:
- Each node needs unique ports (8080, 8081, 8082)
- Each node needs unique data directories
- Need to spawn nexora-app processes or use in-process API

---

### Step 2: Basic Cluster Tests (1 day)

**Tasks**:
- [ ] Implement `test_three_node_cluster_startup`
- [ ] Implement `test_ddl_execution_and_sync`
- [ ] Implement `test_cluster_status_endpoint`

**Dependencies**:
- Requires distributed library cluster initialization in `risingwave_init.rs`
- Requires `AppState.distributed_library_cluster` field

---

### Step 3: Failover Tests (1 day)

**Tasks**:
- [ ] Implement `test_leader_failover`
- [ ] Implement `test_query_during_failover`

**Challenges**:
- Simulating process crash cleanly
- Waiting for election without hardcoded timeouts
- Handling transient query failures

---

### Step 4: Performance Tests (1 day)

**Tasks**:
- [ ] Implement `test_ddl_performance`
- [ ] Implement `test_query_performance`
- [ ] Add metrics collection and reporting

**Tools**:
- Use `criterion` for microbenchmarks
- Use `tokio::time::Instant` for latency measurement
- Generate CSV/JSON reports

---

### Step 5: Documentation (0.5 day)

**Tasks**:
- [ ] Document test setup and execution
- [ ] Document performance baselines
- [ ] Create troubleshooting guide
- [ ] Create `PHASE5.4_COMPLETE.md`

---

## Test Execution

### Running Tests

```bash
# Run all Phase 5.4 tests (requires significant resources)
cargo test --package nexora-app --test phase5_4_e2e_test --features event-streaming,library -- --test-threads=1 --nocapture

# Run specific test
cargo test --package nexora-app --test phase5_4_e2e_test --features event-streaming,library test_three_node_cluster_startup -- --nocapture --ignored

# Run performance tests
cargo test --package nexora-app --test phase5_4_e2e_test --features event-streaming,library test_ddl_performance -- --nocapture --ignored
```

### Test Environment Requirements

**Hardware**:
- CPU: 8+ cores (3 RisingWave clusters in-process)
- RAM: 8GB minimum (3 nodes × 2.2GB each)
- Disk: 10GB free (for Raft logs and RisingWave data)

**Network**:
- Localhost ports 8080-8082 (HTTP API)
- Localhost ports 5690-5692 (Meta gRPC)
- Localhost ports 4566-4568 (Frontend PostgreSQL)

**External Services**:
- MinIO or S3-compatible storage (for Iceberg catalog)
- Shared data directory for Raft logs

---

## Current Blockers (Phase 5.4)

### 1. Distributed Library Cluster Initialization

**Issue**: `risingwave_init.rs` has `init_distributed_node()` stub but doesn't create `AppState.distributed_library_cluster`

**Solution**:
```rust
// In risingwave_init.rs
pub async fn init_distributed_node(
    cli: &crate::Cli,
    config: Option<&crate::config::EventStreamingConfig>,
) -> Result<(
    Option<Arc<EventStreamingModule>>,
    Option<Arc<DistributedLibraryCluster>>, // NEW
)> {
    // 1. Create RaftConsensusClient
    // 2. Create RaftElectionClient
    // 3. Create MetaCluster with election
    // 4. Create FrontendPool
    // 5. Create ComputeCluster
    // 6. Return both EventStreamingModule and cluster tuple
}
```

### 2. AppState Field

**Issue**: `AppState.distributed_library_cluster` doesn't exist yet

**Solution**: Add to `main.rs`:
```rust
pub struct AppState {
    // ... existing fields ...
    
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    pub distributed_library_cluster: Option<Arc<(
        Arc<MetaCluster>,
        Arc<FrontendPool>,
        Arc<ComputeCluster>,
    )>>,
}
```

### 3. Multi-Instance Test Harness

**Issue**: Need to spawn multiple nexora-app processes or APIs

**Options**:
- **Option A**: Spawn processes via `std::process::Command`
  - ✅ True isolation
  - ❌ Complex port management
  - ❌ Hard to debug
- **Option B**: Use in-process Axum servers
  - ✅ Easier to control
  - ✅ Better debugging
  - ❌ Shared process resources

**Recommendation**: Start with Option B for faster iteration

---

## Success Criteria

Phase 5.4 is complete when:
- ✅ All 7 test cases pass
- ✅ 3-node cluster starts reliably
- ✅ Leader failover works within 10s
- ✅ DDL latency <100ms P99
- ✅ Query latency <50ms P95
- ✅ Documentation complete
- ✅ CI/CD integration (optional)

---

## Next Steps (Phase 6)

After Phase 5.4, Phase 6 will focus on:
1. Event pipeline integration (RisingWave → nexora-eventlog)
2. Materialized view CDC to graph
3. End-to-end event → graph projection
4. Performance optimization

---

**Last Updated**: 2026-08-02  
**Document Version**: 1.0  
**Status**: In Progress - Test Framework Created
