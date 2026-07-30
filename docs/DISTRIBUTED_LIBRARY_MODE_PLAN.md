# Distributed Library Mode Implementation Plan

**Status**: Planning  
**Timeline**: 4-5 days  
**Prerequisites**: ✅ Phase 1 (library mode single-node) complete

## Overview

Distributed library mode extends the in-process RisingWave deployment to support multi-node clusters with high availability. Unlike the current single-node library mode, this enables horizontal scaling and fault tolerance while maintaining the zero-external-binary deployment model.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Nexora Cluster                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Node 1 (Meta Leader)        Node 2 (Meta Follower)         │
│  ┌──────────────────┐        ┌──────────────────┐          │
│  │ Nexora Process   │        │ Nexora Process   │          │
│  │ ├─ Graph Engine  │        │ ├─ Graph Engine  │          │
│  │ ├─ RW Meta       │◄──Raft─┤ ├─ RW Meta       │          │
│  │ ├─ RW Frontend   │        │ ├─ RW Frontend   │          │
│  │ └─ RW Compute    │◄───────┤ └─ RW Compute    │          │
│  └──────────────────┘   gRPC └──────────────────┘          │
│           │                           │                      │
│           │    Node 3 (Meta Follower) │                     │
│           │    ┌──────────────────┐   │                     │
│           │    │ Nexora Process   │   │                     │
│           └────┤ ├─ Graph Engine  │───┘                     │
│          gRPC  │ ├─ RW Meta       │                         │
│                │ ├─ RW Frontend   │                         │
│                │ └─ RW Compute    │                         │
│                └──────────────────┘                         │
│                                                               │
└─────────────────────────────────────────────────────────────┘
         │              │              │
         └──────────────┴──────────────┘
                    Shared State
              (Hummock + Meta Catalog)
```

## Key Features

1. **In-Process Cluster**: Each Nexora node runs Meta + Frontend + Compute in-process
2. **Raft Consensus**: Meta nodes use Raft for catalog coordination
3. **Shared Nothing Graph**: Each node's graph engine remains independent (existing cluster mode)
4. **Compute Scale-Out**: Stream processing workload distributed across compute nodes
5. **Zero External Binary**: Everything compiled into nexora binary

## Implementation Phases

### Day 1: Distributed Library Configuration

**Goal**: Define configuration model for multi-node library deployment

**Tasks**:
1. Create `DistributedLibraryConfig` struct
2. Add cluster membership configuration (node ID, peers, addresses)
3. Add Raft configuration (election timeout, heartbeat interval)
4. Add Meta backend selection (etcd vs in-memory for testing)
5. Update CLI flags for distributed library mode

**Deliverables**:
- `crates/nexora-risingwave/src/distributed_library_config.rs`
- Updated CLI in `crates/nexora-app/src/main.rs`
- Configuration examples in `config/distributed-library-3node.yaml`

**Configuration Example**:
```yaml
# Node 1 config
event_streaming:
  mode: distributed-library  # New mode
  node_id: meta-1
  meta:
    listen_addr: "0.0.0.0:5690"
    advertise_addr: "node1.local:5690"
    raft_peers:
      - meta-2@node2.local:5690
      - meta-3@node3.local:5690
  frontend:
    listen_addr: "0.0.0.0:4566"
  compute:
    parallelism: 8
    listen_addr: "0.0.0.0:5688"
```

### Day 2: Meta Cluster Coordination

**Goal**: Enable multi-node Meta with Raft consensus

**Tasks**:
1. Implement `DistributedMetaCluster` wrapper
2. Configure Meta nodes to use etcd backend (shared state)
3. Wire Raft election and log replication
4. Add leader election monitoring
5. Implement catalog synchronization across Meta nodes

**Deliverables**:
- `crates/nexora-risingwave/src/distributed_library_meta.rs`
- Meta cluster startup/shutdown logic
- Raft health checks and leader detection

**Key Code**:
```rust
pub struct DistributedMetaCluster {
    node_id: String,
    meta_handle: MetaServiceHandle,  // RisingWave Meta
    raft_state: Arc<RaftState>,
    is_leader: Arc<AtomicBool>,
}

impl DistributedMetaCluster {
    pub async fn start(config: DistributedLibraryConfig) -> Result<Self>;
    pub async fn is_leader(&self) -> bool;
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<String>;
}
```

### Day 3: Frontend and Compute Distribution

**Goal**: Distribute query workload across Frontend and Compute nodes

**Tasks**:
1. Implement `DistributedFrontendPool` for load balancing
2. Register Compute nodes with Meta cluster
3. Implement work distribution (fragment scheduling)
4. Add query routing to appropriate Frontend
5. Implement health checks for Frontend/Compute nodes

**Deliverables**:
- `crates/nexora-risingwave/src/distributed_library_frontend.rs`
- `crates/nexora-risingwave/src/distributed_library_compute.rs`
- Load balancer for Frontend queries
- Compute node registration and heartbeat

**Key Code**:
```rust
pub struct DistributedFrontendPool {
    frontends: Vec<FrontendHandle>,
    load_balancer: Arc<RoundRobinBalancer>,
}

impl DistributedFrontendPool {
    pub async fn query(&self, sql: &str) -> Result<QueryResult>;
    pub async fn execute_ddl(&self, sql: &str) -> Result<()>;
}
```

### Day 4: Integration and Testing

**Goal**: Wire everything into nexora-app and validate end-to-end

**Tasks**:
1. Create `DistributedLibrary` main entry point
2. Integrate into `nexora-app` startup sequence
3. Add `--distributed-library-event-streaming` CLI flag
4. Implement graceful shutdown for all components
5. Write integration tests for 3-node cluster

**Deliverables**:
- `crates/nexora-risingwave/src/distributed_library.rs`
- Updated `crates/nexora-app/src/main.rs`
- Integration test: `tests/distributed_library_cluster_test.rs`
- End-to-end test script: `scripts/test-distributed-library.sh`

**Test Scenarios**:
- Start 3-node cluster, verify Meta leader election
- Execute DDL on any node, verify catalog sync
- Insert data, query from different Frontend nodes
- Kill Meta leader, verify new leader elected
- Kill Compute node, verify workload redistribution

### Day 5: Documentation and Hardening

**Goal**: Production readiness and operator documentation

**Tasks**:
1. Add failure recovery mechanisms (Meta failover, Compute restart)
2. Implement metrics for cluster health
3. Write operator guide for distributed library deployment
4. Add monitoring examples (Prometheus metrics)
5. Performance tuning and resource limits

**Deliverables**:
- `docs/DISTRIBUTED_LIBRARY_OPERATIONS.md`
- Health check endpoints for cluster status
- Metrics for Meta leadership, Compute load, Frontend latency
- Docker Compose example for 3-node cluster
- Kubernetes Helm chart (optional)

## CLI Design

### Single-Node Library (Current)
```bash
./nexora --library-event-streaming
```

### Distributed Library (New)
```bash
# Node 1 (Meta leader seed)
./nexora --distributed-library-event-streaming \
  --library-node-id meta-1 \
  --library-meta-addr 0.0.0.0:5690 \
  --library-meta-advertise node1:5690 \
  --library-meta-peers meta-2@node2:5690,meta-3@node3:5690

# Node 2 (Meta follower)
./nexora --distributed-library-event-streaming \
  --library-node-id meta-2 \
  --library-meta-addr 0.0.0.0:5690 \
  --library-meta-advertise node2:5690 \
  --library-meta-peers meta-1@node1:5690,meta-3@node3:5690

# Node 3 (Meta follower)
./nexora --distributed-library-event-streaming \
  --library-node-id meta-3 \
  --library-meta-addr 0.0.0.0:5690 \
  --library-meta-advertise node3:5690 \
  --library-meta-peers meta-1@node1:5690,meta-2@node2:5690
```

## Configuration File Approach (Alternative)

```yaml
# config/cluster-library.yaml
cluster:
  nodes:
    - id: meta-1
      host: node1.local
      meta_port: 5690
      frontend_port: 4566
      compute_port: 5688
    - id: meta-2
      host: node2.local
      meta_port: 5690
      frontend_port: 4566
      compute_port: 5688
    - id: meta-3
      host: node3.local
      meta_port: 5690
      frontend_port: 4566
      compute_port: 5688

event_streaming:
  mode: distributed-library
  this_node: meta-1  # Override per node
  meta:
    backend: etcd  # or "sqlite" for single-node
    etcd_endpoints:
      - http://etcd1:2379
      - http://etcd2:2379
      - http://etcd3:2379
  compute:
    parallelism: auto  # num_cpus::get()
```

Startup:
```bash
./nexora --config cluster-library.yaml --library-node-id meta-1
```

## Failure Scenarios and Recovery

### Meta Leader Failure
1. **Detection**: Remaining Meta nodes detect leader timeout
2. **Election**: Raft elects new leader from followers
3. **Promotion**: New leader takes over catalog writes
4. **Duration**: <5 seconds typical

**Client Impact**: Queries succeed (Frontend can read from any Meta), DDL retries automatically

### Compute Node Failure
1. **Detection**: Meta detects missing heartbeat (10s)
2. **Rescheduling**: Meta reassigns fragments to healthy Compute nodes
3. **Recovery**: State rebuilt from Hummock storage

**Client Impact**: In-flight queries fail, automatic retry succeeds

### Frontend Node Failure
1. **Detection**: Nexora graph cluster heartbeat (existing mechanism)
2. **Client Retry**: HTTP clients retry on different node
3. **Load Balancer**: If using LB, automatic failover

**Client Impact**: Connection error, retry succeeds

### Network Partition
1. **Split Brain Prevention**: Raft quorum (2 out of 3 Meta nodes required)
2. **Minority Partition**: Meta minority cannot commit DDL
3. **Majority Partition**: Continues operating normally

**Resolution**: Heal partition, minority Meta rejoins via Raft catch-up

## Resource Requirements

### Minimum (3-node cluster)
- **Per Node**: 4 CPU cores, 8 GB RAM
- **Total**: 12 cores, 24 GB RAM
- **Network**: 1 Gbps between nodes

### Recommended (3-node production)
- **Per Node**: 8 CPU cores, 16 GB RAM
- **Total**: 24 cores, 48 GB RAM
- **Network**: 10 Gbps between nodes
- **Storage**: SSD for Hummock state

## Comparison: Library Modes

| Feature | Single-Node Library | Distributed Library |
|---------|-------------------|-------------------|
| **Deployment** | 1 binary | 3+ binaries (same binary, different config) |
| **HA** | None | Meta Raft (3-node quorum) |
| **Compute Scale** | Single process | Distributed across nodes |
| **State Storage** | Local disk or S3 | Shared S3/MinIO |
| **Catalog** | In-memory | Etcd (shared) |
| **Suitable For** | Dev, single-server prod | Production clusters |
| **Complexity** | Low | Medium |

## Migration Path

### From Single-Node Library to Distributed Library

1. **Backup current data** (Hummock snapshot to S3)
2. **Deploy etcd cluster** (3 nodes)
3. **Update configuration** to distributed-library mode
4. **Start Meta node 1** (seed node, waits for quorum)
5. **Start Meta nodes 2 and 3** (join cluster)
6. **Restore Hummock snapshot** from S3
7. **Verify catalog consistency** across Meta nodes

**Downtime**: ~10 minutes (for catalog migration)

### Rollback Plan
1. Stop all distributed library nodes
2. Revert configuration to single-node library
3. Restore from Hummock snapshot (S3)
4. Start single-node library instance

## Testing Strategy

### Unit Tests
- Meta cluster Raft protocol
- Frontend load balancing
- Compute node registration

### Integration Tests
- 3-node cluster startup/shutdown
- Leader election and failover
- Query execution across nodes
- DDL propagation

### Chaos Tests
- Kill Meta leader during DDL
- Kill Compute node during query
- Network partition and heal
- Slow network (latency injection)

### Performance Tests
- Query throughput (vs single-node)
- Latency percentiles (p50, p95, p99)
- DDL commit time
- Cluster scale-out (3 → 5 → 7 nodes)

## Open Questions

1. **Shared Storage**: Require S3/MinIO for Hummock, or support local disk per node?
   - **Recommendation**: Require S3 for production, allow local for testing

2. **Frontend Stickiness**: Should clients stick to one Frontend, or round-robin?
   - **Recommendation**: Round-robin for load balance, no session state

3. **Compute Specialization**: Should some Compute nodes handle specific workloads?
   - **Recommendation**: Phase 2 feature, start with homogeneous nodes

4. **Zero-Downtime Upgrade**: How to upgrade cluster without downtime?
   - **Recommendation**: Rolling upgrade (one Meta at a time)

## Success Criteria

- [ ] 3-node cluster starts successfully
- [ ] Meta leader election completes in <5s
- [ ] DDL executed on any node propagates to all
- [ ] Query load distributes across Compute nodes
- [ ] Meta leader failure recovers in <10s
- [ ] Compute node failure handled gracefully
- [ ] End-to-end test script passes consistently
- [ ] Documentation covers deployment and operations

## Next Steps After Completion

1. **CLI Simplification** (2-3 days): Profile-based configuration
2. **Monitoring Dashboard**: Grafana + Prometheus integration
3. **Auto-Scaling**: Dynamic Compute node addition/removal
4. **Kubernetes Operator**: Automated cluster lifecycle management

---

**Document Version**: 1.0  
**Last Updated**: 2026-07-29  
**Status**: Planning Phase
