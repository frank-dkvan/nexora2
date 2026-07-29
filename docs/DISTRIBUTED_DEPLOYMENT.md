# Distributed Library Mode Deployment Guide

## Overview

Distributed library mode runs a multi-node RisingWave cluster **in-process** within each nexora-app instance. This provides high availability and horizontal scaling without external binary dependencies.

**Key Benefits**:
- No external RisingWave binaries to manage
- Raft consensus for automatic leader election
- Load-balanced query routing across Frontend nodes
- Distributed fragment scheduling across Compute nodes
- Simple TOML configuration

**Minimum Requirements**:
- 3 nodes (for Raft quorum)
- 2GB RAM per node
- Persistent storage for Meta catalog (etcd/sqlite)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    3-Node Cluster                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Node 1 (meta-1)              Node 2 (meta-2)                │
│  ├─ Meta Leader               ├─ Meta Follower               │
│  ├─ Frontend                  ├─ Frontend                    │
│  └─ Compute (8 workers)       └─ Compute (8 workers)         │
│                                                               │
│                    Node 3 (meta-3)                            │
│                    ├─ Meta Follower                           │
│                    ├─ Frontend                                │
│                    └─ Compute (8 workers)                     │
│                                                               │
│  Total: 24 workers, load-balanced query routing              │
└─────────────────────────────────────────────────────────────┘
```

## Configuration

### Node 1 (meta-1)

Create `nexora-node1.toml`:

```toml
[server]
host = "0.0.0.0"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/node1/graph"

[event_streaming]
enabled = true

[event_streaming.distributed]
enabled = true
node_id = "meta-1"
data_dir = "/data/nexora/node1/risingwave"

[event_streaming.distributed.meta]
listen_addr = "0.0.0.0:5690"
advertise_addr = "node1.example.com:5690"
raft_peers = [
    "meta-2@node2.example.com:5690",
    "meta-3@node3.example.com:5690"
]
backend = "sqlite"
sqlite_path = "/data/nexora/node1/risingwave/meta.db"
election_timeout_ms = 3000
heartbeat_interval_ms = 1000

[event_streaming.distributed.frontend]
listen_addr = "0.0.0.0:4566"

[event_streaming.distributed.compute]
listen_addr = "0.0.0.0:5688"
parallelism = 8
```

### Node 2 (meta-2)

Create `nexora-node2.toml`:

```toml
[server]
host = "0.0.0.0"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/node2/graph"

[event_streaming]
enabled = true

[event_streaming.distributed]
enabled = true
node_id = "meta-2"
data_dir = "/data/nexora/node2/risingwave"

[event_streaming.distributed.meta]
listen_addr = "0.0.0.0:5690"
advertise_addr = "node2.example.com:5690"
raft_peers = [
    "meta-1@node1.example.com:5690",
    "meta-3@node3.example.com:5690"
]
backend = "sqlite"
sqlite_path = "/data/nexora/node2/risingwave/meta.db"
election_timeout_ms = 3000
heartbeat_interval_ms = 1000

[event_streaming.distributed.frontend]
listen_addr = "0.0.0.0:4566"

[event_streaming.distributed.compute]
listen_addr = "0.0.0.0:5688"
parallelism = 8
```

### Node 3 (meta-3)

Create `nexora-node3.toml`:

```toml
[server]
host = "0.0.0.0"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/node3/graph"

[event_streaming]
enabled = true

[event_streaming.distributed]
enabled = true
node_id = "meta-3"
data_dir = "/data/nexora/node3/risingwave"

[event_streaming.distributed.meta]
listen_addr = "0.0.0.0:5690"
advertise_addr = "node3.example.com:5690"
raft_peers = [
    "meta-1@node1.example.com:5690",
    "meta-2@node2.example.com:5690"
]
backend = "sqlite"
sqlite_path = "/data/nexora/node3/risingwave/meta.db"
election_timeout_ms = 3000
heartbeat_interval_ms = 1000

[event_streaming.distributed.frontend]
listen_addr = "0.0.0.0:4566"

[event_streaming.distributed.compute]
listen_addr = "0.0.0.0:5688"
parallelism = 8
```

## Deployment Steps

### 1. Build with Library Feature

```bash
cargo build --release --features library
```

### 2. Create Data Directories

On each node:

```bash
# Node 1
sudo mkdir -p /data/nexora/node1/{graph,risingwave}
sudo chown nexora:nexora /data/nexora/node1

# Node 2
sudo mkdir -p /data/nexora/node2/{graph,risingwave}
sudo chown nexora:nexora /data/nexora/node2

# Node 3
sudo mkdir -p /data/nexora/node3/{graph,risingwave}
sudo chown nexora:nexora /data/nexora/node3
```

### 3. Start Cluster

Start all nodes **simultaneously** (within election timeout):

```bash
# Node 1
./target/release/nexora --config nexora-node1.toml

# Node 2
./target/release/nexora --config nexora-node2.toml

# Node 3
./target/release/nexora --config nexora-node3.toml
```

### 4. Verify Cluster Status

Check that a leader was elected:

```bash
curl http://node1.example.com:8080/api/event-streaming/cluster/status
```

Expected response:
```json
{
  "node_id": "meta-1",
  "is_leader": true,
  "raft_term": 1,
  "healthy_frontends": 3,
  "healthy_computes": 3,
  "total_workers": 24
}
```

Check all Compute nodes:

```bash
curl http://node1.example.com:8080/api/event-streaming/cluster/nodes
```

Expected response:
```json
{
  "compute_nodes": [
    {
      "node_id": "meta-1",
      "listen_addr": "0.0.0.0:5688",
      "health": "Healthy",
      "parallelism": 8
    },
    {
      "node_id": "meta-2",
      "listen_addr": "0.0.0.0:5688",
      "health": "Healthy",
      "parallelism": 8
    },
    {
      "node_id": "meta-3",
      "listen_addr": "0.0.0.0:5688",
      "health": "Healthy",
      "parallelism": 8
    }
  ]
}
```

## Backend Options

### SQLite (Default, Single-Node Catalog)

Best for: Development, testing, single-datacenter deployments

```toml
[event_streaming.distributed.meta]
backend = "sqlite"
sqlite_path = "/data/nexora/node1/risingwave/meta.db"
```

**Limitations**: Each node has its own catalog file. Raft keeps them consistent, but catalog is not shared across datacenters.

### Etcd (Shared Catalog)

Best for: Production, multi-datacenter deployments

```toml
[event_streaming.distributed.meta]
backend = "etcd"
etcd_endpoints = ["http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379"]
```

**Requirements**: External etcd cluster (3+ nodes recommended)

### Memory (Ephemeral, Testing Only)

```toml
[event_streaming.distributed.meta]
backend = "memory"
```

**Warning**: All catalog state lost on restart. Only for integration tests.

## Health Monitoring

### Check Meta Leader

```bash
curl http://node1.example.com:8080/api/event-streaming/cluster/status | jq '.is_leader'
```

### Check Frontend Pool

```bash
curl http://node1.example.com:8080/api/event-streaming/cluster/status | jq '.healthy_frontends'
```

### Check Compute Nodes

```bash
curl http://node1.example.com:8080/api/event-streaming/cluster/status | jq '.healthy_computes'
```

### Check Total Workers

```bash
curl http://node1.example.com:8080/api/event-streaming/cluster/status | jq '.total_workers'
```

## Troubleshooting

### No Leader Elected

**Symptom**: All nodes show `is_leader: false`

**Cause**: Nodes cannot reach each other or started too far apart

**Fix**:
1. Verify network connectivity between nodes
2. Check firewall rules (port 5690 must be open)
3. Restart all nodes within election timeout (default 3 seconds)

```bash
# Check connectivity
telnet node2.example.com 5690
telnet node3.example.com 5690
```

### Split Brain

**Symptom**: Multiple nodes claim to be leader

**Cause**: Network partition

**Fix**:
1. Stop all nodes
2. Ensure network is stable
3. Restart all nodes simultaneously

### Compute Node Unavailable

**Symptom**: `health: "Unavailable"` in cluster nodes response

**Cause**: Node crashed or heartbeat timeout

**Fix**:
1. Check node logs for errors
2. Verify node is running
3. If node restarted, wait 30 seconds for heartbeat recovery

### Frontend Node Degraded

**Symptom**: `health: "Degraded"` in cluster status

**Cause**: High query load or slow response

**Fix**:
1. Check node CPU/memory usage
2. Scale horizontally (add more nodes)
3. Adjust `parallelism` in compute config

## Performance Tuning

### Election Timeout

Default: 3000ms

```toml
[event_streaming.distributed.meta]
election_timeout_ms = 5000  # Increase for high-latency networks
```

**Trade-off**: Higher timeout = slower failover, but more stable in unstable networks

### Heartbeat Interval

Default: 1000ms

```toml
[event_streaming.distributed.meta]
heartbeat_interval_ms = 500  # Decrease for faster failure detection
```

**Trade-off**: Lower interval = faster failure detection, but more network traffic

### Compute Parallelism

Default: Number of CPU cores

```toml
[event_streaming.distributed.compute]
parallelism = 16  # Override for high-throughput workloads
```

**Recommendation**: Set to 2x CPU cores for IO-bound workloads, 1x for CPU-bound

## Security

### Network Isolation

Bind Meta/Frontend/Compute to internal network only:

```toml
[event_streaming.distributed.meta]
listen_addr = "10.0.1.10:5690"  # Internal IP only

[event_streaming.distributed.frontend]
listen_addr = "10.0.1.10:4566"

[event_streaming.distributed.compute]
listen_addr = "10.0.1.10:5688"
```

### TLS (Future)

TLS support for Meta/Frontend/Compute communication is planned for Phase 4.

## Migration

### From Single-Node Library Mode

1. Stop single-node instance
2. Create 3-node config files
3. Copy data directory to all nodes
4. Start 3-node cluster
5. Verify cluster status

### From Embedded Mode (External Binaries)

1. Stop all RisingWave processes
2. Rebuild with `--features library`
3. Create distributed config
4. Start cluster
5. Recreate sources and materialized views (catalog is not compatible)

## Next Steps

- Set up monitoring (Prometheus metrics coming in Phase 4)
- Configure backups for Meta catalog
- Test failover scenarios
- Tune performance for your workload

---

**Last Updated**: 2026-07-29
**Phase**: 2 - Distributed Library Mode (Complete)
