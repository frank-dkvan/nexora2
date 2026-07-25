# Nexora Cluster Setup Guide

This guide demonstrates how to start a 3-node Nexora cluster using the new YAML configuration format.

## Configuration Files

Create three configuration files, one for each node:

### Node 1 Config (`cluster-node1.yaml`)

```yaml
cluster:
  name: "nexora-prod"
  total_shards: 256
  replication_factor: 3

node:
  id: "node-1"
  listen_addr: "127.0.0.1:7000"
  heartbeat_addr: "127.0.0.1:7001"

peers:
  - node_id: "node-2"
    graph_addr: "127.0.0.1:7010"
    heartbeat_addr: "127.0.0.1:7011"
  - node_id: "node-3"
    graph_addr: "127.0.0.1:7020"
    heartbeat_addr: "127.0.0.1:7021"

health:
  heartbeat_interval_secs: 5
  failure_timeout_secs: 15

replication:
  log_dir: "./data/node-1/replog"
  write_timeout_secs: 10
```

### Node 2 Config (`cluster-node2.yaml`)

```yaml
cluster:
  name: "nexora-prod"
  total_shards: 256
  replication_factor: 3

node:
  id: "node-2"
  listen_addr: "127.0.0.1:7010"
  heartbeat_addr: "127.0.0.1:7011"

peers:
  - node_id: "node-1"
    graph_addr: "127.0.0.1:7000"
    heartbeat_addr: "127.0.0.1:7001"
  - node_id: "node-3"
    graph_addr: "127.0.0.1:7020"
    heartbeat_addr: "127.0.0.1:7021"

health:
  heartbeat_interval_secs: 5
  failure_timeout_secs: 15

replication:
  log_dir: "./data/node-2/replog"
  write_timeout_secs: 10
```

### Node 3 Config (`cluster-node3.yaml`)

```yaml
cluster:
  name: "nexora-prod"
  total_shards: 256
  replication_factor: 3

node:
  id: "node-3"
  listen_addr: "127.0.0.1:7020"
  heartbeat_addr: "127.0.0.1:7021"

peers:
  - node_id: "node-1"
    graph_addr: "127.0.0.1:7000"
    heartbeat_addr: "127.0.0.1:7001"
  - node_id: "node-2"
    graph_addr: "127.0.0.1:7010"
    heartbeat_addr: "127.0.0.1:7011"

health:
  heartbeat_interval_secs: 5
  failure_timeout_secs: 15

replication:
  log_dir: "./data/node-3/replog"
  write_timeout_secs: 10
```

## Starting the Cluster

Start each node in a separate terminal:

```bash
# Terminal 1: Start node 1
cargo run -p nexora-app -- \
  --port 8080 \
  --cluster \
  --cluster-config cluster-node1.yaml \
  --rocksdb-path ./data/node-1

# Terminal 2: Start node 2
cargo run -p nexora-app -- \
  --port 8081 \
  --cluster \
  --cluster-config cluster-node2.yaml \
  --rocksdb-path ./data/node-2

# Terminal 3: Start node 3
cargo run -p nexora-app -- \
  --port 8082 \
  --cluster \
  --cluster-config cluster-node3.yaml \
  --rocksdb-path ./data/node-3
```

## Verification

Check that all nodes are healthy:

```bash
# Check node 1
curl http://localhost:8080/health

# Check node 2
curl http://localhost:8081/health

# Check node 3
curl http://localhost:8082/health
```

## Configuration Parameters

- **total_shards**: Number of logical shards (recommended: 64-256)
- **replication_factor**: Number of replicas per shard (1=no replication, 3=tolerate 1 failure)
- **heartbeat_interval_secs**: Seconds between heartbeat messages (default: 5)
- **failure_timeout_secs**: Seconds before declaring a node failed (default: 15)
- **log_dir**: Directory for durable replication log (enables incremental catch-up)

## Migration from CLI Args

The old CLI argument format is still supported but deprecated:

```bash
# Old way (still works)
cargo run -p nexora-app -- \
  --cluster \
  --node-id node-1 \
  --cluster-listen-addr 127.0.0.1:7000 \
  --cluster-heartbeat-addr 127.0.0.1:7001 \
  --peer node-2:127.0.0.1:7010:127.0.0.1:7011 \
  --replication-factor 3
```

The YAML configuration format is now recommended for production deployments.
