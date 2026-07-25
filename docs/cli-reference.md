# CLI Reference

Complete reference for all Nexora-RS command-line flags, organized by functional group.

## Synopsis

```
nexora-app [OPTIONS]
```

## Run Profiles

Nexora-RS supports three run profiles. The profile is auto-derived from flags unless explicitly set with `--profile`.

| Profile | Description | Triggered By |
|---------|-------------|-------------|
| `lite-ephemeral` | In-memory, no persistence | `--no-rocksdb` |
| `single-durable` | RocksDB + WAL (default) | Default (no flags) |
| `clustered` | Distributed multi-node | `--cluster` |

```bash
# Explicit profile
nexora-app --profile single-durable

# Auto-derived profiles
nexora-app --no-rocksdb                    # lite-ephemeral
nexora-app                                  # single-durable (default)
nexora-app --cluster                        # clustered
```

---

## Basic

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--host` | | `0.0.0.0` | HTTP listen address |
| `--port` | `-p` | `8080` | HTTP listen port |

**Example:**
```bash
nexora-app --host 127.0.0.1 --port 9090
```

---

## Storage

Controls the persistence backend and Write-Ahead Log (WAL) for crash recovery.

| Flag | Default | Description |
|------|---------|-------------|
| `--no-rocksdb` | `false` | Use in-memory storage instead of RocksDB (data lost on restart) |
| `--rocksdb-path` | `./nexora-data` | RocksDB data directory |
| `--no-wal` | `false` | Disable WAL (not recommended for production) |
| `--wal-dir` | `./nexora-data/wal` | WAL directory for crash recovery |

> **Note:** WAL requires RocksDB. Using `--no-rocksdb` with WAL enabled will cause an error. Use `--no-wal` if you want in-memory mode.

**Examples:**
```bash
# Default durable mode
nexora-app --rocksdb-path /var/lib/nexora --wal-dir /var/lib/nexora/wal

# In-memory ephemeral mode (testing)
nexora-app --no-rocksdb --no-wal
```

---

## Cluster

Enables distributed multi-node operation. Requires Zenoh transport for inter-node communication.

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster` | `false` | Enable cluster mode (distributed multi-node operation) |
| `--node-id` | `node-{port}` | This node's unique ID in the cluster (e.g., `node-1`) |
| `--cluster-listen-addr` | `0.0.0.0:{port+1000}` | Address for inter-node graph operations (e.g., `127.0.0.1:7000`) |
| `--cluster-heartbeat-addr` | `0.0.0.0:{port+1001}` | Address for heartbeat protocol (e.g., `127.0.0.1:7001`) |
| `--peer` | _(none)_ | Peer node to bootstrap with. Format: `node_id:graph_addr:heartbeat_addr`. Can be specified multiple times. |

**Example: 3-node cluster**

Node 1:
```bash
nexora-app --cluster \
  --node-id node-1 \
  --cluster-listen-addr 0.0.0.0:9000 \
  --cluster-heartbeat-addr 0.0.0.0:9001 \
  --peer node-2:node-2:9000:node-2:9001 \
  --peer node-3:node-3:9000:node-3:9001
```

Node 2:
```bash
nexora-app --cluster \
  --node-id node-2 \
  --cluster-listen-addr 0.0.0.0:9000 \
  --cluster-heartbeat-addr 0.0.0.0:9001 \
  --peer node-1:node-1:9000:node-1:9001 \
  --peer node-3:node-3:9000:node-3:9001
```

---

## Raft Consensus

Opt-in strong consistency via Raft consensus. Requires `--cluster` to be enabled.

| Flag | Default | Description |
|------|---------|-------------|
| `--raft-port` | _(none)_ | TCP port for Raft RPC traffic. When set, quorum-based log replication replaces simple heartbeats. |
| `--raft-peer` | _(none)_ | Raft peer address in `host:port` format. Can be specified multiple times. |

> Quorum is calculated as `(total_nodes / 2) + 1`, where `total_nodes = raft_peers + 1` (self).

**Example: 3-node Raft cluster**

Node 1:
```bash
nexora-app --cluster \
  --node-id node-1 \
  --cluster-listen-addr 0.0.0.0:9000 \
  --cluster-heartbeat-addr 0.0.0.0:9001 \
  --raft-port 9010 \
  --raft-peer node-2:9010 \
  --raft-peer node-3:9010 \
  --peer node-2:0.0.0.0:9000:0.0.0.0:9001 \
  --peer node-3:0.0.0.0:9000:0.0.0.0:9001
```

---

## Security

Authentication, TLS, and access control.

| Flag | Default | Description |
|------|---------|-------------|
| `--require-auth` | `false` | Require JWT authentication for API endpoints |
| `--auth-secret` | _(env: `NEXORA_AUTH_SECRET`) | Secret key for HMAC-SHA256 token signing |
| `--tls-cert` | _(none)_ | TLS certificate file path (enables HTTPS) |
| `--tls-key` | _(none)_ | TLS private key file path (requires `--tls-cert`) |
| `--gen-tls-cert` | _(none)_ | Generate a self-signed TLS certificate and key in the given directory (development only, then exit) |

**Auth secret resolution order:** CLI flag > `NEXORA_AUTH_SECRET` env var > default dev secret (with warning).

**Examples:**
```bash
# Generate self-signed cert for development
nexora-app --gen-tls-cert ./tls

# Run with TLS and auth
nexora-app \
  --tls-cert ./tls/cert.pem \
  --tls-key ./tls/key.pem \
  --require-auth \
  --auth-secret "your-strong-secret-here"
```

---

## Rate Limiting

Token bucket rate limiting per client IP.

| Flag | Default | Description |
|------|---------|-------------|
| `--rate-limit` | `true` | Enable rate limiting |
| `--rate-limit-rate` | `100.0` | Requests per second per client IP |
| `--rate-limit-burst` | `200` | Maximum burst capacity per client IP |

**Examples:**
```bash
# Custom rate limits
nexora-app --rate-limit-rate 50 --rate-limit-burst 100

# Disable rate limiting (not recommended for production)
nexora-app --rate-limit false
```

---

## Kafka Streaming

Real-time streaming data ingestion via Kafka. Requires the `kafka` compile feature.

| Flag | Default | Description |
|------|---------|-------------|
| `--kafka-brokers` | _(none)_ | Kafka bootstrap servers (e.g., `localhost:9092`) |
| `--kafka-topic` | _(none)_ | Kafka topic to consume from (requires `--kafka-brokers`) |
| `--kafka-group-id` | `nexora-app-consumer` | Kafka consumer group ID |

> Build with Kafka support: `cargo build -p nexora-app --features kafka`

**Example:**
```bash
nexora-app \
  --kafka-brokers localhost:9092 \
  --kafka-topic events \
  --kafka-group-id my-consumer-group
```

---

## Storage Tiering

Hot/Warm/Cold tiered storage with automatic lifecycle management.

| Flag | Default | Description |
|------|---------|-------------|
| `--storage-backend` | `memory` | Storage backend: `memory`, `local`, or `s3` |
| `--s3-bucket` | _(none)_ | S3 bucket name (required when `--storage-backend=s3`) |
| `--s3-region` | `us-east-1` | AWS region for S3 |
| `--s3-cold-after-days` | `30` | Archive nodes to S3 after N days of inactivity |

**Examples:**
```bash
# S3 tiered storage
nexora-app \
  --storage-backend s3 \
  --s3-bucket my-nexora-bucket \
  --s3-region us-west-2 \
  --s3-cold-after-days 7

# Local tiered storage (hot/warm/cold on local disk)
nexora-app --storage-backend local
```

**Tier behavior:**

| Backend | Hot | Warm | Cold |
|---------|-----|------|------|
| `memory` (default) | — | — | — |
| `local` | `{rocksdb_path}/hot` | `{rocksdb_path}/warm` | `{rocksdb_path}/cold` |
| `s3` | Memory | S3 bucket | Memory |

---

## Encryption

Data-at-rest encryption for WAL using AES-256-GCM.

| Flag | Default | Description |
|------|---------|-------------|
| `--encrypt-wal` | `false` | Enable WAL encryption (AES-256-GCM) |
| `--encryption-key` | _(none)_ | Hex-encoded AES-256 key (64 hex characters) |
| `--encryption-key-file` | _(none)_ | Path to a file containing a hex-encoded AES-256 key |

**Key resolution order:** `--encryption-key` > `--encryption-key-file` > `NEXORA_ENCRYPTION_KEY` env var

**Examples:**
```bash
# Generate a key
openssl rand -hex 32 > encryption.key

# Use key file
nexora-app --encrypt-wal --encryption-key-file ./encryption.key

# Use key directly (less secure — visible in process list)
nexora-app --encrypt-wal --encryption-key $(cat encryption.key)

# Use environment variable
export NEXORA_ENCRYPTION_KEY=$(cat encryption.key)
nexora-app --encrypt-wal
```

> **Warning:** If the encryption key is lost, WAL data cannot be recovered. Store the key securely (e.g., Kubernetes Secret, AWS KMS, HashiCorp Vault).

---

## User-Defined Functions (UDF)

| Flag | Default | Description |
|------|---------|-------------|
| `--udf-dir` | `./udf` | Directory for UDF scripts and `.wasm` files |

The UDF directory is scanned for Wasm modules and Python scripts that can be registered as user-defined functions callable from Cypher queries.

**Example:**
```bash
nexora-app --udf-dir /opt/nexora/udf
```

---

## Profile

| Flag | Default | Description |
|------|---------|-------------|
| `--profile` | _(auto-derived)_ | Explicit run profile: `lite-ephemeral`, `single-durable`, or `clustered` |

When set explicitly, the profile is validated against other flags. For example, `--profile single-durable` with `--no-rocksdb` will fail validation.

```bash
nexora-app --profile clustered --cluster --node-id node-1
```

---

## Additional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--num-shards` | `256` | Number of graph shards |
| `--max-nodes-per-shard` | `10000` | Max nodes per shard before LRU eviction |
| `--allow-ingest-dir` | _(none)_ | Directory allowed for file ingest (security: prevents path traversal) |
| `--cors-origin` | `*` | CORS allowed origin (set to specific domain for production) |

---

## Common Deployment Scenarios

### Development (Lite Mode)
```bash
nexora-app --no-rocksdb --no-wal --port 8080
```

### Single-Node Production (Durable)
```bash
nexora-app \
  --host 0.0.0.0 \
  --port 8080 \
  --rocksdb-path /var/lib/nexora/data \
  --wal-dir /var/lib/nexora/wal \
  --require-auth \
  --auth-secret "$NEXORA_AUTH_SECRET" \
  --encrypt-wal \
  --encryption-key-file /etc/nexora/encryption.key \
  --rate-limit-rate 100 \
  --rate-limit-burst 200 \
  --cors-origin https://nexora.internal.example.com
```

### Single-Node with TLS
```bash
nexora-app \
  --tls-cert /etc/nexora/tls/cert.pem \
  --tls-key /etc/nexora/tls/key.pem \
  --require-auth \
  --auth-secret "$NEXORA_AUTH_SECRET" \
  --rocksdb-path /var/lib/nexora/data
```

### 3-Node Cluster with Raft
```bash
# Node 1
nexora-app --cluster \
  --node-id node-1 \
  --cluster-listen-addr 0.0.0.0:9000 \
  --cluster-heartbeat-addr 0.0.0.0:9001 \
  --raft-port 9010 \
  --raft-peer node-2:9010 \
  --raft-peer node-3:9010 \
  --peer node-2:node-2:9000:node-2:9001 \
  --peer node-3:node-3:9000:node-3:9001 \
  --rocksdb-path /var/lib/nexora/node-1/data \
  --wal-dir /var/lib/nexora/node-1/wal \
  --require-auth --auth-secret "$NEXORA_AUTH_SECRET"

# Node 2 (and Node 3 similarly)
nexora-app --cluster \
  --node-id node-2 \
  --cluster-listen-addr 0.0.0.0:9000 \
  --cluster-heartbeat-addr 0.0.0.0:9001 \
  --raft-port 9010 \
  --raft-peer node-1:9010 \
  --raft-peer node-3:9010 \
  --peer node-1:node-1:9000:node-1:9001 \
  --peer node-3:node-3:9000:node-3:9001 \
  --rocksdb-path /var/lib/nexora/node-2/data \
  --wal-dir /var/lib/nexora/node-2/wal \
  --require-auth --auth-secret "$NEXORA_AUTH_SECRET"
```

### Streaming Ingestion from Kafka
```bash
nexora-app \
  --kafka-brokers kafka-1:9092,kafka-2:9092 \
  --kafka-topic user-events \
  --kafka-group-id nexora-consumer \
  --rocksdb-path /var/lib/nexora/data
```

### S3 Tiered Storage with Encryption
```bash
nexora-app \
  --storage-backend s3 \
  --s3-bucket nexora-archive \
  --s3-region us-east-1 \
  --s3-cold-after-days 14 \
  --encrypt-wal \
  --encryption-key-file /etc/nexora/encryption.key \
  --rocksdb-path /var/lib/nexora/data
```

### Generate TLS Certificate (then exit)
```bash
nexora-app --gen-tls-cert ./tls-certificates
# Files created: ./tls-certificates/cert.pem, ./tls-certificates/key.pem
```

---

## Environment Variables

The following environment variables can be used instead of (or in addition to) CLI flags:

| Variable | Equivalent Flag | Description |
|----------|----------------|-------------|
| `NEXORA_AUTH_SECRET` | `--auth-secret` | Auth token signing secret |
| `NEXORA_ENCRYPTION_KEY` | `--encryption-key` | WAL encryption key |
| `RUST_LOG` | — | Log level (e.g., `info`, `debug`, `warn`) |
