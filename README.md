# Nexora

> Streaming graph database — Actor-per-Node architecture, Cypher + SQL, real-time standing queries, and distributed graph computation.

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88+-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)
[![Tests](https://img.shields.io/badge/tests-1590-brightgreen.svg)](#)

---

## What is Nexora?

Nexora is an original streaming graph database designed for real-time data pipelines. Inspired by knowledge graph computation, stream processing, array databases, and modern distributed systems, it implements a fine-grained Actor-per-Node architecture where every graph node runs as an independent tokio task, enabling fine-grained concurrency and incremental computation.

### Key Features

| Feature | Description |
|---------|-------------|
| **Actor-per-Node** | Each graph node runs as an isolated tokio task with its own mailbox |
| **Cypher + SQL** | Query with OpenCypher (97% coverage) or SQL (auto-translated to Cypher) |
| **Standing Queries** | Register persistent pattern matches that trigger on data changes |
| **Event Sourcing + WAL** | Every mutation is journaled; crash recovery via WAL replay |
| **Time Travel** | Query graph state at any historical point in time |
| **Vector Search** | HNSW index for approximate nearest-neighbor similarity search |
| **Materialized Views** | Auto-maintained aggregates linked to Standing Queries |
| **Distributed** *(experimental)* | Horizontal sharding with quorum replication (RF configurable), epoch fencing, state transfer, catch-up barrier, distributed Cypher planner (cross-partition joins, aggregations, UNION, WITH pipelines), tiered storage (RocksDB/S3), shard rebalancing, dynamic membership. The control plane runs real openraft consensus; the data plane uses quorum write + epoch fencing (a deliberate primary-backup design, not data-plane consensus). Automatic failover **is** wired into the runtime (failure detector + shard promotion) and Merkle anti-entropy self-repair is wired but **opt-in** (off by default, enable with `--anti-entropy-secs`). Consistency is exercised by a chaos-test + Merkle oracle suite. **Not yet production-ready** — the gap is *validation, not implementation*: the cluster has never been verified under real multi-process fault injection (network partition / kill-owner) and no 72h+ soak has run yet. The separate `--raft-port` path is a log-shipping skeleton, not full Raft consensus. See [HA_ROADMAP](docs/production-planning/HA_ROADMAP.md) and [PRODUCTION_GAP_REASSESSMENT_2026-07-20](docs/production-planning/PRODUCTION_GAP_REASSESSMENT_2026-07-20.md). Single-node durable mode is the supported production configuration today. |
| **UDF** | Native Rust and WebAssembly UDFs. Python UDFs run in a subprocess with rlimits (CPU/memory/fd/no-fork/no-write) but are **not** syscall-sandboxed — they can still read files and open network sockets, so treat Python UDF registration as trusted-code-only. |
| **Streaming Ingest** | Kinesis, Pulsar, and file-based ingestion. Kafka output requires the `kafka` build feature and is not compiled by default. |
| **Security** | HMAC-SHA256 auth, RBAC, TLS, rate limiting, WAL encryption, audit logging |

---

## Quick Start (30 seconds)

### 1. Build

```bash
git clone <repo-url> && cd nexora
cargo build --release
```

This builds three executables:
- `nexora` - Main HTTP API server
- `nex` - CLI client for queries
- `nexora-mcp` - MCP server for AI agents

### 2. Launch

```bash
# Development mode (default config)
./target/release/nexora

# Or with custom config
./target/release/nexora --config config/examples/nexora.dev.toml
```

### 3. Your first Cypher query

```bash
# Create a node
curl -s -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE (p:Person {name: \"Alice\", age: 30}) RETURN p"}' | jq .

# Query it back
curl -s -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age"}' | jq .
```

### 4. Open the Dashboard

Visit **http://localhost:8080/dashboard** in your browser for:
- Graph visualization
- Cypher query editor
- Standing Query management
- Data ingestion interface

### 5. Explore the API

Interactive Swagger UI: **http://localhost:8080/api/v2/docs**

---

## Run Modes

| Mode | Command | Use Case |
|------|---------|----------|
| **Lite Ephemeral** | `--no-rocksdb` | Dev/testing, data lost on restart |
| **Single Durable** | *(default)* | Single-node production, RocksDB + WAL |
| **Clustered HA** *(experimental — see Distributed row)* | `--cluster --node-id n1 --replication-factor 3` | Multi-node with quorum replication, automatic failover, state transfer, distributed Cypher (data replicated RF times). Functionally wired but **not yet validated under real fault injection** — treat as experimental, not production. See [cluster ops guide](docs/cluster-ops.md) |

---

## Configuration

Configuration templates are available in `config/examples/`:

```bash
# Development (relaxed security, verbose logging)
cp config/examples/nexora.dev.toml nexora.toml

# Production (strict security, audit logging)
cp config/examples/nexora.prod.toml nexora.toml

# Cluster (high availability, distributed)
cp config/examples/nexora.cluster.toml nexora.toml
```

Configuration sources (highest priority first):

1. **CLI arguments** — `./nexora --help`
2. **Environment variables** — `NEXORA_*` prefix (e.g., `NEXORA_AUTH_SECRET`)
3. **TOML file** — `nexora.toml` or `--config path/to/config.toml`
4. **Defaults**

### Common environment variables

| Variable | Description |
|----------|-------------|
| `NEXORA_AUTH_SECRET` | HMAC-SHA256 signing key for auth tokens |
| `NEXORA_ENCRYPTION_KEY` | AES-256 key for WAL encryption (64 hex chars) |
| `NEXORA_PROFILE` | Run mode: `lite-ephemeral`, `single-durable`, `clustered` |
| `NEXORA_STRICT_SECURITY` | Set to `true` to refuse starting with CRITICAL security issues |
| `NEXORA_REPLICATION_FACTOR` | Quorum replication factor (default 1 = no replication) |
| `NEXORA_STORAGE_BACKEND` | Storage backend: `memory`, `local`, `s3` (default `local`) |
| `RUST_LOG` | Log level (e.g., `info`, `debug`, `nexora_core=trace`) |

---

## SDKs

| Language | Location | Install |
|----------|----------|---------|
| **Python** | `sdk/python/` | `pip install nexora` |
| **TypeScript** | `sdk/typescript/` | `npm install nexora` |
| **Rust** | `crates/nexora-client/` | `cargo add nexora-client` |

### Python example

```python
from nexora_rs import NexoraClient

client = NexoraClient("http://localhost:8080")
result = client.cypher("MATCH (n) RETURN n LIMIT 10")
print(result)
```

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    nexora-app (HTTP)                    │
│  Axum REST API · WebSocket · Dashboard · OpenAPI      │
├──────────────────────────────────────────────────────┤
│  nexora-cypher  │  nexora-sql  │  nexora-standing-query  │
│  (Cypher exec) │  (SQL→Cypher) │  (pattern matching)  │
├──────────────────────────────────────────────────────┤
│                    nexora-core                          │
│  GraphService · GraphShard · WAL · EventSourcing     │
│  PropertyIndex · LabelIndex · QueryOptimizer · MV    │
├──────────────┬───────────────┬────────────────────────┤
│ nexora-persistor│ nexora-storage │ nexora-zenoh           │
│  (RocksDB)    │ (Tiered/S3)  │ (Distributed transport)│
├──────────────┴───────────────┴────────────────────────┤
│  nexora-hnsw │ nexora-udf │ nexora-recipe │ nexora-stream │
│  (Vector)   │ (Wasm/Py)│ (Pipelines)  │ (Kafka/Kinesis)│
└──────────────────────────────────────────────────────┘
```

### Workspace Crates (24)

| Layer | Crates |
|-------|--------|
| **Core** | `nexora-core`, `nexora-id`, `nexora-value` |
| **Query** | `nexora-cypher`, `nexora-sql`, `nexora-language`, `nexora-standing-query` |
| **Storage** | `nexora-persistor-rocksdb`, `nexora-storage`, `nexora-fragment`, `nexora-barrier`, `nexora-serialization` |
| **Streaming** | `nexora-stream`, `nexora-ingest`, `nexora-fixpoint`, `nexora-output` |
| **Advanced** | `nexora-hnsw`, `nexora-udf`, `nexora-recipe`, `nexora-raft` |
| **Distributed** | `nexora-zenoh` |
| **App** | `nexora-app`, `nexora-client`, `nexora-bench` |

---

## API Endpoints (highlights)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v2/query/cypher` | Execute Cypher query |
| `POST` | `/api/v2/query/sql` | Execute SQL query |
| `POST` | `/api/v2/query/explain` | Get query execution plan |
| `GET` | `/api/v2/graph/history` | Time-travel query |
| `GET/PUT` | `/api/v2/graph/node/{qid}/property/{key}` | Node property CRUD |
| `GET/POST` | `/api/v2/graph/node/{qid}/edges` | Edge operations |
| `POST` | `/api/v2/vector/search` | Vector similarity search |
| `POST` | `/api/v2/ingest/file` | Start file ingestion |
| `GET/POST` | `/api/v2/standing-query` | Standing Query management |
| `GET/POST` | `/api/v2/materialized-views` | Materialized view management |
| `GET/POST` | `/api/v2/udf/...` | UDF management |
| `GET` | `/api/v2/health` | Health check (with uptime) |
| `GET` | `/metrics` | Prometheus metrics |

Full OpenAPI spec: **http://localhost:8080/api/v2/openapi.json**

---

## Deployment

### Docker

```bash
docker build -t nexora .
docker run -p 8080:8080 -v nexora-data:/app/nexora-data nexora
```

### Kubernetes

```bash
kubectl apply -f deploy/k8s/
```

See [`deploy/`](deploy/) for Docker Compose, Kubernetes manifests, and Grafana dashboards.

---

## Development

### Quick Start

```bash
# Build all crates
cargo build --workspace

# Run tests (1200+ tests)
cargo test --workspace

# Lint
cargo clippy --workspace --all-targets -- -D warnings

# Format check
cargo fmt --all -- --check

# Full CI validation (local)
./scripts/local-ci.sh

# Build the dashboard UI (optional)
cd ui && npm install && npm run build
```

### Contributing

**📖 Read First**: [**Development Standards & Discipline**](docs/DEVELOPMENT_STANDARDS.md)

Required before every commit:
- ✅ `cargo fmt --all` — Zero formatting warnings
- ✅ `cargo clippy --all-targets -- -D warnings` — Zero clippy warnings  
- ✅ `cargo test --workspace` — All tests pass
- ✅ Conventional Commits format (`feat:`, `fix:`, etc.)

**Install pre-commit hook** (auto-check before commit):
```bash
cp scripts/git-hooks/pre-commit .git/hooks/
chmod +x .git/hooks/pre-commit
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

---

## Documentation

| Document | Description |
|----------|-------------|
| **[`docs/DEVELOPMENT_STANDARDS.md`](docs/DEVELOPMENT_STANDARDS.md)** | **Development standards & discipline (READ FIRST for contributors)** |
| [**Project Structure**](PROJECT_STRUCTURE.md) | Complete project layout and architecture |
| [**Documentation Index**](docs/INDEX.md) | Full documentation catalog |
| [`QUICKSTART.md`](QUICKSTART.md) | Detailed getting started guide |
| [`docs/api/cypher-support.md`](docs/api/cypher-support.md) | Cypher clause support matrix |
| [`docs/guides/PROPERTY_INDEX_GUIDE.md`](docs/guides/PROPERTY_INDEX_GUIDE.md) | Indexing and query optimization |
| [`docs/guides/BENCHMARK_GUIDE.md`](docs/guides/BENCHMARK_GUIDE.md) | Performance benchmarking |
| [`docs/production-planning/PRODUCTION_READINESS_GAPS.md`](docs/production-planning/PRODUCTION_READINESS_GAPS.md) | Production deployment checklist |
| [`docs/production-planning/PRODUCTION_DEPLOYMENT_PLAN.md`](docs/production-planning/PRODUCTION_DEPLOYMENT_PLAN.md) | Deployment strategies |
| [`docs/production-planning/DISTRIBUTED_EVOLUTION.md`](docs/production-planning/DISTRIBUTED_EVOLUTION.md) | Distributed architecture overview |
| [`docs/production-planning/HA_ROADMAP.md`](docs/production-planning/HA_ROADMAP.md) | HA implementation roadmap |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Contribution guidelines |

---

## License

Apache-2.0 — see [LICENSE](LICENSE).
