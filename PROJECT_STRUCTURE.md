# Nexora Project Structure

## Overview
Nexora is a distributed graph database with Cypher and SQL query support, built in Rust.

## Directory Structure

```
nexora/
├── archive/                      # Local archives (not in git)
│   ├── dev-artifacts/           # Development temporary files
│   ├── docs-history/            # Historical documents
│   └── phase1-complete/         # Completed Phase 1 work
│
├── benches/                      # Workspace-level benchmarks
│
├── bin/                          # Binary utilities
│
├── config/                       # Configuration templates
│   ├── examples/                # Configuration examples
│   │   ├── nexora.dev.toml     # Development config
│   │   ├── nexora.prod.toml    # Production config
│   │   └── nexora.cluster.toml # Cluster config
│   └── docker/                  # Docker configurations
│       └── docker-compose.yml
│
├── crates/                       # Rust workspace crates
│   ├── nexora-app/              # Main HTTP API server
│   ├── nexora-cli/              # CLI client (binary: nex)
│   ├── nexora-mcp/              # MCP server (binary: nexora-mcp)
│   ├── nexora-core/             # Core graph engine
│   ├── nexora-cypher/           # Cypher query executor
│   ├── nexora-sql/              # SQL to Cypher translator
│   ├── nexora-zenoh/            # Distributed coordination
│   ├── nexora-raft/             # Raft consensus
│   ├── nexora-persistor-rocksdb/ # RocksDB persistence
│   ├── nexora-storage/          # Storage abstractions
│   ├── nexora-stream/           # Stream processing
│   ├── nexora-standing-query/   # Standing queries
│   ├── nexora-output/           # Output formatters
│   ├── nexora-recipe/           # Recipe system
│   ├── nexora-client/           # Client library
│   ├── nexora-pgwire/           # PostgreSQL wire protocol
│   ├── nexora-barrier/          # Distributed barriers
│   ├── nexora-fixpoint/         # Fixpoint computations
│   ├── nexora-hnsw/             # Vector similarity search
│   ├── nexora-fragment/         # Query fragments
│   ├── nexora-id/               # Node/edge identifiers
│   ├── nexora-value/            # Value types
│   ├── nexora-serialization/    # Serialization (FlatBuffers)
│   ├── nexora-language/         # Expression evaluator
│   ├── nexora-udf/              # User-defined functions
│   └── nexora-bench/            # Benchmark utilities
│
├── deploy/                       # Deployment configurations
│   └── k8s/                     # Kubernetes manifests
│
├── docs/                         # Documentation
│   ├── INDEX.md                 # Documentation index
│   ├── api/                     # API documentation
│   ├── architecture/            # Architecture docs
│   ├── design/                  # Design documents
│   ├── guides/                  # User guides
│   └── production-planning/     # Production planning
│
├── examples/                     # Example code
│
├── fbs/                          # FlatBuffers schemas
│
├── grafana/                      # Grafana dashboards
│   └── dashboards/
│
├── scripts/                      # Utility scripts
│   ├── build/                   # Build scripts
│   │   ├── build-release.sh    # Build release binaries
│   │   └── build-docker.sh     # Build Docker image
│   ├── deploy/                  # Deployment scripts
│   ├── test/                    # Test scripts
│   │   └── run-tests.sh        # Run all tests
│   └── dev/                     # Development scripts
│       ├── start.sh            # Start development server
│       └── stop.sh             # Stop development server
│
├── sdk/                          # Client SDKs
│   ├── js/                      # JavaScript SDK
│   ├── python/                  # Python SDK
│   └── typescript/              # TypeScript SDK
│
├── target/                       # Cargo build output
│
├── ui/                           # Web UI (React)
│
├── Cargo.toml                    # Workspace configuration
├── Cargo.lock                    # Dependency lock file
├── Dockerfile                    # Docker build file
├── README.md                     # Project README
├── QUICKSTART.md                # Quick start guide
├── CONTRIBUTING.md              # Contribution guidelines
├── LICENSE                       # Apache 2.0 License
└── CHANGELOG.md                 # Version history
```

## Executables

Nexora provides three main executables:

### 1. nexora (Main Server)
- **Location**: `crates/nexora-app/`
- **Binary**: `nexora`
- **Purpose**: HTTP API server, main entry point for the database
- **Build**: `cargo build --release --bin nexora`
- **Run**: `./target/release/nexora --help`

### 2. nex (CLI Client)
- **Location**: `crates/nexora-cli/`
- **Binary**: `nex`
- **Purpose**: Command-line client for interacting with Nexora
- **Build**: `cargo build --release --bin nex`
- **Run**: `./target/release/nex --help`

### 3. nexora-mcp (MCP Server)
- **Location**: `crates/nexora-mcp/`
- **Binary**: `nexora-mcp`
- **Purpose**: Model Context Protocol server for AI agents
- **Build**: `cargo build --release --bin nexora-mcp`
- **Run**: `./target/release/nexora-mcp`

## Configuration

Configuration files are located in `config/examples/`:
- `nexora.dev.toml` - Development (relaxed security, verbose logging)
- `nexora.prod.toml` - Production (strict security, audit logging)
- `nexora.cluster.toml` - Cluster setup (high availability)

Copy the appropriate template to `nexora.toml` and customize:
```bash
cp config/examples/nexora.dev.toml nexora.toml
```

## Development Workflow

### Build
```bash
# All binaries
cargo build --release

# Specific binary
cargo build --release --bin nexora

# With features
cargo build --release --features kafka,mqtt
```

### Test
```bash
# All tests
scripts/test/run-tests.sh

# Specific test
cargo test --test integration_tests

# With filter
scripts/test/run-tests.sh --filter distributed
```

### Run Development Server
```bash
# Using script
./START.sh

# Or directly
cargo run --release --bin nexora -- --config nexora.toml
```

### Docker
```bash
# Build image
scripts/build/build-docker.sh v0.6.0

# Run with Docker Compose
cd config/docker
docker-compose up -d
```

## Architecture

Nexora follows a layered architecture:

1. **API Layer** (`nexora-app`)
   - HTTP REST API
   - WebSocket streaming
   - PostgreSQL wire protocol (`nexora-pgwire`)

2. **Query Layer** (`nexora-cypher`, `nexora-sql`)
   - Cypher query execution
   - SQL to Cypher translation
   - Query optimization

3. **Graph Engine** (`nexora-core`)
   - Graph operations
   - Indexing (labels, properties, edges)
   - WAL (Write-Ahead Log)
   - Transaction management

4. **Storage Layer** (`nexora-storage`, `nexora-persistor-rocksdb`)
   - Persistence abstractions
   - RocksDB backend
   - Tiered storage

5. **Distribution Layer** (`nexora-zenoh`, `nexora-raft`)
   - Cluster coordination (Zenoh)
   - Consensus (Raft)
   - Replication
   - Failover

## Dependencies

### Required
- Rust 1.88+
- RocksDB (via rust-rocksdb)

### Optional (via feature flags)
- Kafka (feature: `kafka`)
- MQTT (feature: `mqtt`)
- WebSocket streaming (feature: `websocket`)
- AWS Kinesis (feature: `kinesis`)
- Zenoh clustering (feature: `zenoh`)
- WASM UDFs (feature: `wasm`)
- OpenTelemetry (feature: `otel`)

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

Apache 2.0 - See [LICENSE](../LICENSE) for details.
