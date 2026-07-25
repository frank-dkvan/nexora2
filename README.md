# Nexora 2.0

> Next-generation streaming graph database with event-first architecture

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88+-orange.svg)](https://www.rust-lang.org)

---

## 🎯 What's New in Nexora 2.0?

Nexora 2.0 introduces a complete architectural evolution with **event-first design** and **distributed storage**.

### Key Improvements

| Feature | Nexora 1.x | Nexora 2.0 |
|---------|------------|------------|
| **API Structure** | `/api/v2/*` | `/api/*` (cleaner, no versioning) |
| **Event Architecture** | Graph-first | **Event-first** with `nexora-eventlog` |
| **Storage Backend** | RocksDB only | RocksDB + **Apache Iceberg** on S3/MinIO |
| **Query Engine** | Custom | Custom Cypher + **DataFusion** for event queries |
| **Dashboard** | Basic | Enhanced with **SQL Query page** and fixed compatibility |
| **Distributed Writes** | Single-node | **Multi-node** S3 concurrent writes with Iceberg's optimistic concurrency |
| **Test Coverage** | Basic | **1590+ tests** with chaos testing |

---

## 🚀 Quick Start

### Prerequisites
- Rust 1.88+
- Optional: MinIO/S3 for distributed storage
- Optional: PostgreSQL for shared catalog (advanced)

### Installation

```bash
# Clone the repository
git clone https://github.com/frank-dkvan/nexora2.git
cd nexora2

# Build
cargo build --release
```

### Launch Server

```bash
# Single-node mode (default, local RocksDB + event log)
./target/release/nexora --host 127.0.0.1 --port 8080 --allow-unauthenticated

# With distributed storage (S3/MinIO + Iceberg)
./target/release/nexora \
  --event-store-type s3 \
  --s3-endpoint http://localhost:9000 \
  --s3-bucket nexora-events \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

### Your First Query

```bash
# Cypher query
curl -X POST http://127.0.0.1:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE (p:Person {name: \"Alice\", age: 30}) RETURN p"}' | jq .

# SQL query
curl -X POST http://127.0.0.1:8080/api/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*) FROM nodes"}' | jq .
```

### Dashboard

Open **http://127.0.0.1:8080/dashboard** in your browser for:
- 📊 Real-time graph visualization
- ✍️ Cypher query editor with syntax highlighting
- 📝 SQL query interface (NEW in 2.0!)
- 🔍 Graph browser with node inspection
- ⚡ Standing queries management
- 📥 Data ingestion interface

---

## 🏗️ Architecture

### Event-First Design

Nexora 2.0 treats **events as the source of truth**, with the graph as a derived projection:

```
Raw Events (Apache Iceberg Event Log)
    ↓
Materialized Graph (RocksDB)
    ↓
Query Engine (Cypher/SQL via DataFusion)
```

### Storage Layers

```
┌─────────────────────────────────────┐
│   Application Layer (HTTP API)      │
├─────────────────────────────────────┤
│   Query Engine                       │
│   ├── Cypher (custom parser)        │
│   └── SQL → Cypher translator       │
├─────────────────────────────────────┤
│   Graph Projection (RocksDB)        │
├─────────────────────────────────────┤
│   Event Log Store (nexora-eventlog) │
│   ├── Apache Iceberg Tables         │
│   ├── Local FS / S3 / MinIO         │
│   └── SQLite Catalog (metadata)     │
└─────────────────────────────────────┘
```

### Event-First Benefits

1. **Immutable Event Log** - All mutations are append-only events
2. **Time Travel** - Query graph state at any historical point
3. **Distributed Writes** - Multiple nodes can write concurrently to S3
4. **Replay & Recovery** - Rebuild graph from event log
5. **DataFusion Integration** - SQL queries over event tables

---

## 📦 Project Structure

```
nexora2/
├── crates/
│   ├── nexora-core/          # Core graph engine
│   ├── nexora-cypher/        # Cypher parser & executor
│   ├── nexora-sql/           # SQL → Cypher translator
│   ├── nexora-eventlog/      # Event-first storage (NEW)
│   │                          # - Apache Iceberg integration
│   │                          # - DataFusion query engine
│   │                          # - SQLite catalog
│   ├── nexora-storage/       # S3/MinIO integration (NEW)
│   ├── nexora-pgwire/        # PostgreSQL wire protocol
│   ├── nexora-app/           # HTTP API server
│   └── nexora-bench/         # Performance benchmarks
├── docs/                     # Documentation
│   ├── architecture/         # Design documents
│   ├── production-planning/  # Roadmap & production readiness
│   └── testing/              # Test reports
├── scripts/                  # Deployment & test scripts
└── ui/                       # Web dashboard (React)
```

---

## 🧪 Testing

Nexora 2.0 includes comprehensive test suites:

```bash
# Run all tests
cargo test --workspace

# Run event log tests
cargo test -p nexora-eventlog

# Run distributed write tests (requires MinIO)
cargo test -p nexora-eventlog concurrent_s3_writes

# Run chaos tests
cargo test chaos
```

### Test Data

Example test data for air cargo terminal:
- `/tmp/load_air_cargo.sh` - Loads sample data
- `/tmp/test_air_cargo_simple.sql` - SQL test queries
- `/tmp/test_air_cargo_cypher.sql` - Cypher test queries

---

## 📚 Documentation

- [Architecture Overview](docs/architecture/)
- [Event Store Design](docs/architecture/DISTRIBUTED_EVENT_WRITE_DESIGN.md)
- [Event Log Completion Summary](docs/EVENTLOG-COMPLETION-SUMMARY.md)
- [Deployment Guide](docs/EVENT_STORE_DEPLOYMENT_QUICK_START.md)
- [API Documentation](http://127.0.0.1:8080/api/docs) (when server is running)

---

## 🛣️ Roadmap

See [ROADMAP_TO_PRODUCTION_LEADING_2026-07-18.md](docs/production-planning/ROADMAP_TO_PRODUCTION_LEADING_2026-07-18.md) for detailed production readiness roadmap.

**Current Status**: 
- ✅ Single-node production-ready
- 🧪 Multi-node experimental (distributed event writes functional, needs validation)

---

## 🔧 Technology Stack

- **Language**: Rust 1.88+
- **Graph Storage**: RocksDB
- **Event Log**: Apache Iceberg (Parquet files)
- **Query Engines**: 
  - Custom Cypher parser & executor
  - DataFusion for SQL/event queries
- **Object Storage**: S3, MinIO, or local filesystem
- **Catalog**: SQLite (for Iceberg metadata)
- **Wire Protocols**: HTTP REST, PostgreSQL wire protocol

---

## 📄 License

Apache License 2.0 - see [LICENSE](LICENSE) for details

---

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

---

## 🔗 Links

- **Original Nexora**: https://github.com/frank-dkvan/nexora
- **Issues**: https://github.com/frank-dkvan/nexora2/issues
- **Discussions**: https://github.com/frank-dkvan/nexora2/discussions

---

## 📖 About Event-First Architecture

Nexora 2.0's event-first design is inspired by:
- **Apache Iceberg** - Table format with ACID guarantees
- **Event Sourcing** - Immutable event log as source of truth
- **RisingWave** - Barrier-based checkpointing (architecture reference only)

The graph is a **materialized view** over the event log, enabling:
- Point-in-time queries
- Event replay for debugging
- Distributed consistency via Iceberg's optimistic concurrency
