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
| **Storage Backend** | RocksDB only | RocksDB + **S3/MinIO** + Lakekeeper REST catalog |
| **Dashboard** | Basic | Enhanced with **SQL Query page** and fixed compatibility |
| **Distributed Writes** | Single-node | **Multi-node** S3 concurrent writes |
| **Test Coverage** | Basic | **1590+ tests** with chaos testing |

---

## 🚀 Quick Start

### Prerequisites
- Rust 1.88+
- Optional: MinIO/S3 for distributed storage
- Optional: Lakekeeper for REST catalog

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
# Single-node mode (default)
./target/release/nexora --host 127.0.0.1 --port 8080 --allow-unauthenticated

# With distributed storage (S3/MinIO)
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
Raw Events (Event Log Store)
    ↓
Materialized Graph (RocksDB)
    ↓
Query Engine (Cypher/SQL)
```

### Storage Layers

```
┌─────────────────────────────────────┐
│   Application Layer (HTTP API)      │
├─────────────────────────────────────┤
│   Query Engine (Cypher + SQL)       │
├─────────────────────────────────────┤
│   Graph Storage (RocksDB)           │
├─────────────────────────────────────┤
│   Event Log Store (nexora-eventlog) │
│   ├── Local FS                       │
│   ├── S3 / MinIO (distributed)      │
│   └── Lakekeeper REST Catalog       │
└─────────────────────────────────────┘
```

---

## 📦 Project Structure

```
nexora2/
├── crates/
│   ├── nexora-core/          # Core graph engine
│   ├── nexora-cypher/        # Cypher parser & executor
│   ├── nexora-sql/           # SQL → Cypher translator
│   ├── nexora-eventlog/      # Event-first storage (NEW)
│   ├── nexora-storage/       # S3/MinIO integration (NEW)
│   ├── nexora-pgwire/        # PostgreSQL wire protocol
│   ├── nexora-app/           # HTTP API server
│   └── nexora-bench/         # Performance benchmarks
├── docs/                     # Documentation
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

# Run distributed write tests
cargo test -p nexora-eventlog concurrent_s3_writes

# Run chaos tests
cargo test chaos
```

### Test Data

Example test data included in `/tmp/`:
- `load_air_cargo.sh` - Loads air cargo terminal sample data
- `test_air_cargo_simple.sql` - SQL test queries
- `test_air_cargo_cypher.sql` - Cypher test queries

---

## 📚 Documentation

- [Architecture Overview](docs/architecture/)
- [Event Store Design](docs/architecture/DISTRIBUTED_EVENT_WRITE_DESIGN.md)
- [Deployment Guide](docs/EVENT_STORE_DEPLOYMENT_QUICK_START.md)
- [API Documentation](http://127.0.0.1:8080/api/docs) (when server is running)

---

## 🛣️ Roadmap

See [ROADMAP_TO_PRODUCTION_LEADING_2026-07-18.md](docs/production-planning/ROADMAP_TO_PRODUCTION_LEADING_2026-07-18.md) for detailed production readiness roadmap.

**Current Status**: Single-node production-ready. Multi-node experimental.

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

