# Contributing to Nexora-RS

Thank you for your interest in contributing to Nexora-RS! This document outlines the process for contributing to the project.

## 🚀 Getting Started

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/frank-dkvan/nexora.git
   cd nexora
   ```
3. **Create a branch** for your work:
   ```bash
   git checkout -b feature/my-feature
   ```

## 🛠️ Development Setup

### Prerequisites

- **Rust** 1.88+ ([rustup.rs](https://rustup.rs))
- **Node.js** 20+ (for the web UI, optional)
- **Docker** (for integration testing, optional)

### Build & Test

```bash
# Build all crates
cargo build --workspace

# Run all tests
cargo test --workspace

# Run clippy (must pass with zero warnings)
cargo clippy --workspace --all-targets -- -D warnings

# Check formatting (must pass with zero diff)
cargo fmt --all -- --check

# Run a specific crate's tests
cargo test -p nexora-core
```

### UI Development

```bash
cd ui
npm install
npm run dev    # Start dev server at localhost:5173
npm run build  # Build production bundle to ui/dist/
```

## 📋 Code Standards

### Rust Code Style

- **Formatting**: Use `cargo fmt` before committing. No exceptions.
- **Linting**: All code must pass `cargo clippy --workspace --all-targets -- -D warnings`.
- **Error Handling**: Use `Result<T, E>` for fallible operations. Avoid `unwrap()` / `expect()` in production code (tests are fine).
- **Documentation**: Public items must have doc comments (`///`). Include examples where helpful.
- **Naming**: Follow Rust naming conventions (snake_case for functions/variables, CamelCase for types).
- **Module Organization**: Keep modules focused. If a file exceeds ~500 lines, consider splitting.

### Testing Requirements

- **New features** must include tests
- **Bug fixes** must include a regression test
- **Target**: Maintain >80% line coverage for core crates
- **Test types**:
  - Unit tests in `#[cfg(test)] mod tests` blocks
  - Integration tests in `tests/` directories
  - Property-based tests using `proptest` for algorithms

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`

**Examples**:
```
feat(cypher): add support for CASE WHEN expressions
fix(core): resolve deadlock in shard write lock
docs(api): update OpenAPI spec for vector search
test(hnsw): add recall rate benchmark tests
```

## 🔄 Pull Request Process

1. **Ensure all checks pass**:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```

2. **Update documentation** if your changes affect:
   - API endpoints → update `docs/api-tutorial.md`
   - Cypher support → update `CYPHER_SUPPORT.md`
   - Configuration → update `QUICKSTART.md`
   - Architecture → update `README.md`

3. **Write a clear PR description** including:
   - What changed and why
   - Breaking changes (if any)
   - Test results
   - Related issues

4. **Request review** from maintainers

5. **Address review feedback** promptly

## 🏗️ Architecture Overview

Nexora-RS is organized as a Cargo workspace with 22 crates:

| Crate | Purpose |
|-------|---------|
| `nexora-core` | Core graph engine (actors, shards, WAL) |
| `nexora-cypher` | Cypher query execution |
| `nexora-language` | Cypher parser (write operations) |
| `nexora-app` | HTTP API server (Axum) |
| `nexora-standing-query` | Standing Query engine |
| `nexora-hnsw` | Vector similarity search |
| `nexora-ingest` | Data ingestion (JSONL, CSV) |
| `nexora-stream` | Streaming sources (Kafka) |
| `nexora-zenoh` | Distributed clustering |
| `nexora-raft` | Raft consensus |
| `nexora-udf` | User-defined functions |
| `nexora-storage` | Tiered storage |
| `nexora-recipe` | Declarative recipes |
| `nexora-sql` | SQL query layer |
| ... | See `Cargo.toml` for full list |

**Key Design Patterns**:
- Actor-per-Node: Each graph node is a tokio task
- Event Sourcing: All mutations are logged to WAL
- Shard-based: Consistent hashing across 256 shards

## 🐛 Reporting Bugs

Use [GitHub Issues](https://github.com/frank-dkvan/nexora/issues) with:
- Nexora-RS version (`nexora-app --version`)
- Rust version (`rustc --version`)
- OS and architecture
- Minimal reproduction steps
- Expected vs actual behavior

## 💡 Feature Requests

We welcome feature suggestions! Please:
1. Check existing issues first
2. Open a discussion with the `enhancement` label
3. Describe the use case and proposed solution

## 📜 License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for contributing to Nexora-RS! 🦀
