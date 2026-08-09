# Nexora 2 Development Guide

## Project Overview

Nexora 2.0 is a **next-generation streaming graph database** with event-first architecture and optional RisingWave integration for advanced SQL-based stream processing.

**Current Status**:
- ✅ Core Platform: Production-ready (1590+ tests)
- 🚧 RisingWave Integration: In Progress (Phase 1 of 6)

## Architecture

### Core Platform (Existing)

```
nexora2/
├── crates/
│   ├── nexora-core/          # Graph engine (RocksDB-backed)
│   ├── nexora-cypher/        # Cypher parser & executor
│   ├── nexora-sql/           # SQL → Cypher translator
│   ├── nexora-eventlog/      # Apache Iceberg event storage
│   ├── nexora-stream/        # Kafka/Kinesis/Pulsar/MQTT connectors
│   ├── nexora-raft/          # Distributed consensus
│   └── nexora-app/           # HTTP API server
├── ui/                       # React dashboard
├── docs/                     # Documentation
└── scripts/                  # Automation tools
```

### RisingWave Integration (New - In Progress)

```
nexora2/
├── vendor/risingwave/        # Git Subtree: RisingWave v3.0.2 (TODO)
├── crates/
│   ├── nexora-risingwave/    # RisingWave wrapper (TODO)
│   ├── nexora-consensus/     # Raft abstraction - shared (TODO)
│   └── nexora-rpc/           # gRPC abstraction - shared (TODO)
└── extensions/
    └── meta_raft/            # RisingWave Raft HA extension (TODO)
```

**Reference Document**: [RisingWave Integration Plan](docs/RISINGWAVE_INTEGRATION_PLAN.md)

## Key Design Principles

### 1. Additive Integration, Not Replacement

- Keep all existing functionality intact
- Event Streaming is **optional** via `--features event-streaming`
- All 1590+ tests must continue to pass
- Zero performance overhead for non-RisingWave builds

### 2. Feature Flags

> **⚠️ Toolchain (read first).** The workspace is pinned to a **specific nightly**
> via `rust-toolchain.toml` (currently `nightly-2026-06-11`, kept in sync with
> `vendor/risingwave/ci/rust-toolchain`). This is mandatory: `.cargo/config.toml`
> uses `-Zhigher-ranked-assumptions` and the root `Cargo.toml` uses the
> `profile-rustflags` cargo feature, both nightly-only.
>
> **The `cargo` you invoke must be the rustup proxy (`~/.cargo/bin/cargo`), not
> Homebrew's standalone stable cargo (`/opt/homebrew/bin/cargo`).** Homebrew's
> cargo ignores `rust-toolchain.toml` and fails with
> `profile-rustflags requires a nightly version of Cargo` or
> `-Z is only accepted on the nightly compiler`. If Homebrew Rust is installed,
> ensure `~/.cargo/bin` comes first on `PATH`:
> ```bash
> export PATH="$HOME/.cargo/bin:$PATH"
> cargo --version   # must print "...-nightly..."; if it says "(Homebrew)" the proxy is shadowed
> ```
> Do **not** hardcode the nightly date or pass `+nightly` — the proxy reads the
> pin from `rust-toolchain.toml` automatically. The `scripts/build*.sh` helpers
> already apply this PATH fix.
>
> **Agent self-check (run before any build/test/clippy).** Confirm cargo resolves
> to the nightly proxy; if it doesn't, fix PATH in-shell, then proceed:
> ```bash
> cargo --version | grep -q nightly || export PATH="$HOME/.cargo/bin:$PATH"
> cargo --version   # verify: must contain "nightly"
> ```
> If it still isn't nightly after the PATH fix, run the build through
> `scripts/build*.sh` (which self-heal PATH) rather than invoking `cargo` directly.

```toml
# Default build (no Event Streaming)
cargo build --release

# With event-first storage (Apache Iceberg)
cargo build --release --features event-first

# With Event Streaming engine (advanced SQL, backed by RisingWave)
cargo build --release --features event-streaming

# Event Streaming compiled in-process (single-node, no external binary)
cargo build --release --features library

# Full stack
cargo build --release --features event-first,event-streaming
```

### 3. Dual Event Processing Paths

Both paths are **parallel**, not serial. The Event Streaming engine has its own
`CREATE SOURCE` that connects directly to upstream sources, so it does not depend
on `nexora-stream`. Both paths converge on `nexora-eventlog`.

```
                   ┌─ Path A:  nexora-stream ───────────────────────────┐
sources (Kafka/…) ─┤                                                     ├─→ nexora-eventlog ─[GraphStreaming]→ nexora-core
                   └─ Path B:  Event Streaming (CREATE SOURCE + SQL/MV) ─┘
```

**Path A - Simple** (current, always available): direct connector ingestion via
`nexora-stream` for simple event-to-graph mapping.

**Path B - Advanced** (new, optional with `--features event-streaming`): SQL-based
stream processing (continuous queries, materialized views, stream joins) via the
embedded Event Streaming engine.

**GraphStreaming** is the conceptual layer that projects converged event-log data
into the graph (`nexora-core`). It is currently a design concept — no code carries
that name yet.

### 4. Shared Infrastructure

Both the Event Streaming engine and Nexora will use:
- **nexora-consensus**: Raft trait abstraction (openraft implementation)
- **nexora-rpc**: gRPC communication layer (tonic-based)
- Unified configuration and monitoring

## Development Workflow

### Daily Development

```bash
# 1. Pull latest changes
git pull origin main

# 2. Create feature branch
git checkout -b feat/risingwave-phase1-subtree

# 3. Make changes and test
cargo test --workspace

# 4. Run checks
cargo fmt
cargo clippy --all-targets --all-features

# 5. Commit and push
git add .
git commit -m "feat(risingwave): add Git Subtree integration script"
git push origin feat/risingwave-phase1-subtree
```

### Testing Strategy

```bash
# Unit tests (fast)
cargo test -p nexora-core
cargo test -p nexora-eventlog

# Integration tests (requires external services)
cargo test --test distributed_integration

# Event Streaming-specific tests (requires feature flag)
cargo test -p nexora-risingwave --features event-streaming

# All tests
cargo test --workspace --all-features
```

### Code Style

- **Language**: Rust — **pinned nightly** via `rust-toolchain.toml` (`nightly-2026-06-11`). Required by RisingWave library-mode integration; invoke through the rustup proxy (see the Toolchain note under Feature Flags).
- **Formatting**: `cargo fmt` (enforced by CI)
- **Linting**: `cargo clippy` (no warnings allowed)
- **Documentation**: All public APIs must have doc comments
- **Comments**: Use English for all code comments and documentation

### Error Handling

- **Application layer**: Use `anyhow::Result<T>` for flexibility
- **Library layer**: Use `thiserror` for custom error types
- **Async code**: Always use `tokio` runtime

### Git Commit Conventions

```
feat: New feature
fix: Bug fix
refactor: Code restructuring (no behavior change)
docs: Documentation updates
test: Test additions or modifications
chore: Build tools, dependencies, CI
perf: Performance optimization
```

**Examples**:
```
feat(risingwave): add Git Subtree integration script
fix(eventlog): handle concurrent S3 writes correctly
docs(risingwave): add integration architecture diagram
test(consensus): add 3-node Raft cluster test
```

## Current Task: RisingWave Integration

### Implementation Plan (6 Weeks)

| Week | Phase | Status |
|------|-------|--------|
| 1 | Repository Setup | 🚧 In Progress |
| 2 | Shared Infrastructure | ⏳ Pending |
| 3 | RisingWave Wrapper | ⏳ Pending |
| 4 | Raft HA Extension | ⏳ Pending |
| 5 | App Integration | ⏳ Pending |
| 6 | Event Pipeline | ⏳ Pending |

### Phase 1: Repository Setup (Current)

**Goal**: Add RisingWave v3.0.2 as Git Subtree

**Tasks**:
1. ✅ Create integration plan document
2. ✅ Create CLAUDE.md
3. ⏳ Create `scripts/init-risingwave.sh`
4. ⏳ Create `scripts/sync-risingwave.sh`
5. ⏳ Create `scripts/apply-patches.sh`
6. ⏳ Update root `Cargo.toml` with new workspace members
7. ⏳ Test: Verify all existing tests still pass

**Deliverables**:
- [x] `docs/RISINGWAVE_INTEGRATION_PLAN.md`
- [x] `CLAUDE.md`
- [ ] `scripts/init-risingwave.sh`
- [ ] `scripts/sync-risingwave.sh`
- [ ] `scripts/apply-patches.sh`
- [ ] Updated `Cargo.toml`
- [ ] Updated `.gitignore`

## Key Files and Locations

### Configuration

**Main Config**: `nexora.toml`
```toml
[server]
host = "127.0.0.1"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/graph"

[event_store]
backend = "rest"
rest_uri = "http://localhost:8181/catalog"

# NEW: Event Streaming engine (optional, only with --features event-streaming)
# Backed by an embedded RisingWave engine.
[event_streaming]
enabled = false
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
```

### Build Configuration

**Workspace**: `Cargo.toml` (root)
- Defines all workspace members
- Shared dependency versions
- Feature flag definitions

**Individual Crates**: `crates/*/Cargo.toml`
- Per-crate dependencies
- Feature gates for optional functionality

### Documentation

- `README.md` - Project overview and quick start
- `docs/RISINGWAVE_INTEGRATION_PLAN.md` - Integration architecture
- `docs/architecture/` - Detailed design documents
- `docs/IMPLEMENTATION_PLAN.md` - Original 15-week plan (reference)

### Scripts

- `scripts/build/` - Build automation
- `scripts/deploy/` - Deployment tools
- `scripts/test/` - Test utilities
- `scripts/dev/` - Development helpers

## RisingWave Integration Details

### Git Subtree Management

**Why Git Subtree?**
- Deep integration with RisingWave internals (>1000 lines of code interaction)
- Need to patch RisingWave for external election support
- Easier to track our modifications vs upstream changes
- Simpler than Git Submodules (no .gitmodules, easier cloning)

**Initial Setup**:
```bash
# Run once to add RisingWave
./scripts/init-risingwave.sh
```

**Sync with Upstream**:
```bash
# Check for new versions
./scripts/sync-risingwave.sh --check

# Upgrade to v3.1.0
./scripts/sync-risingwave.sh --upgrade v3.1.0
```

**Apply Patches**:
```bash
# After sync, re-apply Nexora-specific patches
./scripts/apply-patches.sh
```

### Dependencies Strategy

| Component | Method | Rationale |
|-----------|--------|-----------|
| RisingWave | Git Subtree | Deep integration, need to patch |
| openraft | Cargo dependency | Standard library use |
| kuzu | Cargo dependency | External service |
| tokio-postgres | Cargo dependency | Client library |
| cloudevents-sdk | Cargo dependency | Standard protocol |

### Patch Management

**Location**: `patches/`
```
patches/
├── 001-enable-external-election.patch
├── 002-expose-election-trait.patch
└── README.md
```

**Applying Patches**:
```bash
# Automatic
./scripts/apply-patches.sh

# Manual
git apply patches/001-enable-external-election.patch
```

**Creating New Patches**:
```bash
# Make changes in vendor/risingwave/
cd vendor/risingwave
# ... edit files ...

# Create patch
git diff > ../../patches/003-my-change.patch
```

## Testing RisingWave Integration

### Prerequisites

```bash
# Install Rust 1.75+
rustup update

# Optional: Install RisingWave CLI (for testing)
cargo install risingwave-cli
```

### Running Tests

```bash
# Phase 1: Verify existing tests pass
cargo test --workspace

# Phase 2: Test shared infrastructure
cargo test -p nexora-consensus
cargo test -p nexora-rpc

# Phase 3: Test Event Streaming wrapper
cargo test -p nexora-risingwave --features event-streaming

# Phase 4: Test Raft HA
cargo test -p extensions-meta-raft --features event-streaming -- --test-threads=1

# Phase 5: Test app integration
cargo test -p nexora-app --features event-streaming

# Phase 6: End-to-end test
./scripts/test-risingwave-pipeline.sh
```

### Local Development Setup

```bash
# Start MinIO (for S3-compatible storage)
docker run -d \
  --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Start Kafka (for event streaming)
docker run -d \
  --name kafka \
  -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:latest

# Start Nexora (without Event Streaming)
cargo run --release --features event-first

# Start Nexora (with Event Streaming engine) - after Phase 5
cargo run --release --features event-first,event-streaming
```

## Troubleshooting

### Common Issues

**Q: Git Subtree merge conflicts**
```bash
# Check conflicted files
git status

# Resolve manually, then
git add .
git commit
```

**Q: Patch application fails**
```bash
# Check which patch failed
./scripts/apply-patches.sh --verbose

# Apply manually
git apply patches/001-enable-external-election.patch --reject

# Fix .rej files, then
git add vendor/risingwave
```

**Q: Feature flag not working**
```bash
# Clean and rebuild
cargo clean
cargo build --features event-streaming

# Verify feature is enabled
cargo tree --features event-streaming | grep nexora-risingwave
```

**Q: Existing tests fail after adding the Event Streaming engine**
```bash
# This is a BLOCKER - must fix immediately
# Run git bisect to find the breaking commit
git bisect start
git bisect bad HEAD
git bisect good <last-known-good-commit>

# Revert the change and investigate
```

## Performance Guidelines

### Memory Budgets

- **Nexora Core**: ~500MB (graph + event storage)
- **RisingWave Meta**: ~200MB
- **RisingWave Frontend**: ~500MB
- **RisingWave Compute**: ~1GB per node
- **Total with RisingWave**: ~2.2GB minimum

### When to Enable RisingWave

**Use RisingWave when**:
- Need complex SQL transformations (joins, aggregations, window functions)
- Multi-stream temporal joins required
- Real-time data enrichment before graph ingestion
- Existing SQL expertise in team

**Use Direct Path when**:
- Simple event-to-graph mapping (<10ms latency requirement)
- Memory-constrained environment (<2GB available)
- No SQL transformation needed

## Security Considerations

### Authentication

- Default: `--allow-unauthenticated` (dev only)
- Production: Enable JWT authentication via config

### Network

- Bind to `127.0.0.1` for local dev
- Use TLS for production deployments
- RisingWave Meta should not be exposed externally

### Data Protection

- Event log is immutable (Iceberg guarantees)
- Graph state can be rebuilt from event log
- Regular backups via `scripts/backup-nexora.sh`

## Contributing to RisingWave Integration

### Before Starting a Task

1. Read `docs/RISINGWAVE_INTEGRATION_PLAN.md`
2. Check current phase and task list
3. Create feature branch from `main`
4. Ensure all existing tests pass

### While Working

1. Write tests first (TDD approach)
2. Keep changes focused (one task per PR)
3. Add documentation for new APIs
4. Update CHANGELOG.md

### Before Submitting PR

1. Run `cargo fmt`
2. Run `cargo clippy --all-targets --all-features`
3. Run `cargo test --workspace --all-features`
4. Update documentation if needed
5. Self-review the diff

### PR Checklist

- [ ] All tests pass (including existing ones)
- [ ] New features have tests
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] No compiler warnings
- [ ] No clippy warnings

## Useful Commands

### Build Commands

```bash
# Fast dev build (no optimizations)
cargo build

# Optimized release build
cargo build --release

# Build with specific features
cargo build --features event-streaming

# Check compilation without building
cargo check --workspace --all-features
```

### Development Commands

```bash
# Format code
cargo fmt

# Lint code
cargo clippy --all-targets --all-features

# Generate documentation
cargo doc --open --no-deps

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run benchmark
cargo bench
```

### Debugging

```bash
# Enable debug logs
RUST_LOG=debug cargo run

# Enable trace logs for specific module
RUST_LOG=nexora_risingwave=trace cargo run

# Run with backtrace
RUST_BACKTRACE=1 cargo run
```

## Reference Resources

### Internal Documentation

- [RisingWave Integration Plan](docs/RISINGWAVE_INTEGRATION_PLAN.md)
- [Original Implementation Plan](docs/IMPLEMENTATION_PLAN.md)
- [Nexora Design Guidance](docs/Nexora_2_RnD_Guidance_v1.1.md)
- [Storage Architecture](docs/architecture/STORAGE_ARCHITECTURE.md)

### External Resources

- **RisingWave**: https://docs.risingwave.com
- **openraft**: https://docs.rs/openraft
- **Apache Iceberg**: https://iceberg.apache.org
- **CloudEvents**: https://cloudevents.io

### Source Code References

- **RisingWave Source**: `vendor/risingwave/` (after Phase 1)
- **RisingWave GitHub**: https://github.com/risingwavelabs/risingwave
- **RisingWave Release Notes**: https://github.com/risingwavelabs/risingwave/releases

## Next Steps

### Immediate (Phase 1)

1. Create `scripts/init-risingwave.sh`
2. Test Git Subtree integration
3. Verify existing tests still pass

### Short Term (Phase 2-3)

1. Develop `nexora-consensus` abstraction
2. Develop `nexora-rpc` abstraction
3. Create `nexora-risingwave` wrapper

### Medium Term (Phase 4-6)

1. Implement Raft HA for RisingWave Meta
2. Integrate into nexora-app
3. Build event pipeline

## Contact & Support

- **Issue Tracker**: https://github.com/frank-dkvan/nexora2/issues
- **Discussions**: https://github.com/frank-dkvan/nexora2/discussions

---

**Last Updated**: 2026-07-26  
**Document Version**: 1.0  
**Status**: Phase 1 In Progress
