# Event Streaming Library Mode - Current Status & Next Steps

## Current Status

### ✅ Completed

1. **Terminology Refactoring** (Task #1-8)
   - All external-facing names changed: `risingwave` → `event-streaming` / `EventStreaming`
   - Feature flags: `event-streaming`, `embedded`, `library`
   - CLI parameters: `--enable-event-streaming`, `--library-event-streaming`, etc.
   - HTTP routes: `/api/event-streaming/*`
   - Config: `[event_streaming]` section
   - All tests passing

2. **Documentation**
   - Architecture design: `docs/LIBRARY_DISTRIBUTED_MODE.md`
   - CLI simplification plan: `docs/CLI_SIMPLIFICATION.md`
   - Testing guide: `TESTING.md`

3. **Test Environment**
   - Configuration: `nexora.toml` (library mode enabled)
   - Start script: `start-nexora-library.sh`
   - Test script: `test-event-streaming.sh`
   - Build monitor: `watch-build.sh`
   - Status checker: `check-build.sh`

### 🚧 In Progress

**Compilation** - `cargo build --release --features event-streaming,library`
- Running for: 20+ minutes
- Status: Compiling RisingWave and dependencies (~1000+ crates)
- Binary size: Will be ~100-150MB (includes entire RisingWave)
- Expected time: 10-30 minutes total (depends on machine)

**Why it takes so long:**
- RisingWave is a large streaming database system
- Includes: meta service, frontend, compute engine, compactor
- Many dependencies: tokio, arrow, datafusion, prost, tonic, etc.
- Release mode with optimizations

### 📋 Next Steps

Once compilation completes, we will proceed in this order:

#### 1. Test Single-Node Library Mode (Today)

**Goal:** Verify basic functionality works

```bash
# Start Nexora with Event Streaming
./start-nexora-library.sh

# In another terminal, run tests
./test-event-streaming.sh
```

**Test cases:**
- ✓ Server starts successfully
- ✓ Event Streaming engine initializes
- ✓ Health endpoint returns status
- ✓ DDL execution (CREATE SOURCE, CREATE MATERIALIZED VIEW)
- ✓ Query materialized views
- ✓ List sources and MVs

**Expected outcome:** All tests pass, confirming single-node library mode works.

#### 2. Implement CLI Simplification (2-3 days)

**Goal:** Make CLI intuitive and easy to use

**Before (current):**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --event-streaming-meta-addr 127.0.0.1:5690 \
  --event-streaming-frontend-addr 127.0.0.1:4566
```

**After (simplified):**
```bash
./nexora --event-streams dev
```

**Implementation:**
- Profile-based approach (dev/production/external)
- Config-first with CLI overrides
- Environment variable support
- Backward compatible with deprecation warnings

**Reference:** `docs/CLI_SIMPLIFICATION.md`

#### 3. Implement Distributed Mode (4-5 days)

**Goal:** Support multi-node deployments

**Architecture:**
```
Node 1: Meta + Frontend + Compute
Node 2: Meta + Frontend + Compute  
Node 3: Meta + Frontend + Compute
```

**Key features:**
- Role-based deployment (meta/compute/frontend/compactor)
- Etcd-backed meta service for HA
- Flexible topology (all-in-one or specialized roles)

**Implementation:**
- Use RisingWave's `standalone()` function
- Build `ParsedStandaloneOpts` based on roles
- Configure inter-node communication
- Support etcd as meta backend

**Reference:** `docs/LIBRARY_DISTRIBUTED_MODE.md`

#### 4. Integration Testing (2 days)

**Single-node tests:**
- Memory mode (fast restart)
- Persistent mode (RocksDB)
- Various workloads (streaming ingestion, complex queries)

**Distributed tests:**
- 3-node cluster setup
- Leader election
- Node failure/recovery
- Data consistency

#### 5. Documentation & Examples (1 day)

**User guides:**
- Quick start (single-node)
- Production deployment (distributed)
- Configuration reference
- Troubleshooting

**Example deployments:**
- Docker Compose
- Kubernetes StatefulSet
- Systemd units

## Estimated Timeline

```
Week 1 (Current):
├─ Day 1-2: ✅ Terminology refactoring (Done)
├─ Day 3: ✅ Documentation (Done)
├─ Day 4: 🚧 Compilation + Single-node testing
└─ Day 5: CLI simplification (start)

Week 2:
├─ Day 1-2: CLI simplification (finish)
├─ Day 3-5: Distributed mode implementation
└─ Weekend: Buffer

Week 3:
├─ Day 1-2: Integration testing
├─ Day 3: Documentation
└─ Day 4-5: Buffer / polish
```

## Files Ready for Testing

Once compilation completes, these files are ready to use:

1. **nexora.toml** - Configuration with event_streaming enabled
2. **start-nexora-library.sh** - Start script for library mode
3. **test-event-streaming.sh** - Automated API tests
4. **check-build.sh** - Check if binary is ready
5. **watch-build.sh** - Monitor compilation progress

## Technical Notes

### Build Configuration

**Toolchain:** nightly-2026-06-11 (required for RisingWave)
**Features:** `event-streaming,library`
**Target:** `aarch64-apple-darwin` (Apple Silicon)

### Binary Size

- Without Event Streaming: ~40MB
- With Event Streaming (library): ~100-150MB
- Size increase: ~60-110MB (entire RisingWave included)

### Memory Requirements

- Single-node mode: ~2GB RAM minimum
- Distributed mode (per node): ~1.5-2GB RAM

### Architecture Highlights

**Library Mode Benefits:**
1. **No external binary** - RisingWave compiled into Nexora
2. **Unified deployment** - One binary, one config
3. **Simplified operations** - No version mismatch issues
4. **Flexible topology** - Single-node for dev, distributed for production

**Trade-offs:**
1. **Binary size** - Larger binary (~100MB+ vs ~40MB)
2. **Compilation time** - Slower builds (20-30min vs 2-3min)
3. **Toolchain** - Requires nightly Rust

## CLI Simplification Preview

### Current Problems

1. **Too verbose** - 5+ flags just to enable
2. **Hard to remember** - Multiple address flags
3. **Inconsistent prefixes** - `--event-streaming-*` vs `--library-*`
4. **No clear defaults** - Must specify everything
5. **Poor discovery** - Hard to understand options

### Proposed Solution

**Profile-based deployment:**

```bash
# Development (auto-configured)
./nexora --event-streams dev

# Production (from config file)
./nexora --event-streams production

# External cluster
./nexora --event-streams external
```

**Benefits:**
- 90% fewer flags for common cases
- Clear intent (dev vs production vs external)
- Config-first approach (industry standard)
- Environment variable support (cloud-native)
- Backward compatible (old flags still work with warnings)

## Decision Points

After single-node testing succeeds, we need to decide:

1. **Implement CLI simplification first or distributed mode first?**
   - CLI first: Better UX for distributed mode when it lands
   - Distributed first: More features faster, polish UX later

2. **Keep backward compatibility for old flags?**
   - Yes: Smooth migration, but maintain both paths
   - No: Clean break, force users to migrate

3. **Config file format?**
   - TOML (current): Familiar to Rust ecosystem
   - YAML: More flexible, better for k8s deployments
   - Both: TOML default, YAML optional

**Recommendation:** 
1. CLI simplification first (2-3 days)
2. Then distributed mode (4-5 days)
3. Keep backward compatibility with deprecation warnings
4. TOML primary, consider YAML later if requested

## Compilation Troubleshooting

If compilation fails or hangs:

1. **Check disk space:** Library mode needs ~10GB free
2. **Check memory:** Needs ~8GB RAM for compilation
3. **Clean and retry:**
   ```bash
   cargo clean
   cargo build --release --features event-streaming,library
   ```
4. **Incremental build disabled?** Check if `CARGO_INCREMENTAL=0` is set
5. **Parallel jobs:** Try `cargo build -j 4` to limit parallelism

## Questions for Review

1. Should we prioritize distributed mode or CLI simplification?
2. Is the profile-based approach (dev/production/external) intuitive?
3. Any concerns about binary size (~100-150MB)?
4. Should we support YAML config in addition to TOML?

## Contact

For questions or issues:
- Check `docs/LIBRARY_DISTRIBUTED_MODE.md` for architecture
- Check `docs/CLI_SIMPLIFICATION.md` for CLI design
- Check `TESTING.md` for test procedures

---

**Last Updated:** 2026-07-29 21:45
**Status:** Waiting for compilation to complete
**Next Action:** Run single-node tests
