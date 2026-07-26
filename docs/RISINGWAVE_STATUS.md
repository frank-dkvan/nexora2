# Nexora 2 - RisingWave Integration Status

**Last Updated**: 2026-07-26  
**Current Phase**: Phase 1 - Repository Setup  
**Status**: ✅ READY FOR EXECUTION

---

## 📊 Overall Progress

| Phase | Goal | Status | Duration |
|-------|------|--------|----------|
| **Phase 1** | Repository Setup | ✅ Ready | Week 1 |
| **Phase 2** | Shared Infrastructure | ⏳ Pending | Week 2 |
| **Phase 3** | RisingWave Wrapper | ⏳ Pending | Week 3 |
| **Phase 4** | Raft HA Extension | ⏳ Pending | Week 4 |
| **Phase 5** | App Integration | ⏳ Pending | Week 5 |
| **Phase 6** | Event Pipeline | ⏳ Pending | Week 6 |

**Overall**: 0/6 phases executed, 1/6 ready

---

## ✅ Phase 1: Completed Deliverables

### Documentation (3/3) ✅

- [x] **RISINGWAVE_INTEGRATION_PLAN.md** - Complete 6-phase architecture
- [x] **CLAUDE.md** - Development guide and conventions
- [x] **RISINGWAVE_PHASE1_REPORT.md** - Phase 1 detailed report
- [x] **PHASE1_QUICKSTART.md** - Execution quick start guide

### Automation Scripts (3/3) ✅

- [x] **scripts/init-risingwave.sh** - Initialize RisingWave as Git Subtree
- [x] **scripts/sync-risingwave.sh** - Sync with upstream versions
- [x] **scripts/apply-patches.sh** - Apply Nexora-specific patches

### Implementation (0/1) ⏳

- [ ] **Execute init-risingwave.sh** - Actually add RisingWave to repo

---

## 🎯 Next Action

**Execute Phase 1 integration:**

```bash
cd /Users/frank/aiCoding/nexora2
./scripts/init-risingwave.sh
```

**Expected outcome**:
- `vendor/risingwave/` created with RisingWave v3.0.2
- 4 new placeholder crates created
- All existing tests still pass

**Time required**: 15-30 minutes (mostly Git operations)

---

## 📁 File Inventory

### Created Files

```
docs/
├── RISINGWAVE_INTEGRATION_PLAN.md      # Master plan (6 phases)
├── RISINGWAVE_PHASE1_REPORT.md        # Phase 1 detailed report
└── PHASE1_QUICKSTART.md               # Quick execution guide

CLAUDE.md                              # Development guide (root)

scripts/
├── init-risingwave.sh                 # Phase 1 execution script
├── sync-risingwave.sh                 # Upstream sync tool
└── apply-patches.sh                   # Patch management tool
```

### Files to be Created (by init-risingwave.sh)

```
vendor/
└── risingwave/                        # RisingWave v3.0.2 source

crates/
├── nexora-risingwave/                 # RisingWave wrapper
├── nexora-consensus/                  # Raft abstraction
└── nexora-rpc/                        # gRPC abstraction

extensions/
└── meta_raft/                         # RisingWave Raft HA

patches/
└── README.md                          # Patch documentation
```

---

## 🏗️ Architecture Overview

### Current Nexora 2 (Preserved)

```
Event Sources → nexora-stream → nexora-eventlog → nexora-core → API
                                    ↓
                              (Apache Iceberg)
```

### After Integration (Optional)

```
Event Sources → nexora-stream → RisingWave MV → nexora-eventlog → nexora-core → API
                                     ↓
                                (SQL transforms)
```

**Key principle**: RisingWave is **additive**, not replacement.

---

## 🧪 Testing Status

### Existing Tests (Must Pass)

- **Total**: 1590+ tests
- **Status**: ✅ All passing (baseline)
- **Requirement**: Must continue to pass after Phase 1

### New Tests (To be Added)

| Phase | Test Type | Count | Status |
|-------|-----------|-------|--------|
| Phase 2 | Consensus unit tests | ~10 | ⏳ Pending |
| Phase 2 | RPC unit tests | ~10 | ⏳ Pending |
| Phase 3 | RisingWave wrapper tests | ~20 | ⏳ Pending |
| Phase 4 | Raft HA integration tests | ~5 | ⏳ Pending |
| Phase 5 | App integration tests | ~15 | ⏳ Pending |
| Phase 6 | E2E pipeline tests | ~5 | ⏳ Pending |

**Target**: +65 new tests for RisingWave integration

---

## 🔧 Configuration

### Feature Flags

```toml
# Default: No RisingWave (current behavior)
cargo build --release

# With event-first (Apache Iceberg)
cargo build --release --features event-first

# With RisingWave (Phase 5+)
cargo build --release --features risingwave

# Full stack (Phase 6+)
cargo build --release --features event-first,risingwave
```

### Memory Budget

| Component | Memory | When |
|-----------|--------|------|
| Nexora Core | ~500MB | Always |
| Apache Iceberg | ~200MB | With event-first |
| RisingWave Meta | ~200MB | With risingwave |
| RisingWave Frontend | ~500MB | With risingwave |
| RisingWave Compute | ~1GB | With risingwave |
| **Total (full)** | ~2.4GB | All features |

---

## 📖 Documentation Map

### For Users

- **README.md** - What is Nexora 2?
- **QUICKSTART.md** - Get started in 5 minutes
- **docs/architecture/** - How it works

### For Contributors

- **CLAUDE.md** - Development guide
- **CONTRIBUTING.md** - How to contribute
- **docs/RISINGWAVE_INTEGRATION_PLAN.md** - Integration architecture

### For Integration Work

- **docs/PHASE1_QUICKSTART.md** - Execute Phase 1
- **docs/RISINGWAVE_PHASE1_REPORT.md** - Detailed Phase 1 report
- **patches/README.md** - Patch management (Phase 4+)

---

## ⚠️ Known Constraints

### Phase 1 Execution

1. **Time**: 15-30 minutes for initial RisingWave fetch
2. **Network**: Requires internet to fetch RisingWave from GitHub
3. **Disk**: Requires ~500MB free space for vendor/risingwave/
4. **Git**: Requires Git 2.9+ for subtree support

### Phase 2+ Dependencies

1. **openraft**: 0.9 (Raft consensus)
2. **tonic**: 0.11 (gRPC framework)
3. **prost**: 0.12 (Protocol buffers)

---

## 🛣️ Integration Roadmap

### Week 1 (Phase 1) - Current

- [x] Write integration plan
- [x] Create automation scripts
- [x] Document architecture
- [ ] Execute init-risingwave.sh ← **YOU ARE HERE**
- [ ] Verify all tests pass

### Week 2 (Phase 2)

- [ ] Implement nexora-consensus
- [ ] Implement nexora-rpc
- [ ] Write integration tests
- [ ] 3-node Raft cluster test

### Week 3 (Phase 3)

- [ ] Implement nexora-risingwave wrapper
- [ ] RisingWave Meta node wrapper
- [ ] RisingWave Frontend wrapper
- [ ] Basic DDL execution tests

### Week 4 (Phase 4)

- [ ] Implement extensions/meta_raft
- [ ] Create RisingWave patches
- [ ] Test 3-node HA cluster
- [ ] Leader election tests

### Week 5 (Phase 5)

- [ ] Integrate into nexora-app
- [ ] Add HTTP endpoints
- [ ] Feature flag wiring
- [ ] API integration tests

### Week 6 (Phase 6)

- [ ] Build event pipeline
- [ ] EventLogSink implementation
- [ ] End-to-end tests
- [ ] Performance validation

---

## 🎓 Key Concepts

### Git Subtree

RisingWave is embedded as a Git Subtree, not a dependency:

```bash
# Why Subtree?
✓ Can patch RisingWave internals
✓ Single git clone gets everything
✓ No submodule complexity
✓ Easier for contributors

# How to sync?
./scripts/sync-risingwave.sh --upgrade v3.1.0
```

### Feature Flags

Integration is opt-in via feature flags:

```rust
// Always available
use nexora_core::GraphService;

// Only with --features risingwave
#[cfg(feature = "risingwave")]
use nexora_risingwave::RisingWaveModule;
```

### Shared Infrastructure

Both RisingWave and Nexora use the same:
- **nexora-consensus**: Raft abstraction
- **nexora-rpc**: gRPC communication
- Unified configuration and monitoring

---

## 📞 Support

### Questions?

- Review [CLAUDE.md](CLAUDE.md) for development guidelines
- Check [RISINGWAVE_INTEGRATION_PLAN.md](docs/RISINGWAVE_INTEGRATION_PLAN.md) for architecture
- See [PHASE1_QUICKSTART.md](docs/PHASE1_QUICKSTART.md) for execution steps

### Issues?

- Check troubleshooting section in CLAUDE.md
- Review rollback plan in RISINGWAVE_PHASE1_REPORT.md
- Use `git status` and `git log` to understand current state

---

## 🚦 Status Legend

- ✅ Complete
- 🚧 In Progress
- ⏳ Pending
- ❌ Blocked
- ⚠️ At Risk

---

**Current Status**: Phase 1 preparation complete, ready to execute integration script.

**Next Step**: Run `./scripts/init-risingwave.sh`
