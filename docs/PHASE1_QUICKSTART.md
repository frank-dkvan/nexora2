# RisingWave Integration - Quick Start Guide

## Current Status: Phase 1 Ready ✅

All preparation work is complete. You can now execute Phase 1 to integrate RisingWave.

---

## 🚀 Execute Phase 1 (15-30 minutes)

### Step 1: Pre-flight Check

```bash
cd /Users/frank/aiCoding/nexora2

# Verify clean working directory
git status

# Ensure all current tests pass (baseline)
cargo test --workspace

# Expected: All 1590+ tests pass
```

### Step 2: Run Integration

```bash
# Execute the integration script
./scripts/init-risingwave.sh

# What this does:
# ✓ Adds RisingWave v3.0.2 as Git Subtree (~10 minutes)
# ✓ Creates 4 placeholder crates
# ✓ Updates Cargo.toml workspace
# ✓ Updates .gitignore
# ✓ Commits changes automatically
```

### Step 3: Verify Integration

```bash
# 1. Check directory structure
ls -la vendor/risingwave/        # Should exist
ls -la crates/nexora-risingwave/ # Should exist
ls -la crates/nexora-consensus/  # Should exist
ls -la crates/nexora-rpc/        # Should exist
ls -la extensions/meta_raft/     # Should exist

# 2. Verify workspace compiles
cargo check --workspace

# 3. Verify all tests still pass
cargo test --workspace

# Expected: All tests pass (no regressions)
```

### Step 4: Commit Verification

```bash
# Check what was added
git log --oneline -5

# Should see commits like:
# - "feat(risingwave): add Git Subtree v3.0.2"
# - "feat(risingwave): Phase 1 - add integration scaffolding"
```

---

## 📋 Phase 1 Checklist

Before execution:
- [x] All documentation written
- [x] All scripts created and executable
- [x] Integration plan reviewed
- [x] Rollback plan documented

After execution:
- [ ] `vendor/risingwave/` exists
- [ ] 4 new crates created
- [ ] Cargo.toml updated
- [ ] All tests pass
- [ ] Changes committed

---

## ⚠️ Troubleshooting

### "Subtree pull takes too long"
**Expected**: 5-10 minutes for initial fetch  
**Action**: Be patient, RisingWave is ~500MB

### "Merge conflicts during subtree add"
**Unlikely on first run**, but if it happens:
```bash
git status
git merge --abort
# Review conflicts, re-run script
```

### "Cargo check fails after integration"
**Expected behavior**: Placeholder crates won't build yet  
**Action**: This is normal - Phase 2 will implement them

### "Tests fail after integration"
**This is a BLOCKER** - should not happen:
```bash
# Find the breaking change
git bisect start
git bisect bad HEAD
git bisect good <before-integration-commit>

# Or rollback completely
git reset --hard <before-integration-commit>
```

---

## 🎯 What Happens Next?

### Phase 2: Shared Infrastructure (Week 2)

**Goal**: Implement consensus and RPC abstractions

```bash
# Create feature branch
git checkout -b feat/risingwave-phase2-consensus

# Implement nexora-consensus
vim crates/nexora-consensus/src/lib.rs
# - Define ConsensusClient trait
# - Implement RaftConsensusClient (openraft)
# - Write tests

# Implement nexora-rpc
vim crates/nexora-rpc/src/lib.rs
# - Define RpcServer/RpcClient traits
# - Implement with tonic
# - Write tests
```

**Deliverables**:
- Working Raft consensus abstraction
- Working gRPC RPC layer
- 3-node cluster integration test

---

## 📚 Key Documents

| Document | Purpose |
|----------|---------|
| [RISINGWAVE_INTEGRATION_PLAN.md](RISINGWAVE_INTEGRATION_PLAN.md) | Complete 6-phase architecture |
| [RISINGWAVE_PHASE1_REPORT.md](RISINGWAVE_PHASE1_REPORT.md) | Phase 1 detailed report |
| [CLAUDE.md](../CLAUDE.md) | Development guide & conventions |
| [README.md](../README.md) | Project overview |

---

## 🛠️ Useful Commands

```bash
# Check RisingWave upstream versions
./scripts/sync-risingwave.sh --check

# Test patch application (dry-run)
./scripts/apply-patches.sh --check

# Build with RisingWave feature (Phase 3+)
cargo build --features risingwave

# Full build with all features
cargo build --features event-first,risingwave
```

---

## 🎓 Understanding the Integration

### Why Git Subtree?
- Deep integration needed (>1000 lines of interaction)
- Need to patch RisingWave internals
- Easier than submodules for contributors

### Why Feature Flags?
- Zero overhead for non-RisingWave users
- Gradual rollout
- Preserves existing functionality

### What Gets Added?
```
Before Phase 1:
nexora2/
├── crates/ (existing)
└── scripts/ (existing)

After Phase 1:
nexora2/
├── vendor/risingwave/         # NEW: RisingWave v3.0.2 source
├── crates/
│   ├── (existing crates)
│   ├── nexora-risingwave/     # NEW: Wrapper
│   ├── nexora-consensus/      # NEW: Raft abstraction
│   └── nexora-rpc/            # NEW: gRPC abstraction
├── extensions/
│   └── meta_raft/             # NEW: Raft HA for RisingWave Meta
└── patches/                   # NEW: RisingWave patches
```

---

## 💡 Success Criteria

Phase 1 succeeds when:
1. ✅ No compilation errors
2. ✅ All existing tests pass (1590+)
3. ✅ New directory structure exists
4. ✅ Git history is clean

If any of these fail, use the rollback plan.

---

## 🤝 Need Help?

- Check [RISINGWAVE_PHASE1_REPORT.md](RISINGWAVE_PHASE1_REPORT.md) for detailed information
- Review [CLAUDE.md](../CLAUDE.md) for troubleshooting
- Check git log for what was changed: `git log --stat -5`

---

**Ready to execute? Run:**
```bash
./scripts/init-risingwave.sh
```

Good luck! 🚀
