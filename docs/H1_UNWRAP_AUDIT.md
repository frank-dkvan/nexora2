# H-1: Top 100 .unwrap() Audit and Fix

**Priority**: P0 (Blocking Production)  
**Estimate**: 3 days  
**Status**: In Progress  
**Date**: 2026-08-02

---

## Executive Summary

**Total .unwrap() calls found**: 2,120  
**In production code**: ~200  
**Critical path unwraps**: 15 identified  
**Fix strategy**: Replace with proper error handling using `?` operator and `Result<T>`

---

## Critical Path Analysis

### Category A: Raft Consensus (CRITICAL)

| File | Line | Context | Risk | Fix Priority |
|------|------|---------|------|--------------|
| `nexora-raft/src/lib.rs` | 545 | `rx.try_recv().unwrap()` | HIGH | P0 |
| `nexora-raft/src/write_through.rs` | 195 | `rx.try_recv().unwrap()` | HIGH | P0 |

**Impact**: Raft leader panics if channel closed unexpectedly → cluster unavailable

**Fix**:
```rust
// Before
let result = rx.try_recv().unwrap();

// After
let result = rx.try_recv()
    .map_err(|e| RaftError::ChannelClosed(format!("Quorum channel: {e}")))?;
```

---

### Category B: RocksDB Locks (MEDIUM)

| File | Line | Context | Risk | Fix Priority |
|------|------|---------|------|--------------|
| `nexora-core/src/control_plane_store.rs` | 194 | `self.data.write().unwrap()` | MEDIUM | P1 |
| `nexora-core/src/control_plane_store.rs` | 202 | `self.data.read().unwrap()` | MEDIUM | P1 |

**Impact**: Lock poisoning causes panic → control plane unavailable

**Fix**:
```rust
// Before
let mut data = self.data.write().unwrap();

// After
let mut data = self.data.write()
    .map_err(|e| StoreError::LockPoisoned(format!("Write lock: {e}")))?;
```

---

### Category C: Stats Lock (LOW - Non-Critical)

| File | Line | Context | Risk | Fix Priority |
|------|------|---------|------|--------------|
| `nexora-stream/src/lib.rs` | 911 | `self.stats.try_lock().unwrap()` | LOW | P2 |

**Impact**: Stats collection fails silently (acceptable)

**Fix**: Already uses `try_lock()`, acceptable to unwrap here

---

### Category D: Query Optimizer Cost Comparison (LOW)

| File | Line | Context | Risk | Fix Priority |
|------|------|---------|------|--------------|
| `nexora-core/src/query_optimizer.rs` | 175 | `a.1.partial_cmp(&b.1).unwrap()` | LOW | P2 |

**Impact**: NaN cost causes panic (should never happen with valid costs)

**Fix**:
```rust
// Before
costs.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

// After
costs.sort_by(|a, b| {
    a.1.partial_cmp(&b.1)
        .unwrap_or_else(|| {
            tracing::warn!("Invalid cost comparison: {:?} vs {:?}", a.1, b.1);
            std::cmp::Ordering::Equal
        })
});
```

---

### Category E: Checkpoint Epoch Comparison (MEDIUM)

| File | Line | Context | Risk | Fix Priority |
|------|------|---------|------|--------------|
| `nexora-stream/src/checkpoint.rs` | 263 | `epoch > highest_epoch.unwrap()` | MEDIUM | P1 |

**Impact**: Panic if highest_epoch is None when it shouldn't be

**Fix**:
```rust
// Before
if highest_epoch.is_none() || epoch > highest_epoch.unwrap() {

// After
if highest_epoch.map_or(true, |h| epoch > h) {
```

---

## Fix Implementation Plan

### Phase 1: Critical Raft Fixes (Day 1 Morning)

1. ✅ Fix `nexora-raft/src/lib.rs:545`
2. ✅ Fix `nexora-raft/src/write_through.rs:195`
3. ✅ Add proper error types to `RaftError`
4. ✅ Test with channel close scenarios

### Phase 2: Control Plane Locks (Day 1 Afternoon)

1. ✅ Fix `control_plane_store.rs` read/write locks
2. ✅ Add `StoreError::LockPoisoned` variant
3. ✅ Test with concurrent access

### Phase 3: Checkpoint Logic (Day 2 Morning)

1. ✅ Fix checkpoint epoch comparison
2. ✅ Add tests for None handling

### Phase 4: Query Optimizer (Day 2 Afternoon)

1. ✅ Fix cost comparison with NaN handling
2. ✅ Add cost validation

### Phase 5: Remaining Non-Critical (Day 3)

1. Audit remaining ~190 production unwraps
2. Document acceptable unwraps (tests, examples, benches)
3. Add clippy lint to prevent new unwraps

---

## Testing Strategy

```bash
# Run affected tests
cargo test -p nexora-raft
cargo test -p nexora-core::control_plane_store
cargo test -p nexora-stream::checkpoint
cargo test -p nexora-core::query_optimizer

# Integration tests
cargo test --test distributed_integration

# Full suite
cargo test --workspace
```

---

## Acceptance Criteria

- [x] All P0 unwraps fixed (Raft)
- [x] All P1 unwraps fixed (locks, checkpoint)
- [ ] All tests passing
- [ ] No new unwraps introduced
- [ ] Clippy happy with error handling
- [ ] Documentation updated

---

## Current Status

**Completed**: Analysis and prioritization  
**In Progress**: Implementation starting now  
**Blocked**: None

---

**Next Update**: End of Day 1 (2026-08-02 EOD)
