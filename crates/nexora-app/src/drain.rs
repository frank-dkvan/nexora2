//! Drain state for graceful rolling upgrades (E4).
//!
//! When a node is draining it rejects new inbound writes (returning 503) while
//! still serving in-flight reads.  The HTTP endpoint
//! `POST /api/v2/admin/drain` sets the flag and waits a configurable grace
//! period before responding so callers know the node is safe to stop.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Shared, cheaply-cloneable drain flag.
///
/// Uses an `AtomicBool` (not a `Mutex`) because it is checked on every write
/// path hot loop with `Ordering::Acquire` — a lock-free read.
#[derive(Clone, Default)]
pub struct DrainState(pub Arc<AtomicBool>);

impl DrainState {
    /// Returns `true` once `begin_drain` has been called.
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Flip the drain flag.  Idempotent; subsequent calls are no-ops.
    pub fn begin_drain(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `begin_drain` flips `is_draining` to true; the initial state is false.
    #[test]
    fn drain_state_rejects_writes_after_begin() {
        let state = DrainState::default();
        assert!(!state.is_draining(), "should not be draining initially");

        state.begin_drain();

        assert!(state.is_draining(), "should be draining after begin_drain");

        // Idempotent second call must not panic or revert.
        state.begin_drain();
        assert!(
            state.is_draining(),
            "still draining after second begin_drain"
        );
    }

    /// Clones share the same underlying flag.
    #[test]
    fn drain_state_clone_shares_flag() {
        let a = DrainState::default();
        let b = a.clone();

        a.begin_drain();
        assert!(b.is_draining(), "clone must observe the same flag");
    }
}
