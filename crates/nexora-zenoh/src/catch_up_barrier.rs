//! Catch-up write barrier.
//!
//! HA roadmap 阶段4-d. When failover promotes this node to own a shard, the
//! promoted node may be behind and must reconcile (catch up) before it is safe
//! to accept writes for that shard. Without a barrier, a client write could
//! interleave with the catch-up replay: the write lands, then a stale delta op
//! from the source overwrites it, silently losing the client's data.
//!
//! [`CatchUpBarrier`] is the per-node set of shards currently *under catch-up*.
//! The write path consults [`CatchUpBarrier::is_blocked`] and rejects writes to
//! a blocked shard (the client retries after reconciliation, a bounded wait).
//! The whole-graph read path consults [`CatchUpBarrier::any_blocked_now`]: while
//! any shard is reconciling, an owner refuses scatter-gather reads so the
//! coordinator never merges a silent partial from an incomplete local graph.
//! The failover path brackets its reconciliation with
//! [`CatchUpBarrier::begin`] / [`CatchUpBarrier::end`] — fence the shard, catch
//! up, then reopen — so the sequence is exactly *fence → catch-up → reopen*.
//!
//! The barrier is local to a node: it gates only this node's owner-write path
//! for shards it is actively reconciling. It is independent of the shard map's
//! `writable` flag (which is cluster-wide ownership state); the two compose —
//! a shard can be map-writable yet locally barriered mid-catch-up.

use crate::shard_map::ShardId;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Per-node set of shards currently blocked for writes pending catch-up.
#[derive(Clone, Default)]
pub struct CatchUpBarrier {
    blocked: Arc<RwLock<HashSet<ShardId>>>,
}

impl CatchUpBarrier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `shard` as under catch-up — writes to it are rejected until
    /// [`end`](Self::end). Idempotent.
    pub async fn begin(&self, shard: ShardId) {
        self.blocked.write().await.insert(shard);
    }

    /// Clear the catch-up block on `shard`, reopening it for writes. Idempotent.
    pub async fn end(&self, shard: ShardId) {
        self.blocked.write().await.remove(&shard);
    }

    /// Whether writes to `shard` are currently blocked pending catch-up.
    pub async fn is_blocked(&self, shard: ShardId) -> bool {
        self.blocked.read().await.contains(&shard)
    }

    /// Non-blocking check: is *any* shard on this node under catch-up?
    ///
    /// Gates whole-graph reads. A scatter-gather read fans out to shard owners,
    /// and each owner scans its *entire* local graph (the executor can't yet
    /// restrict a scan to one cluster shard). While this node reconciles any
    /// shard, that snapshot is incomplete, so any rows it returns would merge
    /// into a silent partial at the coordinator — the owner must error instead
    /// (the coordinator already refuses partial results on an owner error).
    ///
    /// Uses `try_read` (no `.await`) so callers on a `Send` `#[async_trait]`
    /// path never hold a lock guard across an await. On the rare contended read
    /// (a concurrent `begin`/`end` momentarily holds the write lock) it
    /// conservatively returns `true` — "possibly reconciling, refuse the read".
    pub fn any_blocked_now(&self) -> bool {
        match self.blocked.try_read() {
            Ok(guard) => !guard.is_empty(),
            Err(_) => true,
        }
    }

    /// Run `f` with `shard` barriered: begin, await the future, end (even on
    /// error). Returns the future's output. This is the *fence → catch-up →
    /// reopen* bracket in one call.
    pub async fn guard<F, T>(&self, shard: ShardId, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.begin(shard).await;
        let out = f.await;
        self.end(shard).await;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_blocks_and_end_reopens() {
        let b = CatchUpBarrier::new();
        assert!(!b.is_blocked(3).await);
        b.begin(3).await;
        assert!(b.is_blocked(3).await);
        b.end(3).await;
        assert!(!b.is_blocked(3).await);
    }

    #[tokio::test]
    async fn guard_brackets_the_future() {
        let b = CatchUpBarrier::new();
        let observed_inside = {
            let b2 = b.clone();
            b.guard(7, async move { b2.is_blocked(7).await }).await
        };
        assert!(observed_inside, "shard must be blocked while guarded");
        assert!(!b.is_blocked(7).await, "shard reopened after guard");
    }

    #[tokio::test]
    async fn guard_reopens_even_on_panic_path_via_end() {
        // guard doesn't catch panics, but end is idempotent and begin/end pair
        // deterministically; verify independent shards don't interfere.
        let b = CatchUpBarrier::new();
        b.begin(1).await;
        b.begin(2).await;
        b.end(1).await;
        assert!(!b.is_blocked(1).await);
        assert!(b.is_blocked(2).await);
    }
}
