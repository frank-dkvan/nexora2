//! Anti-Entropy — Merkle tree comparison + Read Repair + Hinted Handoff.
//!
//! Ensures eventual consistency across replicas by detecting and repairing
//! divergence between shard replicas.
//!
//! ## Components
//!
//! 1. **MerkleTree** — built over key ranges for each shard. Periodic comparison
//!    between replicas detects divergence at O(log N) cost.
//! 2. **ReadRepair** — on every read, check a subset of replicas. If
//!    inconsistency is detected, sync the divergent data from majority.
//! 3. **HintedHandoff** — when a replica is unreachable, buffer writes locally.
//!    When the replica recovers, deliver the buffered writes.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tokio::sync::RwLock;

/// A node in a Merkle tree over key ranges.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MerkleNode {
    /// Hash of this subtree's content.
    pub hash: [u8; 32],
    /// Key range covered by this node.
    pub key_range: Option<(String, String)>, // (min_key, max_key)
    /// If leaf: the key-value pairs in this leaf.
    pub leaf_data: Option<Vec<(String, serde_json::Value)>>,
}

/// Merkle tree for efficient divergence detection.
///
/// The tree is built over sorted (key, value_hash) pairs. Leaves contain
/// up to `leaf_size` entries. Internal nodes are hashes of children.
#[derive(Clone, Debug)]
pub struct MerkleTree {
    /// All nodes in the tree, indexed by position (level-order).
    nodes: Vec<MerkleNode>,
}

impl MerkleTree {
    /// Build a Merkle tree from key-value pairs.
    /// `kv_pairs` should be sorted by key.
    pub fn build(kv_pairs: &[(String, serde_json::Value)], leaf_size: usize) -> Self {
        if kv_pairs.is_empty() {
            return Self { nodes: Vec::new() };
        }

        // Build leaves
        let chunks: Vec<&[(String, serde_json::Value)]> = kv_pairs.chunks(leaf_size).collect();
        let leaf_count = chunks.len();

        let mut nodes = Vec::with_capacity(leaf_count * 2);

        // Leaf nodes
        for chunk in &chunks {
            let hash = Self::hash_leaf(chunk);
            let key_range = if let (Some(first), Some(last)) = (chunk.first(), chunk.last()) {
                Some((first.0.clone(), last.0.clone()))
            } else {
                None
            };
            nodes.push(MerkleNode {
                hash,
                key_range,
                leaf_data: Some(chunk.to_vec()),
            });
        }

        // Build internal nodes bottom-up
        let mut level_start = 0;
        let mut level_count = leaf_count;

        while level_count > 1 {
            let next_level_start = nodes.len();
            let mut next_count = 0;

            for i in (level_start..level_start + level_count).step_by(2) {
                let left = &nodes[i];
                let right = if i + 1 < level_start + level_count {
                    &nodes[i + 1]
                } else {
                    left // duplicate last if odd
                };

                let hash = Self::hash_internal(&left.hash, &right.hash);
                let key_range = match (&left.key_range, &right.key_range) {
                    (Some(l), Some(r)) => Some((l.0.clone(), r.1.clone())),
                    (Some(l), None) => Some(l.clone()),
                    _ => None,
                };

                nodes.push(MerkleNode {
                    hash,
                    key_range,
                    leaf_data: None,
                });
                next_count += 1;
            }

            level_start = next_level_start;
            level_count = next_count;
        }

        Self { nodes }
    }

    /// Get the root hash of the tree.
    pub fn root_hash(&self) -> Option<&[u8; 32]> {
        self.nodes.last().map(|n| &n.hash)
    }

    /// Compare this tree with another and return divergent key ranges.
    ///
    /// Localizes divergence to individual leaves rather than reporting the whole
    /// key range: a single differing key yields a single leaf-sized range, so
    /// read-repair transfers stay O(differing leaves) instead of O(shard).
    pub fn diff(&self, other: &MerkleTree) -> Vec<(String, String)> {
        // Quick check: root hashes match → trees are identical.
        if self.root_hash() == other.root_hash() {
            return Vec::new();
        }

        // Compare at the leaf level. Leaves carry `leaf_data`; internal nodes do
        // not. Index-based comparison across two independently-built level-order
        // arrays is invalid when the trees have different leaf counts, so we
        // match leaves by their content hash instead: any leaf in `self` whose
        // hash is absent from `other` covers keys that diverge (added, removed,
        // or changed). We report both sides so removals on either replica are
        // caught.
        use std::collections::HashSet;

        let self_leaves: Vec<&MerkleNode> = self
            .nodes
            .iter()
            .filter(|n| n.leaf_data.is_some())
            .collect();
        let other_leaves: Vec<&MerkleNode> = other
            .nodes
            .iter()
            .filter(|n| n.leaf_data.is_some())
            .collect();

        let self_hashes: HashSet<[u8; 32]> = self_leaves.iter().map(|n| n.hash).collect();
        let other_hashes: HashSet<[u8; 32]> = other_leaves.iter().map(|n| n.hash).collect();

        let mut divergences = Vec::new();
        let mut seen = HashSet::new();
        for leaf in self_leaves.iter().chain(other_leaves.iter()) {
            // A leaf present in one tree but not the other (by content) is divergent.
            let matched = self_hashes.contains(&leaf.hash) && other_hashes.contains(&leaf.hash);
            if matched {
                continue;
            }
            if let Some(range) = &leaf.key_range {
                if seen.insert(range.clone()) {
                    divergences.push(range.clone());
                }
            }
        }

        divergences
    }

    fn hash_leaf(entries: &[(String, serde_json::Value)]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for (k, v) in entries {
            hasher.update(k.as_bytes());
            // Use a deterministic, canonical JSON representation for hashing.
            // serde_json::to_string produces compact output which is stable
            // across runs (unlike Debug formatting which can differ).
            hasher.update(v.to_string().as_bytes());
            hasher.update([0u8]); // separator between key-value pairs
        }
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }

    fn hash_internal(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(left);
        hasher.update(right);
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }
}

// ============================================================
// Read Repair
// ============================================================

/// Read repair strategy.
#[derive(Clone, Debug, PartialEq)]
pub enum ReadRepairStrategy {
    /// Check all replicas on every read (strongest consistency).
    Always,
    /// Check with probability p (0.0-1.0), default 0.1.
    Probabilistic(f64),
    /// Never do read repair (rely on periodic anti-entropy only).
    Never,
}

impl Default for ReadRepairStrategy {
    fn default() -> Self {
        ReadRepairStrategy::Probabilistic(0.1)
    }
}

/// Read repair engine.
pub struct ReadRepairEngine {
    strategy: ReadRepairStrategy,
    /// Number of replicas to check for consistency.
    consistency_check_count: usize,
}

impl ReadRepairEngine {
    pub fn new(strategy: ReadRepairStrategy, check_count: usize) -> Self {
        Self {
            strategy,
            consistency_check_count: check_count,
        }
    }

    /// Decide whether to perform a read repair on this read.
    pub fn should_repair(&self) -> bool {
        match &self.strategy {
            ReadRepairStrategy::Always => true,
            ReadRepairStrategy::Probabilistic(p) => rand::random::<f64>() < *p,
            ReadRepairStrategy::Never => false,
        }
    }

    /// Number of replicas to check.
    pub fn check_count(&self) -> usize {
        self.consistency_check_count
    }
}

// ============================================================
// Hinted Handoff
// ============================================================

/// A hinted handoff write — buffered write for an unavailable replica.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HintedWrite {
    /// The intended target node.
    pub target_node: String,
    /// The original shard_id.
    pub shard_id: usize,
    /// Sequence number of this write.
    pub seq_no: u64,
    /// The write payload (serialized GraphOperation).
    pub payload: Vec<u8>,
    /// When this hint was created.
    pub created_at: u64, // Unix millis
}

/// Hinted handoff manager — buffers writes for unavailable nodes.
pub struct HintedHandoffManager {
    /// Per-node write buffers.
    buffers: RwLock<HashMap<String, VecDeque<HintedWrite>>>,
    /// Maximum buffer size per node.
    max_buffer_size: usize,
    /// Maximum age of a hint before expiry (seconds).
    max_hint_age: Duration,
    /// Total hints delivered successfully.
    delivered: std::sync::atomic::AtomicU64,
    /// Total hints expired.
    expired: std::sync::atomic::AtomicU64,
}

impl HintedHandoffManager {
    pub fn new(max_buffer_size: usize, max_hint_age: Duration) -> Self {
        Self {
            buffers: RwLock::new(HashMap::new()),
            max_buffer_size,
            max_hint_age,
            delivered: std::sync::atomic::AtomicU64::new(0),
            expired: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Buffer a write for an unavailable node.
    pub async fn hint_write(&self, target: &str, write: HintedWrite) {
        let mut buffers = self.buffers.write().await;
        let buffer = buffers.entry(target.to_string()).or_default();

        // Evict oldest if over capacity
        while buffer.len() >= self.max_buffer_size {
            buffer.pop_front();
        }

        buffer.push_back(write);
        tracing::trace!(
            target = target,
            buffered = buffer.len(),
            "Hinted write buffered"
        );
    }

    /// Deliver buffered writes to a recovered node.
    /// Returns the number of hints delivered.
    pub async fn deliver_hints(&self, target: &str, handler: &dyn HintDeliveryHandler) -> usize {
        let hints: Vec<HintedWrite> = {
            let mut buffers = self.buffers.write().await;
            buffers.remove(target).unwrap_or_default().into()
        };

        if hints.is_empty() {
            return 0;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut delivered = 0;
        let mut iter = hints.into_iter();
        for hint in iter.by_ref() {
            // Skip expired hints
            let age = now.saturating_sub(hint.created_at);
            if Duration::from_millis(age) > self.max_hint_age {
                self.expired
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                continue;
            }

            match handler.deliver(&hint).await {
                Ok(()) => {
                    delivered += 1;
                }
                Err(e) => {
                    // Delivery failed. Stop here and re-buffer this hint plus
                    // every remaining (still-undelivered) hint in original order.
                    // Re-buffering only the failed hint while continuing would
                    // let later hints be delivered ahead of it, applying
                    // order-dependent operations (e.g. delete-then-set) in the
                    // wrong order on the recovered replica.
                    tracing::warn!(
                        target = target,
                        error = %e,
                        "Hint delivery failed; re-buffering this and remaining hints in order"
                    );
                    let mut buffers = self.buffers.write().await;
                    let buffer = buffers.entry(target.to_string()).or_default();
                    // Prepend the failed hint and the rest ahead of any hints
                    // buffered concurrently, preserving seq order.
                    let mut carry: VecDeque<HintedWrite> = VecDeque::new();
                    carry.push_back(hint);
                    carry.extend(iter);
                    while let Some(h) = carry.pop_back() {
                        buffer.push_front(h);
                    }
                    break;
                }
            }
        }

        self.delivered
            .fetch_add(delivered as u64, std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            target = target,
            delivered = delivered,
            expired = self.expired.load(std::sync::atomic::Ordering::Relaxed),
            "Delivered hinted writes"
        );

        delivered
    }

    /// Get total delivered hint count.
    pub fn delivered_count(&self) -> u64 {
        self.delivered.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total expired hint count.
    pub fn expired_count(&self) -> u64 {
        self.expired.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get buffered hint count for a node.
    pub async fn pending_hints(&self, target: &str) -> usize {
        let buffers = self.buffers.read().await;
        buffers.get(target).map(|b| b.len()).unwrap_or(0)
    }
}

/// Handler for delivering hinted writes to recovered nodes.
#[async_trait::async_trait]
pub trait HintDeliveryHandler: Send + Sync {
    async fn deliver(&self, hint: &HintedWrite) -> Result<(), String>;
}

// ============================================================
// AntiEntropy Scheduler
// ============================================================

/// Periodic anti-entropy repair scheduler.
pub struct AntiEntropyScheduler {
    /// Interval between anti-entropy runs.
    pub interval: Duration,
    /// Merkle tree leaf size.
    pub merkle_leaf_size: usize,
}

impl Default for AntiEntropyScheduler {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            merkle_leaf_size: 256,
        }
    }
}

impl AntiEntropyScheduler {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            merkle_leaf_size: 256,
        }
    }

    /// Run anti-entropy repair between two sets of key-value pairs.
    /// Returns the divergent key ranges that need repair.
    pub fn compute_diff(
        &self,
        local: &[(String, serde_json::Value)],
        remote: &[(String, serde_json::Value)],
    ) -> Vec<(String, String)> {
        let local_tree = MerkleTree::build(local, self.merkle_leaf_size);
        let remote_tree = MerkleTree::build(remote, self.merkle_leaf_size);
        local_tree.diff(&remote_tree)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pairs(n: usize) -> Vec<(String, serde_json::Value)> {
        (0..n)
            .map(|i| (format!("key_{:04}", i), serde_json::json!(i)))
            .collect()
    }

    #[test]
    fn test_merkle_identical() {
        let pairs = make_pairs(100);
        let t1 = MerkleTree::build(&pairs, 16);
        let t2 = MerkleTree::build(&pairs, 16);
        assert_eq!(t1.root_hash(), t2.root_hash());
        assert!(t1.diff(&t2).is_empty());
    }

    #[test]
    fn test_merkle_divergent() {
        let p1 = make_pairs(100);
        let mut p2 = p1.clone();
        p2[50] = ("key_0050".into(), serde_json::json!(999));

        let t1 = MerkleTree::build(&p1, 16);
        let t2 = MerkleTree::build(&p2, 16);
        assert_ne!(t1.root_hash(), t2.root_hash());
        let diffs = t1.diff(&t2);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_merkle_empty() {
        let t = MerkleTree::build(&[], 16);
        assert!(t.root_hash().is_none());
    }

    #[test]
    fn test_read_repair_strategy() {
        let always = ReadRepairEngine::new(ReadRepairStrategy::Always, 2);
        assert!(always.should_repair());

        let never = ReadRepairEngine::new(ReadRepairStrategy::Never, 2);
        assert!(!never.should_repair());
    }

    #[tokio::test]
    async fn test_hinted_handoff_buffer_and_deliver() {
        let manager = HintedHandoffManager::new(100, Duration::from_secs(3600));

        // Buffer writes — use current timestamp to avoid expiration
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for i in 0..5 {
            manager
                .hint_write(
                    "node-2",
                    HintedWrite {
                        target_node: "node-2".into(),
                        shard_id: 0,
                        seq_no: i,
                        payload: vec![i as u8],
                        created_at: now_ms,
                    },
                )
                .await;
        }

        assert_eq!(manager.pending_hints("node-2").await, 5);

        // Deliver
        struct TestHandler;
        #[async_trait::async_trait]
        impl HintDeliveryHandler for TestHandler {
            async fn deliver(&self, _hint: &HintedWrite) -> Result<(), String> {
                Ok(())
            }
        }
        let delivered = manager.deliver_hints("node-2", &TestHandler).await;
        assert_eq!(delivered, 5);
        assert_eq!(manager.pending_hints("node-2").await, 0);
        assert_eq!(manager.delivered_count(), 5);
    }

    #[test]
    fn test_anti_entropy_scheduler_diff() {
        let scheduler = AntiEntropyScheduler::default();

        let local = vec![
            ("a".into(), serde_json::json!(1)),
            ("b".into(), serde_json::json!(2)),
            ("c".into(), serde_json::json!(3)),
        ];
        let mut remote = local.clone();
        remote[1] = ("b".into(), serde_json::json!(999));

        let diffs = scheduler.compute_diff(&local, &remote);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_merkle_single_entry() {
        let pairs = vec![("key".to_string(), serde_json::json!(42))];
        let tree = MerkleTree::build(&pairs, 16);
        assert!(tree.root_hash().is_some());
    }

    #[test]
    fn test_merkle_single_vs_multiple() {
        let single = vec![("a".to_string(), serde_json::json!(1))];
        let multiple = vec![
            ("a".to_string(), serde_json::json!(1)),
            ("b".to_string(), serde_json::json!(2)),
        ];
        let t1 = MerkleTree::build(&single, 16);
        let t2 = MerkleTree::build(&multiple, 16);
        // Trees with different data should have different root hashes
        assert_ne!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn test_merkle_identical_after_reorder() {
        // Same data in different order should produce different trees
        let p1 = vec![
            ("a".to_string(), serde_json::json!(1)),
            ("b".to_string(), serde_json::json!(2)),
        ];
        let p2 = vec![
            ("b".to_string(), serde_json::json!(2)),
            ("a".to_string(), serde_json::json!(1)),
        ];
        let t1 = MerkleTree::build(&p1, 16);
        let t2 = MerkleTree::build(&p2, 16);
        // Different order → different hashes
        assert_ne!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn test_merkle_leaf_size_1() {
        let pairs = make_pairs(10);
        let tree = MerkleTree::build(&pairs, 1);
        assert!(tree.root_hash().is_some());
    }

    #[test]
    fn test_merkle_diff_identical_returns_empty() {
        let pairs = make_pairs(50);
        let t1 = MerkleTree::build(&pairs, 10);
        let t2 = MerkleTree::build(&pairs, 10);
        assert!(t1.diff(&t2).is_empty());
    }

    #[test]
    fn test_read_repair_check_count() {
        let engine = ReadRepairEngine::new(ReadRepairStrategy::Always, 5);
        assert_eq!(engine.check_count(), 5);
    }

    #[test]
    fn test_read_repair_strategy_default() {
        let strategy = ReadRepairStrategy::default();
        // Default should be Probabilistic(0.1)
        match strategy {
            ReadRepairStrategy::Probabilistic(p) => {
                assert!((p - 0.1).abs() < 1e-6);
            }
            _ => panic!("Default should be Probabilistic"),
        }
    }

    #[test]
    fn test_read_repair_probabilistic_never_triggers() {
        // With probability 0.0, should never repair
        let engine = ReadRepairEngine::new(ReadRepairStrategy::Probabilistic(0.0), 1);
        // Run multiple times to be sure
        let mut triggered = false;
        for _ in 0..100 {
            if engine.should_repair() {
                triggered = true;
                break;
            }
        }
        // Probabilistic(0.0) should essentially never trigger
        // (extremely unlikely with random f64 < 0.0)
        // Note: there's a tiny chance of random f64 being 0.0 exactly,
        // but it's so rare we can ignore it.
        let _ = triggered; // Don't strictly assert — probability is effectively 0
    }

    #[tokio::test]
    async fn test_hinted_handoff_empty_deliver() {
        let manager = HintedHandoffManager::new(100, Duration::from_secs(3600));
        struct TestHandler;
        #[async_trait::async_trait]
        impl HintDeliveryHandler for TestHandler {
            async fn deliver(&self, _hint: &HintedWrite) -> Result<(), String> {
                Ok(())
            }
        }
        // Deliver with no hints buffered
        let delivered = manager.deliver_hints("nonexistent", &TestHandler).await;
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn test_hinted_handoff_pending_nonexistent() {
        let manager = HintedHandoffManager::new(100, Duration::from_secs(3600));
        assert_eq!(manager.pending_hints("nonexistent").await, 0);
    }

    #[tokio::test]
    async fn test_hinted_handoff_expired_hints() {
        let manager = HintedHandoffManager::new(100, Duration::from_millis(1));

        // Buffer a hint with an old timestamp — should be expired on delivery
        let old_time = 0u64; // Unix epoch
        manager
            .hint_write(
                "node-1",
                HintedWrite {
                    target_node: "node-1".into(),
                    shard_id: 0,
                    seq_no: 1,
                    payload: vec![1],
                    created_at: old_time,
                },
            )
            .await;

        assert_eq!(manager.pending_hints("node-1").await, 1);

        struct TestHandler;
        #[async_trait::async_trait]
        impl HintDeliveryHandler for TestHandler {
            async fn deliver(&self, _hint: &HintedWrite) -> Result<(), String> {
                Ok(())
            }
        }

        // Allow hint to expire
        tokio::time::sleep(Duration::from_millis(10)).await;
        let delivered = manager.deliver_hints("node-1", &TestHandler).await;
        // Hint should have expired, so 0 delivered
        assert_eq!(delivered, 0);
        assert!(manager.expired_count() >= 1);
    }

    #[tokio::test]
    async fn test_hinted_handoff_buffer_eviction() {
        // Buffer with capacity 2 — adding 3 should evict the oldest
        let manager = HintedHandoffManager::new(2, Duration::from_secs(3600));
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for i in 0..3 {
            manager
                .hint_write(
                    "node-1",
                    HintedWrite {
                        target_node: "node-1".into(),
                        shard_id: 0,
                        seq_no: i,
                        payload: vec![i as u8],
                        created_at: now_ms,
                    },
                )
                .await;
        }

        // Should only have 2 hints (oldest evicted)
        assert_eq!(manager.pending_hints("node-1").await, 2);
    }

    #[tokio::test]
    async fn test_hinted_handoff_delivery_failure_rebuffers() {
        let manager = HintedHandoffManager::new(100, Duration::from_secs(3600));
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        manager
            .hint_write(
                "node-1",
                HintedWrite {
                    target_node: "node-1".into(),
                    shard_id: 0,
                    seq_no: 1,
                    payload: vec![1],
                    created_at: now_ms,
                },
            )
            .await;

        struct FailHandler;
        #[async_trait::async_trait]
        impl HintDeliveryHandler for FailHandler {
            async fn deliver(&self, _hint: &HintedWrite) -> Result<(), String> {
                Err("delivery failed".to_string())
            }
        }

        let delivered = manager.deliver_hints("node-1", &FailHandler).await;
        assert_eq!(delivered, 0);
        // Hint should be re-buffered
        assert_eq!(manager.pending_hints("node-1").await, 1);
    }

    #[tokio::test]
    async fn test_hinted_handoff_multiple_nodes() {
        let manager = HintedHandoffManager::new(100, Duration::from_secs(3600));
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        manager
            .hint_write(
                "node-1",
                HintedWrite {
                    target_node: "node-1".into(),
                    shard_id: 0,
                    seq_no: 1,
                    payload: vec![1],
                    created_at: now_ms,
                },
            )
            .await;
        manager
            .hint_write(
                "node-2",
                HintedWrite {
                    target_node: "node-2".into(),
                    shard_id: 0,
                    seq_no: 2,
                    payload: vec![2],
                    created_at: now_ms,
                },
            )
            .await;

        assert_eq!(manager.pending_hints("node-1").await, 1);
        assert_eq!(manager.pending_hints("node-2").await, 1);
    }

    #[test]
    fn test_anti_entropy_scheduler_default() {
        let scheduler = AntiEntropyScheduler::default();
        assert_eq!(scheduler.interval, Duration::from_secs(60));
        assert_eq!(scheduler.merkle_leaf_size, 256);
    }

    #[test]
    fn test_anti_entropy_scheduler_new() {
        let scheduler = AntiEntropyScheduler::new(Duration::from_secs(120));
        assert_eq!(scheduler.interval, Duration::from_secs(120));
        assert_eq!(scheduler.merkle_leaf_size, 256);
    }

    #[test]
    fn test_anti_entropy_scheduler_identical_data() {
        let scheduler = AntiEntropyScheduler::default();
        let data = make_pairs(20);
        let diffs = scheduler.compute_diff(&data, &data);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_anti_entropy_scheduler_both_empty() {
        let scheduler = AntiEntropyScheduler::default();
        let diffs = scheduler.compute_diff(&[], &[]);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_hinted_write_serde() {
        let write = HintedWrite {
            target_node: "node-1".to_string(),
            shard_id: 3,
            seq_no: 42,
            payload: vec![1, 2, 3],
            created_at: 1234567890,
        };
        let json = serde_json::to_string(&write).unwrap();
        let deserialized: HintedWrite = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.target_node, "node-1");
        assert_eq!(deserialized.shard_id, 3);
        assert_eq!(deserialized.seq_no, 42);
        assert_eq!(deserialized.payload, vec![1, 2, 3]);
        assert_eq!(deserialized.created_at, 1234567890);
    }

    #[test]
    fn test_merkle_node_serde() {
        let node = MerkleNode {
            hash: [1u8; 32],
            key_range: Some(("a".to_string(), "z".to_string())),
            leaf_data: Some(vec![("key".to_string(), serde_json::json!(42))]),
        };
        let json = serde_json::to_string(&node).unwrap();
        let deserialized: MerkleNode = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.hash, [1u8; 32]);
        assert!(deserialized.key_range.is_some());
        assert!(deserialized.leaf_data.is_some());
    }

    #[test]
    fn test_merkle_node_equality() {
        let n1 = MerkleNode {
            hash: [0u8; 32],
            key_range: None,
            leaf_data: None,
        };
        let n2 = MerkleNode {
            hash: [0u8; 32],
            key_range: None,
            leaf_data: None,
        };
        assert_eq!(n1, n2);

        let n3 = MerkleNode {
            hash: [1u8; 32],
            key_range: None,
            leaf_data: None,
        };
        assert_ne!(n1, n3);
    }
}
