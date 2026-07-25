//! Shared benchmark utilities — graph construction, ID generation, timing, stats.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for benchmark runs.
#[derive(Debug, Clone, Copy)]
pub struct BenchConfig {
    /// Number of shards in the graph service.
    pub num_shards: usize,
    /// Max nodes per shard before LRU eviction.
    pub max_nodes_per_shard: usize,
    /// Channel buffer size per node task.
    pub node_channel_size: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            num_shards: 16,
            max_nodes_per_shard: 100_000,
            node_channel_size: 128,
        }
    }
}

impl BenchConfig {
    /// Convert to GraphServiceConfig.
    pub fn to_graph_config(&self) -> GraphServiceConfig {
        GraphServiceConfig {
            num_shards: self.num_shards,
            max_nodes_per_shard: self.max_nodes_per_shard,
            node_channel_size: self.node_channel_size,
        }
    }
}

/// Create a GraphService backed by InMemoryPersistor.
pub fn make_test_graph(cfg: &BenchConfig) -> GraphService {
    let persistor = Arc::new(InMemoryPersistor::new());
    GraphService::new(cfg.to_graph_config(), persistor)
}

/// Generate `n` deterministic NexoraIds (derived from index).
pub fn generate_random_ids(n: usize) -> Vec<NexoraId> {
    (0..n)
        .map(|i| NexoraId::from_bytes(i.to_be_bytes().to_vec()))
        .collect()
}

/// Generate `n` random NexoraIds using a seeded RNG (reproducible).
pub fn generate_seeded_ids(n: usize, seed: u64) -> Vec<NexoraId> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| {
            let bytes: [u8; 16] = rng.gen();
            NexoraId::from_bytes(bytes.to_vec())
        })
        .collect()
}

/// Create an outgoing HalfEdge of the given type.
pub fn make_edge(edge_type: &str, target: NexoraId) -> HalfEdge {
    HalfEdge::out(Symbol::new(edge_type), target)
}

/// Run a closure and return (result, elapsed).
pub fn timed<T, F: FnOnce() -> T>(f: F) -> (T, Duration) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}

/// Latency statistics computed from a vector of durations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LatencyStats {
    pub count: usize,
    pub min_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
}

impl LatencyStats {
    /// Compute stats from a list of durations (will be sorted in place).
    pub fn from_durations(durations: &mut [Duration]) -> Self {
        if durations.is_empty() {
            return Self {
                count: 0,
                min_us: 0.0,
                max_us: 0.0,
                mean_us: 0.0,
                p50_us: 0.0,
                p95_us: 0.0,
                p99_us: 0.0,
            };
        }
        durations.sort();
        let count = durations.len();
        let min_us = durations[0].as_secs_f64() * 1e6;
        let max_us = durations[count - 1].as_secs_f64() * 1e6;
        let mean_us = durations.iter().map(|d| d.as_secs_f64() * 1e6).sum::<f64>() / count as f64;
        Self {
            count,
            min_us,
            max_us,
            mean_us,
            p50_us: percentile(durations, 50.0),
            p95_us: percentile(durations, 95.0),
            p99_us: percentile(durations, 99.0),
        }
    }
}

/// Compute the p-th percentile from sorted durations. Returns microseconds.
pub fn percentile(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)].as_secs_f64() * 1e6
}

/// Pick a random edge target from a list of node IDs.
pub fn random_target<'a>(
    ids: &'a [NexoraId],
    exclude: &NexoraId,
    rng: &mut StdRng,
) -> &'a NexoraId {
    loop {
        let pick = ids.choose(rng).unwrap();
        if pick != exclude {
            return pick;
        }
    }
}

/// Populate a graph with `n` nodes, each with 3 properties.
pub async fn populate_nodes(svc: &GraphService, ids: &[NexoraId]) {
    for (i, qid) in ids.iter().enumerate() {
        svc.set_property(qid, "id", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
        svc.set_property(qid, "name", PropertyValue::String(format!("node_{i}")))
            .await
            .unwrap();
        svc.set_property(qid, "score", PropertyValue::Float(i as f64 * 0.1))
            .await
            .unwrap();
    }
}

/// Add random edges between nodes. Each node gets `edges_per_node` outgoing edges.
pub async fn populate_edges(svc: &GraphService, ids: &[NexoraId], edges_per_node: usize) {
    let mut rng = StdRng::seed_from_u64(42);
    for qid in ids {
        for _ in 0..edges_per_node {
            let target = random_target(ids, qid, &mut rng);
            let edge = make_edge("LINKS_TO", target.clone());
            svc.add_edge(qid, edge).await.unwrap();
        }
    }
}
