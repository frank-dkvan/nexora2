//! TPC-H style graph benchmark — synthetic social network.
//!
//! Generates an LDBC-like social network with Persons, Posts, and friendships.
//! Scale factors: SF-1 (1K persons, 10K posts), SF-3 (3K/30K), SF-10 (10K/100K).
//!
//! Standard queries:
//! 1. Friends of friends (2-hop)
//! 2. Shortest path between two persons
//! 3. Most active users (degree centrality)
//! 4. Common friends between two persons
//! 5. Trending topics in a user's network
//! 6. Shortest message path
//! 7. Friend recommendations (common friends count)

use nexora_core::GraphService;
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

use crate::util::{BenchConfig, LatencyStats};

/// Scale factor for the synthetic graph.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TpcScaleFactor {
    /// SF-1: 1K persons, 10K posts.
    Sf1,
    /// SF-3: 3K persons, 30K posts.
    Sf3,
    /// SF-10: 10K persons, 100K posts.
    Sf10,
}

impl TpcScaleFactor {
    pub fn person_count(&self) -> usize {
        match self {
            Self::Sf1 => 1_000,
            Self::Sf3 => 3_000,
            Self::Sf10 => 10_000,
        }
    }

    pub fn post_count(&self) -> usize {
        match self {
            Self::Sf1 => 10_000,
            Self::Sf3 => 30_000,
            Self::Sf10 => 100_000,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Sf1 => "SF-1",
            Self::Sf3 => "SF-3",
            Self::Sf10 => "SF-10",
        }
    }
}

/// Result of a single TPC query benchmark.
#[derive(Debug, Clone, Serialize)]
pub struct TpcQueryResult {
    pub query_name: String,
    pub scale_factor: String,
    pub iterations: usize,
    pub throughput_qps: f64,
    pub latency: LatencyStats,
    pub result_size: usize,
}

/// The TPC graph benchmark runner.
pub struct TpcGraphBenchmark {
    config: BenchConfig,
    scale: TpcScaleFactor,
    person_ids: Vec<NexoraId>,
    post_ids: Vec<NexoraId>,
    topics: Vec<String>,
}

impl TpcGraphBenchmark {
    pub fn new(config: BenchConfig, scale: TpcScaleFactor) -> Self {
        Self {
            config,
            scale,
            person_ids: Vec::new(),
            post_ids: Vec::new(),
            topics: vec![
                "rust".into(),
                "graph".into(),
                "database".into(),
                "async".into(),
                "benchmark".into(),
                "streaming".into(),
                "distributed".into(),
                "open-source".into(),
            ],
        }
    }

    /// Generate the synthetic social network graph.
    pub async fn generate(&mut self) -> GraphService {
        let persistor = Arc::new(nexora_core::InMemoryPersistor::new());
        let svc = GraphService::new(self.config.to_graph_config(), persistor);

        let person_count = self.scale.person_count();
        let post_count = self.scale.post_count();

        // Generate deterministic IDs
        self.person_ids = (0..person_count)
            .map(|i| NexoraId::from_bytes(format!("p{i:08}").into_bytes()))
            .collect();
        self.post_ids = (0..post_count)
            .map(|i| NexoraId::from_bytes(format!("post{i:08}").into_bytes()))
            .collect();

        let mut rng = StdRng::seed_from_u64(42);

        // Create persons with properties
        for (i, pid) in self.person_ids.iter().enumerate() {
            svc.set_property(pid, "label", PropertyValue::String("Person".into()))
                .await
                .unwrap();
            svc.set_property(pid, "name", PropertyValue::String(format!("Person{i:06}")))
                .await
                .unwrap();
            svc.set_property(pid, "age", PropertyValue::Integer((20 + i % 50) as i64))
                .await
                .unwrap();
        }

        // Create friendships: each person knows 5–20 random others
        let knows_sym = Symbol::new("KNOWS");
        for pid in &self.person_ids {
            let friend_count = rng.gen_range(5..=20);
            for _ in 0..friend_count {
                let friend = self.person_ids.choose(&mut rng).unwrap();
                if friend != pid {
                    let edge = HalfEdge::out(knows_sym.clone(), friend.clone());
                    svc.add_edge(pid, edge).await.unwrap();
                }
            }
        }

        // Create posts with properties and link to authors
        let created_sym = Symbol::new("CREATED");
        let has_creator_sym = Symbol::new("HAS_CREATOR");
        for (i, post_id) in self.post_ids.iter().enumerate() {
            let author = &self.person_ids[i % person_count];
            svc.set_property(post_id, "label", PropertyValue::String("Post".into()))
                .await
                .unwrap();
            svc.set_property(
                post_id,
                "content",
                PropertyValue::String(format!("Post content #{i}")),
            )
            .await
            .unwrap();
            let topic = self.topics.choose(&mut rng).unwrap();
            svc.set_property(post_id, "topic", PropertyValue::String(topic.clone()))
                .await
                .unwrap();

            // Post -> author edge
            svc.add_edge(
                post_id,
                HalfEdge::out(has_creator_sym.clone(), author.clone()),
            )
            .await
            .unwrap();
            // Author -> post edge
            svc.add_edge(author, HalfEdge::out(created_sym.clone(), post_id.clone()))
                .await
                .unwrap();
        }

        svc
    }

    /// Run all 7 standard queries and return results.
    pub async fn run_all_queries(&self, svc: &GraphService) -> Vec<TpcQueryResult> {
        let mut results = Vec::new();

        results.push(self.query_friends_of_friends(svc).await);
        results.push(self.query_shortest_path(svc).await);
        results.push(self.query_degree_centrality(svc).await);
        results.push(self.query_common_friends(svc).await);
        results.push(self.query_trending_topics(svc).await);
        results.push(self.query_shortest_message_path(svc).await);
        results.push(self.query_friend_recommendations(svc).await);

        results
    }

    /// Q1: Friends of friends (2-hop) — find friends-of-friends not already friends.
    async fn query_friends_of_friends(&self, svc: &GraphService) -> TpcQueryResult {
        let sample_size = 50.min(self.person_ids.len());
        let mut latencies = Vec::with_capacity(sample_size);
        let mut total_results = 0;

        let start = Instant::now();
        for i in 0..sample_size {
            let pid = &self.person_ids[i];
            let t0 = Instant::now();
            let fof = self.two_hop(svc, pid).await;
            latencies.push(t0.elapsed());
            total_results += fof.len();
        }
        let elapsed = start.elapsed();

        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        TpcQueryResult {
            query_name: "Q1_friends_of_friends".into(),
            scale_factor: self.scale.name().into(),
            iterations: sample_size,
            throughput_qps: sample_size as f64 / elapsed.as_secs_f64(),
            latency: stats,
            result_size: total_results / sample_size,
        }
    }

    /// Q2: Shortest path between two random persons (BFS).
    async fn query_shortest_path(&self, svc: &GraphService) -> TpcQueryResult {
        let mut rng = StdRng::seed_from_u64(99);
        let sample_size = 20.min(self.person_ids.len());
        let mut latencies = Vec::with_capacity(sample_size);
        let mut total_path_len = 0;
        let mut found = 0;

        let start = Instant::now();
        for _ in 0..sample_size {
            let src = self.person_ids.choose(&mut rng).unwrap();
            let dst = self.person_ids.choose(&mut rng).unwrap();
            if src == dst {
                continue;
            }
            let t0 = Instant::now();
            if let Some(path) = self.bfs_shortest_path(svc, src, dst, 6).await {
                total_path_len += path;
                found += 1;
            }
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();

        let iters = latencies.len();
        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        TpcQueryResult {
            query_name: "Q2_shortest_path".into(),
            scale_factor: self.scale.name().into(),
            iterations: iters,
            throughput_qps: iters as f64 / elapsed.as_secs_f64(),
            latency: stats,
            result_size: if found > 0 { total_path_len / found } else { 0 },
        }
    }

    /// Q3: Most active users (degree centrality) — top-K by edge count.
    async fn query_degree_centrality(&self, svc: &GraphService) -> TpcQueryResult {
        let sample_size = 200.min(self.person_ids.len());
        let mut latencies = Vec::with_capacity(sample_size);
        let mut degrees = Vec::with_capacity(sample_size);

        let start = Instant::now();
        for i in 0..sample_size {
            let pid = &self.person_ids[i];
            let t0 = Instant::now();
            let edges = svc.get_edges(pid).await.unwrap();
            latencies.push(t0.elapsed());
            degrees.push(edges.len());
        }
        let elapsed = start.elapsed();

        degrees.sort_by(|a, b| b.cmp(a));
        let top_k_avg = degrees.iter().take(10).sum::<usize>() / 10.min(degrees.len()).max(1);

        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        TpcQueryResult {
            query_name: "Q3_degree_centrality".into(),
            scale_factor: self.scale.name().into(),
            iterations: sample_size,
            throughput_qps: sample_size as f64 / elapsed.as_secs_f64(),
            latency: stats,
            result_size: top_k_avg,
        }
    }

    /// Q4: Common friends between two persons.
    async fn query_common_friends(&self, svc: &GraphService) -> TpcQueryResult {
        let mut rng = StdRng::seed_from_u64(77);
        let sample_size = 50.min(self.person_ids.len());
        let mut latencies = Vec::with_capacity(sample_size);
        let mut total_common = 0;

        let start = Instant::now();
        for _ in 0..sample_size {
            let a = self.person_ids.choose(&mut rng).unwrap();
            let b = self.person_ids.choose(&mut rng).unwrap();
            if a == b {
                continue;
            }
            let t0 = Instant::now();
            let common = self.common_friends(svc, a, b).await;
            latencies.push(t0.elapsed());
            total_common += common;
        }
        let elapsed = start.elapsed();

        let iters = latencies.len();
        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        TpcQueryResult {
            query_name: "Q4_common_friends".into(),
            scale_factor: self.scale.name().into(),
            iterations: iters,
            throughput_qps: iters as f64 / elapsed.as_secs_f64(),
            latency: stats,
            result_size: if iters > 0 { total_common / iters } else { 0 },
        }
    }

    /// Q5: Trending topics in a user's network (2-hop posts).
    async fn query_trending_topics(&self, svc: &GraphService) -> TpcQueryResult {
        let sample_size = 30.min(self.person_ids.len());
        let mut latencies = Vec::with_capacity(sample_size);

        let start = Instant::now();
        for i in 0..sample_size {
            let pid = &self.person_ids[i];
            let t0 = Instant::now();
            let _topics = self.trending_in_network(svc, pid).await;
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();

        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        TpcQueryResult {
            query_name: "Q5_trending_topics".into(),
            scale_factor: self.scale.name().into(),
            iterations: sample_size,
            throughput_qps: sample_size as f64 / elapsed.as_secs_f64(),
            latency: stats,
            result_size: self.topics.len(),
        }
    }

    /// Q6: Shortest message path (path via posts between persons).
    async fn query_shortest_message_path(&self, svc: &GraphService) -> TpcQueryResult {
        let mut rng = StdRng::seed_from_u64(55);
        let sample_size = 20.min(self.person_ids.len());
        let mut latencies = Vec::with_capacity(sample_size);

        let start = Instant::now();
        for _ in 0..sample_size {
            let src = self.person_ids.choose(&mut rng).unwrap();
            let t0 = Instant::now();
            // Find shortest path from src to any post via KNOWS edges (depth 3)
            let _ = self.bfs_to_post(svc, src, 3).await;
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();

        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        TpcQueryResult {
            query_name: "Q6_shortest_message_path".into(),
            scale_factor: self.scale.name().into(),
            iterations: sample_size,
            throughput_qps: sample_size as f64 / elapsed.as_secs_f64(),
            latency: stats,
            result_size: 0,
        }
    }

    /// Q7: Friend recommendations (common friends count).
    async fn query_friend_recommendations(&self, svc: &GraphService) -> TpcQueryResult {
        let sample_size = 30.min(self.person_ids.len());
        let mut latencies = Vec::with_capacity(sample_size);
        let mut total_recs = 0;

        let start = Instant::now();
        for i in 0..sample_size {
            let pid = &self.person_ids[i];
            let t0 = Instant::now();
            let recs = self.friend_recommendations(svc, pid).await;
            latencies.push(t0.elapsed());
            total_recs += recs;
        }
        let elapsed = start.elapsed();

        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        TpcQueryResult {
            query_name: "Q7_friend_recommendations".into(),
            scale_factor: self.scale.name().into(),
            iterations: sample_size,
            throughput_qps: sample_size as f64 / elapsed.as_secs_f64(),
            latency: stats,
            result_size: if sample_size > 0 {
                total_recs / sample_size
            } else {
                0
            },
        }
    }

    // ====== Graph algorithms ======

    /// 2-hop traversal: friends of friends, excluding direct friends and self.
    async fn two_hop(&self, svc: &GraphService, start: &NexoraId) -> Vec<NexoraId> {
        let mut direct = HashSet::new();
        direct.insert(start.clone());

        // Get direct friends
        let edges = svc.get_edges(start).await.unwrap();
        for e in &edges {
            if e.edge_type.as_str() == "KNOWS" && e.direction.is_out() {
                direct.insert(e.other.clone());
            }
        }

        // Get friends-of-friends
        let mut fof = Vec::new();
        let mut seen = direct.clone();
        for e in &edges {
            if e.edge_type.as_str() == "KNOWS" && e.direction.is_out() {
                let f_edges = svc.get_edges(&e.other).await.unwrap();
                for fe in &f_edges {
                    if fe.edge_type.as_str() == "KNOWS"
                        && fe.direction.is_out()
                        && !seen.contains(&fe.other)
                    {
                        seen.insert(fe.other.clone());
                        fof.push(fe.other.clone());
                    }
                }
            }
        }
        fof
    }

    /// BFS shortest path between two nodes, up to max_depth hops.
    async fn bfs_shortest_path(
        &self,
        svc: &GraphService,
        src: &NexoraId,
        dst: &NexoraId,
        max_depth: usize,
    ) -> Option<usize> {
        let mut visited = HashSet::new();
        visited.insert(src.clone());
        let mut queue = VecDeque::new();
        queue.push_back((src.clone(), 0usize));

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            let edges = svc.get_edges(&node).await.unwrap();
            for e in edges {
                if e.edge_type.as_str() == "KNOWS" && e.direction.is_out() {
                    if &e.other == dst {
                        return Some(depth + 1);
                    }
                    if visited.insert(e.other.clone()) {
                        queue.push_back((e.other, depth + 1));
                    }
                }
            }
        }
        None
    }

    /// Count common friends between two persons.
    async fn common_friends(&self, svc: &GraphService, a: &NexoraId, b: &NexoraId) -> usize {
        let edges_a = svc.get_edges(a).await.unwrap();
        let edges_b = svc.get_edges(b).await.unwrap();
        let friends_a: HashSet<NexoraId> = edges_a
            .into_iter()
            .filter(|e| e.edge_type.as_str() == "KNOWS" && e.direction.is_out())
            .map(|e| e.other)
            .collect();
        let friends_b: HashSet<NexoraId> = edges_b
            .into_iter()
            .filter(|e| e.edge_type.as_str() == "KNOWS" && e.direction.is_out())
            .map(|e| e.other)
            .collect();
        friends_a.intersection(&friends_b).count()
    }

    /// Find trending topics in a user's 2-hop network.
    async fn trending_in_network(
        &self,
        svc: &GraphService,
        start: &NexoraId,
    ) -> HashMap<String, usize> {
        let mut topic_counts: HashMap<String, usize> = HashMap::new();

        // Get direct friends
        let edges = svc.get_edges(start).await.unwrap();
        let friends: Vec<NexoraId> = edges
            .into_iter()
            .filter(|e| e.edge_type.as_str() == "KNOWS" && e.direction.is_out())
            .map(|e| e.other)
            .collect();

        // For each friend, get their posts and collect topics
        for friend in &friends {
            let f_edges = svc.get_edges(friend).await.unwrap();
            for fe in f_edges {
                if fe.edge_type.as_str() == "CREATED" && fe.direction.is_out() {
                    if let Ok(Some(PropertyValue::String(topic))) =
                        svc.get_property(&fe.other, "topic").await
                    {
                        *topic_counts.entry(topic).or_default() += 1;
                    }
                }
            }
        }
        topic_counts
    }

    /// BFS to find nearest post node from a person.
    async fn bfs_to_post(
        &self,
        svc: &GraphService,
        src: &NexoraId,
        max_depth: usize,
    ) -> Option<NexoraId> {
        let mut visited = HashSet::new();
        visited.insert(src.clone());
        let mut queue = VecDeque::new();
        queue.push_back((src.clone(), 0usize));

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            let edges = svc.get_edges(&node).await.unwrap();
            for e in edges {
                if e.edge_type.as_str() == "CREATED" && e.direction.is_out() {
                    return Some(e.other);
                }
                if e.edge_type.as_str() == "KNOWS"
                    && e.direction.is_out()
                    && visited.insert(e.other.clone())
                {
                    queue.push_back((e.other, depth + 1));
                }
            }
        }
        None
    }

    /// Friend recommendations: count common friends with friends-of-friends.
    async fn friend_recommendations(&self, svc: &GraphService, start: &NexoraId) -> usize {
        let fof = self.two_hop(svc, start).await;
        // Each fof is a potential recommendation; count those with >= 2 common friends
        let mut recs = 0;
        for candidate in &fof {
            let common = self.common_friends(svc, start, candidate).await;
            if common >= 2 {
                recs += 1;
            }
        }
        recs
    }

    /// Run the full TPC benchmark and return all query results.
    /// This is a convenience method that creates its own runtime.
    pub fn run_full(mut self) -> Vec<TpcQueryResult> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let svc = self.generate().await;
            self.run_all_queries(&svc).await
        })
    }
}
