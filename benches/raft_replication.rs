use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use nexora_raft::{
    AppendEntriesResponse, LogEntry, RaftConfig, RaftLogReplicator, ReplicationError,
    ReplicationTarget, SnapshotData,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

// Mock replication target that simulates network latency
struct MockRaftTarget {
    latency: Duration,
    fail_rate: f64,
}

impl MockRaftTarget {
    fn new(latency: Duration) -> Self {
        Self {
            latency,
            fail_rate: 0.0,
        }
    }
}

#[async_trait::async_trait]
impl ReplicationTarget for MockRaftTarget {
    async fn append_entries(
        &self,
        _entries: Vec<LogEntry>,
        _leader_commit: u64,
    ) -> Result<AppendEntriesResponse, ReplicationError> {
        // Simulate network latency
        tokio::time::sleep(self.latency).await;

        Ok(AppendEntriesResponse {
            term: 1,
            success: true,
            last_committed: 10,
            last_log_seq: 10,
        })
    }

    async fn install_snapshot(
        &self,
        _shard_id: usize,
        _from_seq: u64,
    ) -> Result<SnapshotData, ReplicationError> {
        unimplemented!()
    }
}

// 串行复制基准（优化前）
async fn serial_replication(replicator: &Arc<RaftLogReplicator>, peers: &[String], latency: Duration) {
    for peer in peers {
        let target = MockRaftTarget::new(latency);
        let _ = replicator.replicate_to(&target, peer).await;
    }
}

// 并行复制基准（优化后）
async fn parallel_replication(replicator: &Arc<RaftLogReplicator>, peers: &[String], latency: Duration) {
    use futures::stream::{FuturesUnordered, StreamExt};

    let mut replication_futures = FuturesUnordered::new();

    for peer in peers {
        let target = MockRaftTarget::new(latency);
        let replicator_clone = Arc::clone(replicator);
        let peer_clone = peer.clone();

        replication_futures.push(async move {
            replicator_clone.replicate_to(&target, &peer_clone).await
        });
    }

    while let Some(_) = replication_futures.next().await {}
}

fn bench_replication_serial(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("raft_replication_serial");

    for num_followers in [2, 4, 8].iter() {
        let config = RaftConfig {
            quorum_size: (*num_followers / 2) + 1,
            total_nodes: num_followers + 1,
            rpc_timeout: Duration::from_secs(5),
            ..Default::default()
        };

        let replicator = Arc::new(RaftLogReplicator::new(config));
        let peers: Vec<String> = (0..*num_followers)
            .map(|i| format!("peer-{}", i))
            .collect();

        // 注册 followers
        rt.block_on(async {
            for peer in &peers {
                replicator.register_follower(peer, 0, 0).await;
            }
        });

        group.bench_with_input(
            BenchmarkId::new("30ms_latency", num_followers),
            num_followers,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    serial_replication(
                        black_box(&replicator),
                        black_box(&peers),
                        black_box(Duration::from_millis(30)),
                    )
                    .await
                });
            },
        );
    }

    group.finish();
}

fn bench_replication_parallel(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("raft_replication_parallel");

    for num_followers in [2, 4, 8].iter() {
        let config = RaftConfig {
            quorum_size: (*num_followers / 2) + 1,
            total_nodes: num_followers + 1,
            rpc_timeout: Duration::from_secs(5),
            ..Default::default()
        };

        let replicator = Arc::new(RaftLogReplicator::new(config));
        let peers: Vec<String> = (0..*num_followers)
            .map(|i| format!("peer-{}", i))
            .collect();

        // 注册 followers
        rt.block_on(async {
            for peer in &peers {
                replicator.register_follower(peer, 0, 0).await;
            }
        });

        group.bench_with_input(
            BenchmarkId::new("30ms_latency", num_followers),
            num_followers,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    parallel_replication(
                        black_box(&replicator),
                        black_box(&peers),
                        black_box(Duration::from_millis(30)),
                    )
                    .await
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_replication_serial, bench_replication_parallel);
criterion_main!(benches);
