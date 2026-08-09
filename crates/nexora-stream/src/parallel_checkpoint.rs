//! P1-4: 并行 Checkpoint 刷新优化
//!
//! **问题**: 原始实现串行刷新所有分片，导致延迟与分片数量成正比
//! **优化**: 并行刷新所有分片，利用 RocksDB 的多线程写入能力
//! **预期**: 10倍加速（N个分片串行 → 并行）

use nexora_core::GraphService;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{debug, warn};

/// 并行 Checkpoint 刷新器
///
/// 优化前（串行）:
/// ```text
/// for shard in 0..N {
///     flush_shard(shard).await;  // 串行
/// }
/// 总延迟 = N * avg_flush_time
/// ```
///
/// 优化后（并行）:
/// ```text
/// tokio::spawn for each shard
/// join_all().await
/// 总延迟 = max(flush_time_i)
/// ```
pub struct ParallelCheckpointFlusher {
    /// 最大并行刷新任务数（默认: CPU核心数）
    max_parallelism: usize,
}

impl ParallelCheckpointFlusher {
    pub fn new() -> Self {
        Self {
            max_parallelism: num_cpus::get(),
        }
    }

    pub fn with_parallelism(max_parallelism: usize) -> Self {
        Self { max_parallelism }
    }

    /// 并行刷新多个分片范围
    ///
    /// # Arguments
    /// * `graph` - 图服务实例
    /// * `shard_ranges` - 要刷新的分片范围列表 `[(start, end), ...]`
    ///
    /// # Returns
    /// 每个范围内节点总数 `Vec<usize>`
    pub async fn flush_ranges(
        &self,
        graph: Arc<GraphService>,
        shard_ranges: Vec<(usize, usize)>,
    ) -> Result<Vec<usize>, String> {
        let total_ranges = shard_ranges.len();
        debug!(
            "Starting parallel checkpoint flush: {} ranges, max_parallelism={}",
            total_ranges, self.max_parallelism
        );

        let mut join_set = JoinSet::new();
        let mut results = vec![0; total_ranges];

        // 分批并行刷新（避免过度并发）
        for (batch_idx, batch) in shard_ranges.chunks(self.max_parallelism).enumerate() {
            // 启动当前批次的并行刷新
            for (local_idx, &(start, end)) in batch.iter().enumerate() {
                let graph_clone = Arc::clone(&graph);
                let global_idx = batch_idx * self.max_parallelism + local_idx;

                join_set.spawn(async move {
                    let mut total_nodes = 0usize;
                    for shard_id in start..end {
                        match graph_clone.flush_shard(shard_id).await {
                            Ok(count) => {
                                debug!("Flushed shard {}: {} nodes", shard_id, count);
                                total_nodes += count;
                            }
                            Err(e) => {
                                warn!("Failed to flush shard {}: {}", shard_id, e);
                                return Err(format!("shard {} flush failed: {}", shard_id, e));
                            }
                        }
                    }
                    Ok((global_idx, total_nodes))
                });
            }

            // 等待当前批次完成
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok((idx, count))) => {
                        results[idx] = count;
                    }
                    Ok(Err(e)) => {
                        // 刷新失败 - 取消所有剩余任务
                        join_set.abort_all();
                        return Err(e);
                    }
                    Err(e) => {
                        join_set.abort_all();
                        return Err(format!("task join error: {}", e));
                    }
                }
            }
        }

        debug!(
            "Parallel checkpoint flush completed: {} ranges, total nodes: {}",
            total_ranges,
            results.iter().sum::<usize>()
        );

        Ok(results)
    }

    /// 并行刷新所有分片（简化接口）
    ///
    /// # Arguments
    /// * `graph` - 图服务实例
    /// * `total_shards` - Barrier 协调器的分片数
    /// * `graph_shards` - 图引擎实际的分片数
    ///
    /// # Returns
    /// `(nodes_flushed, shard_counts)` - 总节点数和每个逻辑分片的节点数
    pub async fn flush_all(
        &self,
        graph: Arc<GraphService>,
        total_shards: usize,
        graph_shards: usize,
    ) -> Result<(u64, Vec<usize>), String> {
        // 构建分片范围（与原始逻辑相同）
        let mut shard_ranges = Vec::with_capacity(total_shards);
        for shard_id in 0..total_shards {
            let (start, end) = if total_shards == graph_shards {
                (shard_id, shard_id + 1)
            } else if shard_id + 1 == total_shards {
                // 最后一个逻辑分片吸收所有剩余的图分片
                (shard_id, graph_shards.max(shard_id + 1))
            } else {
                (shard_id, (shard_id + 1).min(graph_shards))
            };
            shard_ranges.push((start, end));
        }

        // 并行刷新
        let shard_counts = self.flush_ranges(graph, shard_ranges).await?;
        let nodes_flushed: u64 = shard_counts.iter().sum::<usize>() as u64;

        Ok((nodes_flushed, shard_counts))
    }
}

impl Default for ParallelCheckpointFlusher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parallel_flush_basic() {
        // 需要实际的 GraphService 实例
        // 这里只是结构测试
        let flusher = ParallelCheckpointFlusher::new();
        assert!(flusher.max_parallelism > 0);
    }

    #[test]
    fn test_shard_range_logic() {
        // 测试分片范围计算逻辑
        let total_shards = 4;
        let graph_shards = 4;

        let mut ranges = Vec::new();
        for shard_id in 0..total_shards {
            let (start, end) = if total_shards == graph_shards {
                (shard_id, shard_id + 1)
            } else if shard_id + 1 == total_shards {
                (shard_id, graph_shards.max(shard_id + 1))
            } else {
                (shard_id, (shard_id + 1).min(graph_shards))
            };
            ranges.push((start, end));
        }

        assert_eq!(ranges, vec![(0, 1), (1, 2), (2, 3), (3, 4)]);
    }

    #[test]
    fn test_shard_range_absorb_tail() {
        // 测试最后分片吸收剩余逻辑
        let total_shards = 2;
        let graph_shards = 4;

        let mut ranges = Vec::new();
        for shard_id in 0..total_shards {
            let (start, end) = if total_shards == graph_shards {
                (shard_id, shard_id + 1)
            } else if shard_id + 1 == total_shards {
                (shard_id, graph_shards.max(shard_id + 1))
            } else {
                (shard_id, (shard_id + 1).min(graph_shards))
            };
            ranges.push((start, end));
        }

        // 第一个分片: [0, 1)
        // 最后一个分片: [1, 4) - 吸收剩余的 shard 1, 2, 3
        assert_eq!(ranges, vec![(0, 1), (1, 4)]);
    }
}
