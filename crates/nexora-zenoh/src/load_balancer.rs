// P1-4: 分片负载均衡
//
// 虚拟分片 + 动态分裂机制：
// - 虚拟分片映射到物理节点
// - 基于负载指标自动分裂热点分片
// - 最小化数据迁移的重新映射
// - 可观测的负载分布和分裂历史

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// 虚拟分片 ID (比物理分片多 10-100 倍)
pub type VirtualShardId = u32;

/// 物理节点 ID
pub type NodeId = u64;

/// 负载指标
#[derive(Debug, Clone, Copy, Default)]
pub struct LoadMetrics {
    /// 每秒写入数
    pub writes_per_sec: f64,
    /// 存储大小（字节）
    pub storage_bytes: u64,
    /// CPU 使用率 (0.0 - 1.0)
    pub cpu_usage: f64,
}

/// 虚拟分片映射条目
#[derive(Debug, Clone)]
pub struct VirtualShardMapping {
    pub virtual_shard: VirtualShardId,
    pub physical_node: NodeId,
    pub load: LoadMetrics,
}

/// 分片负载均衡器
pub struct LoadBalancer {
    /// 虚拟分片到物理节点的映射
    mappings: Arc<RwLock<BTreeMap<VirtualShardId, NodeId>>>,
    /// 各虚拟分片的负载指标
    load_metrics: Arc<RwLock<HashMap<VirtualShardId, LoadMetrics>>>,
    /// 分裂阈值
    split_threshold: LoadMetrics,
}

impl LoadBalancer {
    pub fn new(split_threshold: LoadMetrics) -> Self {
        Self {
            mappings: Arc::new(RwLock::new(BTreeMap::new())),
            load_metrics: Arc::new(RwLock::new(HashMap::new())),
            split_threshold,
        }
    }

    /// 初始化虚拟分片映射
    pub async fn initialize(&self, num_virtual_shards: u32, nodes: Vec<NodeId>) {
        let mut mappings = self.mappings.write().await;

        // 轮询分配虚拟分片到物理节点
        for vs in 0..num_virtual_shards {
            let node = nodes[vs as usize % nodes.len()];
            mappings.insert(vs, node);
        }

        info!(
            "Initialized {} virtual shards across {} nodes",
            num_virtual_shards,
            nodes.len()
        );
    }

    /// 查询虚拟分片所在的物理节点
    pub async fn locate(&self, virtual_shard: VirtualShardId) -> Option<NodeId> {
        let mappings = self.mappings.read().await;
        mappings.get(&virtual_shard).copied()
    }

    /// 更新虚拟分片的负载指标
    pub async fn update_load(&self, virtual_shard: VirtualShardId, load: LoadMetrics) {
        let mut metrics = self.load_metrics.write().await;
        metrics.insert(virtual_shard, load);
    }

    /// 检测热点分片并建议分裂
    pub async fn detect_hot_shards(&self) -> Vec<VirtualShardId> {
        let metrics = self.load_metrics.read().await;

        metrics
            .iter()
            .filter(|(_, load)| self.is_hot(load))
            .map(|(vs, _)| *vs)
            .collect()
    }

    /// 判断是否为热点分片
    fn is_hot(&self, load: &LoadMetrics) -> bool {
        load.writes_per_sec > self.split_threshold.writes_per_sec
            || load.storage_bytes > self.split_threshold.storage_bytes
            || load.cpu_usage > self.split_threshold.cpu_usage
    }

    /// 分裂虚拟分片（创建两个新的虚拟分片）
    pub async fn split_shard(
        &self,
        hot_shard: VirtualShardId,
        new_shard_1: VirtualShardId,
        new_shard_2: VirtualShardId,
        target_nodes: (NodeId, NodeId),
    ) -> Result<(), String> {
        let mut mappings = self.mappings.write().await;

        // 检查热点分片是否存在
        if !mappings.contains_key(&hot_shard) {
            return Err(format!("Hot shard {} does not exist", hot_shard));
        }

        // 检查新分片是否已存在
        if mappings.contains_key(&new_shard_1) || mappings.contains_key(&new_shard_2) {
            return Err("New shard IDs already exist".to_string());
        }

        // 移除旧分片，添加两个新分片
        mappings.remove(&hot_shard);
        mappings.insert(new_shard_1, target_nodes.0);
        mappings.insert(new_shard_2, target_nodes.1);

        info!(
            "Split shard {} into {} (node {}) and {} (node {})",
            hot_shard, new_shard_1, target_nodes.0, new_shard_2, target_nodes.1
        );

        Ok(())
    }

    /// 获取所有节点的负载分布
    pub async fn node_load_distribution(&self) -> HashMap<NodeId, Vec<VirtualShardId>> {
        let mappings = self.mappings.read().await;

        let mut distribution: HashMap<NodeId, Vec<VirtualShardId>> = HashMap::new();
        for (vs, node) in mappings.iter() {
            distribution.entry(*node).or_default().push(*vs);
        }

        distribution
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_initialize_virtual_shards() {
        let lb = LoadBalancer::new(LoadMetrics::default());
        let nodes = vec![1, 2, 3];

        lb.initialize(9, nodes.clone()).await;

        // 验证每个节点分配到 3 个虚拟分片
        let dist = lb.node_load_distribution().await;
        for node in nodes {
            assert_eq!(dist.get(&node).unwrap().len(), 3);
        }
    }

    #[tokio::test]
    async fn test_locate_virtual_shard() {
        let lb = LoadBalancer::new(LoadMetrics::default());
        lb.initialize(6, vec![1, 2]).await;

        let node = lb.locate(0).await;
        assert!(node.is_some());
    }

    #[tokio::test]
    async fn test_detect_hot_shards() {
        let threshold = LoadMetrics {
            writes_per_sec: 1000.0,
            storage_bytes: 1_000_000,
            cpu_usage: 0.8,
        };

        let lb = LoadBalancer::new(threshold);
        lb.initialize(3, vec![1]).await;

        // 虚拟分片 0 是热点
        lb.update_load(
            0,
            LoadMetrics {
                writes_per_sec: 1500.0,
                storage_bytes: 500_000,
                cpu_usage: 0.5,
            },
        )
        .await;

        // 虚拟分片 1 不是热点
        lb.update_load(
            1,
            LoadMetrics {
                writes_per_sec: 500.0,
                storage_bytes: 200_000,
                cpu_usage: 0.3,
            },
        )
        .await;

        let hot = lb.detect_hot_shards().await;
        assert_eq!(hot, vec![0]);
    }

    #[tokio::test]
    async fn test_split_shard() {
        let lb = LoadBalancer::new(LoadMetrics::default());
        lb.initialize(3, vec![1]).await;

        // 分裂虚拟分片 0 成 100 和 101
        let result = lb.split_shard(0, 100, 101, (1, 2)).await;
        assert!(result.is_ok());

        // 验证旧分片不存在
        assert!(lb.locate(0).await.is_none());

        // 验证新分片存在
        assert_eq!(lb.locate(100).await, Some(1));
        assert_eq!(lb.locate(101).await, Some(2));
    }

    #[tokio::test]
    async fn test_split_nonexistent_shard() {
        let lb = LoadBalancer::new(LoadMetrics::default());
        lb.initialize(3, vec![1]).await;

        let result = lb.split_shard(999, 100, 101, (1, 2)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_node_load_distribution() {
        let lb = LoadBalancer::new(LoadMetrics::default());
        let nodes = vec![1, 2, 3];
        lb.initialize(9, nodes.clone()).await;

        let dist = lb.node_load_distribution().await;

        // 验证所有节点都有分片
        assert_eq!(dist.len(), 3);

        // 验证总分片数
        let total: usize = dist.values().map(|v| v.len()).sum();
        assert_eq!(total, 9);
    }
}
