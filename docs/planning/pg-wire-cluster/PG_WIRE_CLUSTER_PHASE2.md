# Phase 2: 三节点集群E2E测试（预计3-4天）

## Task 2.1: 搭建三节点测试集群

**目标**: 创建可重复使用的三节点测试框架  
**工期**: 1天

### ThreeNodeCluster测试工具

```rust
// crates/nexora-distributed/tests/common/three_node_cluster.rs

use nexora_distributed::{HybridRouter, ClusterState, DistributedQueryService};
use nexora_core::GraphService;
use std::sync::Arc;

pub struct ThreeNodeCluster {
    pub nodes: Vec<NodeHandle>,
    pub query_service: Arc<DistributedQueryService>,
    pub cluster_state: Arc<ClusterState>,
}

pub struct NodeHandle {
    pub node_id: String,
    pub graph: Arc<GraphService>,
    pub listening_addr: SocketAddr,
}

impl ThreeNodeCluster {
    /// Create a new 3-node cluster for testing
    pub async fn new() -> Self {
        let config = ClusterConfig {
            num_shards: 12,
            replication_factor: 1, // Phase 2只测试单副本
        };
        
        // 创建3个GraphService实例
        let node1 = Self::create_node("node1", vec![0, 1, 2, 3]).await;
        let node2 = Self::create_node("node2", vec![4, 5, 6, 7]).await;
        let node3 = Self::create_node("node3", vec![8, 9, 10, 11]).await;
        
        // 初始化ClusterState
        let cluster_state = Arc::new(ClusterState::new());
        cluster_state.register_node(node1.node_id.clone(), node1.listening_addr).await;
        cluster_state.register_node(node2.node_id.clone(), node2.listening_addr).await;
        cluster_state.register_node(node3.node_id.clone(), node3.listening_addr).await;
        
        // 分配shard ownership
        for shard_id in 0..12 {
            let owner = match shard_id / 4 {
                0 => &node1.node_id,
                1 => &node2.node_id,
                _ => &node3.node_id,
            };
            cluster_state.set_shard_owner(shard_id, owner.clone()).await;
        }
        
        // 创建HybridRouter
        let router = Arc::new(HybridRouter::new(cluster_state.clone()));
        
        // 创建DistributedQueryService
        let query_service = Arc::new(DistributedQueryService::new(
            router,
            node1.graph.clone(), // local graph (任意选一个)
            cluster_state.clone(),
        ));
        
        Self {
            nodes: vec![node1, node2, node3],
            query_service,
            cluster_state,
        }
    }
    
    async fn create_node(node_id: &str, owned_shards: Vec<u32>) -> NodeHandle {
        let graph = Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: owned_shards.len() as u32,
                max_nodes_per_shard: 1000,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ));
        
        // 启动gRPC服务监听随机端口
        let addr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let listening_addr = listener.local_addr().unwrap();
        
        // 启动gRPC server (后台任务)
        tokio::spawn(serve_graph_service(listener, graph.clone()));
        
        NodeHandle {
            node_id: node_id.to_string(),
            graph,
            listening_addr,
        }
    }
    
    /// Shutdown one node to simulate failure
    pub async fn shutdown_node(&mut self, node_id: &str) {
        self.cluster_state.mark_node_dead(node_id).await;
    }
    
    /// Verify data distribution across shards
    pub async fn verify_distribution(&self) -> DistributionStats {
        let mut stats = DistributionStats::default();
        
        for node in &self.nodes {
            let count = node.graph.all_node_ids().await.unwrap().len();
            stats.node_counts.insert(node.node_id.clone(), count);
        }
        
        stats
    }
}

#[derive(Debug, Default)]
pub struct DistributionStats {
    pub node_counts: HashMap<String, usize>,
}

impl DistributionStats {
    /// Check if data is roughly balanced (within 20% variance)
    pub fn is_balanced(&self) -> bool {
        let values: Vec<usize> = self.node_counts.values().copied().collect();
        let avg = values.iter().sum::<usize>() as f64 / values.len() as f64;
        
        values.iter().all(|&count| {
            let diff = (count as f64 - avg).abs();
            diff / avg < 0.2 // 20% tolerance
        })
    }
}
```

### 验收标准

- ✅ 三个节点成功启动
- ✅ ClusterState正确记录拓扑
- ✅ 各节点可通过gRPC通信
- ✅ 测试结束后资源清理完整

---

## Task 2.2: 分布式写入E2E测试

**目标**: 验证INSERT/UPDATE/DELETE跨节点正确执行  
**工期**: 1天

### 测试用例

```rust
// crates/nexora-distributed/tests/distributed_write_e2e.rs

#[tokio::test]
async fn test_distributed_insert_1000_records() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入1000条Customer记录
    for i in 0..1000 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Customer (id, name, region, balance) VALUES \
             ('c{}', 'Customer {}', 'US', {})",
            i, i, i * 100
        )).await.unwrap();
    }
    
    // 验证数据分布
    let stats = cluster.verify_distribution().await;
    println!("Distribution: {:?}", stats);
    
    // 每个节点应该有约333条记录（4 shards per node）
    assert!(stats.is_balanced(), "Data not balanced: {:?}", stats);
    
    // 验证总数
    let result = cluster.query_service.execute_sql(
        "SELECT COUNT(*) FROM Customer"
    ).await.unwrap();
    assert_eq!(result.rows[0][0], 1000);
}

#[tokio::test]
async fn test_distributed_update_cross_shard() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入100条记录
    for i in 0..100 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Product (id, name, price) VALUES ('p{}', 'Product {}', {})",
            i, i, 100
        )).await.unwrap();
    }
    
    // 批量更新：将所有价格翻倍
    cluster.query_service.execute_sql(
        "UPDATE Product SET price = price * 2 WHERE price > 0"
    ).await.unwrap();
    
    // 验证更新成功
    let result = cluster.query_service.execute_sql(
        "SELECT AVG(price) FROM Product"
    ).await.unwrap();
    
    assert_eq!(result.rows[0][0], 200.0);
}

#[tokio::test]
async fn test_distributed_delete_batch() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入200条记录
    for i in 0..200 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Order (id, status, amount) VALUES ('o{}', '{}', {})",
            i,
            if i % 2 == 0 { "pending" } else { "completed" },
            i * 10
        )).await.unwrap();
    }
    
    // 删除所有pending订单
    cluster.query_service.execute_sql(
        "DELETE FROM Order WHERE status = 'pending'"
    ).await.unwrap();
    
    // 验证删除成功
    let result = cluster.query_service.execute_sql(
        "SELECT COUNT(*) FROM Order"
    ).await.unwrap();
    
    assert_eq!(result.rows[0][0], 100); // 只剩completed订单
}

#[tokio::test]
async fn test_epoch_fencing_prevents_stale_writes() {
    let mut cluster = ThreeNodeCluster::new().await;
    
    // 插入一条记录
    cluster.query_service.execute_sql(
        "INSERT INTO Account (id, balance) VALUES ('acc1', 1000)"
    ).await.unwrap();
    
    // 记录当前epoch
    let old_epoch = cluster.cluster_state.current_epoch();
    
    // 模拟节点故障触发epoch变更
    cluster.shutdown_node("node1").await;
    cluster.cluster_state.increment_epoch().await;
    
    // 尝试用旧epoch写入（应该失败）
    let result = cluster.query_service.execute_sql_with_epoch(
        "UPDATE Account SET balance = 2000 WHERE id = 'acc1'",
        old_epoch
    ).await;
    
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), QueryError::EpochMismatch { .. }));
}
```

### 验收标准

- ✅ 1000条INSERT均匀分布到3节点
- ✅ UPDATE跨节点批量生效
- ✅ DELETE跨节点批量生效
- ✅ Epoch fencing阻止过期写入

---

## Task 2.3: 分布式查询E2E测试

**目标**: 验证SELECT从多个节点聚合数据  
**工期**: 1天

### 测试用例

```rust
// crates/nexora-distributed/tests/distributed_read_e2e.rs

#[tokio::test]
async fn test_select_all_from_distributed_table() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入500条记录
    for i in 0..500 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Employee (id, name, salary) VALUES ('e{}', 'Emp {}', {})",
            i, i, 50000 + i * 100
        )).await.unwrap();
    }
    
    // 查询所有记录
    let result = cluster.query_service.execute_sql(
        "SELECT * FROM Employee ORDER BY id LIMIT 10"
    ).await.unwrap();
    
    assert_eq!(result.rows.len(), 10);
    assert_eq!(result.row_count, 500); // 总记录数
}

#[tokio::test]
async fn test_where_clause_cross_shard() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入1000条记录
    for i in 0..1000 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Transaction (id, amount, category) VALUES \
             ('t{}', {}, '{}')",
            i, i * 10, if i % 3 == 0 { "food" } else { "other" }
        )).await.unwrap();
    }
    
    // 查询特定category
    let result = cluster.query_service.execute_sql(
        "SELECT COUNT(*) FROM Transaction WHERE category = 'food'"
    ).await.unwrap();
    
    let expected = (0..1000).filter(|i| i % 3 == 0).count();
    assert_eq!(result.rows[0][0], expected as i64);
}

#[tokio::test]
async fn test_single_node_query_optimization() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入一条已知shard的记录
    let specific_id = "prod_abc123";
    cluster.query_service.execute_sql(&format!(
        "INSERT INTO Product (id, name, stock) VALUES ('{}', 'Widget', 100)",
        specific_id
    )).await.unwrap();
    
    // 查询该记录（应该只路由到一个节点）
    let result = cluster.query_service.execute_sql(&format!(
        "SELECT * FROM Product WHERE id = '{}'",
        specific_id
    )).await.unwrap();
    
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], specific_id);
}

#[tokio::test]
async fn test_query_after_node_failure() {
    let mut cluster = ThreeNodeCluster::new().await;
    
    // 插入数据到3个节点
    for i in 0..300 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Item (id, name) VALUES ('i{}', 'Item {}')",
            i, i
        )).await.unwrap();
    }
    
    // 关闭node2
    cluster.shutdown_node("node2").await;
    
    // 查询应该返回明确错误（而非不完整数据）
    let result = cluster.query_service.execute_sql(
        "SELECT COUNT(*) FROM Item"
    ).await;
    
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), QueryError::OwnerUnavailable { .. }));
}
```

### 验收标准

- ✅ SELECT * 返回所有节点的数据
- ✅ WHERE条件跨节点过滤正确
- ✅ 单节点查询优化生效（不scatter-gather）
- ✅ 节点故障返回明确错误

---

## Task 2.4: 分布式聚合E2E测试

**目标**: 验证COUNT/SUM/AVG/GROUP BY正确性  
**工期**: 1天

### 测试用例

```rust
// crates/nexora-distributed/tests/distributed_aggregate_e2e.rs

#[tokio::test]
async fn test_count_aggregation() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入1000条记录到各shard
    for i in 0..1000 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Event (id, type, timestamp) VALUES ('ev{}', 'click', {})",
            i, i
        )).await.unwrap();
    }
    
    // 执行COUNT(*)
    let result = cluster.query_service.execute_sql(
        "SELECT COUNT(*) FROM Event"
    ).await.unwrap();
    
    assert_eq!(result.rows[0][0], 1000);
}

#[tokio::test]
async fn test_sum_aggregation() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入100条订单
    for i in 1..=100 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO SalesOrder (id, amount) VALUES ('so{}', {})",
            i, i * 10
        )).await.unwrap();
    }
    
    // 执行SUM(amount)
    let result = cluster.query_service.execute_sql(
        "SELECT SUM(amount) FROM SalesOrder"
    ).await.unwrap();
    
    let expected: i64 = (1..=100).map(|i| i * 10).sum();
    assert_eq!(result.rows[0][0], expected);
}

#[tokio::test]
async fn test_avg_aggregation() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入100条记录
    for i in 1..=100 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Measurement (id, value) VALUES ('m{}', {})",
            i, i * 2
        )).await.unwrap();
    }
    
    // 执行AVG(value)
    let result = cluster.query_service.execute_sql(
        "SELECT AVG(value) FROM Measurement"
    ).await.unwrap();
    
    let expected: f64 = (1..=100).map(|i| i * 2).sum::<i64>() as f64 / 100.0;
    assert!((result.rows[0][0].as_f64().unwrap() - expected).abs() < 0.01);
}

#[tokio::test]
async fn test_group_by_aggregation() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入不同region的销售数据
    let regions = vec!["US", "EU", "APAC"];
    for i in 0..300 {
        let region = regions[i % 3];
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Sale (id, region, revenue) VALUES ('s{}', '{}', {})",
            i, region, (i + 1) * 100
        )).await.unwrap();
    }
    
    // 执行GROUP BY
    let result = cluster.query_service.execute_sql(
        "SELECT region, SUM(revenue) as total_revenue \
         FROM Sale \
         GROUP BY region \
         ORDER BY region"
    ).await.unwrap();
    
    assert_eq!(result.rows.len(), 3);
    
    // 验证每个region的总和
    for row in &result.rows {
        let region = row[0].as_str().unwrap();
        let total = row[1].as_i64().unwrap();
        
        // 每个region有100条记录
        let indices: Vec<usize> = (0..300).filter(|i| i % 3 == regions.iter().position(|&r| r == region).unwrap()).collect();
        let expected: i64 = indices.iter().map(|&i| ((i + 1) * 100) as i64).sum();
        
        assert_eq!(total, expected, "Mismatch for region {}", region);
    }
}

#[tokio::test]
async fn test_aggregation_with_where_clause() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入1000条订单
    for i in 0..1000 {
        cluster.query_service.execute_sql(&format!(
            "INSERT INTO Invoice (id, status, total) VALUES \
             ('inv{}', '{}', {})",
            i,
            if i % 2 == 0 { "paid" } else { "unpaid" },
            (i + 1) * 50
        )).await.unwrap();
    }
    
    // 查询paid订单的总金额
    let result = cluster.query_service.execute_sql(
        "SELECT SUM(total) FROM Invoice WHERE status = 'paid'"
    ).await.unwrap();
    
    let expected: i64 = (0..1000)
        .filter(|i| i % 2 == 0)
        .map(|i| ((i + 1) * 50) as i64)
        .sum();
    
    assert_eq!(result.rows[0][0], expected);
}
```

### 验收标准

- ✅ COUNT返回正确总数
- ✅ SUM返回正确总和
- ✅ AVG计算weighted average正确
- ✅ GROUP BY正确合并各shard结果
- ✅ 聚合+WHERE条件组合正确

---

## Phase 2 总验收

运行完整测试套件：

```bash
# 1. 分布式写入测试
cargo test --test distributed_write_e2e -- --test-threads=1

# 2. 分布式查询测试
cargo test --test distributed_read_e2e -- --test-threads=1

# 3. 分布式聚合测试
cargo test --test distributed_aggregate_e2e -- --test-threads=1

# 4. 完整E2E（包含SQ+MV）
cargo test --test three_node_complete_pipeline_e2e -- --test-threads=1
```

**Phase 2完成标志**:

- ✅ 所有三节点集群测试通过
- ✅ 数据分布均匀（20% tolerance）
- ✅ 节点故障返回明确错误（不返回部分数据）
- ✅ 聚合结果与单机oracle一致
