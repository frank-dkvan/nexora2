# Phase 1: PG-Wire接入分布式路由（预计5-7天）

## Task 1.1: 重构PgAppState支持QueryService

**目标**: 将PgAppState从直接依赖GraphService改为依赖QueryService接口  
**工期**: 1-2天

### 代码变更

```rust
// crates/nexora-pgwire/src/lib.rs

pub struct PgAppState {
    /// Distributed or local query executor
    pub query_service: Arc<dyn QueryService>,
    
    /// DEPRECATED: direct graph access for migration compatibility
    #[deprecated(note = "Use query_service instead")]
    pub graph: Arc<GraphService>,
    
    pub mv_manager: Arc<MaterializedViewManager>,
    pub sq_manager: Option<Arc<StandingQueryManager>>,
    pub trust_auth: bool,
    pub users: Arc<HashMap<String, (String, String)>>,
    pub server_version: String,
}
```

### 迁移策略

1. **保留兼容性**: 保留`graph`字段但标记为deprecated
2. **渐进式迁移**: 新代码全部使用`query_service`
3. **分阶段重构**: 
   - Step 1: simple_query.rs 迁移到 query_service
   - Step 2: extended_query.rs 迁移（如果有）
   - Step 3: mv_handler.rs 使用 query_service

### 验收标准

- ✅ 编译通过，无破坏性变更
- ✅ 所有现有PG-Wire测试通过
- ✅ 单机模式下行为不变

---

## Task 1.2: 实现分布式SQL执行路径

**目标**: 在DistributedQueryService中实现SQL查询的分布式路由逻辑  
**工期**: 2-3天

### 核心逻辑

```rust
// crates/nexora-distributed/src/distributed_query_service.rs

impl DistributedQueryService {
    async fn execute_sql(&self, query: &str) -> Result<SqlResult, QueryError> {
        let (cypher, is_write) = translate_sql_to_cypher(query)
            .map_err(|e| QueryError::SqlError(e.to_string()))?;
        
        if is_write {
            self.execute_distributed_write(&cypher).await
        } else if self.requires_distributed_execution(&cypher) {
            self.execute_distributed_read(&cypher).await
        } else {
            // Single-shard query, route to local graph
            self.execute_local_cypher(&cypher).await
        }
    }
    
    fn requires_distributed_execution(&self, cypher: &str) -> bool {
        has_aggregation(cypher) || 
        has_global_scan(cypher) ||
        has_cross_shard_join(cypher)
    }
}
```

### 分布式读取实现

```rust
async fn execute_distributed_read(&self, cypher: &str) -> Result<CypherResult, QueryError> {
    // 1. 分析查询，判断是否可以优化
    let plan = self.analyze_query(cypher)?;
    
    match plan {
        QueryPlan::SingleNode(qid) => {
            // 单点查询，直接路由到owner
            let owner = self.router.route(qid).await?;
            self.router.execute_on_node(owner, cypher).await
        }
        QueryPlan::FullScan => {
            // 全表扫描，scatter-gather
            self.scatter_gather_query(cypher).await
        }
        QueryPlan::IndexScan(label) => {
            // 标签索引扫描，查询相关shard
            self.label_index_query(label, cypher).await
        }
    }
}

async fn scatter_gather_query(&self, cypher: &str) -> Result<CypherResult, QueryError> {
    let all_nodes = self.cluster_state.get_all_nodes().await;
    
    // 并行查询所有节点
    let futures = all_nodes.into_iter().map(|node_id| {
        let router = self.router.clone();
        let query = cypher.to_string();
        async move {
            router.execute_on_node(node_id, &query).await
        }
    });
    
    let results = try_join_all(futures).await?;
    
    // 合并结果
    self.merge_query_results(results)
}
```

### 验收标准

- ✅ 单点查询（WHERE id = 'xxx'）路由到正确owner
- ✅ 全表扫描返回所有shard的数据
- ✅ 标签查询（WHERE label）利用索引
- ✅ 结果与单机oracle一致

---

## Task 1.3: 实现分布式写入路径

**目标**: 实现INSERT/UPDATE/DELETE的分布式路由  
**工期**: 2天

### WriteCoordinator设计

```rust
// crates/nexora-distributed/src/write_coordinator.rs

pub struct WriteCoordinator {
    router: Arc<HybridRouter>,
    epoch_validator: Arc<EpochValidator>,
}

impl WriteCoordinator {
    /// Execute INSERT across shards
    pub async fn execute_insert(
        &self, 
        table: &str, 
        rows: Vec<InsertRow>
    ) -> Result<WriteResult, WriteError> {
        // 1. 为每行分配shard (hash(id) % num_shards)
        let shard_groups = self.group_by_shard(rows);
        
        // 2. 并行写入各shard，包含epoch校验
        let futures: Vec<_> = shard_groups.into_iter().map(|(shard_id, batch)| {
            let router = self.router.clone();
            let epoch = self.epoch_validator.current_epoch();
            async move {
                let owner = router.get_shard_owner(shard_id).await?;
                router.mutate_with_epoch_check(owner, shard_id, epoch, batch).await
            }
        }).collect();
        
        let results = try_join_all(futures).await?;
        
        // 3. 汇总结果
        Ok(self.merge_write_results(results))
    }
    
    /// Execute UPDATE with WHERE clause
    pub async fn execute_update(
        &self,
        table: &str,
        assignments: Vec<Assignment>,
        where_clause: &WhereClause,
    ) -> Result<WriteResult, WriteError> {
        // 1. 先查询符合WHERE条件的节点ID
        let matching_ids = self.query_matching_ids(table, where_clause).await?;
        
        // 2. 按shard分组
        let shard_groups = self.group_ids_by_shard(matching_ids);
        
        // 3. 并行执行UPDATE
        let futures: Vec<_> = shard_groups.into_iter().map(|(shard_id, ids)| {
            let router = self.router.clone();
            let updates = assignments.clone();
            async move {
                let owner = router.get_shard_owner(shard_id).await?;
                router.batch_update(owner, ids, updates).await
            }
        }).collect();
        
        try_join_all(futures).await?;
        Ok(WriteResult::default())
    }
    
    /// Execute DELETE with WHERE clause
    pub async fn execute_delete(
        &self,
        table: &str,
        where_clause: &WhereClause,
    ) -> Result<WriteResult, WriteError> {
        // 与UPDATE类似：先查询再删除
        let matching_ids = self.query_matching_ids(table, where_clause).await?;
        
        let shard_groups = self.group_ids_by_shard(matching_ids);
        
        let futures: Vec<_> = shard_groups.into_iter().map(|(shard_id, ids)| {
            let router = self.router.clone();
            async move {
                let owner = router.get_shard_owner(shard_id).await?;
                router.batch_delete(owner, ids).await
            }
        }).collect();
        
        try_join_all(futures).await?;
        Ok(WriteResult::default())
    }
}
```

### Epoch Fencing

```rust
pub struct EpochValidator {
    cluster_state: Arc<ClusterState>,
}

impl EpochValidator {
    pub fn current_epoch(&self) -> u64 {
        self.cluster_state.current_epoch()
    }
    
    pub async fn validate_write(
        &self,
        shard_id: u32,
        epoch: u64,
    ) -> Result<(), EpochError> {
        let current = self.current_epoch();
        if epoch != current {
            return Err(EpochError::Mismatch {
                expected: current,
                actual: epoch,
            });
        }
        
        let owner = self.cluster_state.get_shard_owner(shard_id).await?;
        if !self.cluster_state.is_node_alive(owner).await {
            return Err(EpochError::OwnerUnavailable { shard_id });
        }
        
        Ok(())
    }
}
```

### 验收标准

- ✅ INSERT 100条数据均匀分布到3个shard
- ✅ UPDATE按WHERE条件路由到正确shard
- ✅ DELETE按WHERE条件路由到正确shard
- ✅ Epoch不匹配返回明确错误
- ✅ Owner不可用返回明确错误

---

## Task 1.4: 实现分布式聚合查询

**目标**: 实现COUNT/SUM/AVG/GROUP BY的分布式执行  
**工期**: 2天

### AggregateCoordinator设计

```rust
// crates/nexora-distributed/src/aggregate_coordinator.rs

pub struct AggregateCoordinator {
    router: Arc<HybridRouter>,
}

impl AggregateCoordinator {
    /// Execute aggregate query across all shards
    pub async fn execute_aggregate(
        &self,
        query: &AggregateQuery
    ) -> Result<AggregateResult, AggregateError> {
        // 1. 构建部分聚合查询（每个shard执行）
        let partial_query = self.build_partial_aggregate(query);
        
        // 2. 广播到所有shard
        let partial_results = self.scatter_aggregate(&partial_query).await?;
        
        // 3. Coordinator合并结果
        self.merge_aggregate(query.agg_type, partial_results)
    }
    
    async fn scatter_aggregate(
        &self,
        partial_query: &str,
    ) -> Result<Vec<PartialAggregateResult>, AggregateError> {
        let all_nodes = self.router.get_all_node_ids().await;
        
        let futures = all_nodes.into_iter().map(|node_id| {
            let router = self.router.clone();
            let query = partial_query.to_string();
            async move {
                router.execute_on_node(node_id, &query).await
            }
        });
        
        try_join_all(futures).await
    }
    
    fn merge_aggregate(
        &self,
        agg_type: AggType,
        partials: Vec<PartialAggregateResult>,
    ) -> Result<AggregateResult, AggregateError> {
        match agg_type {
            AggType::Count => {
                let total: i64 = partials.iter().map(|p| p.count).sum();
                Ok(AggregateResult::Count(total))
            }
            AggType::Sum => {
                let total: f64 = partials.iter().map(|p| p.sum).sum();
                Ok(AggregateResult::Sum(total))
            }
            AggType::Avg => {
                // 需要传递(sum, count)，然后计算weighted average
                let total_sum: f64 = partials.iter().map(|p| p.sum).sum();
                let total_count: i64 = partials.iter().map(|p| p.count).sum();
                Ok(AggregateResult::Avg(total_sum / total_count as f64))
            }
            AggType::GroupBy => {
                // 按group_key合并各shard的结果
                self.merge_group_by(partials)
            }
        }
    }
}
```

### 测试场景

```rust
// crates/nexora-distributed/tests/aggregate_test.rs

#[tokio::test]
async fn test_distributed_count() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入100条数据
    for i in 0..100 {
        cluster.insert("Customer", &format!("c{}", i), "name", "value").await;
    }
    
    // 从任意节点查询COUNT
    let result = cluster.query_service.execute_sql(
        "SELECT COUNT(*) FROM Customer"
    ).await.unwrap();
    
    assert_eq!(result.rows[0][0], 100);
}

#[tokio::test]
async fn test_distributed_group_by() {
    let cluster = ThreeNodeCluster::new().await;
    
    // 插入不同region的订单
    cluster.execute_sql("
        INSERT INTO Order (id, region, amount) VALUES
        ('o1', 'US', 100), ('o2', 'EU', 200),
        ('o3', 'US', 150), ('o4', 'APAC', 300),
        ('o5', 'EU', 250)
    ").await.unwrap();
    
    // 查询GROUP BY
    let result = cluster.execute_sql("
        SELECT region, SUM(amount) as total
        FROM Order
        GROUP BY region
        ORDER BY region
    ").await.unwrap();
    
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0], vec!["APAC", 300]);
    assert_eq!(result.rows[1], vec!["EU", 450]);
    assert_eq!(result.rows[2], vec!["US", 250]);
}
```

### 验收标准

- ✅ COUNT返回所有shard的总和
- ✅ SUM返回所有shard的总和
- ✅ AVG正确计算weighted average
- ✅ GROUP BY按key合并各shard结果
- ✅ 结果与单机oracle一致

---

## Phase 1 总验收

运行以下测试套件确认Phase 1完成：

```bash
# 1. 分布式写入测试
cargo test --test distributed_write_test

# 2. 分布式查询测试
cargo test --test distributed_read_test

# 3. 分布式聚合测试
cargo test --test distributed_aggregate_test

# 4. 单机兼容性测试（确保未破坏）
cargo test --test complete_data_pipeline_e2e
```

**Phase 1完成标志**:

- ✅ PgAppState使用QueryService接口
- ✅ DistributedQueryService实现完整
- ✅ 写入、查询、聚合分布式路由正常
- ✅ 单机模式行为不变
