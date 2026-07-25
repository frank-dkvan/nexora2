# PG-Wire集群支持开发计划

**文档版本**: v1.0  
**创建日期**: 2026-07-11  
**目标**: 实现PG-Wire分布式集群支持，达到生产级要求

---

## 📋 执行摘要

根据Review团队反馈，当前Nexora PG-Wire需要接入分布式路由以支持集群模式。本计划分5个Phase，预计16-23天完成，包括：

1. **Phase 0**: 清理阻断问题 + 抽象QueryService接口
2. **Phase 1**: PG-Wire接入分布式路由
3. **Phase 2**: 三节点集群E2E测试
4. **Phase 3**: 真实Sink和MV集成验证
5. **Phase 4**: 生产就绪（文档、监控、CI）

---

## 🎯 验收标准矩阵

| 能力 | 验收条件 | 测试场景 |
|------|---------|---------|
| **分布式写入** | ✅ 三节点INSERT 1000条，每个shard存储约333条 | `test_distributed_insert` |
| **分布式查询** | ✅ 从任意节点SELECT返回完整数据 | `test_distributed_query` |
| **全局聚合** | ✅ COUNT/SUM/AVG/GROUP BY与单机oracle一致 | `test_distributed_aggregation` |
| **Owner故障** | ✅ 返回清晰错误，不返回不完整数据 | `test_owner_unavailable` |
| **MV查询** | ✅ 真实psql客户端查询MV，支持WHERE/ORDER/LIMIT | `test_real_pg_client_mv` |
| **Webhook投递** | ✅ 本地HTTP服务器收到正确POST请求 | `test_real_webhook_delivery` |
| **重试机制** | ✅ 500/429/timeout会重试，最终进入DLQ | `test_webhook_retry_and_dlq` |
| **SQL回归** | ✅ 边操作测试24/24通过 | `cargo test -p nexora-sql` |
| **无重复投递** | ✅ 单个SQ Match只触发每个Sink一次 | `test_single_delivery` |
| **性能SLO** | ✅ 写入→SQ P95 < 100ms, 写入→Sink P95 < 500ms | Prometheus metrics |

---

## ⏱️ 总体时间表

| 阶段 | 工期 | 关键里程碑 |
|------|------|-----------|
| Phase 0 | 1天 | 清理P0阻断问题 |
| Phase 0 | 2-4天 | QueryService接口定义 |
| Phase 1 | 5-7天 | DistributedQueryService实现 |
| Phase 2 | 3-4天 | 三节点E2E测试通过 |
| Phase 3 | 3-4天 | 真实Sink/MV测试 |
| Phase 4 | 2-3天 | 生产就绪 |
| **总计** | **16-23天** | 生产级发布 |

---

## 📁 文档结构

本开发计划分为5个独立文档：

1. **[PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md](./PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md)** (本文档)
   - 执行摘要
   - 验收标准矩阵
   - 总体时间表
   - 当前进度追踪

2. **[PG_WIRE_CLUSTER_PHASE1.md](./PG_WIRE_CLUSTER_PHASE1.md)**
   - Task 1.1: 重构PgAppState支持QueryService
   - Task 1.2: 实现分布式SQL执行路径
   - Task 1.3: 实现分布式写入路径
   - Task 1.4: 实现分布式聚合查询

3. **[PG_WIRE_CLUSTER_PHASE2.md](./PG_WIRE_CLUSTER_PHASE2.md)**
   - Task 2.1: 搭建三节点测试集群
   - Task 2.2: 分布式写入E2E测试
   - Task 2.3: 分布式查询E2E测试
   - Task 2.4: 分布式聚合E2E测试

4. **[PG_WIRE_CLUSTER_PHASE3.md](./PG_WIRE_CLUSTER_PHASE3.md)**
   - Task 3.1: 真实WebhookSink E2E测试
   - Task 3.2: 真实MV查询E2E测试
   - Task 3.3: 完整链路压力测试

5. **[PG_WIRE_CLUSTER_PHASE4.md](./PG_WIRE_CLUSTER_PHASE4.md)**
   - Task 4.1: 补充文档和示例
   - Task 4.2: 监控和可观测性
   - Task 4.3: CI/CD和发布流程

---

## 🎯 当前状态（2026-07-11）

### 已完成 ✅
- 单机模式下完整数据链路验证通过
- PG-Wire INSERT/DELETE操作正常
- Standing Query自动触发机制工作
- Materialized View增量更新正确
- WebhookSink基础实现完成
- SinkRegistry订阅机制实现

### 阻断问题 ⚠️
1. **P0 Critical**: UPDATE操作不稳定触发Standing Query
   - 症状: `test_complete_insert_update_delete_pipeline` 断言失败
   - 预期: match_count = 3，实际: match_count = 2
   - 影响: 阻断后续分布式开发

2. **P1 High**: SQL边操作测试9项失败
   - 影响: 边查询功能不稳定

3. **P1 High**: main.rs中重复SinkRegistry创建
   - 影响: 可能导致重复订阅

4. **P2 Medium**: NodeTask中8处eprintln!调试输出
   - 影响: 生产环境性能和日志质量

### 下一步行动 🎬

**立即开始**: Phase 0 - 清理当前阻断问题

**优先级**:
1. 修复UPDATE触发SQ的bug（预计4-6小时）
2. 修复SQL边操作测试（预计2-3小时）
3. 删除重复SinkRegistry和调试输出（预计1小时）
4. 验证单机模式完整稳定后，进入Phase 1

---

## 🔄 进度追踪

| Phase | 状态 | 完成度 | 预计完成日期 |
|-------|------|--------|-------------|
| Phase 0 | 🟡 In Progress | 0% | 2026-07-15 |
| Phase 1 | ⚪ Not Started | 0% | 2026-07-22 |
| Phase 2 | ⚪ Not Started | 0% | 2026-07-26 |
| Phase 3 | ⚪ Not Started | 0% | 2026-07-30 |
| Phase 4 | ⚪ Not Started | 0% | 2026-08-02 |

**整体进度**: 0% (0/16-23天)

---

## 🚨 风险和依赖

### 技术风险
1. **分布式聚合复杂度**: GROUP BY合并逻辑可能需要比预期更多时间
2. **Epoch机制测试**: 故障场景测试可能暴露边界条件bug
3. **性能SLO**: 分布式overhead可能导致延迟不达标

### 外部依赖
- ✅ nexora-distributed crate已存在（HybridRouter, ClusterState）
- ✅ tokio-postgres客户端用于测试
- ✅ axum用于mock webhook服务器
- ⚠️ 需要添加prometheus依赖（Phase 4）
- ⚠️ 需要添加tracing依赖（Phase 4）

### 缓解措施
- 每个Phase结束前进行完整回归测试
- 单机模式行为不变作为硬性约束
- 提前预留20%缓冲时间用于bug修复

---

## 📊 成功指标

### 功能完整性
- [ ] 所有验收标准矩阵测试通过
- [ ] 单机模式无回归
- [ ] 三节点集群稳定运行

### 性能指标
- [ ] 写入→SQ P95 < 100ms
- [ ] SQ处理 P95 < 50ms
- [ ] Webhook投递 P95 < 500ms
- [ ] 10k插入 < 30秒

### 生产就绪
- [ ] 文档齐全（7份用户文档）
- [ ] 监控完整（15+指标）
- [ ] CI/CD自动化
- [ ] Docker部署方案

---

## 👥 团队分工（建议）

如果是团队协作，建议分工：

| 角色 | 职责 | Phase重点 |
|------|------|----------|
| **Backend Lead** | 架构设计、Code Review | Phase 0-1 |
| **分布式专家** | 路由逻辑、Epoch机制 | Phase 1-2 |
| **测试工程师** | E2E测试、性能测试 | Phase 2-3 |
| **DevOps** | 监控、CI/CD、Docker | Phase 4 |
| **技术写作** | 文档、示例 | Phase 4 |

如果是单人开发，按Phase顺序执行，预计16-23天完成。

---

## 📞 获取帮助

遇到问题时：

1. **查阅文档**: 先查看对应Phase的详细文档
2. **查看已完成**: 参考`COMPLETE_PIPELINE_VALIDATION_REPORT.md`了解已验证的架构
3. **检查测试**: 运行`cargo test -p nexora-pgwire`查看单机模式是否正常
4. **日志分析**: 使用`RUST_LOG=debug cargo test`查看详细日志

---

## 📝 更新日志

### 2026-07-11
- 创建完整开发计划
- 拆分为5个独立文档
- 识别当前P0阻断问题
- 定义验收标准矩阵
## Phase 0: 架构准备与重构（预计3-5天）

### Task 0.1: 清理当前阻断问题 ⚠️ **P0 - 必须先完成**

**目标**: 清理影响后续开发的基础问题  
**工期**: 1天

**子任务**:

1. 删除main.rs中重复的SinkRegistry创建（保留line 593，删除line 727）
2. 删除NodeTask中8处eprintln!调试输出
3. 移动嵌套测试文件到正确位置
4. 修复9项SQL边操作回归测试

**验收标准**:

- ✅ cargo test --all 无ignored核心测试
- ✅ SQL边操作测试24/24通过
- ✅ 无重复SinkRegistry订阅任务
- ✅ 热路径无同步输出

**文件变更**:

- `crates/nexora-app/src/main.rs`: 删除line 727的重复SinkRegistry
- `crates/nexora-core/src/node_task.rs`: 删除8处eprintln!
- `crates/nexora-sql/tests/`: 修复边操作测试

---

### Task 0.2: 抽象DistributedQueryService接口

**目标**: 定义统一的查询服务接口，支持本地和分布式两种实现  
**工期**: 2-4天

**核心接口设计**:

```rust
// crates/nexora-distributed/src/query_service.rs

use async_trait::async_trait;
use nexora_id::NexoraId;
use nexora_cypher::CypherResult;
use nexora_sql::SqlResult;

#[async_trait]
pub trait QueryService: Send + Sync {
    /// Execute Cypher query with distributed routing
    async fn execute_cypher(&self, query: &str) -> Result<CypherResult, QueryError>;
    
    /// Execute SQL query (translated to Cypher)
    async fn execute_sql(&self, query: &str) -> Result<SqlResult, QueryError>;
    
    /// Get node by ID with owner routing
    async fn get_node(&self, qid: NexoraId) -> Result<Option<NodeSnapshot>, QueryError>;
    
    /// Batch get nodes across shards
    async fn get_nodes(&self, qids: Vec<NexoraId>) -> Result<Vec<NodeSnapshot>, QueryError>;
    
    /// Write operations with epoch/owner validation
    async fn mutate_node(&self, qid: NexoraId, ops: Vec<MutationOp>) -> Result<(), QueryError>;
    
    /// Global aggregation (COUNT/SUM/AVG/GROUP BY)
    async fn aggregate(&self, query: &AggregateQuery) -> Result<AggregateResult, QueryError>;
}
```

**验收标准**:

- ✅ LocalQueryService通过所有现有单机测试
- ✅ QueryService trait编译通过
- ✅ DistributedQueryService结构定义清晰
- ✅ 单机模式下行为与之前完全一致

---

