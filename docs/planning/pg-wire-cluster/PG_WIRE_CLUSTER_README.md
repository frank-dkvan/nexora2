# Nexora PG-Wire集群开发指南索引

**创建日期**: 2026-07-11  
**版本**: v1.0  
**目标**: 达到生产级分布式集群支持

---

## 📚 文档导航

### 1. 开发计划总览
**[PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md](./PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md)**
- 执行摘要和验收标准
- 总体时间表（16-23天）
- 当前进度和阻断问题
- 风险评估和成功指标

### 2. Phase 1: PG-Wire接入分布式路由（5-7天）
**[PG_WIRE_CLUSTER_PHASE1.md](./PG_WIRE_CLUSTER_PHASE1.md)**
- 重构PgAppState支持QueryService接口
- 实现分布式SQL执行路径（单点查询、全表扫描）
- 实现分布式写入路径（INSERT/UPDATE/DELETE with Epoch fencing）
- 实现分布式聚合查询（COUNT/SUM/AVG/GROUP BY）

### 3. Phase 2: 三节点集群E2E测试（3-4天）
**[PG_WIRE_CLUSTER_PHASE2.md](./PG_WIRE_CLUSTER_PHASE2.md)**
- 搭建ThreeNodeCluster测试框架
- 验证分布式写入（1000条数据均匀分布）
- 验证分布式查询（scatter-gather）
- 验证分布式聚合（与单机oracle一致）
- 验证故障场景（Owner不可用返回明确错误）

### 4. Phase 3: 真实Sink和MV集成验证（3-4天）
**[PG_WIRE_CLUSTER_PHASE3.md](./PG_WIRE_CLUSTER_PHASE3.md)**
- 真实HTTP服务器验证Webhook投递
- 真实PostgreSQL客户端查询MV
- 完整链路压力测试（10k插入，并发读写）
- 延迟SLO验证（P95 < 100ms）

### 5. Phase 4: 生产就绪（2-3天）
**[PG_WIRE_CLUSTER_PHASE4.md](./PG_WIRE_CLUSTER_PHASE4.md)**
- 用户文档（7份）和代码示例
- Prometheus metrics和Grafana dashboard
- 结构化日志和告警规则
- CI/CD自动化和Docker部署

---

## 🎯 快速开始

### 对于开发者

1. **了解当前状态**:
   ```bash
   # 查看已验证的架构
   cat COMPLETE_PIPELINE_VALIDATION_REPORT.md
   
   # 运行现有测试
   cargo test -p nexora-pgwire --test complete_data_pipeline_e2e
   ```

2. **开始Phase 1开发**:
   - 阅读 [PHASE1.md](./PG_WIRE_CLUSTER_PHASE1.md) Task 1.1
   - 创建新分支: `git checkout -b feature/distributed-query-service`
   - 实现QueryService trait
   - 运行测试: `cargo test -p nexora-distributed`

3. **提交代码**:
   - 确保所有测试通过
   - 添加集成测试
   - 提交PR并引用对应Task编号

### 对于项目管理者

1. **追踪进度**:
   - 查看 [DEVELOPMENT_PLAN.md](./PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md) 的"进度追踪"表格
   - 每完成一个Task，更新完成度

2. **风险管理**:
   - 定期检查"阻断问题"清单
   - 评估"技术风险"是否已缓解
   - 确保每个Phase验收标准达成

3. **发布决策**:
   - Phase 4完成后检查"生产就绪检查清单"
   - 所有验收标准通过后批准发布

---

## 🔑 关键概念

### QueryService接口
统一的查询服务抽象，支持本地和分布式两种实现：
- **LocalQueryService**: 单机模式（保持现有行为）
- **DistributedQueryService**: 集群模式（路由到多个节点）

### Epoch Fencing
分布式一致性机制：
- 每次集群拓扑变化时递增epoch
- 写入时校验epoch，防止过期操作
- Owner不可用时返回明确错误（不返回部分数据）

### Scatter-Gather查询
分布式查询模式：
1. **Scatter**: 将查询广播到所有相关节点
2. **Execute**: 各节点并行执行
3. **Gather**: Coordinator合并结果
4. **Return**: 返回完整结果集

### Standing Query → Sink Pipeline
实时数据流水线：
```
INSERT/UPDATE → GraphService → StandingQueryManager 
  → broadcast channel → WebhookSinkRunner → HTTP POST
```

---

## 📊 验收标准速查

| 能力 | 验收条件 | 对应Phase |
|------|---------|----------|
| 分布式写入 | 1000条INSERT均匀分布 | Phase 2 |
| 分布式查询 | 任意节点返回完整数据 | Phase 2 |
| 全局聚合 | COUNT/SUM/AVG一致性 | Phase 2 |
| Owner故障 | 返回明确错误 | Phase 2 |
| MV查询 | 真实psql客户端查询 | Phase 3 |
| Webhook投递 | 真实HTTP POST收到 | Phase 3 |
| 性能SLO | 写入→SQ P95 < 100ms | Phase 3 |
| 文档齐全 | 7份用户文档 | Phase 4 |
| 监控完整 | 15+Prometheus指标 | Phase 4 |
| CI/CD | GitHub Actions通过 | Phase 4 |

---

## 🚦 当前状态（2026-07-11）

### 🟢 已完成
- 单机模式完整链路验证
- INSERT/DELETE自动触发SQ
- MV增量更新正确
- WebhookSink基础实现

### 🔴 阻断问题
1. **P0**: UPDATE操作不稳定触发SQ
2. **P1**: SQL边操作测试失败
3. **P1**: 重复SinkRegistry创建

### ⚪ 待开始
- Phase 1-4 所有Task

---

## 📞 支持和反馈

- **技术问题**: 查阅对应Phase文档的"验收标准"和"测试用例"
- **进度汇报**: 更新DEVELOPMENT_PLAN.md的"进度追踪"表格
- **Bug报告**: 添加到DEVELOPMENT_PLAN.md的"阻断问题"清单

---

## 📝 文档维护

每完成一个Task后：
1. 更新DEVELOPMENT_PLAN.md的进度表格
2. 在对应Phase文档中标记✅完成
3. 如遇到新问题，添加到"阻断问题"清单
4. 更新"更新日志"记录关键决策

---

**总计**: 5个Phase，16-23天，达到生产级集群支持 🚀
