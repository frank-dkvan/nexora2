# PG-Wire 集群模式开发规划文档索引

**创建日期**: 2026-07-11  
**文档目录**: `/docs/planning/pg-wire-cluster/`

---

## 📋 快速导航

| 文档 | 用途 | 预计工作量 |
|------|------|-----------|
| [README.md](PG_WIRE_CLUSTER_README.md) | 项目概述和架构说明 | - |
| [NEXT_STEPS.md](NEXT_STEPS.md) | **当前优先任务**（P0-P3） | 8-12小时 |
| [DEVELOPMENT_PLAN.md](PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md) | 总体开发计划（4个Phase） | 16-23天 |
| [PHASE1.md](PG_WIRE_CLUSTER_PHASE1.md) | Phase 1: 基础架构 | 3-5天 |
| [PHASE2.md](PG_WIRE_CLUSTER_PHASE2.md) | Phase 2: 核心分布式能力 | 5-7天 |
| [PHASE3.md](PG_WIRE_CLUSTER_PHASE3.md) | Phase 3: 完整链路验证 | 3-4天 |
| [PHASE4.md](PG_WIRE_CLUSTER_PHASE4.md) | Phase 4: 生产级特性 | 5-7天 |

---

## 🎯 当前阶段

**Phase 0（清理阶段）** - 修复已知问题和技术债

- [ ] P0.1: 修复UPDATE自动触发SQ失败
- [ ] P0.2: 删除重复SinkRegistry创建
- [ ] P0.3: 清理NodeTask的8处eprintln!
- [ ] P0.4: 修复9项SQL边操作回归测试

**预计完成时间**: 1-2天

---

## 📦 Phase 概览

### Phase 1: 基础架构（3-5天）
- 分布式节点ID生成器（Snowflake）
- 基于RAFT的配置协调器
- GraphService分片路由层

### Phase 2: 核心分布式能力（5-7天）
- StandingQueryManager跨节点协调
- MaterializedViewManager分布式聚合
- SinkRegistry HA和故障转移
- 2PC事务协调器

### Phase 3: 完整链路验证（3-4天）
- 真实HTTP Webhook E2E测试
- 真实PostgreSQL客户端MV查询测试
- 跨节点数据一致性验证

### Phase 4: 生产级特性（5-7天）
- 节点动态上下线
- 数据分片重平衡
- 性能优化和压测
- 监控和可观测性

---

## 🚀 开始开发

1. **阅读顺序**:
   - 先读 [README.md](PG_WIRE_CLUSTER_README.md) 了解架构
   - 再读 [NEXT_STEPS.md](NEXT_STEPS.md) 了解当前任务
   - 最后按需查看各Phase详细设计

2. **开发顺序**:
   - 从 [NEXT_STEPS.md](NEXT_STEPS.md) 的优先级1任务开始
   - 完成P0清理后进入Phase 1
   - 严格按Phase顺序推进，每个Phase完成后进行验收

3. **验收标准**:
   - 每个Phase都有明确的验收标准
   - 所有测试必须通过
   - 单机模式保持100%兼容

---

## 📊 进度跟踪

| Phase | 状态 | 开始日期 | 完成日期 | 实际工作量 |
|-------|------|----------|----------|-----------|
| Phase 0 (清理) | 🔴 进行中 | 2026-07-11 | - | - |
| Phase 1 | ⚪ 未开始 | - | - | - |
| Phase 2 | ⚪ 未开始 | - | - | - |
| Phase 3 | ⚪ 未开始 | - | - | - |
| Phase 4 | ⚪ 未开始 | - | - | - |

---

## 📝 变更日志

- **2026-07-11**: 创建规划文档，定义4个Phase开发计划
- **2026-07-11**: 识别P0清理任务，开始Phase 0
