# Nexora 下一步工作指南

**日期**: 2026-07-11  
**状态**: Phase 0 准备阶段

---

## 🎯 当前任务：修复P0阻断问题

### 优先级 1: 修复UPDATE不触发Standing Query (预计4-6小时)

**症状**:
- 测试`test_complete_insert_update_delete_pipeline`在UPDATE阶段失败
- 预期match_count=3，实际match_count=2
- INSERT和DELETE操作正常，唯独UPDATE不稳定

**诊断步骤**:
```bash
# 1. 运行测试查看详细日志
RUST_LOG=debug cargo test -p nexora-pgwire --test complete_data_pipeline_e2e -- --nocapture

# 2. 检查UPDATE路径是否提取节点ID
grep -A 30 "Statement::Update" crates/nexora-pgwire/src/simple_query.rs

# 3. 验证extract_node_ids_from_where是否被调用
grep -B 5 -A 10 "extract_node_ids_from_where" crates/nexora-pgwire/src/simple_query.rs
```

**修复方向**:
1. 确认`simple_query.rs`中UPDATE分支调用了`trigger_standing_queries()`
2. 验证`extract_node_ids_from_where_clause_for_statement()`正确处理UPDATE
3. 对比DELETE的工作实现，检查UPDATE缺少什么
4. 增加UPDATE路径的debug日志

**验证**:
```bash
# 运行UPDATE相关测试
cargo test -p nexora-pgwire update

# 运行完整E2E
cargo test -p nexora-pgwire --test complete_data_pipeline_e2e
```

---

### 优先级 2: 修复SQL边操作测试 (预计2-3小时)

**症状**:
- 9项SQL边操作测试失败
- 错误可能是Cypher翻译或边查询执行问题

**诊断步骤**:
```bash
# 运行SQL边测试查看失败详情
cargo test -p nexora-sql edge -- --nocapture

# 检查edge_table分类逻辑
grep -A 20 "classify_table" crates/nexora-sql/src/edge_table.rs
```

**修复方向**:
1. 检查`edge_*`表名是否正确识别
2. 验证边INSERT/DELETE的Cypher翻译
3. 确认GraphService的边操作API调用正确

**验证**:
```bash
cargo test -p nexora-sql
```

---

### 优先级 3: 清理代码质量问题 (预计1小时)

**任务清单**:

1. **删除重复SinkRegistry创建**:
   ```bash
   # 查看重复创建位置
   grep -n "SinkRegistry::new" crates/nexora-app/src/main.rs
   
   # 保留line 593，删除line 727附近的重复代码
   ```

2. **删除NodeTask调试输出**:
   ```bash
   # 查找所有eprintln!
   grep -n "eprintln!" crates/nexora-core/src/node_task.rs
   
   # 替换为tracing::debug!或删除
   ```

3. **清理嵌套测试文件** (如果存在):
   ```bash
   find . -name "tests" -type d -path "*/tests/tests"
   ```

**验收**:
```bash
# 确保所有测试通过
cargo test --all

# 确保clippy无警告
cargo clippy --all-targets -- -D warnings

# 确保格式正确
cargo fmt -- --check
```

---

## 📋 Phase 0 完成检查清单

- [ ] UPDATE触发SQ测试通过
- [ ] SQL边操作测试24/24通过
- [ ] 无重复SinkRegistry创建
- [ ] 无eprintln!在热路径
- [ ] cargo test --all 全部通过
- [ ] cargo clippy 无警告
- [ ] 所有E2E测试稳定通过

**完成后**: 进入Phase 1，开始实现DistributedQueryService接口

---

## 🚀 Phase 1 预览：抽象QueryService接口 (接下来2-4天)

**目标**: 定义统一的查询服务接口，为分布式路由做准备

**主要任务**:
1. 创建`crates/nexora-distributed/src/query_service.rs`
2. 定义`QueryService` trait
3. 实现`LocalQueryService`（包装现有GraphService）
4. 实现`DistributedQueryService`框架（Phase 1填充实现）
5. 重构`PgAppState`使用QueryService接口

**关键文件**:
- `crates/nexora-distributed/src/query_service.rs`
- `crates/nexora-distributed/src/local_query_service.rs`
- `crates/nexora-distributed/src/distributed_query_service.rs`
- `crates/nexora-pgwire/src/lib.rs` (PgAppState)

**验收标准**:
- LocalQueryService通过所有现有测试
- PgAppState使用QueryService接口
- 单机模式行为完全不变

---

## 📖 参考文档

- **总览**: [PG_WIRE_CLUSTER_README.md](./docs/PG_WIRE_CLUSTER_README.md)
- **开发计划**: [PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md](./docs/PG_WIRE_CLUSTER_DEVELOPMENT_PLAN.md)
- **Phase 1详情**: [PG_WIRE_CLUSTER_PHASE1.md](./docs/PG_WIRE_CLUSTER_PHASE1.md)
- **已完成验证**: [COMPLETE_PIPELINE_VALIDATION_REPORT.md](./COMPLETE_PIPELINE_VALIDATION_REPORT.md)

---

## 💡 开发提示

### 调试技巧
```bash
# 查看完整日志
RUST_LOG=nexora_pgwire=debug,nexora_standing_query=debug cargo test

# 只运行特定测试
cargo test -p nexora-pgwire test_complete_insert_update_delete_pipeline -- --nocapture

# 检查编译警告
cargo build --all 2>&1 | grep warning
```

### Git工作流
```bash
# 创建feature分支
git checkout -b fix/update-trigger-sq

# 提交前确保测试通过
cargo test --all
cargo clippy --all-targets

# 提交信息格式
git commit -m "fix(pgwire): ensure UPDATE triggers Standing Query evaluation

- Add node ID extraction for UPDATE statements
- Add debug logging for UPDATE path
- Add regression test for UPDATE → SQ → MV chain

Fixes #issue-number"
```

---

**开始时间**: 现在  
**预计完成**: 2026-07-15  
**下一个里程碑**: Phase 1 - QueryService接口抽象
