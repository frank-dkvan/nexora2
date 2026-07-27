# CLI 参数重命名说明

**日期**: 2026-07-27  
**版本**: 2.1.0  
**原因**: 隐藏底层实现细节（RisingWave），使用业务语义化的参数名称

---

## 变更摘要

将所有暴露 "RisingWave" 第三方组件名称的 CLI 参数和配置项，统一改为业务语义化的名称 **"Event Streams"（事件流）**。

---

## CLI 参数对照表

| 旧参数名 (v2.0) | 新参数名 (v2.1) | 说明 |
|----------------|----------------|------|
| `--enable-risingwave` | `--enable-event-streams` | 启用 SQL 事件流处理 |
| `--enable-embedded-risingwave` | `--embedded-event-streams` | 嵌入式运行（子进程） |
| `--risingwave-meta-addr` | `--event-streams-meta-addr` | Meta 节点地址 |
| `--risingwave-frontend-addr` | `--event-streams-frontend-addr` | Frontend 节点地址 |
| `--risingwave-cluster-mode` | `--event-streams-cluster` | 3 节点 HA 集群模式 |
| `--risingwave-raft-node-id` | `--event-streams-raft-node-id` | Raft 节点 ID（HA 模式） |
| `--risingwave-raft-peers` | `--event-streams-raft-peers` | Raft peer 节点列表 |

---

## TOML 配置对照表

| 旧配置段 (v2.0) | 新配置段 (v2.1) | 说明 |
|----------------|----------------|------|
| `[risingwave]` | `[event_streams]` | 事件流配置段 |
| `risingwave.enabled` | `event_streams.enabled` | 启用标志 |
| `risingwave.embedded` | `event_streams.embedded` | 嵌入式模式 |
| `risingwave.cluster_mode` | `event_streams.cluster_mode` | 集群模式 |
| `risingwave.meta_addr` | `event_streams.meta_addr` | Meta 地址 |
| `risingwave.frontend_addr` | `event_streams.frontend_addr` | Frontend 地址 |
| `risingwave.data_dir` | `event_streams.data_dir` | 数据目录 |
| `risingwave.meta_nodes` | `event_streams.meta_nodes` | Meta 节点列表 |
| `risingwave.compute_nodes` | `event_streams.compute_nodes` | Compute 节点列表 |

---

## 使用示例对比

### CLI 启动命令

#### 旧版本 (v2.0)

```bash
# 单节点模式
./target/release/nexora \
  --enable-risingwave \
  --enable-embedded-risingwave

# 集群模式
./target/release/nexora \
  --enable-risingwave \
  --enable-embedded-risingwave \
  --risingwave-cluster-mode
```

#### 新版本 (v2.1)

```bash
# 单节点模式
./target/release/nexora \
  --enable-event-streams \
  --embedded-event-streams

# 集群模式
./target/release/nexora \
  --enable-event-streams \
  --embedded-event-streams \
  --event-streams-cluster
```

---

### TOML 配置文件

#### 旧版本 (v2.0)

```toml
[risingwave]
enabled = true
embedded = true
cluster_mode = true
data_dir = "./nexora-data/risingwave-cluster"

[[risingwave.meta_nodes]]
node_id = 1
listen_addr = "127.0.0.1:5690"

[[risingwave.compute_nodes]]
listen_addr = "127.0.0.1:5688"
```

#### 新版本 (v2.1)

```toml
[event_streams]
enabled = true
embedded = true
cluster_mode = true
data_dir = "./nexora-data/event-streams-cluster"

[[event_streams.meta_nodes]]
node_id = 1
listen_addr = "127.0.0.1:5690"

[[event_streams.compute_nodes]]
listen_addr = "127.0.0.1:5688"
```

---

## 向后兼容性

### ⚠️ 破坏性变更

此次重命名是 **破坏性变更**，v2.0 的 CLI 参数和 TOML 配置将 **不再有效**。

### 升级指南

#### 1. 更新 CLI 脚本

如果您有启动脚本使用了旧参数，请全局替换：

```bash
# 在您的脚本中执行替换
sed -i 's/--enable-risingwave/--enable-event-streams/g' your-start-script.sh
sed -i 's/--enable-embedded-risingwave/--embedded-event-streams/g' your-start-script.sh
sed -i 's/--risingwave-cluster-mode/--event-streams-cluster/g' your-start-script.sh
```

#### 2. 更新 TOML 配置文件

```bash
# 备份原配置
cp nexora.toml nexora.toml.bak

# 替换配置段
sed -i 's/\[risingwave\]/[event_streams]/g' nexora.toml
sed -i 's/risingwave\./event_streams./g' nexora.toml
```

#### 3. 更新数据目录路径（可选）

如果配置中指定了数据目录，建议同步重命名：

```bash
# 重命名数据目录
mv ./nexora-data/risingwave-cluster ./nexora-data/event-streams-cluster

# 更新配置
sed -i 's/risingwave-cluster/event-streams-cluster/g' nexora.toml
```

---

## 内部实现未变

**重要**: 此次变更 **仅影响用户界面**（CLI 参数、TOML 配置、日志输出），底层实现依然使用 RisingWave：

- ✅ Feature flag 仍然是 `--features risingwave`（Cargo 编译）
- ✅ Rust crate 仍然是 `nexora-risingwave`
- ✅ 二进制文件仍然是 `risingwave` (RisingWave 官方)
- ✅ 进程名仍然包含 `risingwave` (`ps aux | grep risingwave`)
- ✅ API 路径 `/api/risingwave/*` 保持不变（内部 API）

---

## 理由说明

### 为什么要重命名？

#### 1. 业务语义化
- **旧名称**: `--enable-risingwave` - 用户需要知道 RisingWave 是什么
- **新名称**: `--enable-event-streams` - 直接表达功能：启用事件流处理

#### 2. 隐藏实现细节
- 用户不需要关心底层用的是 RisingWave、Flink 还是其他引擎
- 未来替换底层实现时，CLI 参数无需变更

#### 3. 统一命名风格
- Nexora 的其他特性都用业务语义命名：
  - `--event-store-backend` (事件存储)
  - `--enable-event-first` (事件优先架构)
  - `--event-time-field` (事件时间字段)

#### 4. 品牌一致性
- 对外宣传 "Nexora 事件流图数据库"，而不是 "Nexora with RisingWave"
- 用户认知：Nexora = 完整产品，而非 RisingWave 的集成器

---

## 组件名称对照

虽然 CLI 参数改名，但内部架构组件仍保留原名：

| 组件 | 内部名称 | 对外名称 |
|------|---------|---------|
| Meta 节点 | RisingWave Meta | Event Streams Meta |
| Frontend 节点 | RisingWave Frontend | Event Streams Frontend |
| Compute 节点 | RisingWave Compute | Event Streams Compute |
| 集群模式 | RisingWave Cluster | Event Streams Cluster |

---

## 日志输出变更

### 旧版本 (v2.0)

```
🚀 Nexora 2.0 starting...
   HTTP:   listening on 0.0.0.0:8080
   RisingWave: starting distributed cluster (3 nodes)...
      Meta-1:   127.0.0.1:5690 (dashboard: 5691)
      Meta-2:   127.0.0.1:5692 (dashboard: 5693)
      Meta-3:   127.0.0.1:5694 (dashboard: 5695)
   RisingWave: distributed cluster started
✅ Nexora started successfully
```

### 新版本 (v2.1)

```
🚀 Nexora 2.0 starting...
   HTTP:   listening on 0.0.0.0:8080
   Event Streams: starting distributed cluster (3 nodes)...
      Meta-1:   127.0.0.1:5690 (dashboard: 5691)
      Meta-2:   127.0.0.1:5692 (dashboard: 5693)
      Meta-3:   127.0.0.1:5694 (dashboard: 5695)
   Event Streams: distributed cluster started
✅ Nexora started successfully
```

---

## 文档更新清单

以下文档已同步更新：

- ✅ `docs/RUNTIME_OPERATIONS_GUIDE.md` - 运行操作指南
- ✅ `docs/CLI_PARAMETER_RENAME_2026-07-27.md` - 本文档
- ✅ `nexora-cluster.toml.example` - 集群配置示例
- ✅ `crates/nexora-app/src/config.rs` - 配置结构定义
- ✅ `crates/nexora-app/src/main.rs` - CLI 参数定义和日志输出
- ⏳ `docs/PRODUCTION_READINESS_AUDIT_2026-07-27.md` - 待更新
- ⏳ `README.md` - 待更新

---

## 快速验证

```bash
# 1. 查看新参数
./target/release/nexora --help | grep event-streams

# 2. 测试启动
./target/release/nexora \
  --enable-event-streams \
  --embedded-event-streams \
  --event-streams-cluster

# 3. 验证日志输出
# 应该看到 "Event Streams: starting distributed cluster"
```

---

## 常见问题

### Q1: 为什么不在参数中也改成 Meta/Frontend/Compute？

**A**: Meta/Frontend/Compute 是架构组件名称，具有明确的技术含义：
- **Meta**: 元数据管理 + Raft 共识
- **Frontend**: SQL 查询前端
- **Compute**: 流计算引擎

这些名称在流处理领域是通用的（Flink、Spark 也类似），保留它们有助于技术理解。

### Q2: API 路径为什么不改？

**A**: API 路径 `/api/risingwave/*` 是内部接口，主要用于：
- Nexora 内部组件通信
- 开发调试
- 监控集成

改动会影响现有监控脚本和内部工具，且用户很少直接调用。

### Q3: 编译时为什么还是 `--features risingwave`？

**A**: Cargo feature flag 是编译时概念，变更会影响：
- 所有依赖 nexora-app 的 crate
- CI/CD 构建脚本
- 第三方集成

保持不变避免了 Rust 生态中的破坏性变更。

### Q4: 旧配置文件能自动迁移吗？

**A**: 暂不支持自动迁移。建议：
1. 使用 `sed` 批量替换（见升级指南）
2. 或手动编辑（变更点很少）

未来版本可能添加配置迁移工具。

---

## 相关文档

- **运行操作指南**: [docs/RUNTIME_OPERATIONS_GUIDE.md](RUNTIME_OPERATIONS_GUIDE.md)
- **生产就绪度审核**: [docs/PRODUCTION_READINESS_AUDIT_2026-07-27.md](PRODUCTION_READINESS_AUDIT_2026-07-27.md)
- **RisingWave Phase 8 总结**: [docs/RISINGWAVE_PHASE8_FINAL_SUMMARY.md](RISINGWAVE_PHASE8_FINAL_SUMMARY.md)

---

**生成时间**: 2026-07-27  
**作者**: Claude (Nexora 开发团队)  
**版本**: 1.0
