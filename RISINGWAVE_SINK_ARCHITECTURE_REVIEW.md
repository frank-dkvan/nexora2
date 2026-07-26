# RisingWave Sink 架构审查

**日期**: 2026-07-26  
**当前状态**: Phase 1 完成，正在审查目录结构

---

## 🔍 现状分析

### 现有的 Sink 实现

**位置**: `crates/nexora-output/`

**功能**: Standing Query 结果输出
- Console sink (stdout)
- File sink (JSON Lines)
- Webhook sink (HTTP POST)
- Kafka sink (Kafka topic)
- Drop sink (no-op)

**特点**:
- 用于 Standing Query 结果的实时推送
- 数据流向: `Graph → Standing Query → nexora-output → 外部系统`

### 计划中的 RisingWave Sink

**位置**: `crates/nexora-risingwave/src/event_sink.rs` (Phase 6实现)

**功能**: RisingWave MV → EventLog 同步
- 订阅 RisingWave Materialized View 变更
- 将变更写入 nexora-eventlog
- 数据流向: `RisingWave MV → EventLogSink → nexora-eventlog → Graph`

**特点**:
- 反向数据流：从 RisingWave 回到 Nexora
- 用于复杂 SQL 转换后的事件摄入

---

## 📊 两种 Sink 的区别

| 维度 | nexora-output | EventLogSink |
|------|---------------|--------------|
| **目的** | 输出 Standing Query 结果 | 摄入 RisingWave MV 变更 |
| **方向** | Graph → 外部 | RisingWave → Graph |
| **数据源** | Standing Query | Materialized View |
| **目标** | Console/File/Webhook/Kafka | nexora-eventlog |
| **阶段** | 已实现（现有功能） | Phase 6实现 |
| **独立性** | 独立crate | nexora-risingwave的一部分 |

---

## 🏗️ 建议的目录结构

### ✅ 推荐方案：保持当前结构

**理由**:
1. **职责清晰**: 
   - `nexora-output` 专注于查询结果输出
   - `nexora-risingwave` 专注于 RisingWave 集成

2. **耦合度低**:
   - `nexora-output` 不依赖 RisingWave
   - EventLogSink 作为 RisingWave 模块的内部实现

3. **可选性**:
   - `nexora-output` 始终可用
   - EventLogSink 仅在 `--features risingwave` 时可用

### 📁 推荐的文件组织

```
crates/
├── nexora-output/              # ✅ 保持不变
│   ├── src/
│   │   ├── console.rs          # 现有: Console sink
│   │   ├── file.rs             # 现有: File sink
│   │   ├── webhook.rs          # 现有: Webhook sink
│   │   ├── kafka.rs            # 现有: Kafka sink
│   │   ├── drop.rs             # 现有: Drop sink
│   │   └── lib.rs
│   └── Cargo.toml
│
└── nexora-risingwave/          # Phase 3-6 实现
    ├── src/
    │   ├── lib.rs              # RisingWave 包装器
    │   ├── meta.rs             # Meta 节点包装
    │   ├── frontend.rs         # Frontend 节点包装
    │   ├── event_sink.rs       # ✅ 新增: EventLogSink (Phase 6)
    │   └── ddl.rs              # DDL 执行器
    └── Cargo.toml
```

---

## 🔄 数据流对比

### 现有架构（nexora-output）

```
Kafka/Stream
    ↓
nexora-stream
    ↓
nexora-eventlog (Iceberg)
    ↓
nexora-core (Graph)
    ↓
Standing Query
    ↓
nexora-output (Sink)  ← 这里
    ↓
Console/File/Webhook/Kafka (外部)
```

### RisingWave 集成架构（EventLogSink）

```
Kafka/Stream
    ↓
RisingWave Source
    ↓
SQL Transformations (复杂处理)
    ↓
Materialized View
    ↓
EventLogSink  ← 新增 (Phase 6)
    ↓
nexora-eventlog (Iceberg)
    ↓
nexora-core (Graph)
    ↓
Standing Query
    ↓
nexora-output (Sink)
    ↓
外部系统
```

**关键点**: EventLogSink 是 **输入路径** 的一部分，nexora-output 是 **输出路径** 的一部分。

---

## ✅ 无需调整的理由

### 1. 语义清晰

- **nexora-output**: "Standing Query 的输出"
- **EventLogSink**: "RisingWave 到 EventLog 的桥接"

名称和位置都清晰表达了各自的职责。

### 2. 依赖关系正确

```toml
# nexora-output/Cargo.toml
[dependencies]
nexora-standing-query = { path = "../nexora-standing-query" }
# 不依赖 RisingWave

# nexora-risingwave/Cargo.toml (Phase 6)
[dependencies]
nexora-eventlog = { path = "../nexora-eventlog" }
# EventLogSink 在这里实现
```

### 3. Feature Flag 隔离

```bash
# 不使用 RisingWave - nexora-output 仍然可用
cargo build --release --features event-first

# 使用 RisingWave - EventLogSink 才编译
cargo build --release --features event-first,risingwave
```

---

## 🎯 Phase 6 实现建议

### EventLogSink 实现位置

**文件**: `crates/nexora-risingwave/src/event_sink.rs`

**原因**:
1. 与 RisingWave 紧密耦合（订阅 MV 变更）
2. 仅在启用 risingwave feature 时需要
3. 是 RisingWave 集成的核心组件

### 示例代码结构

```rust
// crates/nexora-risingwave/src/event_sink.rs

use nexora_eventlog::EventLogStore;

pub struct EventLogSink {
    event_store: Arc<EventLogStore>,
}

impl EventLogSink {
    pub fn new(event_store: Arc<EventLogStore>) -> Self {
        Self { event_store }
    }

    /// 订阅 RisingWave MV 变更并写入 EventLog
    pub async fn start_sync(
        &self, 
        frontend: &FrontendClient,
        mv_name: &str, 
        topic: &str
    ) -> Result<()> {
        // 1. 订阅 MV 变更
        let mut stream = frontend.subscribe_mv(mv_name).await?;
        
        // 2. 转换并写入 EventLog
        while let Some(change) = stream.next().await {
            let event = self.convert_to_event(change)?;
            self.event_store.append(topic, event).await?;
        }
        
        Ok(())
    }
}
```

---

## 📋 Phase 6 集成清单

当实现 EventLogSink 时：

- [ ] 在 `nexora-risingwave/src/event_sink.rs` 创建 EventLogSink
- [ ] 实现 MV 变更订阅逻辑
- [ ] 实现 Row → CloudEvent 转换
- [ ] 添加单元测试（模拟 MV 变更）
- [ ] 添加集成测试（端到端流程）
- [ ] 在 `nexora-app` 中配置 EventLogSink 启动
- [ ] 更新文档说明双路径数据流

**不需要**:
- ❌ 移动或重命名 `nexora-output`
- ❌ 在 `nexora-output` 中添加 RisingWave 依赖
- ❌ 合并两种 Sink 的实现

---

## 🎓 设计原则总结

### Single Responsibility Principle (单一职责)

- `nexora-output`: 负责查询结果的输出
- `nexora-risingwave`: 负责 RisingWave 集成（包括 EventLogSink）

### Separation of Concerns (关注点分离)

- 输出层（output）和摄入层（event_sink）各司其职
- 不混合不同的数据流方向

### Optional Dependencies (可选依赖)

- 核心功能不依赖 RisingWave
- RisingWave 集成通过 feature flag 隔离

---

## ✅ 结论

**无需调整目录结构！**

当前的目录组织是合理的：
- ✅ `nexora-output` 保持为独立的 Standing Query 输出模块
- ✅ `EventLogSink` 将在 Phase 6 实现于 `nexora-risingwave/src/event_sink.rs`
- ✅ 两者职责清晰，互不干扰

继续按照原计划推进 Phase 2。

---

**审查日期**: 2026-07-26  
**审查结论**: ✅ 架构合理，无需调整  
**下一步**: Phase 2 - 实现 nexora-consensus 和 nexora-rpc
