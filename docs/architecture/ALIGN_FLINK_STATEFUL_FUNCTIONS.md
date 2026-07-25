# Nexora 对齐 Flink Stateful Functions 模型 —— 完整工作路线图

**日期**: 2026-07-18  
**目标**: 将 Nexora 从"Actor + Event Sourcing"演化为"完整 Stateful Streaming Graph Platform"  
**对标**: Apache Flink Stateful Functions + RisingWave

---

## 一、Flink Stateful Functions 核心模型回顾

### 1.1 核心概念

```
┌────────────────────────────────────────┐
│     Stateful Functions (SF) 核心        │
└────────────────────────────────────────┘

1. Function = Stateful Actor
   - 每个实体（如 User / Device）= 一个 Function 实例
   - Function 有本地状态（类似 Nexora 的 Actor-per-Node）
   - 状态只能通过消息修改

2. Message = Event
   - Function 之间通过消息通信
   - 消息路由：function_type/function_id

3. Global Checkpoint = 一致性快照
   - 所有 Functions 的状态 + 消息流的 offset
   - Chandy-Lamport Barrier 机制
   - 崩溃恢复：从最后 checkpoint 重放

4. Exactly-Once 语义
   - (input_offset, state) 的原子快照
   - 重放时：从 offset 恢复 + 状态恢复
```

---

### 1.2 Nexora 当前位置（对照表）

| Flink SF 概念 | Nexora 对应 | 完成度 |
|--------------|------------|:---:|
| **Stateful Function** | Actor-per-Node | ✅ 100% |
| **Function State** | Node properties + edges | ✅ 100% |
| **Message Routing** | Zenoh pub/sub + Shard routing | ✅ 95% |
| **Event Sourcing** | WAL + Journal | ✅ 90% |
| **Global Checkpoint** | `nexora-barrier` (骨架) | ⚠️ 30% |
| **Exactly-Once** | Per-shard offset store | ⚠️ 40% |
| **State Backend** | RocksDB (snapshot) | ✅ 80% |
| **Watermark** | 无 | ❌ 0% |
| **Side Outputs** | Standing Query sinks | ✅ 70% |
| **Dynamic Scaling** | 无 | ❌ 0% |

**结论**：Nexora 已有 **60-70%** 的 Flink SF 核心能力，主要缺失"全局一致性检查点"和"动态扩缩容"。

---

## 二、核心差距分析

### 差距 1：Global Checkpoint 未完整（最关键）

**Nexora 现状**（`nexora-barrier`）：
```rust
// ✅ 已有：Epoch Barrier 调度器
pub struct BarrierScheduler {
    current_epoch: RwLock<Epoch>,
    epoch_states: HashMap<Epoch, EpochState>,
    committed_epoch: RwLock<Epoch>,
}

// ✅ 已有：Barrier 流程
// 1. create_barrier() → 创建 epoch N
// 2. report_shard(epoch, shard_id, status) → 各 shard 报告完成
// 3. commit_epoch() → 所有 shard 完成后提交

// ❌ 缺失：
// - Barrier 注入到数据流（Kafka / Zenoh）
// - Shard 收到 Barrier 后的 flush 逻辑
// - Checkpoint 与 Kafka offset 的绑定
// - 崩溃恢复时从 checkpoint 恢复
```

**Flink SF 实现**：
```java
// Barrier 流式传播
Source → Barrier(epoch=100) → Function_1 → Function_2 → Sink
                                    ↓             ↓
                              flush state   flush state
                                    ↓             ↓
                           checkpoint(epoch=100, offset=12345)
```

---

### 差距 2：Exactly-Once 语义不完整

**Nexora 现状**：
```
✅ Per-shard offset store (nexora-stream/rocksdb_offset_store.rs)
   - 可以存储 Kafka offset
   - 但与 shard 状态快照不绑定

❌ 缺失：
   - (offset, graph_state) 原子快照
   - 崩溃恢复时：offset 和 state 不一致
```

**Flink SF 实现**：
```
Checkpoint {
  epoch: 100,
  kafka_offset: {
    "events": {0: 12345, 1: 23456},
  },
  state_backend: {
    "user_123": {...},
    "user_456": {...},
  }
}
→ 原子写入，要么全部成功，要么全部失败
```

---

### 差距 3：Watermark（事件时间）支持

**Nexora 现状**：
```
❌ 无 Watermark 机制
   - Standing Query 基于"到达顺序"，非"事件时间"
   - 无法处理乱序事件
```

**Flink SF 需要**：
```
Event {
  event_time: 2026-07-18T10:00:00,  ← 业务时间
  processing_time: 2026-07-18T10:05:00,  ← 到达时间
}

Watermark = "事件时间已进展到 T"
→ T 之前的事件不会再来
→ 可以触发时间窗口聚合
```

**影响**：
- 当前 Nexora 无法做"基于事件时间的窗口聚合"
- 如"每小时的平均电量"（按事件发生时间，非处理时间）

---

### 差距 4：动态扩缩容（非必须）

**Nexora 现状**：
```
✅ Shard 数固定（256）
❌ 无法动态调整 shard 数
❌ 无法触发 shard 重新分布（rebalance）
```

**Flink SF 实现**：
```
原 8 个 Task → 扩容到 16 个 Task
1. 暂停消费
2. 重新分配 key range
3. State migration（状态迁移）
4. 恢复消费
```

---

## 三、对齐路线图（分 4 个阶段）

### 阶段 1：完成 Global Checkpoint（核心，4-6 周）⭐

**目标**：实现"(Kafka offset, Graph state) 原子快照"

#### 任务 1.1：Barrier 注入到 Kafka 消费（1 周）

```rust
// nexora-stream/src/kafka_consumer.rs
pub struct BarrierInjectingConsumer {
    kafka: KafkaConsumer,
    barrier_scheduler: Arc<BarrierScheduler>,
    barrier_interval: Duration,  // 如 10 秒
}

impl BarrierInjectingConsumer {
    async fn poll_with_barriers(&self) -> StreamBatch {
        // 1. 拉取 Kafka 消息
        let mut batch = self.kafka.poll().await?;
        
        // 2. 检查是否该注入 Barrier
        if self.should_inject_barrier() {
            let barrier = self.barrier_scheduler.create_barrier(BarrierKind::Checkpoint).await;
            
            // 3. 插入 Barrier 到批次末尾
            batch.events.push(Event::Barrier(barrier));
        }
        
        batch
    }
}
```

---

#### 任务 1.2：Shard 处理 Barrier（2 周）

```rust
// nexora-core/src/graph/shard/mod.rs
impl Shard {
    async fn handle_barrier(&mut self, barrier: Barrier) -> Result<()> {
        tracing::info!("Shard {} received barrier {}", self.shard_id, barrier.epoch);
        
        // 1. 停止接收新事件（排空队列）
        self.drain_pending_events().await?;
        
        // 2. Flush 所有 actors 的状态到 RocksDB
        let (node_count, event_count) = self.flush_all_nodes().await?;
        
        // 3. Flush WAL（group commit 确保持久化）
        self.wal.flush().await?;
        
        // 4. 记录当前 Kafka offset
        let offset = self.current_kafka_offset;
        
        // 5. 原子写入 Checkpoint 元数据
        self.checkpoint_store.save(CheckpointMetadata {
            epoch: barrier.epoch,
            shard_id: self.shard_id,
            kafka_offset: offset,
            node_count,
            event_count,
            snapshot_path: self.snapshot_path(),
        }).await?;
        
        // 6. 报告 BarrierScheduler
        self.barrier_scheduler.report_shard(
            barrier.epoch,
            self.shard_id,
            ShardStatus::Flushed { node_count, event_count }
        ).await?;
        
        // 7. 恢复处理新事件
        Ok(())
    }
}
```

---

#### 任务 1.3：Checkpoint 元数据存储（1 周）

```rust
// nexora-core/src/checkpoint/metadata.rs
pub struct CheckpointMetadata {
    pub epoch: Epoch,
    pub shard_id: usize,
    pub kafka_offset: u64,  // ← 关键：绑定 offset
    pub node_count: usize,
    pub event_count: u64,
    pub snapshot_path: PathBuf,  // RocksDB snapshot 路径
    pub created_at: DateTime<Utc>,
}

pub struct CheckpointStore {
    rocksdb: Arc<DB>,
}

impl CheckpointStore {
    pub async fn save(&self, meta: CheckpointMetadata) -> Result<()> {
        // Key: "checkpoint/{epoch}/{shard_id}"
        // Value: JSON(meta)
        let key = format!("checkpoint/{}/{}", meta.epoch.value(), meta.shard_id);
        self.rocksdb.put(key.as_bytes(), serde_json::to_vec(&meta)?)?;
        Ok(())
    }
    
    pub async fn load_latest(&self) -> Result<HashMap<usize, CheckpointMetadata>> {
        // 读取最新 committed epoch 的所有 shard 元数据
        let committed_epoch = self.get_latest_committed_epoch()?;
        let mut result = HashMap::new();
        
        for shard_id in 0..256 {
            let key = format!("checkpoint/{}/{}", committed_epoch, shard_id);
            if let Some(bytes) = self.rocksdb.get(key.as_bytes())? {
                let meta: CheckpointMetadata = serde_json::from_slice(&bytes)?;
                result.insert(shard_id, meta);
            }
        }
        Ok(result)
    }
}
```

---

#### 任务 1.4：崩溃恢复从 Checkpoint 重启（1-2 周）

```rust
// nexora-zenoh/src/cluster.rs
impl ClusterManager {
    pub async fn recover_from_checkpoint(&self) -> Result<()> {
        tracing::info!("Starting crash recovery from checkpoint...");
        
        // 1. 读取最新 checkpoint 元数据
        let checkpoint_meta = self.checkpoint_store.load_latest().await?;
        let epoch = checkpoint_meta.values().next().unwrap().epoch;
        
        tracing::info!("Recovering from epoch {}", epoch);
        
        // 2. 每个 shard 恢复
        for (shard_id, meta) in checkpoint_meta {
            let shard = self.shards.get(shard_id).unwrap();
            
            // 2.1 恢复 RocksDB snapshot
            shard.load_snapshot(meta.snapshot_path).await?;
            
            // 2.2 恢复 Kafka offset
            self.kafka_consumer.seek(shard_id, meta.kafka_offset).await?;
            
            tracing::info!(
                "Shard {} recovered: {} nodes, offset {}",
                shard_id,
                meta.node_count,
                meta.kafka_offset
            );
        }
        
        // 3. 恢复 Barrier Scheduler 状态
        self.barrier_scheduler.set_committed_epoch(epoch).await;
        
        tracing::info!("Crash recovery complete, resuming from epoch {}", epoch);
        Ok(())
    }
}
```

---

### 阶段 2：Exactly-Once 端到端验证（2 周）

**测试场景**：
```rust
#[tokio::test]
async fn test_exactly_once_on_crash() {
    // 1. 启动 Nexora + Kafka
    let nexora = start_nexora().await;
    let kafka = start_kafka().await;
    
    // 2. 写入 10k 事件
    for i in 0..10_000 {
        kafka.produce(Event::PropertySet { qid: "node_1", key: "counter", value: i }).await;
    }
    
    // 3. 等待 checkpoint 完成（epoch 1）
    nexora.wait_for_checkpoint(Epoch::new(1)).await;
    
    // 4. 写入另外 5k 事件
    for i in 10_000..15_000 {
        kafka.produce(...).await;
    }
    
    // 5. 模拟崩溃（kill -9）
    nexora.kill().await;
    
    // 6. 重启 Nexora（从 checkpoint 恢复）
    let nexora2 = start_nexora().await;
    nexora2.recover_from_checkpoint().await;
    
    // 7. 继续写入 5k 事件
    for i in 15_000..20_000 {
        kafka.produce(...).await;
    }
    
    // 8. 验证 exactly-once
    let final_value = nexora2.get_property("node_1", "counter").await;
    assert_eq!(final_value, 19_999);  // ← 精确一次，无重复无丢失
}
```

---

### 阶段 3：Watermark + 事件时间窗口（3-4 周，可选）

**任务 3.1：Watermark 生成器（1 周）**

```rust
// nexora-stream/src/watermark.rs
pub struct WatermarkGenerator {
    source_watermarks: HashMap<usize, u64>,  // shard_id → watermark
    global_watermark: u64,
}

impl WatermarkGenerator {
    pub fn update(&mut self, shard_id: usize, event_time: u64) {
        // 1. 更新该 shard 的 watermark（event_time - tolerance）
        self.source_watermarks.insert(shard_id, event_time - 5_000_000);  // -5秒容忍
        
        // 2. 全局 watermark = 所有 shard 的最小值
        self.global_watermark = self.source_watermarks.values().min().copied().unwrap_or(0);
    }
    
    pub fn global_watermark(&self) -> u64 {
        self.global_watermark
    }
}
```

**任务 3.2：事件时间窗口聚合（2-3 周）**

```cypher
// Cypher 扩展：事件时间窗口
MATCH (f:Forklift {id: 'F002'})
CALL time_window.tumbling(
    f,
    'battery_level',
    duration('PT1H'),      // 1 小时窗口
    'event_time'           // ← 基于事件时间（vs processing_time）
) YIELD window_start, window_end, avg_value
RETURN window_start, avg_value
```

---

### 阶段 4：动态扩缩容（2-3 月，非必须）

**复杂度高，优先级低**，Nexora 当前固定 256 shard 已够用。

---

## 四、优先级排序

### P0（必须做，6-8 周）

1. **Global Checkpoint**（阶段 1）：4-6 周
   - Barrier 注入 + Shard 处理 + Metadata 存储 + 崩溃恢复
2. **Exactly-Once 验证**（阶段 2）：2 周
   - 端到端测试 + Chaos 注入

**完成后**：Nexora 达到 **Flink SF 核心能力的 85%**

---

### P1（提升完整性，3-4 周）

3. **Watermark + 事件时间窗口**（阶段 3）：3-4 周
   - 支持乱序事件 + 基于事件时间的聚合

**完成后**：Nexora 达到 **Flink SF 核心能力的 95%**

---

### P2（长期演进，2-3 月）

4. **动态扩缩容**（阶段 4）：2-3 月
   - Shard 重新分布 + 状态迁移

---

## 五、与其他工作的集成

### 与 Track A-D 的关系

| Track | 与 Checkpoint 的依赖 |
|-------|-------------------|
| **Track A（共识/恢复）** | ✅ **互补**：Checkpoint 是另一种崩溃恢复机制 |
| **Track B（备份/快照）** | ✅ **复用**：Checkpoint = 自动化快照 |
| **Track C（分布式扩展）** | ⚠️ 弱依赖：Checkpoint 可以在单机验证 |
| **Track D（遍历性能）** | 无依赖 |

**建议顺序**：
1. Track A（共识）→ 2-3 周
2. Track B（备份）→ 4-6 周
3. **Global Checkpoint** → 4-6 周（可与 Track B 并行）
4. Track D（性能）→ 2-3 周

---

### 与 WAL + ReductStore 的关系

```
统一架构：

Event Stream（Kafka）
    │
    ├─ Barrier 注入
    │
    ▼
┌───────────────────────────────────┐
│  Global Checkpoint (Epoch N)      │
│  - Kafka offset: 12345            │
│  - Graph state: RocksDB snapshot  │
│  - ReductStore offset: 67890      │  ← 异步复制的进度也在 checkpoint
└───────────────────────────────────┘
    │
    ├─ 图状态投影（Actor-per-Node）
    ├─ 时序投影（ReductStore 异步复制）
    └─ 其他投影（Standing Query / MV）

崩溃恢复：
  1. 恢复 Kafka offset → 12345
  2. 恢复图状态 → RocksDB snapshot
  3. 恢复 ReductStore 复制进度 → 67890
  4. 继续消费 Kafka from 12345
```

**优势**：
- ✅ 所有投影的进度都在同一个 checkpoint
- ✅ 崩溃恢复时，所有系统同步恢复到一致状态
- ✅ Exactly-Once 语义跨越图 + 时序两个系统

---

## 六、最终对齐效果

### 对齐前（Nexora 当前）

```
Nexora = Actor-per-Node + Event Sourcing
       + 不完整的 Checkpoint（WAL per-shard）
       + 无 Exactly-Once（offset 与 state 不绑定）
```

**定位**：Actor 框架 + 图数据库

---

### 对齐后（完成阶段 1-2）

```
Nexora = Stateful Streaming Graph Platform
       + Global Checkpoint（Epoch Barrier）
       + Exactly-Once（(offset, state) 原子快照）
       + Multi-Projection（图 + 时序 + SQ/MV）
```

**定位**：**Flink Stateful Functions + 图能力**

---

### 对齐后（完成阶段 3）

```
Nexora = 上述 + Watermark + 事件时间窗口
```

**定位**：**对标 RisingWave（流式数据库）+ 图能力**

---

## 七、工程量总结

| 阶段 | 工作量 | 交付物 | 优先级 |
|------|--------|--------|:---:|
| **1. Global Checkpoint** | 4-6 周 | Barrier 流程 + Metadata 存储 + 崩溃恢复 | P0 |
| **2. Exactly-Once 验证** | 2 周 | 端到端测试 + Chaos | P0 |
| **3. Watermark + 时间窗口** | 3-4 周 | 事件时间聚合 | P1 |
| **4. 动态扩缩容** | 2-3 月 | Shard rebalance | P2 |
| **总计（P0+P1）** | **9-12 周** | | |

---

## 八、决策建议

### 短期（当前 Track A/B 完成后）

**先做 Global Checkpoint（阶段 1-2）**，理由：
1. ✅ 与 Track B（备份）协同（Checkpoint = 自动快照）
2. ✅ 6-8 周即可达到 Flink SF 核心能力的 85%
3. ✅ Exactly-Once 是生产级必须项

---

### 中期（3-6 月后）

**按需做 Watermark（阶段 3）**，理由：
- 如果用户场景有"乱序事件"或"事件时间窗口聚合" → 做
- 否则暂缓（优先做 Track D 性能优化）

---

### 长期（1 年+）

**评估动态扩缩容（阶段 4）**，理由：
- 256 shard 已支持数亿节点，扩缩容优先级不高
- 如果成为"云原生多租户平台" → 做

---

## 九、最终判断

### Nexora 对齐 Flink Stateful Functions，还需要：

**核心（P0，6-8 周）**：
1. ✅ Global Checkpoint（Epoch Barrier 已有骨架，需完成流程）
2. ✅ Exactly-Once 语义（绑定 offset + state）
3. ✅ 崩溃恢复从 Checkpoint 重启

**完整性（P1，3-4 周）**：
4. ⚠️ Watermark + 事件时间窗口（按需）

**高级（P2，2-3 月）**：
5. ⚠️ 动态扩缩容（非必须）

---

### 与其他方案的优先级

| 方案 | 工作量 | 价值 | 推荐度 |
|------|--------|------|:---:|
| **Global Checkpoint** | 6-8 周 | 生产必须（Exactly-Once） | ✅✅✅ |
| **WAL + ReductStore 异步复制** | 2-3 周 | 时序能力 + 架构统一 | ✅✅✅ |
| **Watermark** | 3-4 周 | 事件时间窗口（可选） | ✅ |
| **Cypher 时序扩展** | 3-4 月 | 查询便利性（边际收益） | ⚠️ |

**建议顺序**：
1. Track A（共识）：2-3 周
2. WAL + ReductStore 异步复制：2-3 周
3. **Global Checkpoint**：4-6 周
4. Exactly-Once 验证：2 周
5. Track B/D：按需

---

**一句话总结**：
> Nexora 距离 Flink Stateful Functions 只差"Global Checkpoint + Exactly-Once"（6-8 周），完成后即可成为真正的"Stateful Streaming Graph Platform"，对标 Flink SF + RisingWave + 图能力的三合一架构。
