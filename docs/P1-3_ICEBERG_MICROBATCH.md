# P1-3: Iceberg 微批处理优化

**目标**: 将 Iceberg 写入延迟从 100-1000ms 降低到 10-100ms（10-100倍提升）

**问题**: 当前每个事件都触发一次完整的 Iceberg 事务（写数据文件 + commit snapshot）

---

## 当前实现分析

### 写入路径

```rust
// crates/nexora-eventlog/src/event_log_store.rs:217

pub async fn append(&self, events: &[RawEvent]) -> Result<u64> {
    // 1. 懒创建表
    let mut table = self.ensure_table(topic, events).await?;
    
    // 2. RawEvent → RecordBatch
    let batch = raw_events_to_record_batch(events)?;
    
    // 3. 写入数据文件（Parquet）
    let data_files = self.write_data_files(&table, batch).await?;
    
    // 4. 提交事务（创建新 snapshot）
    let _updated_table = self.commit_data_files(&mut table, data_files).await?;
    
    Ok(events.len() as u64)
}
```

### 性能瓶颈

#### 问题 1: 每次调用都创建新 snapshot

```
Event 1 → write_data_files(10ms) + commit(100ms) = 110ms
Event 2 → write_data_files(10ms) + commit(100ms) = 110ms
Event 3 → write_data_files(10ms) + commit(100ms) = 110ms
---
总延迟: 330ms
S3 PUTs: 3 data files + 3 manifest files + 3 metadata files = 9 次
```

**开销分解**:
- `write_data_files()`: ~10ms（写 Parquet 到 S3）
- `commit()`: ~100ms（写 manifest + metadata + 更新 catalog）

#### 问题 2: S3 写入延迟高

| 操作 | 延迟 | 频率 |
|------|------|------|
| PUT data file | ~10ms | 每批事件 |
| PUT manifest file | ~20ms | 每次 commit |
| PUT metadata.json | ~30ms | 每次 commit |
| PUT version-hint | ~10ms | 每次 commit |
| **总计** | **~70ms** | **每次 commit** |

#### 问题 3: Catalog 更新开销

REST Catalog (Iceberg REST API):
- HTTP POST `/v1/{namespace}/{table}/transactions/commit`
- 需要序列化 TableMetadata (~10KB)
- 网络 RTT: ~30ms
- Catalog 锁竞争: ~10-50ms

**总开销**: ~40-80ms 每次 commit

---

## 优化方案：微批处理

### 核心思想

**收集多个事件 → 单次 Iceberg 事务提交**

```
优化前（每事件一次 commit）:
Event 1 → commit(100ms)
Event 2 → commit(100ms)  
Event 3 → commit(100ms)
---
总延迟: 300ms
Snapshot 数量: 3

优化后（微批处理）:
Event 1 ┐
Event 2 ├→ buffer → commit(100ms)
Event 3 ┘
---
总延迟: 100ms
Snapshot 数量: 1
加速比: 3x
```

### 实现：MicrobatchWriter

```rust
// crates/nexora-eventlog/src/microbatch_writer.rs

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

/// 微批处理写入器：收集事件到缓冲区，批量提交
pub struct MicrobatchWriter {
    /// 每个 topic 的缓冲区
    buffers: Arc<Mutex<HashMap<String, TopicBuffer>>>,
    /// EventLogStore 引用
    store: Arc<EventLogStore>,
    /// 配置
    config: MicrobatchConfig,
}

#[derive(Clone, Debug)]
pub struct MicrobatchConfig {
    /// 最大缓冲事件数（达到后立即提交）
    pub max_batch_size: usize,
    /// 最大缓冲时间（超时后提交，即使未满）
    pub max_delay_ms: u64,
    /// 是否启用自适应批量大小
    pub adaptive: bool,
}

impl Default for MicrobatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,     // 1000 事件 或
            max_delay_ms: 100,        // 100ms 超时
            adaptive: true,           // 自适应批量
        }
    }
}

struct TopicBuffer {
    events: Vec<RawEvent>,
    first_event_time: Instant,
    waiting_senders: Vec<oneshot::Sender<Result<u64>>>,
}

impl MicrobatchWriter {
    pub fn new(store: Arc<EventLogStore>, config: MicrobatchConfig) -> Self {
        let writer = Self {
            buffers: Arc::new(Mutex::new(HashMap::new())),
            store,
            config,
        };
        
        // 启动后台刷新任务
        writer.start_flush_worker();
        
        writer
    }
    
    /// 追加事件（非阻塞，返回 future）
    pub async fn append(&self, event: RawEvent) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        let topic = event.topic.clone();
        
        let should_flush = {
            let mut buffers = self.buffers.lock().await;
            let buffer = buffers.entry(topic.clone()).or_insert_with(|| TopicBuffer {
                events: Vec::new(),
                first_event_time: Instant::now(),
                waiting_senders: Vec::new(),
            });
            
            buffer.events.push(event);
            buffer.waiting_senders.push(tx);
            
            // 检查是否需要立即刷新
            buffer.events.len() >= self.config.max_batch_size
        };
        
        if should_flush {
            // 异步刷新（不阻塞当前调用）
            self.flush_topic(&topic).await?;
        }
        
        // 等待刷新完成
        rx.await.map_err(|_| anyhow!("Flush worker died"))?
    }
    
    /// 后台刷新任务：定期检查超时的缓冲区
    fn start_flush_worker(&self) {
        let buffers = Arc::clone(&self.buffers);
        let store = Arc::clone(&self.store);
        let max_delay = Duration::from_millis(self.config.max_delay_ms);
        
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(10));
            
            loop {
                ticker.tick().await;
                
                let topics_to_flush: Vec<String> = {
                    let buffers = buffers.lock().await;
                    buffers
                        .iter()
                        .filter(|(_, buf)| {
                            !buf.events.is_empty()
                                && buf.first_event_time.elapsed() >= max_delay
                        })
                        .map(|(topic, _)| topic.clone())
                        .collect()
                };
                
                for topic in topics_to_flush {
                    if let Err(e) = Self::flush_topic_static(&buffers, &store, &topic).await {
                        tracing::error!("Failed to flush topic {}: {}", topic, e);
                    }
                }
            }
        });
    }
    
    /// 刷新指定 topic 的缓冲区
    async fn flush_topic(&self, topic: &str) -> Result<()> {
        Self::flush_topic_static(&self.buffers, &self.store, topic).await
    }
    
    async fn flush_topic_static(
        buffers: &Arc<Mutex<HashMap<String, TopicBuffer>>>,
        store: &Arc<EventLogStore>,
        topic: &str,
    ) -> Result<()> {
        let (events, senders) = {
            let mut buffers = buffers.lock().await;
            match buffers.remove(topic) {
                Some(buffer) => (buffer.events, buffer.waiting_senders),
                None => return Ok(()), // 已被其他任务刷新
            }
        };
        
        if events.is_empty() {
            return Ok(());
        }
        
        // 批量写入 Iceberg（单次事务）
        let start = Instant::now();
        let result = store.append(&events).await;
        let elapsed = start.elapsed();
        
        tracing::info!(
            "Flushed {} events to topic '{}' in {:?}",
            events.len(),
            topic,
            elapsed
        );
        
        // 通知所有等待者
        match &result {
            Ok(count) => {
                for sender in senders {
                    let _ = sender.send(Ok(*count));
                }
            }
            Err(e) => {
                let err = anyhow!("{}", e);
                for sender in senders {
                    let _ = sender.send(Err(anyhow!("{}", err)));
                }
            }
        }
        
        result.map(|_| ())
    }
    
    /// 手动刷新所有缓冲区（关闭前调用）
    pub async fn flush_all(&self) -> Result<()> {
        let topics: Vec<String> = {
            let buffers = self.buffers.lock().await;
            buffers.keys().cloned().collect()
        };
        
        for topic in topics {
            self.flush_topic(&topic).await?;
        }
        
        Ok(())
    }
}
```

---

## 自适应批量大小

```rust
impl MicrobatchWriter {
    /// 根据延迟动态调整批量大小
    fn adjust_batch_size(&mut self, topic: &str, elapsed: Duration) {
        if !self.config.adaptive {
            return;
        }
        
        let current_size = self.config.max_batch_size;
        
        if elapsed < Duration::from_millis(50) {
            // 延迟低 → 增大批量（减少 commit 频率）
            self.config.max_batch_size = (current_size * 12 / 10).min(10000);
            tracing::debug!(
                "Topic '{}': low latency ({:?}), increasing batch size to {}",
                topic, elapsed, self.config.max_batch_size
            );
        } else if elapsed > Duration::from_millis(200) {
            // 延迟高 → 减小批量（提高响应性）
            self.config.max_batch_size = (current_size * 8 / 10).max(100);
            tracing::debug!(
                "Topic '{}': high latency ({:?}), decreasing batch size to {}",
                topic, elapsed, self.config.max_batch_size
            );
        }
    }
}
```

---

## 集成到 nexora-app

### 修改：使用 MicrobatchWriter

```rust
// crates/nexora-app/src/main.rs

let event_log_config = EventLogConfig {
    catalog_uri: config.event_store.rest_uri.clone(),
    namespace: "nexora_events".to_string(),
    warehouse: config.event_store.warehouse.clone(),
};

let event_log_store = Arc::new(EventLogStore::new(event_log_config).await?);

// ✅ 使用微批处理包装
let microbatch_config = MicrobatchConfig {
    max_batch_size: 1000,
    max_delay_ms: 100,
    adaptive: true,
};
let event_writer = Arc::new(MicrobatchWriter::new(
    Arc::clone(&event_log_store),
    microbatch_config,
));

// 注入到 GraphService
let graph_service = GraphService::new(config.clone())
    .with_event_writer(event_writer);
```

### 修改：GraphService 使用 writer

```rust
// crates/nexora-core/src/graph/mod.rs

pub struct GraphService {
    // ...
    event_writer: Option<Arc<MicrobatchWriter>>,
}

impl GraphService {
    pub async fn set_property(&mut self, qid: &NexoraId, key: &str, value: Value) -> Result<()> {
        // 1. 写入 WAL
        let entry = WalEntry::SetProperty { ... };
        let seq_no = self.wal.append(entry).await?;
        
        // 2. 发射事件（异步，非阻塞）
        if let Some(writer) = &self.event_writer {
            let event = RawEvent {
                topic: "graph.mutations".to_string(),
                payload: json!({
                    "type": "set_property",
                    "qid": qid.to_hex(),
                    "key": key,
                    "value": value,
                    "seq_no": seq_no,
                }),
                timestamp: Utc::now().timestamp_millis(),
            };
            
            // ✅ 微批处理（返回 Future，不阻塞）
            tokio::spawn(async move {
                if let Err(e) = writer.append(event).await {
                    tracing::error!("Failed to append event: {}", e);
                }
            });
        }
        
        // 3. 提交到内存
        self.commit_to_memory(...)?;
        
        Ok(())
    }
}
```

---

## 性能预测

### 场景 1: 低负载（100 events/sec）

**优化前**:
```
100 events/sec × 110ms/event = 需要 11 秒处理 1 秒的事件
→ 无法实时处理，积压增长
```

**优化后**:
```
100ms 超时 → 每 100ms 一批
每批 ~10 events
吞吐量: 10 events / 100ms = 100 events/sec ✅
延迟: P99 < 200ms（100ms 缓冲 + 100ms commit）
```

### 场景 2: 高负载（10,000 events/sec）

**优化前**:
```
10,000 events × 110ms = 1,100 秒 = 18.3 分钟
→ 严重积压
```

**优化后**:
```
1000 events/batch（达到 max_batch_size）
10,000 / 1000 = 10 batches
每批 110ms → 总计 1.1 秒 ✅
加速比: 1100s / 1.1s = 1000x
延迟: P99 < 150ms（50ms 缓冲 + 100ms commit）
```

### 场景 3: 突发流量（1000 events 瞬间到达）

**优化前**:
```
1000 × 110ms = 110 秒串行处理
```

**优化后**:
```
1000 events → 单批处理
延迟: 110ms（一次 commit）
加速比: 1000x
```

---

## S3 开销优化

### 问题: 小文件过多

当前每批创建 1 个 Parquet 文件:
```
1000 batches/hour × 24 hours = 24,000 files/day
→ S3 LIST 性能下降
→ Iceberg snapshot 过大
```

### 优化: 合并小文件

```rust
impl MicrobatchWriter {
    async fn flush_with_compaction(&self, topic: &str) -> Result<()> {
        // 1. 检查最近的数据文件大小
        let recent_files = self.list_recent_data_files(topic).await?;
        let total_size: u64 = recent_files.iter().map(|f| f.file_size_in_bytes).sum();
        
        // 2. 如果小文件过多，触发合并
        const MAX_SMALL_FILES: usize = 100;
        const MIN_FILE_SIZE: u64 = 1 * 1024 * 1024; // 1MB
        
        let small_files: Vec<_> = recent_files
            .iter()
            .filter(|f| f.file_size_in_bytes < MIN_FILE_SIZE)
            .collect();
        
        if small_files.len() > MAX_SMALL_FILES {
            tracing::info!(
                "Compacting {} small files for topic '{}'",
                small_files.len(),
                topic
            );
            self.compact_files(topic, small_files).await?;
        }
        
        Ok(())
    }
}
```

---

## 测试计划

### 单元测试

```rust
#[tokio::test]
async fn test_microbatch_batches_events() {
    let store = Arc::new(MockEventLogStore::new());
    let config = MicrobatchConfig {
        max_batch_size: 3,
        max_delay_ms: 1000,
        adaptive: false,
    };
    let writer = MicrobatchWriter::new(store.clone(), config);
    
    // 发送 2 个事件（未达到 batch size）
    let e1 = RawEvent { topic: "test".into(), ... };
    let e2 = RawEvent { topic: "test".into(), ... };
    
    writer.append(e1).await;
    writer.append(e2).await;
    
    // 未触发刷新
    assert_eq!(store.commit_count(), 0);
    
    // 发送第 3 个事件（达到 batch size）
    let e3 = RawEvent { topic: "test".into(), ... };
    writer.append(e3).await.unwrap();
    
    // 触发刷新
    assert_eq!(store.commit_count(), 1);
    assert_eq!(store.last_batch_size(), 3);
}

#[tokio::test]
async fn test_microbatch_timeout_flush() {
    let store = Arc::new(MockEventLogStore::new());
    let config = MicrobatchConfig {
        max_batch_size: 1000,
        max_delay_ms: 100,  // 100ms 超时
        adaptive: false,
    };
    let writer = MicrobatchWriter::new(store.clone(), config);
    
    // 发送 1 个事件
    let e1 = RawEvent { topic: "test".into(), ... };
    writer.append(e1).await;
    
    // 等待超时
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // 应该已自动刷新
    assert_eq!(store.commit_count(), 1);
    assert_eq!(store.last_batch_size(), 1);
}
```

### 性能基准测试

```bash
# benches/eventlog_microbatch.rs
cargo bench --bench eventlog_microbatch

# 预期结果:
# Direct append (no batching):  110ms/event
# Microbatch (batch=1000):      0.11ms/event (1000x faster)
# Microbatch (batch=100):       1.1ms/event (100x faster)
```

---

## 风险与缓解

### 风险 1: 数据丢失（进程崩溃）

**问题**: 缓冲区中的事件未刷新

**缓解**:
```rust
// 优雅关闭
impl Drop for MicrobatchWriter {
    fn drop(&mut self) {
        // 同步刷新所有缓冲区
        tokio::runtime::Handle::current().block_on(async {
            if let Err(e) = self.flush_all().await {
                tracing::error!("Failed to flush on drop: {}", e);
            }
        });
    }
}
```

### 风险 2: 延迟增加（低负载场景）

**问题**: 低流量时，事件等待 100ms 才刷新

**缓解**: 可配置的超时时间
```rust
// 低延迟模式
let config = MicrobatchConfig {
    max_batch_size: 100,
    max_delay_ms: 10,  // 10ms 超时
    adaptive: true,
};
```

### 风险 3: 内存占用增加

**问题**: 缓冲大量事件

**缓解**: 限制缓冲区大小
```rust
const MAX_BUFFER_MEMORY: usize = 100 * 1024 * 1024; // 100MB

if buffer.estimated_size() > MAX_BUFFER_MEMORY {
    // 强制刷新
    self.flush_topic(topic).await?;
}
```

---

## 交付物

- [x] 设计文档（本文档）
- [ ] 实现 `MicrobatchWriter`（`crates/nexora-eventlog/src/microbatch_writer.rs`）
- [ ] 集成到 `nexora-app`
- [ ] 单元测试
- [ ] 性能基准测试
- [ ] 生产配置指南

---

**状态**: ⏳ 设计完成，待实现  
**预期工作量**: 4-6 小时  
**预期性能提升**: 10-1000倍（取决于负载模式）
