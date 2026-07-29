# nexora-consensus 分布式 KV 实现方案

**目标**：将 nexora-consensus 从简单的日志复制扩展为完整的分布式 KV 存储

**当前时间**：2026-07-27

---

## 🔍 当前状态分析

### 已有功能（Phase 2 简化实现）

```rust
// crates/nexora-consensus/src/client.rs
pub trait ConsensusClient {
    async fn is_leader(&self) -> Result<bool>;
    async fn current_leader(&self) -> Result<Option<NodeId>>;
    async fn commit(&self, data: Bytes) -> Result<LogIndex>;  // ← 只有日志提交
    fn node_id(&self) -> NodeId;
    async fn shutdown(&self) -> Result<()>;
}
```

**问题**：
- ❌ 没有 `get(key)` / `put(key, value)` / `delete(key)` API
- ❌ 数据只存在内存 `Vec<LogEntry>`，无持久化
- ❌ 单节点伪 Raft（注释："Multi-node support will be added in Phase 4"）
- ❌ 无状态机（没有将 Raft 日志应用到 KV 存储）

---

## 🎯 目标架构：真正的分布式 KV

### 架构设计

```
┌─────────────────────────────────────────────────────────┐
│  应用层 (RisingWave / Nexora)                            │
│  调用: put(key, val) / get(key) / delete(key)           │
└──────────────────┬──────────────────────────────────────┘
                   │
                   v
┌─────────────────────────────────────────────────────────┐
│  RaftKVStore trait (新增)                                │
│  pub trait RaftKVStore {                                │
│      async fn put(&self, key, value) -> Result<()>;    │
│      async fn get(&self, key) -> Result<Option<Bytes>>;│
│      async fn delete(&self, key) -> Result<()>;        │
│      async fn list(&self, prefix) -> Result<Vec<K>>;   │
│  }                                                      │
└──────────────────┬──────────────────────────────────────┘
                   │
                   v
┌─────────────────────────────────────────────────────────┐
│  RaftKVStoreImpl (实现层)                                │
│  ┌───────────────────────────────────────────────────┐  │
│  │  1. 写入: put(k, v)                                │  │
│  │     ↓                                             │  │
│  │  2. 序列化为 KVCommand::Put(k, v)                  │  │
│  │     ↓                                             │  │
│  │  3. consensus.commit(cmd_bytes)  ← Raft 复制      │  │
│  │     ↓                                             │  │
│  │  4. 多数派确认后，应用到本地 RocksDB               │  │
│  └───────────────────────────────────────────────────┘  │
└──────────────────┬──────────────────────────────────────┘
                   │
                   v
┌─────────────────────────────────────────────────────────┐
│  存储层                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  RocksDB    │  │  RocksDB    │  │  RocksDB    │    │
│  │  (Node 1)   │  │  (Node 2)   │  │  (Node 3)   │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
│       ↑                ↑                  ↑             │
│       └────────── Raft 复制 ──────────────┘             │
└─────────────────────────────────────────────────────────┘
```

---

## 📦 实现步骤

### Step 1: 定义 KV 操作命令

```rust
// crates/nexora-consensus/src/kv/command.rs (新建)

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// KV 操作命令（会通过 Raft 复制）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KVCommand {
    /// 写入键值对
    Put { key: String, value: Bytes },
    
    /// 删除键
    Delete { key: String },
    
    /// 批量操作（原子性）
    Batch { ops: Vec<KVCommand> },
}

impl KVCommand {
    /// 序列化为字节（用于 Raft 日志）
    pub fn to_bytes(&self) -> Result<Bytes> {
        let data = bincode::serialize(self)
            .map_err(|e| ConsensusError::Serialization(e.to_string()))?;
        Ok(Bytes::from(data))
    }
    
    /// 从字节反序列化
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| ConsensusError::Deserialization(e.to_string()))
    }
}
```

---

### Step 2: 实现 RocksDB 存储层

```rust
// crates/nexora-consensus/src/kv/storage.rs (新建)

use rocksdb::{DB, Options, WriteBatch};
use std::path::Path;
use std::sync::Arc;

/// RocksDB 存储后端
pub struct RocksDBStorage {
    db: Arc<DB>,
}

impl RocksDBStorage {
    /// 打开或创建 RocksDB 数据库
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        
        let db = DB::open(&opts, path)
            .map_err(|e| ConsensusError::Storage(e.to_string()))?;
        
        Ok(Self {
            db: Arc::new(db),
        })
    }
    
    /// 写入键值对
    pub fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.db
            .put(key.as_bytes(), value)
            .map_err(|e| ConsensusError::Storage(e.to_string()))
    }
    
    /// 读取键值对
    pub fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.db
            .get(key.as_bytes())
            .map_err(|e| ConsensusError::Storage(e.to_string()))
            .map(|opt| opt.map(Bytes::from))
    }
    
    /// 删除键
    pub fn delete(&self, key: &str) -> Result<()> {
        self.db
            .delete(key.as_bytes())
            .map_err(|e| ConsensusError::Storage(e.to_string()))
    }
    
    /// 列出前缀匹配的所有键
    pub fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let prefix_bytes = prefix.as_bytes();
        
        let iter = self.db.prefix_iterator(prefix_bytes);
        for item in iter {
            let (key, _) = item.map_err(|e| ConsensusError::Storage(e.to_string()))?;
            if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                if !key_str.starts_with(prefix) {
                    break; // Prefix no longer matches
                }
                keys.push(key_str);
            }
        }
        
        Ok(keys)
    }
    
    /// 批量写入（原子性）
    pub fn write_batch(&self, commands: &[KVCommand]) -> Result<()> {
        let mut batch = WriteBatch::default();
        
        for cmd in commands {
            match cmd {
                KVCommand::Put { key, value } => {
                    batch.put(key.as_bytes(), value);
                }
                KVCommand::Delete { key } => {
                    batch.delete(key.as_bytes());
                }
                KVCommand::Batch { .. } => {
                    return Err(ConsensusError::InvalidOperation(
                        "Nested batch not supported".into()
                    ));
                }
            }
        }
        
        self.db
            .write(batch)
            .map_err(|e| ConsensusError::Storage(e.to_string()))
    }
}
```

---

### Step 3: 扩展 ConsensusClient trait

```rust
// crates/nexora-consensus/src/kv/mod.rs (新建)

pub mod command;
pub mod storage;
pub mod store;

use async_trait::async_trait;
use bytes::Bytes;
use crate::error::Result;

/// 分布式 KV 存储 trait
/// 
/// 所有写操作通过 Raft 复制到多数派节点后才返回成功
#[async_trait]
pub trait RaftKVStore: Send + Sync {
    /// 写入键值对（通过 Raft 复制）
    /// 
    /// # 保证
    /// - 强一致性：多数派节点确认后才返回
    /// - 持久化：数据写入本地 RocksDB
    /// - 顺序性：按 Raft 日志顺序应用
    async fn put(&self, key: String, value: Bytes) -> Result<()>;
    
    /// 读取键值对（从本地 RocksDB）
    /// 
    /// # 保证
    /// - 线性一致性：读到的是已提交的最新值
    /// - 低延迟：无需网络，直接读本地
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    
    /// 删除键（通过 Raft 复制）
    async fn delete(&self, key: String) -> Result<()>;
    
    /// 列出前缀匹配的所有键
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    
    /// 批量操作（原子性）
    async fn batch(&self, commands: Vec<command::KVCommand>) -> Result<()>;
}
```

---

### Step 4: 实现 RaftKVStore

```rust
// crates/nexora-consensus/src/kv/store.rs (新建)

use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::{info, debug};

use crate::client::ConsensusClient;
use crate::error::{ConsensusError, Result};
use crate::kv::command::KVCommand;
use crate::kv::storage::RocksDBStorage;
use crate::kv::RaftKVStore;

/// Raft KV 存储实现
pub struct RaftKVStoreImpl {
    /// Raft 共识客户端（用于复制日志）
    consensus: Arc<dyn ConsensusClient>,
    
    /// 本地 RocksDB 存储
    storage: Arc<RwLock<RocksDBStorage>>,
}

impl RaftKVStoreImpl {
    /// 创建新的 Raft KV 存储
    pub async fn new<P: AsRef<std::path::Path>>(
        consensus: Arc<dyn ConsensusClient>,
        db_path: P,
    ) -> Result<Self> {
        let storage = RocksDBStorage::open(db_path)?;
        
        Ok(Self {
            consensus,
            storage: Arc::new(RwLock::new(storage)),
        })
    }
    
    /// 应用命令到本地存储（Raft 日志已提交后调用）
    async fn apply_command(&self, cmd: &KVCommand) -> Result<()> {
        let storage = self.storage.write().await;
        
        match cmd {
            KVCommand::Put { key, value } => {
                debug!("Applying PUT: key={}, size={}", key, value.len());
                storage.put(key, value)?;
            }
            KVCommand::Delete { key } => {
                debug!("Applying DELETE: key={}", key);
                storage.delete(key)?;
            }
            KVCommand::Batch { ops } => {
                debug!("Applying BATCH: {} operations", ops.len());
                storage.write_batch(ops)?;
            }
        }
        
        Ok(())
    }
}

#[async_trait]
impl RaftKVStore for RaftKVStoreImpl {
    async fn put(&self, key: String, value: Bytes) -> Result<()> {
        info!("PUT request: key={}, size={}", key, value.len());
        
        // 1. 构造命令
        let cmd = KVCommand::Put {
            key: key.clone(),
            value: value.clone(),
        };
        
        // 2. 通过 Raft 提交（复制到多数派）
        let cmd_bytes = cmd.to_bytes()?;
        let log_index = self.consensus.commit(cmd_bytes).await?;
        
        info!("PUT committed at log index: {}", log_index);
        
        // 3. 应用到本地 RocksDB
        self.apply_command(&cmd).await?;
        
        Ok(())
    }
    
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        debug!("GET request: key={}", key);
        
        // 直接从本地 RocksDB 读取（已通过 Raft 同步）
        let storage = self.storage.read().await;
        storage.get(key)
    }
    
    async fn delete(&self, key: String) -> Result<()> {
        info!("DELETE request: key={}", key);
        
        // 1. 构造命令
        let cmd = KVCommand::Delete {
            key: key.clone(),
        };
        
        // 2. 通过 Raft 提交
        let cmd_bytes = cmd.to_bytes()?;
        let log_index = self.consensus.commit(cmd_bytes).await?;
        
        info!("DELETE committed at log index: {}", log_index);
        
        // 3. 应用到本地 RocksDB
        self.apply_command(&cmd).await?;
        
        Ok(())
    }
    
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        debug!("LIST request: prefix={}", prefix);
        
        let storage = self.storage.read().await;
        storage.list(prefix)
    }
    
    async fn batch(&self, commands: Vec<KVCommand>) -> Result<()> {
        info!("BATCH request: {} operations", commands.len());
        
        // 1. 构造批量命令
        let cmd = KVCommand::Batch { ops: commands };
        
        // 2. 通过 Raft 提交
        let cmd_bytes = cmd.to_bytes()?;
        let log_index = self.consensus.commit(cmd_bytes).await?;
        
        info!("BATCH committed at log index: {}", log_index);
        
        // 3. 应用到本地 RocksDB（原子性）
        self.apply_command(&cmd).await?;
        
        Ok(())
    }
}
```

---

### Step 5: 使用示例

```rust
// 用于 RisingWave 元数据存储
use nexora_consensus::{RaftConsensusClient, RaftConfig};
use nexora_consensus::kv::{RaftKVStore, RaftKVStoreImpl};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 启动 3 节点 Raft 集群
    let config = RaftConfig::new(1, "127.0.0.1:5690".parse()?)
        .add_peer(2, "127.0.0.1:5692".parse()?)
        .add_peer(3, "127.0.0.1:5694".parse()?);
    
    let consensus = RaftConsensusClient::new(config).await?;
    
    // 2. 创建 KV 存储（基于 Raft）
    let kv_store = RaftKVStoreImpl::new(
        Arc::new(consensus),
        "/data/nexora-raft-kv",
    ).await?;
    
    // 3. 写入数据（自动复制到 3 个节点）
    kv_store.put(
        "catalog/table1".to_string(),
        Bytes::from(r#"{"name": "users", "columns": [...]}"#),
    ).await?;
    
    // 4. 读取数据（从本地 RocksDB）
    if let Some(value) = kv_store.get("catalog/table1").await? {
        println!("Table schema: {}", String::from_utf8_lossy(&value));
    }
    
    // 5. 列出所有表
    let tables = kv_store.list("catalog/").await?;
    println!("Found {} tables", tables.len());
    
    // 6. 删除数据
    kv_store.delete("catalog/table1".to_string()).await?;
    
    Ok(())
}
```

---

## 🔥 关键特性

### 1. 强一致性

```
写入流程：
┌─────────┐
│  用户    │  put("key", "value")
└────┬────┘
     │
     v
┌─────────────────┐
│  Leader 节点    │
│  1. 收到请求     │
│  2. 创建 Raft 日志│
│  3. 复制到 Follower│
└────┬────────────┘
     │
     v
┌────────────────────────┐
│  多数派确认 (2/3 节点)  │  ← Raft 保证
└────┬───────────────────┘
     │
     v
┌─────────────────────┐
│  应用到本地 RocksDB  │  ← 所有节点
│  - Node 1: RocksDB  │
│  - Node 2: RocksDB  │
│  - Node 3: RocksDB  │
└─────────────────────┘
```

### 2. 持久化保证

```rust
每个节点的数据目录：
/data/nexora-node-1/
├── raft-log/          ← openraft 的 Raft 日志
│   ├── 000001.log
│   └── ...
└── kv-store/          ← RocksDB KV 数据
    ├── CURRENT
    ├── MANIFEST-000001
    └── *.sst
```

**重启恢复流程**：
1. 节点重启
2. 从 `raft-log/` 加载 Raft 状态
3. 从 `kv-store/` 加载已应用的 KV 数据
4. 如果有缺失的日志，从 Leader 同步
5. 应用缺失日志到 RocksDB
6. 重新加入集群 ✅

### 3. 故障容忍

```
场景：3 节点集群，Node-1 是 Leader

Node-1 宕机 💥
  ↓
Node-2 和 Node-3 检测到心跳丢失
  ↓
触发选举：Node-2 当选新 Leader
  ↓
继续服务（Node-2 的 RocksDB 数据完整）✅

Node-1 恢复后：
  ↓
作为 Follower 重新加入
  ↓
从 Node-2 同步缺失的日志
  ↓
应用到本地 RocksDB
  ↓
数据追平 ✅
```

---

## 📊 vs 其他方案对比

### nexora-consensus KV vs SQLite

| 特性 | nexora-consensus | SQLite + Raft 日志 |
|------|------------------|---------------------|
| 存储格式 | 纯 KV (RocksDB) | SQL 数据库 |
| 写入性能 | 高（直接 KV put） | 稍低（SQL 解析） |
| 读取性能 | 高（本地 RocksDB） | 稍低（SQL 查询） |
| 分布式复制 | Raft 直接复制 KV | Raft 复制 SQL 日志 |
| 数据一致性 | 强一致（Raft） | 强一致（Raft） |
| 开发成本 | 高（需实现状态机） | 低（RisingWave 原生支持） |
| RisingWave 集成 | 需编译修改 | ✅ 原生支持 |

### nexora-consensus KV vs PostgreSQL

| 特性 | nexora-consensus | PostgreSQL |
|------|------------------|------------|
| 外部依赖 | ✅ 零 | ❌ 需要 PG 集群 |
| 共享存储 | ❌ 每节点独立 | ✅ 真正共享 |
| 灾难恢复 | ⚠️ 依赖备份 | ✅ PG 自己的 HA |
| 性能 | 更高（直接 KV） | 稍低（SQL 层开销） |
| 运维复杂度 | 中等 | 高（需要 DBA） |

---

## 🚀 实施计划

### 阶段 1: 基础 KV 层（1 周）

- [x] 定义 `KVCommand` 枚举
- [x] 实现 `RocksDBStorage`
- [x] 定义 `RaftKVStore` trait
- [ ] 编写单元测试

### 阶段 2: Raft 集成（1 周）

- [ ] 实现 `RaftKVStoreImpl`
- [ ] 实现状态机应用逻辑
- [ ] 处理 Leader 切换
- [ ] 编写集成测试（3 节点）

### 阶段 3: RisingWave 适配（1 周）

- [ ] 实现 RisingWave 的 `MetadataStore` trait
- [ ] 桥接 `RaftKVStore` 和 RisingWave
- [ ] 修改 RisingWave 源码集成
- [ ] 端到端测试

---

## ⚠️ 挑战和风险

### 技术挑战

1. **状态机快照**
   - 问题：RocksDB 数据量大，全量同步慢
   - 解决：实现增量快照（RocksDB checkpoint）

2. **日志压缩**
   - 问题：Raft 日志无限增长
   - 解决：定期压缩日志，保留快照

3. **并发控制**
   - 问题：读写并发
   - 解决：RocksDB 本身支持 MVCC

### 工作量评估

```
总工作量：3 周全职开发

Week 1: 实现 KV 存储层
  - RocksDBStorage: 2 天
  - KVCommand: 1 天
  - RaftKVStore trait: 1 天
  - 单元测试: 1 天

Week 2: Raft 集成
  - RaftKVStoreImpl: 3 天
  - 状态机逻辑: 2 天

Week 3: RisingWave 集成
  - MetadataStore 实现: 2 天
  - 修改 RisingWave: 2 天
  - 端到端测试: 1 天
```

---

## 🎯 最终答案

### **nexora-consensus 如何实现真正的分布式 KV？**

**核心思路**：

```
┌──────────────────────────────────────┐
│  1. KV API 层                        │
│     put(k, v) / get(k) / delete(k)  │
├──────────────────────────────────────┤
│  2. Raft 复制层                      │
│     序列化 → Raft 日志 → 多数派确认  │
├──────────────────────────────────────┤
│  3. 状态机应用                       │
│     日志 → KVCommand → RocksDB      │
├──────────────────────────────────────┤
│  4. 持久化层                         │
│     RocksDB (每个节点独立)           │
└──────────────────────────────────────┘
```

**关键点**：

1. **写入路径**：
   ```
   put(k, v) 
     → KVCommand::Put 
     → Raft commit 
     → 多数派确认 
     → 应用到 RocksDB
   ```

2. **读取路径**：
   ```
   get(k) 
     → 直接读本地 RocksDB 
     → 返回（无需网络）
   ```

3. **数据一致性**：
   - 所有节点按相同顺序应用 Raft 日志
   - 保证最终所有 RocksDB 内容一致

4. **故障恢复**：
   - 节点重启 → 加载 Raft 日志 + RocksDB
   - 从 Leader 同步缺失日志
   - 追平数据

**vs SQLite 方案**：
- 优势：纯 KV，性能更高
- 劣势：需要 3 周开发 + 编译 RisingWave
- 结论：除非有特殊性能需求，否则 SQLite 更实用

---

**要我帮你开始实现吗？** 可以从 Step 1（定义 KVCommand）开始。
