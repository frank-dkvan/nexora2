# 终极方案：内置持久化的 RisingWave 集成

**你的需求非常合理！** 这正是一个优秀架构应该追求的目标。

---

## 🎯 需求明确

### 你想要的：

```
✅ 不部署 etcd
✅ 不部署 PostgreSQL  
✅ 不部署任何外部依赖
✅ 元数据持久化（重启不丢失）
✅ 3 节点 HA（自动故障转移）
```

### 问题：nexora-consensus 和 nexora-rpc 能实现吗？

**答案**：✅ **理论上完全可以！而且这正是它们应该做的事。**

---

## 📊 当前状态分析

### RisingWave 的两个存储层

```
┌─────────────────────────────────────────┐
│  Meta Storage (元数据)                   │
│  --backend mem/etcd/postgres            │ ← 当前问题
│  存储：catalog, DDL, schema              │
├─────────────────────────────────────────┤
│  State Storage (状态数据)                │
│  --state-store hummock+memory/s3/fs     │ ← 当前配置
│  存储：materialized view 的计算状态      │
└─────────────────────────────────────────┘
```

### 当前 Nexora 的配置

```rust
// distributed.rs (第 271-272 行)
cmd.arg("--backend").arg("mem")                    // ← 内存，无持久化
   .arg("--state-store").arg("hummock+memory")     // ← 内存，无持久化
   .arg("--data-directory").arg(&node_data_dir);
```

**问题**：
- ❌ `--backend mem` → Meta 数据重启丢失
- ❌ `--state-store hummock+memory` → 计算状态重启丢失

---

## 💡 解决方案：三层递进

### 方案 1: 文件系统持久化（最简单，推荐）

#### 实现方式

```rust
// distributed.rs - 修改启动参数
cmd.arg("--backend").arg("sql")  // ← 使用 SQLite (RisingWave 内置)
   .arg("--sql-endpoint").arg(&format!(
       "sqlite://{}/.risingwave/meta.db", 
       node_data_dir.display()
   ))
   .arg("--state-store").arg(&format!(
       "hummock+fs://{}/.risingwave/state", 
       node_data_dir.display()
   ))
   .arg("--data-directory").arg(&node_data_dir);
```

**效果**：
- ✅ **零外部依赖**
- ✅ 元数据持久化到本地 SQLite
- ✅ 状态数据持久化到本地文件系统
- ✅ 3 节点 HA 仍然工作（Raft 选举独立）
- ⚠️ 每个节点独立存储（非共享）

**数据目录结构**：
```
/tmp/nexora-risingwave-cluster/
├── meta-1/
│   └── .risingwave/
│       ├── meta.db          ← SQLite 元数据
│       └── state/           ← Hummock 状态
├── meta-2/
│   └── .risingwave/...
└── meta-3/
    └── .risingwave/...
```

**优点**：
- ✅ 实现简单（改 3 行代码）
- ✅ 完全本地化
- ✅ 重启后数据不丢失

**缺点**：
- ⚠️ 每个节点独立存储（但 Raft 会同步元数据）
- ⚠️ 磁盘故障会丢失该节点数据

---

### 方案 2: nexora-consensus 作为分布式存储（深度集成）

#### 架构设计

```
┌─────────────────────────────────────────────────┐
│  RisingWave Meta Node                           │
│  ┌───────────────────────────────────────────┐  │
│  │  MetadataStore Trait                      │  │
│  │  ├─ MemBackend (当前)                     │  │
│  │  ├─ EtcdBackend                           │  │
│  │  ├─ PostgresBackend                       │  │
│  │  └─ NexoraRaftBackend ← 新实现！          │  │
│  └───────────────┬───────────────────────────┘  │
│                  │                               │
│                  v                               │
│  ┌───────────────────────────────────────────┐  │
│  │  nexora-consensus (Raft Storage)          │  │
│  │  - 自动跨节点复制                          │  │
│  │  - 强一致性保证                            │  │
│  │  - 本地磁盘持久化                          │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

#### 实现步骤

**Step 1: 扩展 nexora-consensus 添加存储功能**

```rust
// crates/nexora-consensus/src/storage.rs (新建)
use bytes::Bytes;

#[async_trait]
pub trait RaftStorage: Send + Sync {
    /// 写入键值对（通过 Raft 同步）
    async fn put(&self, key: String, value: Bytes) -> Result<()>;
    
    /// 读取键值对
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    
    /// 删除键值对
    async fn delete(&self, key: &str) -> Result<()>;
    
    /// 列出前缀匹配的所有键
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

pub struct RocksDBRaftStorage {
    db: rocksdb::DB,           // 本地持久化
    consensus: RaftConsensusClient,  // 跨节点同步
}

impl RaftStorage for RocksDBRaftStorage {
    async fn put(&self, key: String, value: Bytes) -> Result<()> {
        // 1. 通过 Raft 提交（确保多数派确认）
        let log_data = serialize(&KVOperation::Put(key.clone(), value.clone()));
        self.consensus.commit(log_data).await?;
        
        // 2. 本地 RocksDB 持久化
        self.db.put(key.as_bytes(), &value)?;
        
        Ok(())
    }
    
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // 直接从本地读取（已通过 Raft 同步）
        Ok(self.db.get(key.as_bytes())?.map(Bytes::from))
    }
}
```

**Step 2: 实现 RisingWave MetadataStore**

```rust
// extensions/meta_raft/src/metadata_store.rs (新建)
use risingwave_meta::storage::MetadataStore;  // RisingWave trait
use nexora_consensus::RaftStorage;

pub struct NexoraMetadataStore {
    storage: Arc<dyn RaftStorage>,
}

#[async_trait]
impl MetadataStore for NexoraMetadataStore {
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.storage.put(
            String::from_utf8_lossy(&key).to_string(),
            Bytes::from(value)
        ).await
    }
    
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.storage.get(&String::from_utf8_lossy(key))
            .await
            .map(|opt| opt.map(|b| b.to_vec()))
    }
    
    // ... 其他 MetadataStore 方法
}
```

**Step 3: 修改 RisingWave 启动**

```rust
// crates/nexora-risingwave/src/distributed.rs
async fn start_meta_with_nexora_backend(
    config: &DistributedConfig,
    meta_cfg: &MetaNodeConfig,
) -> Result<EmbeddedProcess> {
    // 1. 启动 nexora-consensus Raft 节点
    let raft_config = RaftConfig::new(
        meta_cfg.node_id as u64,
        meta_cfg.listen_addr.parse()?,
    );
    
    for peer in &config.meta_nodes {
        if peer.node_id != meta_cfg.node_id {
            raft_config.add_peer(
                peer.node_id as u64,
                peer.advertise_addr.parse()?,
            );
        }
    }
    
    let consensus = RaftConsensusClient::new(raft_config).await?;
    
    // 2. 创建 Raft 存储层
    let storage = RocksDBRaftStorage::new(
        &node_data_dir.join("raft-storage"),
        consensus,
    ).await?;
    
    // 3. 创建 MetadataStore
    let meta_store = NexoraMetadataStore::new(storage);
    
    // 4. 启动 RisingWave Meta（编译模式）
    let meta = risingwave_meta::MetaService::new(
        meta_cfg.listen_addr.parse()?,
        meta_store,  // ← 使用 nexora-consensus 作为后端
    ).await?;
    
    meta.start().await?;
    
    Ok(/* ... */)
}
```

**效果**：
- ✅ **零外部依赖**（无 etcd，无 PostgreSQL）
- ✅ 元数据通过 Raft 自动同步到 3 节点
- ✅ 每个节点本地 RocksDB 持久化
- ✅ 任意节点重启，数据不丢失
- ✅ 任意 1 节点故障，集群继续工作

**挑战**：
- ❌ 需要编译 vendor/risingwave（~800,000 行代码）
- ❌ 需要维护 RisingWave 补丁
- ⏱️ 开发时间：2-3 周

---

### 方案 3: 混合方案（进程模式 + 本地持久化）

**最实用的折中方案**：

```rust
// distributed.rs - 改进版
impl DistributedEmbeddedRisingWave {
    async fn start_meta_node_persistent(
        binary_path: &PathBuf,
        config: &DistributedConfig,
        meta_cfg: &MetaNodeConfig,
        is_first: bool,
    ) -> Result<EmbeddedProcess> {
        let node_data_dir = config.data_dir.join(format!("meta-{}", meta_cfg.node_id));
        std::fs::create_dir_all(&node_data_dir)?;
        
        let mut cmd = Command::new(binary_path);
        cmd.arg("meta-node")
            .arg("--listen-addr").arg(&meta_cfg.listen_addr)
            .arg("--advertise-addr").arg(&meta_cfg.advertise_addr)
            .arg("--dashboard-host").arg(&meta_cfg.dashboard_addr)
            
            // ← 关键改进：使用 SQLite + 文件系统
            .arg("--backend").arg("sql")
            .arg("--sql-endpoint").arg(&format!(
                "sqlite://{}/.risingwave/meta.db",
                node_data_dir.display()
            ))
            .arg("--state-store").arg(&format!(
                "hummock+fs://{}/.risingwave/state",
                node_data_dir.display()
            ))
            .arg("--data-directory").arg(&node_data_dir);
        
        if !is_first {
            cmd.arg("--join").arg(&config.meta_nodes[0].advertise_addr);
        }
        
        let process = cmd.spawn()?;
        
        Ok(EmbeddedProcess {
            process,
            node_id: meta_cfg.node_id,
            node_type: NodeType::Meta,
            listen_addr: meta_cfg.listen_addr.clone(),
        })
    }
}
```

**配置结构**：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedConfig {
    // ... 现有字段 ...
    
    /// 持久化模式
    pub persistence_mode: PersistenceMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PersistenceMode {
    /// 内存模式（测试用）
    Memory,
    
    /// 本地文件系统（推荐）
    LocalFS {
        base_dir: PathBuf,
    },
    
    /// 外部 PostgreSQL
    Postgres {
        uri: String,
    },
    
    /// 外部 etcd
    Etcd {
        endpoints: Vec<String>,
    },
    
    /// nexora-consensus（未来）
    #[cfg(feature = "nexora-backend")]
    NexoraRaft {
        raft_port_base: u16,
    },
}

impl Default for PersistenceMode {
    fn default() -> Self {
        Self::LocalFS {
            base_dir: PathBuf::from("/tmp/nexora-risingwave-cluster"),
        }
    }
}
```

---

## 🎯 三个方案对比

| 方案 | 外部依赖 | 持久化 | 开发成本 | 推荐度 |
|------|----------|--------|----------|--------|
| **方案 1: 本地 SQLite** | ✅ 零 | ✅ 本地文件 | ⭐ 1 小时 | ⭐⭐⭐⭐⭐ |
| **方案 2: nexora-consensus** | ✅ 零 | ✅ 分布式 Raft | ⭐⭐⭐ 2-3 周 | ⭐⭐⭐ |
| **方案 3: 混合配置** | ✅ 零 | ✅ 可选 | ⭐ 4 小时 | ⭐⭐⭐⭐ |

---

## ✅ 立即可用的方案（推荐）

### 代码修改（5 分钟）

```rust
// crates/nexora-risingwave/src/distributed.rs
// 修改第 271-272 行

// 旧代码
.arg("--backend").arg("mem")
.arg("--state-store").arg(format!("hummock+memory"))

// 新代码
.arg("--backend").arg("sql")
.arg("--sql-endpoint").arg(&format!(
    "sqlite://{}/.risingwave/meta.db",
    node_data_dir.display()
))
.arg("--state-store").arg(&format!(
    "hummock+fs://{}/.risingwave/state",
    node_data_dir.display()
))
```

### 测试

```bash
# 启动集群
cargo run --release --features embedded -- \
  --enable-risingwave \
  --enable-embedded-risingwave \
  --risingwave-cluster-mode

# 创建表
psql -h 127.0.0.1 -p 4566 -d dev <<EOF
CREATE TABLE t1 (id INT, name VARCHAR);
INSERT INTO t1 VALUES (1, 'test');
EOF

# 重启 Nexora
# <Ctrl+C>
cargo run --release --features embedded -- ...

# 验证数据仍然存在
psql -h 127.0.0.1 -p 4566 -d dev -c "SELECT * FROM t1;"
# 预期输出：(1, 'test')
```

**效果**：
- ✅ 零外部依赖
- ✅ 数据持久化
- ✅ 重启不丢失
- ✅ 5 分钟搞定

---

## 🎓 nexora-consensus 和 nexora-rpc 的真正价值

### 当前状态

**能否实现你的需求？** ⚠️ **理论上可以，但需要大量额外工作**

### 需要做的工作

1. **扩展 nexora-consensus**
   - 添加 `RaftStorage` trait
   - 实现 RocksDB 本地持久化
   - 实现跨节点数据同步
   - **工作量**：~1000 行代码，1 周

2. **实现 RisingWave 适配器**
   - 实现 `MetadataStore` trait
   - 桥接 nexora-consensus 和 RisingWave
   - **工作量**：~500 行代码，3 天

3. **编译 RisingWave**
   - 修改 vendor/risingwave 源码
   - 集成 NexoraMetadataStore
   - 维护补丁
   - **工作量**：需要 nightly Rust，持续维护

4. **测试和调试**
   - 单元测试
   - 集成测试
   - 性能测试
   - **工作量**：1 周

**总成本**：2-3 周全职开发 + 持续维护

### vs 本地 SQLite 方案

**SQLite 方案**：
- ✅ 5 分钟修改 3 行代码
- ✅ RisingWave 原生支持
- ✅ 零维护成本
- ⚠️ 每个节点独立存储（但 Raft 会同步元数据）

**nexora-consensus 方案**：
- ✅ 分布式存储（理论上更优雅）
- ❌ 2-3 周开发成本
- ❌ 持续维护补丁
- ❌ 升级 RisingWave 时需要重新适配

---

## 💬 最终建议

### 给你的答案

**问题**：nexora-consensus 和 nexora-rpc 能否实现内置持久化？

**答案**：✅ **理论上可以，但完全不值得**

### 最佳方案（立即可用）

```rust
// 修改 3 行代码
.arg("--backend").arg("sql")
.arg("--sql-endpoint").arg("sqlite://...")
.arg("--state-store").arg("hummock+fs://...")
```

**效果**：
- ✅ 零外部依赖（无 etcd，无 PostgreSQL）
- ✅ 元数据持久化（SQLite）
- ✅ 状态持久化（文件系统）
- ✅ 3 节点 HA（内置 Raft）
- ✅ 5 分钟搞定

### 如果真的想用 nexora-consensus

**前提条件**：
1. 有 2-3 周开发时间
2. 愿意维护 RisingWave 补丁
3. 需要真正的分布式元数据存储（每个节点独立 SQLite 不够）

**否则**：用本地 SQLite，它完美解决你的需求。

---

**总结**：你的需求合理，本地 SQLite 完美满足。nexora-consensus 理论上能做，但开发成本是 SQLite 方案的 100 倍。

代码改动已准备好，要我帮你实现方案 1 吗？
