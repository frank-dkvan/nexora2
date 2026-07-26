# 为什么需要两个 Raft 实现？

**问题**：Nexora 2 已经有 `nexora-raft` 和 `nexora-zenoh`，为什么 Phase 2 还要创建 `nexora-consensus` 和 `nexora-rpc`？

---

## ⚠️ 重要澄清：名字会误导人！

### nexora-raft ≠ 基于 openraft
- ❌ **没有使用** openraft 库
- ✅ 自己实现的 Raft **部分逻辑**（quorum commit）
- ✅ **没有 leader election**（使用 ShardMap 代替）

### nexora-zenoh ≠ 基于 Eclipse Zenoh
- ❌ **默认不使用** zenoh 库（`zenoh` feature 是可选的）
- ✅ 默认使用**自定义 TCP 协议**（length-prefixed JSON）
- ✅ 注释明确说："Replaces Zenoh with a simple, debuggable protocol"

---

## 🎯 简短回答

**nexora-raft** 和 **nexora-consensus** 解决的是**不同的问题**：

| 特性 | nexora-raft | nexora-consensus |
|------|-------------|------------------|
| **用途** | Nexora 图数据库的 WAL 复制 | RisingWave Meta 节点的 HA |
| **Leader Election** | ❌ 无（使用 ShardMap + OwnerEpoch） | ✅ 有（完整 Raft） |
| **应用场景** | 分片数据复制 | 服务高可用 |
| **实现** | 自实现 Raft 部分逻辑 | ✅ openraft 0.9 库 |
| **依赖 openraft** | ❌ 否 | ✅ 是 |

**nexora-zenoh** 和 **nexora-rpc** 也解决的是**不同的问题**：

| 特性 | nexora-zenoh | nexora-rpc |
|------|-------------|------------|
| **用途** | Nexora 节点间 P2P 路由 | RisingWave 节点间 gRPC |
| **默认协议** | 自定义 TCP (length-prefixed JSON) | gRPC (tonic) + protobuf |
| **应用场景** | Graph operation 分布式路由 | RisingWave 内部 RPC |
| **依赖 zenoh** | ❌ 否（可选 feature） | N/A |
| **依赖 tonic** | ❌ 否 | ✅ 是 |

---

## 📖 详细解释

### 1. nexora-raft 的设计 - 为 WAL 复制优化

**关键事实**：
- ❌ **不是基于 openraft** - 查看 `Cargo.toml`，没有 `openraft` 依赖
- ✅ 自己实现的 Raft **部分逻辑**（只有 quorum commit）
- ✅ 名字叫 "raft" 只是因为借鉴了 Raft 的 quorum 思想

**设计目标**：
- 将 Nexora 的 WAL 条目复制到多个副本
- 确保 quorum commit
- 不需要 leader election（Shard owner 就是 leader）

**关键代码**（`crates/nexora-raft/src/lib.rs:21`）：
```rust
//! - **No leader election**: In nexora, shard ownership is managed by
//!   ShardMap + OwnerEpoch. A shard owner is the de facto leader for that
//!   shard. Leader election is replaced by the ControlPlane's failover
//!   mechanism.
//! - **Raft log = WAL**: We reuse the existing WAL as the Raft log. Each
//!   WAL entry (with its seq_no) serves as a Raft log entry.
```

**依赖验证**（`crates/nexora-raft/Cargo.toml`）：
```toml
[dependencies]
# 注意：没有 openraft！
nexora-id = { path = "../nexora-id" }
serde = { workspace = true }
tokio = { workspace = true }
# ... 其他基础库
```

**为什么不能用于 RisingWave？**
- ❌ RisingWave Meta 需要 **真正的 leader election**
- ❌ RisingWave 不知道 Nexora 的 ShardMap/OwnerEpoch
- ❌ RisingWave Meta 不是基于 WAL 的
- ❌ 没有基于 openraft，API 不兼容

### 2. nexora-consensus 的设计 - 为 RisingWave HA 优化

**关键事实**：
- ✅ **基于 openraft 0.9** - 完整的生产级 Raft 实现
- ✅ 有完整的 leader election
- ✅ 标准 Raft 协议（log replication + leader election + safety）

**设计目标**：
- 提供完整的 Raft leader election
- 实现 RisingWave 的 `ElectionClient` trait
- 支持 3-5 节点 Meta 集群 HA

**依赖验证**（`crates/nexora-consensus/Cargo.toml`）：
```toml
[dependencies]
openraft = { version = "0.9", features = ["serde"] }  # ← 真正的 openraft！
async-trait = "0.1"
bytes = "1"
tokio = { version = "1", features = ["full"] }
```

**接口**（`crates/nexora-consensus/src/client.rs`）：
```rust
#[async_trait]
pub trait ConsensusClient: Send + Sync {
    async fn is_leader(&self) -> Result<bool>;  // ← RisingWave 需要这个
    async fn commit(&self, data: Bytes) -> Result<LogIndex>;
    fn node_id(&self) -> NodeId;
    async fn shutdown(&self) -> Result<()>;
}
```

**Phase 4 将实现**（`extensions/meta_raft/src/client.rs`）：
```rust
// RisingWave 的 ElectionClient trait
impl ElectionClient for RaftElectionClient {
    fn is_leader(&self) -> bool {
        self.consensus.is_leader()  // ← 使用 nexora-consensus
    }
    
    async fn run_once(&self, ttl: i64, stop: Receiver<()>) -> MetaResult<()> {
        // Leader election loop for RisingWave Meta
    }
}
```

### 3. nexora-zenoh 的设计 - 为 P2P 路由优化

**关键事实**：
- ❌ **默认不使用 zenoh 库** - `zenoh` 是可选 feature
- ✅ 默认使用**自定义 TCP 协议**（length-prefixed JSON）
- ✅ 名字叫 "zenoh" 是因为最初想用 Eclipse Zenoh，但后来改用自定义实现

**依赖验证**（`crates/nexora-zenoh/Cargo.toml`）：
```toml
[features]
default = []
zenoh = ["dep:zenoh"]  # ← zenoh 是可选的！

[dependencies]
# 默认依赖（无 zenoh）
nexora-id = { path = "../nexora-id" }
serde = { workspace = true }
tokio = { workspace = true }
# ...

# 可选依赖
zenoh = { version = "1", optional = true }  # ← 不是默认启用的
```

**关键代码**（`crates/nexora-zenoh/src/tcp_transport.rs:1`）：
```rust
//! TCP transport layer for distributed graph operations.
//!
//! Replaces Zenoh with a simple, debuggable length-prefixed JSON protocol.
//!
//! Wire format:
//!   Handshake (4 bytes, sent once by the client immediately on connect):
//!     [0x4E, 0x58, major, minor]  — 'N','X' magic + wire version
//!   Request:  4B BE length | JSON (target_node: String, op: GraphOperation)
//!   Response: 4B BE length | JSON (Result<GraphResult, String>)
```

**设计目标**：
- Nexora 节点间的 graph operation 路由
- P2P 网络（不需要中心化的服务发现）
- 零外部依赖（默认模式）

**不能用于 RisingWave 的原因**：
- ❌ RisingWave 强依赖 **gRPC**（protobuf 协议）
- ❌ RisingWave 有自己的服务定义（`.proto` 文件）
- ❌ 协议完全不兼容（JSON vs protobuf）

### 4. nexora-rpc 的设计 - 为 RisingWave gRPC 优化

**关键事实**：
- ✅ **基于 tonic 0.12** - 标准的 Rust gRPC 框架
- ✅ 使用 protobuf 作为序列化格式
- ✅ 专门为 RisingWave 设计

**依赖验证**（`crates/nexora-rpc/Cargo.toml`）：
```toml
[dependencies]
tonic = "0.12"  # ← 真正的 gRPC 框架
async-trait = "0.1"
bytes = "1"
tokio = { version = "1", features = ["full"] }
```

**设计目标**：
- 提供标准 gRPC 服务器/客户端
- 支持 RisingWave 的 protobuf 消息
- Phase 4 将添加完整的 RisingWave service definitions

**Phase 2 接口**（`crates/nexora-rpc/src/server.rs`）：
```rust
#[async_trait]
pub trait RpcServer: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    fn local_addr(&self) -> Option<SocketAddr>;
    fn is_running(&self) -> bool;
}

// Phase 2: 简化实现（基本的 trait 和生命周期管理）
// Phase 4: 将添加 RisingWave 的 .proto service definitions
```

---

## 🏗️ 架构关系

```
Nexora Graph Cluster (现有)
├── nexora-raft ────────────► WAL 复制 (无 leader election, 自实现)
└── nexora-zenoh ───────────► Graph operation P2P 路由 (自定义 TCP, zenoh 可选)

RisingWave Integration (新增)
├── nexora-consensus ───────► RisingWave Meta HA (有 leader election, 基于 openraft)
└── nexora-rpc ─────────────► RisingWave 节点间 gRPC (基于 tonic)
```

**关键发现**：两套系统的需求不同，且现有实现并非基于标准库。

---

## 📊 依赖对比表

| Crate | 名字暗示 | 实际实现 | 外部依赖 |
|-------|---------|---------|---------|
| nexora-raft | 使用 Raft | ❌ 自实现 Raft 部分逻辑 | ❌ 无 openraft |
| nexora-consensus | 使用共识 | ✅ 完整 Raft (openraft) | ✅ openraft 0.9 |
| nexora-zenoh | 使用 Zenoh | ❌ 默认自定义 TCP | ❌ zenoh 是可选 feature |
| nexora-rpc | 使用 RPC | ✅ 标准 gRPC | ✅ tonic 0.12 |

**教训**：不能只看名字，要看实际依赖和实现！

---

## 🔄 未来可能的统一

**可能性 1**：Nexora 未来也使用 leader election
- 如果 Nexora 放弃 ShardMap，改用标准 Raft
- 那时可以合并到 nexora-consensus
- **但这是架构级别的重构，不在当前 scope**

**可能性 2**：RisingWave 支持自定义传输
- 如果 RisingWave 支持非 gRPC 传输
- 那时可以考虑使用 nexora-zenoh
- **但 RisingWave 目前强依赖 gRPC**

---

## ✅ 总结

| Crate | 服务对象 | 用途 | 基于标准库？ | 能否互换？ |
|-------|---------|------|------------|----------|
| nexora-raft | Nexora | WAL 复制（无 leader election） | ❌ 自实现 | ❌ |
| nexora-consensus | RisingWave | Meta HA（有 leader election） | ✅ openraft 0.9 | ❌ |
| nexora-zenoh | Nexora | P2P 路由（自定义协议） | ❌ 自定义 TCP | ❌ |
| nexora-rpc | RisingWave | gRPC 通信 | ✅ tonic 0.12 | ❌ |

**你的理解完全正确**：
1. ✅ **nexora-raft** 只是名字上有 raft，实际上**不基于 openraft**
2. ✅ **nexora-zenoh** 只是名字上有 zenoh，实际上**默认不使用 zenoh**（自定义 TCP）

**为什么需要新的实现？**

Phase 2 创建的 `nexora-consensus` 和 `nexora-rpc` 是**必需的**，因为：

1. RisingWave Meta **必须有** leader election
   - nexora-raft **没有** leader election ❌
   - nexora-consensus **基于 openraft**，有完整 Raft ✅

2. RisingWave **必须用** gRPC
   - nexora-zenoh 是**自定义 TCP**（JSON） ❌
   - nexora-rpc **基于 tonic**，标准 gRPC ✅

3. RisingWave 需要**生产级标准实现**
   - 现有实现都是**为 Nexora 定制的轻量级实现** ❌
   - 新实现基于**成熟的开源库**（openraft + tonic） ✅

**核心矛盾**：
- Nexora 选择了**轻量级、定制化**的实现（降低依赖、便于调试）
- RisingWave 需要**标准化、完整功能**的实现（生产级 HA）

这是 Phase 2 的**最小必要基础设施**。

---

**参考**：
- [RISINGWAVE_INTEGRATION_PLAN.md](RISINGWAVE_INTEGRATION_PLAN.md) - Phase 2 设计
- [nexora-raft/src/lib.rs:21](../crates/nexora-raft/src/lib.rs) - "No leader election" 说明
- [RISINGWAVE_PHASE2_REPORT.md](RISINGWAVE_PHASE2_REPORT.md) - 实施报告
