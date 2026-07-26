# 为什么需要两个 Raft 实现？

**问题**：Nexora 2 已经有 `nexora-raft` 和 `nexora-zenoh`，为什么 Phase 2 还要创建 `nexora-consensus` 和 `nexora-rpc`？

---

## 🎯 简短回答

**nexora-raft** 和 **nexora-consensus** 解决的是**不同的问题**：

| 特性 | nexora-raft | nexora-consensus |
|------|-------------|------------------|
| **用途** | Nexora 图数据库的 WAL 复制 | RisingWave Meta 节点的 HA |
| **Leader Election** | ❌ 无（使用 ShardMap + OwnerEpoch） | ✅ 有（完整 Raft） |
| **应用场景** | 分片数据复制 | 服务高可用 |
| **依赖** | 自实现 Raft 核心逻辑 | openraft 0.9 库 |

**nexora-zenoh** 和 **nexora-rpc** 也解决的是**不同的问题**：

| 特性 | nexora-zenoh | nexora-rpc |
|------|-------------|------------|
| **用途** | Nexora 节点间 P2P 路由 | RisingWave 节点间 gRPC |
| **协议** | Zenoh P2P / 自定义 TCP | gRPC (tonic) |
| **应用场景** | Graph operation 分布式路由 | RisingWave 内部 RPC |

---

## 📖 详细解释

### 1. nexora-raft 的设计 - 为 WAL 复制优化

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

**为什么不能用于 RisingWave？**
- ❌ RisingWave Meta 需要 **真正的 leader election**
- ❌ RisingWave 不知道 Nexora 的 ShardMap/OwnerEpoch
- ❌ RisingWave Meta 不是基于 WAL 的

### 2. nexora-consensus 的设计 - 为 RisingWave HA 优化

**设计目标**：
- 提供完整的 Raft leader election
- 实现 RisingWave 的 `ElectionClient` trait
- 支持 3-5 节点 Meta 集群 HA

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

**设计目标**：
- Nexora 节点间的 graph operation 路由
- P2P 网络（不需要中心化的服务发现）
- 支持多种传输（TCP, Zenoh, UDP）

**不能用于 RisingWave 的原因**：
- ❌ RisingWave 强依赖 **gRPC**（protobuf 协议）
- ❌ RisingWave 有自己的服务定义（`.proto` 文件）
- ❌ 协议不兼容

### 4. nexora-rpc 的设计 - 为 RisingWave gRPC 优化

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

// Phase 2: 简化实现
// Phase 4: 将添加 RisingWave 的 .proto service definitions
```

---

## 🏗️ 架构关系

```
Nexora Graph Cluster (现有)
├── nexora-raft ────────────► WAL 复制 (无 leader election)
└── nexora-zenoh ───────────► Graph operation P2P 路由

RisingWave Integration (新增)
├── nexora-consensus ───────► RisingWave Meta HA (有 leader election)
└── nexora-rpc ─────────────► RisingWave 节点间 gRPC
```

**关键点**：两套系统的需求不同，不能互相替代。

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

| Crate | 服务对象 | 用途 | 能否互换？ |
|-------|---------|------|----------|
| nexora-raft | Nexora | WAL 复制（无 leader election） | ❌ |
| nexora-consensus | RisingWave | Meta HA（有 leader election） | ❌ |
| nexora-zenoh | Nexora | P2P 路由（自定义协议） | ❌ |
| nexora-rpc | RisingWave | gRPC 通信 | ❌ |

**结论**：Phase 2 创建的 `nexora-consensus` 和 `nexora-rpc` 是**必需的**，因为：

1. RisingWave Meta **必须有** leader election（nexora-raft 没有）
2. RisingWave **必须用** gRPC（nexora-zenoh 不是）
3. 这是 RisingWave 集成的**最小必要基础设施**

---

**参考**：
- [RISINGWAVE_INTEGRATION_PLAN.md](RISINGWAVE_INTEGRATION_PLAN.md) - Phase 2 设计
- [nexora-raft/src/lib.rs:21](../crates/nexora-raft/src/lib.rs) - "No leader election" 说明
- [RISINGWAVE_PHASE2_REPORT.md](RISINGWAVE_PHASE2_REPORT.md) - 实施报告
