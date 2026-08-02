# P1-2: Raft 并行复制优化

**目标**: 将 Raft 日志复制延迟从 ~100ms 降低到 ~10ms（10倍提升）

**问题**: 当前实现在 `nexora-app/src/raft_handler.rs:278-294` 中串行遍历所有 followers

---

## 当前实现（串行）

```rust
// ❌ 串行复制：每个 peer 依次等待
for peer in &peers_for_bg {
    let target = TcpRaftTarget::new(peer.clone(), rpc_timeout);
    let result = replicator.replicate_to(&target, peer).await; // 阻塞等待
    if let Err(ref e) = result {
        // 错误处理
    }
}
```

**性能瓶颈**:
- 3 个 followers，每个 RPC 延迟 30ms
- 总延迟 = 30ms × 3 = **90ms**
- 最慢的 follower 阻塞整个循环

---

## 优化方案（并行）

```rust
// ✅ 并行复制：所有 peers 同时发送
let mut replication_tasks = Vec::new();

for peer in &peers_for_bg {
    let target = TcpRaftTarget::new(peer.clone(), rpc_timeout);
    let replicator_clone = replicator.clone();
    let peer_clone = peer.clone();
    
    let task = tokio::spawn(async move {
        let result = replicator_clone.replicate_to(&target, &peer_clone).await;
        (peer_clone, result)
    });
    
    replication_tasks.push(task);
}

// 等待所有任务完成
for task in replication_tasks {
    match task.await {
        Ok((peer, result)) => {
            if let Err(ref e) = result {
                if !matches!(e, ReplicationError::Connection(_)) {
                    tracing::warn!(
                        node = %raft_node_id,
                        peer = %peer,
                        error = %e,
                        "Raft replication failed"
                    );
                }
            }
        }
        Err(join_err) => {
            tracing::error!("Replication task panicked: {}", join_err);
        }
    }
}
```

**性能提升**:
- 3 个 followers，每个 RPC 延迟 30ms
- 总延迟 = max(30ms, 30ms, 30ms) = **30ms**
- 理论加速比 = 90ms / 30ms = **3倍**
- 实际加速取决于 follower 数量（N followers → N倍加速）

---

## 进一步优化：使用 FuturesUnordered

```rust
use futures::stream::{FuturesUnordered, StreamExt};

let mut replication_futures = FuturesUnordered::new();

for peer in &peers_for_bg {
    let target = TcpRaftTarget::new(peer.clone(), rpc_timeout);
    let replicator_clone = replicator.clone();
    let peer_clone = peer.clone();
    let node_id_clone = raft_node_id.clone();
    
    replication_futures.push(async move {
        let result = replicator_clone.replicate_to(&target, &peer_clone).await;
        (peer_clone, result, node_id_clone)
    });
}

// 并发执行，完成一个处理一个（无需等待全部）
while let Some((peer, result, node_id)) = replication_futures.next().await {
    if let Err(ref e) = result {
        if !matches!(e, ReplicationError::Connection(_)) {
            tracing::warn!(
                node = %node_id,
                peer = %peer,
                error = %e,
                "Raft replication failed"
            );
        }
    }
}
```

**优势**:
- 流式处理，不需要 `Vec` 缓冲
- 第一个完成的 follower 立即处理
- 更好的内存局部性

---

## 实现步骤

### Step 1: 修改 `nexora-app/src/raft_handler.rs`

**位置**: 第 278-294 行

**修改前**:
```rust
for peer in &peers_for_bg {
    let target = TcpRaftTarget::new(peer.clone(), rpc_timeout);
    let result = replicator.replicate_to(&target, peer).await;
    // ...
}
```

**修改后**:
```rust
use futures::stream::{FuturesUnordered, StreamExt};

let mut replication_futures = FuturesUnordered::new();

for peer in &peers_for_bg {
    let target = TcpRaftTarget::new(peer.clone(), rpc_timeout);
    let replicator_clone = Arc::clone(&replicator);
    let peer_clone = peer.clone();
    let node_id_clone = raft_node_id.clone();
    
    replication_futures.push(async move {
        let result = replicator_clone.replicate_to(&target, &peer_clone).await;
        (peer_clone, result, node_id_clone)
    });
}

while let Some((peer, result, node_id)) = replication_futures.next().await {
    if let Err(ref e) = result {
        if !matches!(e, ReplicationError::Connection(_)) {
            tracing::warn!(
                node = %node_id,
                peer = %peer,
                error = %e,
                "Raft replication failed"
            );
        }
    }
}
```

### Step 2: 添加依赖

**文件**: `crates/nexora-app/Cargo.toml`

```toml
[dependencies]
# 已有依赖...
futures = "0.3"  # 添加这一行
```

### Step 3: 验证 RaftLogReplicator 是否 Clone

**检查**: `crates/nexora-raft/src/lib.rs`

```rust
pub struct RaftLogReplicator {
    // 需要确保所有字段都是 Arc/Mutex 包裹的，支持跨任务共享
}
```

如果不支持 Clone，需要将 `replicator` 包装为 `Arc<RaftLogReplicator>`。

### Step 4: 性能测试

**基准测试脚本**:
```bash
# 测试串行复制延迟
cargo bench --bench raft_replication_serial

# 测试并行复制延迟
cargo bench --bench raft_replication_parallel

# 预期结果：
# Serial:   90ms (3 × 30ms)
# Parallel: 30ms (max of 30ms)
# Speedup:  3x
```

---

## 性能预测

### 3 节点集群（1 leader + 2 followers）

| 场景 | 串行延迟 | 并行延迟 | 加速比 |
|------|----------|----------|--------|
| 理想网络（10ms RTT） | 20ms | 10ms | 2x |
| 正常网络（30ms RTT） | 60ms | 30ms | 2x |
| 慢网络（50ms RTT） | 100ms | 50ms | 2x |

### 5 节点集群（1 leader + 4 followers）

| 场景 | 串行延迟 | 并行延迟 | 加速比 |
|------|----------|----------|--------|
| 理想网络（10ms RTT） | 40ms | 10ms | 4x |
| 正常网络（30ms RTT） | 120ms | 30ms | 4x |
| 慢网络（50ms RTT） | 200ms | 50ms | 4x |

**关键洞察**:
- 加速比 = follower 数量（N-1，其中 N = 总节点数）
- 对于生产环境的 5 节点集群，可实现 **4倍加速**
- 对于典型的 3 节点集群，可实现 **2倍加速**

---

## Quorum 逻辑优化

### 当前实现

```rust
// 在 RaftLogReplicator::check_commit_progress() 中
// 等待所有 followers 响应后才检查 quorum
```

### 优化：早期终止

```rust
// 一旦 quorum 达到，立即返回，无需等待所有 followers
pub async fn check_commit_with_early_exit(&self, seq_no: u64) -> QuorumResult {
    let quorum_size = self.config.quorum_size;
    let mut acked = 1; // leader 自己
    let mut total = 1;
    
    let followers = self.followers.lock().await;
    total += followers.len();
    
    for progress in followers.values() {
        if progress.match_seq >= seq_no {
            acked += 1;
            
            // 早期终止：quorum 达到即返回
            if acked >= quorum_size {
                return QuorumResult::Committed { acked, total };
            }
        }
    }
    
    QuorumResult::NotCommitted {
        acked,
        required: quorum_size,
    }
}
```

**优势**:
- 3 节点集群，quorum = 2（leader + 1 follower）
- 第一个 follower 响应后立即提交，无需等待第二个
- 进一步降低 P99 延迟

---

## 风险与缓解

### 风险 1: 过多并发连接

**问题**: 100 个 followers 同时发起 TCP 连接可能耗尽端口

**缓解**:
```rust
// 限制并发数
use tokio::sync::Semaphore;

let semaphore = Arc::new(Semaphore::new(10)); // 最多 10 个并发

for peer in &peers_for_bg {
    let permit = semaphore.clone().acquire_owned().await.unwrap();
    let task = tokio::spawn(async move {
        let _permit = permit; // 持有 permit 直到任务完成
        replicator.replicate_to(&target, &peer).await
    });
    replication_futures.push(task);
}
```

### 风险 2: 一个 follower 永久挂起

**问题**: `replicate_to` 内部已有超时，但如果超时过长（5s），会拖慢整体

**缓解**: 已在 `RaftConfig::rpc_timeout` 中配置，默认 5s 可调低到 1s

### 风险 3: Clone 开销

**问题**: `Arc::clone(&replicator)` 每次循环都增加引用计数

**缓解**: Arc clone 开销极小（原子加 1），相比网络延迟可忽略

---

## 测试计划

### 单元测试

```rust
#[tokio::test]
async fn test_parallel_replication_faster_than_serial() {
    let config = RaftConfig {
        quorum_size: 2,
        total_nodes: 3,
        rpc_timeout: Duration::from_millis(30),
        ..Default::default()
    };
    
    let replicator = Arc::new(RaftLogReplicator::new(config));
    replicator.register_follower("f1", 0, 0).await;
    replicator.register_follower("f2", 0, 0).await;
    
    let start = Instant::now();
    
    // 并行复制
    let mut futures = FuturesUnordered::new();
    for follower in &["f1", "f2"] {
        let r = Arc::clone(&replicator);
        let f = follower.to_string();
        futures.push(async move {
            let target = MockTarget::new(Duration::from_millis(30));
            r.replicate_to(&target, &f).await
        });
    }
    
    while let Some(_) = futures.next().await {}
    
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_millis(50)); // 应该 ~30ms，不是 60ms
}
```

### 集成测试

```bash
# 启动 3 节点集群
./scripts/start-cluster.sh --nodes 3

# 发送 1000 个写请求
./scripts/load-test.sh --ops 1000 --concurrency 10

# 测量 P50/P95/P99 延迟
./scripts/measure-latency.sh

# 预期：
# P50: 30ms → 15ms
# P95: 80ms → 40ms
# P99: 150ms → 60ms
```

---

## 回滚计划

如果并行复制导致问题，可快速回滚：

```rust
// 添加 feature flag 控制
#[cfg(feature = "parallel-replication")]
{
    // 并行逻辑
}
#[cfg(not(feature = "parallel-replication"))]
{
    // 原串行逻辑
}
```

**Cargo.toml**:
```toml
[features]
default = ["parallel-replication"]
parallel-replication = []
```

**回滚命令**:
```bash
cargo build --release --no-default-features
```

---

## 交付物

- [x] 设计文档（本文档）
- [ ] 代码修改（`nexora-app/src/raft_handler.rs`）
- [ ] 单元测试（`nexora-raft/src/lib.rs` 添加测试）
- [ ] 基准测试（`benches/raft_replication.rs`）
- [ ] 性能验证报告

---

**状态**: ⏳ 设计完成，待实现  
**预期工作量**: 2-3 小时  
**预期性能提升**: 2-4倍延迟降低（取决于集群规模）
