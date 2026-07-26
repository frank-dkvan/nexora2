# Nexora 2 开发方案 - 完整实施计划

## Context（背景）

基于前面的深入讨论，我们确定了以下关键决策：

1. **仓库策略**：采用方案A - 在单一nexora2仓库中集中开发
2. **RisingWave集成**：使用Git Subtree将RisingWave作为内部模块（vendor/risingwave）
3. **Raft HA实现**：开发嵌入式Raft，无需外部PostgreSQL/etcd依赖
4. **代码复用**：通过共享基础库（crates/）避免重复开发
5. **架构定位**：Nexora是完整平台，RisingWave是其Event Stream Module

### 核心架构

```
nexora2/（唯一主仓库）
├── vendor/risingwave/          # Git Subtree: RisingWave源码
├── crates/                     # 共享基础库（Raft/RPC/Protocol）
├── extensions/meta_raft/       # RisingWave Raft HA增强
├── src/                        # Nexora核心模块
│   ├── event_stream/           # 封装RisingWave
│   ├── semantic_event/         # 语义事件引擎
│   ├── graph_mutation/         # 图变化引擎
│   ├── graph_engine/           # 分布式图引擎
│   └── temporal_graph/         # 时态图引擎
└── patches/                    # 最小化补丁
```

---

## 实施计划

### Phase 1: 仓库初始化与基础架构（第1-2周）

#### 目标
建立nexora2仓库基础结构，集成RisingWave，搭建开发环境

#### 关键文件

**1. 仓库初始化脚本**
- `scripts/init-repo.sh`
  - 初始化Git仓库
  - 添加RisingWave作为Subtree
  - 创建目录结构
  - 设置Git hooks

**2. 工作空间配置**
- `Cargo.toml`
  - 定义workspace成员
  - 统一依赖版本
  - 配置features（raft-ha, nexora等）

**3. 构建工具**
- `Makefile`
  - build: 统一构建命令
  - test: 运行所有测试
  - sync: 同步RisingWave上游
  - docker: 构建Docker镜像

**4. 自动化脚本**
- `scripts/sync-risingwave.sh` - 升级RisingWave版本
- `scripts/apply-patches.sh` - 应用补丁文件
- `scripts/check-style.sh` - 代码风格检查

#### 关键操作步骤

```bash
# 1. 创建仓库
mkdir nexora2 && cd nexora2
git init

# 2. 添加RisingWave
git remote add risingwave-upstream https://github.com/risingwavelabs/risingwave.git
git subtree add --prefix=vendor/risingwave risingwave-upstream v3.0.2 --squash

# 3. 创建目录结构
mkdir -p crates/{consensus,rpc,storage,protocol}
mkdir -p extensions/meta_raft
mkdir -p src/{event_stream,semantic_event,graph_mutation,graph_engine,temporal_graph,api}
mkdir -p patches scripts docker web docs

# 4. 初始化Cargo工作空间
# 创建Cargo.toml（详见下方）

# 5. 首次提交
git add .
git commit -m "Initial commit: Nexora 2 Platform with embedded RisingWave v3.0.2"
```

---

### Phase 2: 共享基础库开发（第3-4周）

#### 目标
开发可复用的分布式基础能力，为RisingWave和Nexora提供统一抽象

#### 2.1 共识协议抽象（crates/consensus）

**核心接口**
```rust
// crates/consensus/src/lib.rs
#[async_trait::async_trait]
pub trait ConsensusClient: Send + Sync + 'static {
    async fn init(&self, peers: Vec<String>) -> Result<()>;
    async fn run(&self) -> Result<()>;
    fn is_leader(&self) -> bool;
    fn subscribe_leader_change(&self) -> Receiver<LeaderChange>;
    async fn commit(&self, data: Bytes) -> Result<LogIndex>;
    async fn members(&self) -> Result<Vec<Member>>;
}
```

**实现类**
- `RaftConsensusClient` - 基于openraft的生产实现
- `MemoryConsensusClient` - 单节点测试实现

**依赖**
- openraft = "0.9"
- tokio, serde, bytes

#### 2.2 RPC通信抽象（crates/rpc）

**核心接口**
```rust
#[async_trait::async_trait]
pub trait RpcServer: Send + Sync {
    async fn start(&self, addr: SocketAddr) -> Result<()>;
    async fn register_service(&self, service: Arc<dyn RpcService>);
}

#[async_trait::async_trait]
pub trait RpcClient: Send + Sync {
    async fn call(&self, endpoint: &str, request: Bytes) -> Result<Bytes>;
}
```

**实现类**
- `TonicRpcServer/Client` - 基于tonic的gRPC实现

#### 2.3 协议层抽象（crates/protocol）

**PostgreSQL协议服务器**
```rust
// crates/protocol/src/pgwire/mod.rs
pub trait QueryHandler: Send + Sync {
    async fn handle_query(&self, sql: &str) -> Result<QueryResult>;
}

pub struct PgWireServer {
    handler: Arc<dyn QueryHandler>,
}
```

**依赖**
- pgwire或从RisingWave提取相关代码

#### 2.4 存储抽象（crates/storage）

```rust
pub trait StorageEngine: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;
    async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}
```

---

### Phase 3: RisingWave Raft HA开发（第5-6周）

#### 目标
实现RisingWave的嵌入式Raft选举，替代外部PostgreSQL依赖

#### 关键文件

**1. Raft Election Client实现**
- `extensions/meta_raft/src/lib.rs`
- `extensions/meta_raft/src/client.rs`
- `extensions/meta_raft/src/storage.rs`
- `extensions/meta_raft/src/network.rs`

**核心逻辑**
```rust
// extensions/meta_raft/src/client.rs
use nexora_consensus::{ConsensusClient, RaftConsensusClient};
use risingwave_meta::ElectionClient;

pub struct RisingWaveRaftElectionClient {
    consensus: Arc<dyn ConsensusClient>,
    is_leader_sender: broadcast::Sender<bool>,
}

#[async_trait::async_trait]
impl ElectionClient for RisingWaveRaftElectionClient {
    async fn init(&self) -> MetaResult<()> {
        self.consensus.init(self.peers.clone()).await?;
        Ok(())
    }
    
    fn is_leader(&self) -> bool {
        self.consensus.is_leader()
    }
    
    async fn run_once(&self, ttl: i64, stop: Receiver<()>) -> MetaResult<()> {
        // 启动Raft共识
        let mut leader_rx = self.consensus.subscribe_leader_change();
        
        tokio::spawn(async move {
            self.consensus.run().await
        });
        
        // 监听Leader变化
        loop {
            tokio::select! {
                Ok(change) = leader_rx.recv() => {
                    let is_leader = change.new_leader == self.id;
                    self.is_leader_sender.send(is_leader)?;
                }
                _ = stop.changed() => return Ok(()),
            }
        }
    }
    
    // ... 其他方法实现
}
```

**2. 补丁文件**
- `patches/001-enable-external-election.patch`
  - 修改 `vendor/risingwave/src/meta/src/lib.rs`
  - 添加 `MetaStoreBackend::External` 变体
  - 修改 `vendor/risingwave/src/meta/node/src/server.rs`
  - 添加外部election插件加载逻辑

补丁示例：
```diff
diff --git a/vendor/risingwave/src/meta/src/lib.rs b/vendor/risingwave/src/meta/src/lib.rs
@@ -56,6 +56,10 @@ pub enum MetaStoreBackend {
     Sql {
         endpoint: String,
         config: MetaStoreConfig,
     },
+    #[cfg(feature = "raft-ha")]
+    External {
+        plugin: String,
+        config: String,
+    },
 }
```

**3. 集成测试**
- `extensions/meta_raft/tests/integration_test.rs`
  - 3节点集群启动测试
  - Leader选举测试
  - 节点故障恢复测试
  - 网络分区测试

---

### Phase 4: Nexora核心引擎开发（第7-12周）

#### 4.1 Event Stream Module（第7周）

封装RisingWave，提供统一的事件流接口

**关键文件**
- `src/event_stream/src/lib.rs`
- `src/event_stream/src/rw_wrapper.rs`
- `src/event_stream/src/materialized_view.rs`

**核心功能**
```rust
pub struct EventStreamModule {
    meta_handle: JoinHandle<()>,
    frontend_handle: JoinHandle<()>,
    event_tx: broadcast::Sender<CloudEvent>,
}

impl EventStreamModule {
    // 启动内嵌的RisingWave Meta/Frontend节点
    pub async fn new(config: EventStreamConfig) -> Result<Self>;
    
    // 创建物化视图
    pub async fn create_materialized_view(&self, name: &str, sql: &str) -> Result<()>;
    
    // 查询物化视图
    pub async fn query_materialized_view(&self, view: &str) -> Result<Vec<CloudEvent>>;
    
    // 订阅事件流
    pub async fn subscribe(&self) -> Receiver<CloudEvent>;
}
```

#### 4.2 Semantic Event Engine（第8周）

**关键文件**
- `src/semantic_event/src/lib.rs`
- `src/semantic_event/src/ontology.rs`
- `src/semantic_event/src/mapper.rs`

**核心功能**
```rust
pub struct SemanticEventEngine {
    ontology: EventOntology,
}

impl SemanticEventEngine {
    // CloudEvent → BusinessEvent 转换
    pub fn transform(&self, event: CloudEvent) -> Result<BusinessEvent>;
}
```

#### 4.3 Graph Mutation Engine（第9周）

**关键文件**
- `src/graph_mutation/src/lib.rs`
- `src/graph_mutation/src/executor.rs`
- `src/graph_mutation/src/rules/`

**核心功能**
```rust
pub struct GraphMutationEngine {
    graph_store: Arc<dyn GraphStore>,
}

impl GraphMutationEngine {
    // BusinessEvent → GraphMutationEvent
    pub fn plan(&self, event: BusinessEvent) -> Result<GraphMutationEvent>;
    
    // 执行图变化
    pub async fn apply(&self, mutation: GraphMutationEvent) -> Result<()>;
}
```

#### 4.4 Distributed Graph Engine（第10-11周）

使用nexora-consensus实现分布式图引擎

**关键文件**
- `src/graph_engine/src/lib.rs`
- `src/graph_engine/src/cluster.rs`
- `src/graph_engine/src/replication.rs`

**核心功能**
```rust
pub struct GraphClusterManager {
    consensus: Arc<dyn ConsensusClient>,
    graph_store: Arc<dyn GraphStore>,
}

impl GraphClusterManager {
    // 分布式图变更（需要共识）
    pub async fn apply_graph_mutation(&self, mutation: GraphMutation) -> Result<()>;
    
    // 处理Leader变化
    pub async fn handle_leader_change(&self);
}
```

#### 4.5 Temporal Graph Engine（第12周）

**关键文件**
- `src/temporal_graph/src/lib.rs`
- `src/temporal_graph/src/bitemporal.rs`
- `src/temporal_graph/src/time_travel.rs`

**核心功能**
```rust
pub struct TemporalGraphEngine {
    graph_store: Arc<dyn GraphStore>,
}

impl TemporalGraphEngine {
    // 时间旅行查询
    pub async fn query_as_of(&self, cypher: &str, timestamp: DateTime) -> Result<QueryResult>;
    
    // 历史版本查询
    pub async fn query_versions(&self, entity_id: &str) -> Result<Vec<Version>>;
}
```

---

### Phase 5: 集成与测试（第13-14周）

#### 5.1 主程序集成

**关键文件**
- `src/main.rs` - Nexora主入口
- `src/config.rs` - 配置管理

**启动流程**
```rust
#[tokio::main]
async fn main() -> Result<()> {
    // 1. 初始化Event Stream Module (内嵌RisingWave)
    let event_stream = EventStreamModule::new(config.risingwave).await?;
    
    // 2. 初始化Semantic Event Engine
    let semantic_engine = SemanticEventEngine::new(config.ontology_path)?;
    
    // 3. 初始化Graph Storage
    let graph_store = KuzuStore::open(&config.graph_data_dir)?;
    
    // 4. 初始化Temporal Graph Engine
    let temporal_engine = TemporalGraphEngine::new(graph_store.clone());
    
    // 5. 初始化Graph Mutation Engine
    let mutation_engine = GraphMutationEngine::new(graph_store.clone());
    
    // 6. 初始化Distributed Graph Engine
    let graph_cluster = GraphClusterManager::new(config.graph_cluster).await?;
    
    // 7. 启动事件处理管道
    tokio::spawn(event_pipeline(event_stream, semantic_engine, mutation_engine));
    
    // 8. 启动API服务
    let api_server = ApiServer::new(temporal_engine);
    api_server.serve("0.0.0.0:8000").await?;
    
    Ok(())
}
```

#### 5.2 端到端测试

**测试场景：货运追踪系统**

```bash
# 1. 启动Nexora（包含内嵌RisingWave）
cargo run --release --features raft-ha -- --config nexora.toml

# 2. 在RisingWave中创建CDC源
psql -h localhost -p 4566 -d dev <<EOF
CREATE SOURCE cargo_events WITH (
    connector = 'kafka',
    topic = 'logistics.cargo_status',
    properties.bootstrap.server = 'kafka:9092'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW cargo_transitions AS
SELECT cargo_id, status, location, event_time
FROM cargo_events;
EOF

# 3. 注入测试数据
kafka-console-producer --topic logistics.cargo_status <<EOF
{"cargo_id":"AWB001","status":"DEPARTED","location":"PVG"}
EOF

# 4. 验证Nexora图中的节点
curl http://localhost:8000/api/graph/query \
  -d '{"cypher": "MATCH (c:Cargo {id: \"AWB001\"}) RETURN c"}'

# 预期返回：包含Cargo节点及属性
```

#### 5.3 性能测试

- 事件吞吐量测试：10K events/s
- Raft选举延迟：<5秒
- 图查询性能：复杂查询<100ms

---

### Phase 6: Docker化与部署（第15周）

#### 6.1 Docker镜像

**Dockerfile**
```dockerfile
FROM rust:1.75 AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --features raft-ha

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /build/target/release/nexora /usr/local/bin/
EXPOSE 4566 5432 8000
ENTRYPOINT ["/usr/local/bin/nexora"]
```

#### 6.2 部署配置

**docker-compose.yml - 3节点HA部署**
```yaml
services:
  nexora-1:
    image: frank-dkvan/nexora2:latest
    command: --node-id 1 --peers nexora-1:5690,nexora-2:5690,nexora-3:5690
    volumes:
      - nexora-1:/data
    ports:
      - "4566:4566"  # RisingWave SQL
      - "5432:5432"  # Nexora Cypher/SQL
      - "8001:8000"  # Nexora API
    networks:
      - nexora-net

  nexora-2:
    image: frank-dkvan/nexora2:latest
    command: --node-id 2 --peers nexora-1:5690,nexora-2:5690,nexora-3:5690
    volumes:
      - nexora-2:/data
    ports:
      - "8002:8000"
    networks:
      - nexora-net

  nexora-3:
    image: frank-dkvan/nexora2:latest
    command: --node-id 3 --peers nexora-1:5690,nexora-2:5690,nexora-3:5690
    volumes:
      - nexora-3:/data
    ports:
      - "8003:8000"
    networks:
      - nexora-net

  kafka:
    image: redpandadata/redpanda:latest
    ports:
      - "9092:9092"
    networks:
      - nexora-net

  minio:
    image: minio/minio:latest
    command: server /data
    ports:
      - "9000:9000"
    networks:
      - nexora-net

volumes:
  nexora-1:
  nexora-2:
  nexora-3:

networks:
  nexora-net:
```

---

## 验证计划

### 构建验证

```bash
# 1. 完整构建
make build

# 2. 运行测试
make test

# 3. 检查代码风格
make check
```

### 功能验证

**1. RisingWave Raft HA**
```bash
# 启动3节点集群
docker-compose up -d

# 验证Leader选举
curl http://localhost:8001/api/cluster/status

# 杀死Leader，验证自动切换
docker stop nexora-1
sleep 10
curl http://localhost:8002/api/cluster/status
```

**2. Event → Graph 数据流**
```bash
# 注入事件
kafka-console-producer --topic test.events <<EOF
{"type":"cargo.arrived","cargo_id":"AWB001"}
EOF

# 验证图节点
curl http://localhost:8001/api/graph/query \
  -d '{"cypher": "MATCH (c:Cargo {id: \"AWB001\"}) RETURN c"}'
```

**3. Temporal Query**
```bash
# 时间旅行查询
curl http://localhost:8001/api/graph/query \
  -d '{
    "cypher": "MATCH (c:Cargo {id: \"AWB001\"}) AS OF TIMESTAMP \"2024-01-01T00:00:00Z\" RETURN c"
  }'
```

---

## 关键依赖项

### 外部依赖（Cargo.toml）

```toml
[workspace.dependencies]
# Raft共识
openraft = "0.9"

# 异步运行时
tokio = { version = "1.35", features = ["full"] }

# RPC框架
tonic = "0.11"
prost = "0.12"

# 序列化
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"

# 图存储
kuzu = "0.1"

# 数据库客户端
tokio-postgres = "0.7"

# 事件标准
cloudevents-sdk = "0.7"

# 日志
tracing = "0.1"
tracing-subscriber = "0.3"

# 错误处理
anyhow = "1.0"
thiserror = "1.0"
```

### 内部依赖

```toml
# Nexora共享基础库
nexora-consensus = { path = "crates/consensus" }
nexora-rpc = { path = "crates/rpc" }
nexora-storage = { path = "crates/storage" }
nexora-protocol = { path = "crates/protocol" }

# RisingWave（Subtree）
risingwave_meta = { path = "vendor/risingwave/src/meta" }
risingwave_frontend = { path = "vendor/risingwave/src/frontend" }
risingwave_stream = { path = "vendor/risingwave/src/stream" }
```

---

## 风险与缓解

### 风险1：RisingWave内部API变化
**影响**：升级RisingWave时补丁可能失效
**缓解**：
- 最小化补丁（<100行）
- 使用Feature Gate隔离
- 定期同步上游（每月一次）

### 风险2：openraft性能不足
**影响**：Raft选举延迟过高
**缓解**：
- 预留备选方案（etcd-client）
- 性能测试（第5周）
- 参数调优

### 风险3：Kuzu图存储能力受限
**影响**：时态查询性能差
**缓解**：
- 抽象GraphStore接口
- 预留Nebula Graph集成
- 第10周验证性能

---

## 里程碑

| 时间 | 里程碑 | 交付物 |
|-----|-------|--------|
| Week 2 | 仓库初始化完成 | nexora2仓库，集成RisingWave |
| Week 4 | 共享库完成 | crates/consensus, rpc, protocol, storage |
| Week 6 | Raft HA完成 | RisingWave 3节点无外部依赖HA |
| Week 9 | 核心引擎完成 | Event Stream + Semantic Event + Graph Mutation |
| Week 12 | 图引擎完成 | Distributed Graph + Temporal Graph |
| Week 14 | 集成测试通过 | 端到端货运追踪演示 |
| Week 15 | Docker发布 | Docker镜像，部署文档 |

---

## 资源需求

### 开发团队
- 后端工程师 x 3（Rust）
- 系统架构师 x 1
- 测试工程师 x 1

### 基础设施
- 开发环境：3台服务器（4核16G）
- 测试环境：3台服务器（8核32G）
- CI/CD：GitHub Actions

### 预算估算
- 人力成本：5人 x 15周 = 75人周
- 基础设施：$500/月 x 4个月 = $2,000
- 总预算：~$150,000（按$2,000/人周）

---

## 总结

**核心策略：在nexora2单一仓库中推进所有工作**

优势：
- ✅ 架构清晰，职责明确
- ✅ RisingWave通过Subtree管理，易于升级
- ✅ 共享基础库避免重复开发
- ✅ 统一构建、测试、部署
- ✅ 代码复用最大化（Raft/RPC/Protocol）

实施路径：
1. Week 1-2: 初始化仓库，集成RisingWave
2. Week 3-4: 开发共享基础库
3. Week 5-6: RisingWave Raft HA
4. Week 7-12: Nexora核心引擎
5. Week 13-15: 集成测试与Docker化

最终交付：
- 单一Docker镜像包含完整平台
- 3节点HA部署，无外部依赖
- Event → Dynamic Graph → AI完整数据流
