# RisingWave 与 Nexora PG-Wire 架构说明

**日期**: 2026-07-27  
**版本**: 1.0

---

## 问题

**用户提问**: PG-wire 数据访问操作，是怎么统一处理的？我记得 RisingWave 和 Nexora 分别都支持的。

---

## 简短回答

**RisingWave 和 Nexora 的 PG-Wire 是完全独立的两个实现，没有统一处理**。

它们各自监听不同的端口，服务不同的用途：

| 组件 | 端口 | 用途 | 后端 |
|------|------|------|------|
| **Nexora PG-Wire** | 5432 (可配置) | 访问 Nexora 图数据库 | nexora-core (RocksDB) |
| **RisingWave Frontend** | 4566 (默认) | 访问 RisingWave 流处理 | RisingWave Meta + Compute |

---

## 详细架构

### 1. Nexora PG-Wire 服务器

**模块**: `crates/nexora-pgwire/`

**职责**: 
- 提供 PostgreSQL 协议访问 **Nexora 图数据库**
- 将 SQL 翻译为 Cypher 查询
- 执行图查询和物化视图查询

**架构**:
```
psql / 其他 PG 客户端
    ↓ (PostgreSQL Wire Protocol)
:5432 nexora-pgwire 服务器
    ↓
nexora-sql (SQL → Cypher 翻译)
    ↓
nexora-cypher (Cypher 执行器)
    ↓
nexora-core::GraphService (图引擎)
    ↓
RocksDB / Event Log
```

**实现细节**:
```rust
// crates/nexora-pgwire/src/lib.rs
pub struct PgAppState {
    pub graph: Arc<nexora_core::GraphService>,         // 图引擎
    pub mv_manager: Arc<MaterializedViewManager>,       // MV 管理器
    pub sq_manager: Option<Arc<StandingQueryManager>>,  // SQ 管理器
    pub router: Option<Arc<HybridRouter>>,              // 分布式路由
    pub event_store: Option<Arc<EventLogStore>>,        // 事件存储
    // ... 其他字段
}
```

**启动代码** (`crates/nexora-app/src/main.rs:3020`):
```rust
let server = nexora_pgwire::spawn_pg_server_with_router(
    graph.clone(),           // Nexora 图引擎
    mv_manager.clone(),      // Nexora MV 管理器
    Some(sq_manager.clone()),
    app_router.clone(),
    app_replication_progress.clone(),
    query_pool_for_pg,
    #[cfg(feature = "event-first")]
    event_store.clone(),
    pg_config,
).await?;
```

**特性**:
- ✅ 支持 SQL → Cypher 翻译
- ✅ 支持图查询（节点、边、路径）
- ✅ 支持 Nexora 物化视图（基于图的 MV）
- ✅ 支持分布式查询路由（集群模式）
- ✅ 支持事件表查询（event-first 特性）
- ✅ Trust / SCRAM-SHA-256 认证
- ✅ TLS 支持

### 2. RisingWave Frontend (PG-Wire)

**组件**: RisingWave 自带的 Frontend 节点

**职责**:
- 提供 PostgreSQL 协议访问 **RisingWave 流处理引擎**
- 执行 RisingWave DDL（CREATE SOURCE, CREATE MATERIALIZED VIEW）
- 查询 RisingWave 物化视图

**架构**:
```
psql / 其他 PG 客户端
    ↓ (PostgreSQL Wire Protocol)
:4566 RisingWave Frontend 节点
    ↓
RisingWave SQL Parser & Planner
    ↓
RisingWave Meta Node (元数据 + Raft)
    ↓
RisingWave Compute Node (流处理)
    ↓
RisingWave State Store (内存 / etcd / S3)
```

**Nexora 如何启动 RisingWave**:

**Phase 7 (单节点)** - `crates/nexora-risingwave/src/embedded_process.rs`:
```rust
pub struct EmbeddedRisingWave {
    process: Option<Child>,  // RisingWave 进程句柄
    // ...
}

// 启动 RisingWave standalone 进程
Command::new("risingwave")
    .arg("standalone")
    .arg("--meta-opts=--listen-addr 127.0.0.1:5690")
    .arg("--frontend-opts=--listen-addr 127.0.0.1:4566")
    .spawn()?
```

**Phase 8 (集群)** - `crates/nexora-risingwave/src/distributed.rs`:
```rust
pub struct DistributedEmbeddedRisingWave {
    meta_nodes: Vec<EmbeddedProcess>,     // 3 个 Meta 节点
    frontend: EmbeddedProcess,             // 1 个 Frontend 节点
    compute_nodes: Vec<EmbeddedProcess>,   // N 个 Compute 节点
}

// 分别启动多个进程
// Meta-1: risingwave meta-node --listen-addr 127.0.0.1:5690
// Meta-2: risingwave meta-node --listen-addr 127.0.0.1:5692 --join 127.0.0.1:5690
// Meta-3: risingwave meta-node --listen-addr 127.0.0.1:5694 --join 127.0.0.1:5690
// Frontend: risingwave frontend-node --listen-addr 127.0.0.1:4566 --meta-addr 127.0.0.1:5690,127.0.0.1:5692,127.0.0.1:5694
// Compute: risingwave compute-node --listen-addr 127.0.0.1:5688 --meta-addr 127.0.0.1:5690,127.0.0.1:5692,127.0.0.1:5694
```

**特性**:
- ✅ 原生 PostgreSQL 协议支持
- ✅ 标准 SQL 流处理语法
- ✅ 复杂 SQL 转换（JOIN、聚合、窗口函数）
- ✅ 多流时态 JOIN
- ✅ Kafka/Kinesis/Pulsar connector
- ⚠️ **完全独立于 Nexora PG-Wire**

---

## 两个 PG-Wire 的关系

### 完全独立，无统一处理

```
┌─────────────────────────────────────────────────────────┐
│                   Nexora App                             │
│                                                          │
│  ┌─────────────────────┐    ┌──────────────────────┐   │
│  │  nexora-pgwire      │    │  RisingWave Frontend │   │
│  │  (Crate)            │    │  (External Process)  │   │
│  │                     │    │                      │   │
│  │  Port: 5432         │    │  Port: 4566          │   │
│  │  Backend: Nexora    │    │  Backend: RisingWave │   │
│  │  Graph Engine       │    │  Stream Engine       │   │
│  └──────────┬──────────┘    └──────────┬───────────┘   │
│             │                           │               │
│             v                           v               │
│  ┌──────────────────┐       ┌─────────────────────┐    │
│  │ nexora-core      │       │ RisingWave          │    │
│  │ GraphService     │       │ Meta + Compute      │    │
│  │ (RocksDB)        │       │ (Raft + State)      │    │
│  └──────────────────┘       └─────────────────────┘    │
└─────────────────────────────────────────────────────────┘

客户端连接:
- psql -h localhost -p 5432 → Nexora Graph (图查询)
- psql -h localhost -p 4566 → RisingWave (流处理)
```

### 为什么要两个独立的 PG-Wire？

#### Nexora PG-Wire 的目的
1. **图查询访问**: 让 psql 客户端能执行 Cypher 风格的图查询
2. **SQL 兼容层**: SQL → Cypher 翻译，降低学习曲线
3. **标准工具支持**: 任何 PostgreSQL 客户端都能访问 Nexora

#### RisingWave Frontend 的目的
1. **流处理 SQL**: 执行标准 SQL 流处理（CREATE SOURCE, CREATE MATERIALIZED VIEW）
2. **复杂转换**: 在数据进入图之前进行 SQL 转换、聚合、JOIN
3. **RisingWave 生态**: 利用 RisingWave 的 connector 和优化器

---

## 数据流：RisingWave → Nexora

虽然两个 PG-Wire 是独立的，但数据可以从 RisingWave 流向 Nexora：

### Path B: 高级流处理路径

```
Kafka Topic
    ↓
RisingWave CREATE SOURCE (via psql :4566)
    ↓
RisingWave CREATE MATERIALIZED VIEW (SQL 转换、聚合)
    ↓
RisingWave Compute Node 计算
    ↓
Nexora Event Log Sink (crates/nexora-risingwave/src/event_sink.rs)
    ↓
nexora-eventlog::EventLogStore (Iceberg)
    ↓
nexora-core::GraphService (图引擎)
    ↓
查询通过 Nexora PG-Wire (:5432) 或 HTTP API (:8080)
```

**关键组件**: `EventLogSink`

```rust
// crates/nexora-risingwave/src/event_sink.rs
pub struct EventLogSink {
    event_store: Arc<nexora_eventlog::EventLogStore>,
}

impl EventLogSink {
    pub async fn write_change(&self, change: Change) -> Result<()> {
        // 将 RisingWave MV 的变更写入 Nexora Event Log
        self.event_store.append_events(vec![event]).await?;
        Ok(())
    }
}
```

**使用场景**:
```sql
-- 连接到 RisingWave (psql -h localhost -p 4566)
CREATE SOURCE kafka_events WITH (
    connector = 'kafka',
    topic = 'user_actions',
    properties.bootstrap.server = 'localhost:9092'
) FORMAT PLAIN ENCODE JSON;

-- 复杂 SQL 转换
CREATE MATERIALIZED VIEW enriched_users AS
SELECT 
    user_id,
    COUNT(*) as action_count,
    MAX(timestamp) as last_seen
FROM kafka_events
WHERE action_type = 'click'
GROUP BY user_id;

-- Nexora 的 EventLogSink 会自动消费这个 MV 的变更
-- 然后写入 nexora-eventlog
```

```sql
-- 连接到 Nexora (psql -h localhost -p 5432)
-- 现在可以查询图数据（已包含从 RisingWave 来的数据）
MATCH (u:User) WHERE u.action_count > 10 RETURN u;
```

---

## HTTP API 访问 RisingWave

Nexora 提供了 HTTP API 来间接访问 RisingWave：

**端点**: `crates/nexora-app/src/handlers/risingwave.rs`

```rust
// POST /api/risingwave/ddl - 执行 RisingWave DDL
pub async fn execute_ddl(
    State(state): State<AppState>,
    Json(req): Json<RisingWaveDdlRequest>,
) -> Result<Json<RisingWaveDdlResponse>, ApiError> {
    let rw = state.risingwave.as_ref().ok_or(...)?;
    rw.execute_ddl(&req.sql).await?;
    // ...
}

// POST /api/risingwave/query - 查询 RisingWave MV
pub async fn query_mv(
    State(state): State<AppState>,
    Json(req): Json<RisingWaveQueryRequest>,
) -> Result<Json<RisingWaveQueryResponse>, ApiError> {
    let rw = state.risingwave.as_ref().ok_or(...)?;
    let results = rw.query_mv(&req.sql).await?;
    // ...
}

// GET /api/risingwave/cluster - 集群健康状态 (Phase 8)
pub async fn get_cluster_status(...) -> Result<Json<ClusterStatusResponse>, ApiError>
```

**内部实现**: 通过 `tokio-postgres` 客户端连接到 RisingWave Frontend (:4566)

```rust
// crates/nexora-risingwave/src/frontend_wrapper.rs
impl FrontendNode {
    pub async fn query_mv(&self, sql: &str) -> Result<String> {
        // 通过 tokio-postgres 连接到 RisingWave Frontend
        let client = tokio_postgres::connect(
            &format!("host=127.0.0.1 port=4566 user=root"),
            NoTls
        ).await?;
        
        let rows = client.query(sql, &[]).await?;
        // 序列化为 JSON 返回
    }
}
```

---

## 总结对比

| 特性 | Nexora PG-Wire | RisingWave Frontend |
|------|---------------|---------------------|
| **实现** | nexora-pgwire crate | RisingWave 自带 |
| **端口** | 5432 (可配置) | 4566 (默认) |
| **后端** | Nexora Graph (RocksDB) | RisingWave (Meta + Compute) |
| **查询语言** | SQL (翻译为 Cypher) | SQL (标准流处理) |
| **主要用途** | 图查询、节点/边访问 | 流处理、MV 查询 |
| **支持的操作** | MATCH, CREATE, SET, DELETE | CREATE SOURCE/MV, SELECT |
| **分布式** | 通过 HybridRouter | 通过 RisingWave Meta (Raft) |
| **认证** | Trust / SCRAM-SHA-256 | RisingWave 内置认证 |
| **启动方式** | nexora-app 启动 PG 服务器 | nexora-app 启动 RisingWave 进程 |
| **数据来源** | nexora-core GraphService | RisingWave Meta State |
| **是否统一** | ❌ 完全独立 | ❌ 完全独立 |

---

## 为什么不统一？

### 1. 职责分离
- **Nexora PG-Wire**: 图数据库访问层
- **RisingWave Frontend**: 流处理引擎

### 2. 技术栈不同
- Nexora 使用 RocksDB + Event Log
- RisingWave 使用自己的 State Store + Raft

### 3. 协议实现不同
- Nexora PG-Wire 是自定义实现（基于 `pgwire` crate）
- RisingWave Frontend 是 RisingWave 官方实现

### 4. 查询语义不同
- Nexora: 图查询语义（节点、边、路径）
- RisingWave: 流处理语义（时间窗口、聚合、JOIN）

### 5. 集成方式
- 数据通过 **EventLogSink** 从 RisingWave 流向 Nexora
- HTTP API 提供统一的访问接口
- 两个 PG-Wire 各自服务不同的场景

---

## 使用建议

### 何时使用 Nexora PG-Wire (:5432)
```bash
psql -h localhost -p 5432 -U admin -d nexora
```

✅ 查询图数据  
✅ 执行 Cypher 风格的模式匹配  
✅ 访问 Nexora 物化视图  
✅ 访问 Event Log 表

### 何时使用 RisingWave Frontend (:4566)
```bash
psql -h localhost -p 4566 -U root -d dev
```

✅ 创建 RisingWave SOURCE  
✅ 创建 RisingWave MATERIALIZED VIEW  
✅ 执行复杂 SQL 转换  
✅ 查询 RisingWave MV 的实时结果

### 何时使用 HTTP API (:8080)
```bash
curl -X POST http://localhost:8080/api/risingwave/query \
  -d '{"sql": "SELECT * FROM my_mv"}'
```

✅ Web 应用访问  
✅ RESTful API 集成  
✅ 无需 PostgreSQL 客户端

---

## 配置示例

### 启动两个 PG-Wire

```bash
# 启动 Nexora (包含 Nexora PG-Wire + RisingWave)
cargo run --release --features risingwave,embedded -- \
  --pg-port 5432 \              # Nexora PG-Wire
  --pg-trust \                  # Trust 认证
  --enable-risingwave \         # 启用 RisingWave
  --enable-embedded-risingwave  # 启用嵌入式 RisingWave
  # RisingWave Frontend 会自动监听 4566
```

### 验证两个端口

```bash
# 测试 Nexora PG-Wire
psql -h localhost -p 5432 -U admin -d nexora -c "SELECT 1"

# 测试 RisingWave Frontend
psql -h localhost -p 4566 -U root -d dev -c "SELECT 1"

# 测试 HTTP API
curl http://localhost:8080/api/health
curl http://localhost:8080/api/risingwave/status
```

---

## 未来可能的统一方向

虽然目前两个 PG-Wire 是独立的，但未来可能的统一方案：

### 方案 1: 多数据库支持
```sql
-- 通过 Nexora PG-Wire 访问 RisingWave (类似 PostgreSQL FDW)
psql -h localhost -p 5432
> SELECT * FROM risingwave.my_mv;  -- 自动路由到 RisingWave
> SELECT * FROM nexora.my_graph;   -- 访问 Nexora 图
```

### 方案 2: 统一查询引擎
```
Unified PG-Wire (:5432)
    ↓
Query Router
    ├─→ Graph Query → Nexora Core
    └─→ Stream Query → RisingWave Frontend
```

### 方案 3: RisingWave 作为 Nexora 后端
```
Nexora PG-Wire (:5432)
    ↓
Nexora Query Engine
    ├─→ Graph Storage → RocksDB
    └─→ Stream Processing → RisingWave (internal)
```

**当前状态**: 未实现，保持独立

---

## 参考文档

- **Nexora PG-Wire**: `crates/nexora-pgwire/src/lib.rs`
- **RisingWave 集成**: `crates/nexora-risingwave/src/module.rs`
- **Event Log Sink**: `crates/nexora-risingwave/src/event_sink.rs`
- **HTTP API**: `crates/nexora-app/src/handlers/risingwave.rs`
- **Phase 8 总结**: `docs/RISINGWAVE_PHASE8_FINAL_SUMMARY.md`

---

**生成时间**: 2026-07-27  
**作者**: Claude (Nexora 开发团队)
