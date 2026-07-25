# Event Store REST Catalog 集成（Lakekeeper）

> 生产多节点部署的推荐方案。用 REST catalog 统一管理 Iceberg 元数据，
> 多节点天然共享一套数据，无需 NFS 共享 SQLite 文件。

## 背景：为什么需要 REST Catalog

`EventLogStore` 支持三种后端，多节点语义各不相同：

| 后端 | catalog | 数据文件 | 多节点共享 |
|------|---------|---------|-----------|
| `LocalFs` | 本地 SQLite | 本地 FS | ❌ 单机 |
| `S3` | 本地 SQLite | 共享 S3 | ⚠️ 各节点 catalog 独立 |
| `Rest` | **远程 REST 服务** | 共享 S3 | ✅ **天然共享** |

`S3` 后端的痛点：即使所有节点写到同一个 S3 bucket，每个节点的 SQLite catalog
是独立的 —— 节点 B 看不到节点 A 创建的表（catalog 里查不到）。要多节点共享
必须把 SQLite 文件放到 NFS 上，运维复杂且有文件锁瓶颈。

`Rest` 后端把元数据管理交给一个独立的 REST catalog 服务（[Lakekeeper](https://github.com/lakekeeper/lakekeeper)，
Rust 原生、Apache 许可）。所有节点通过 HTTP 访问同一套元数据，只需约定
`uri` + `warehouse` 名，各节点其它配置完全独立。

## 架构

```
       ┌─────────────────────────────────────┐
       │  Lakekeeper REST Catalog (:8181)     │
       │  - 元数据(表/schema/snapshot)         │
       │  - 凭证下发(credential vending)       │
       │  Postgres 后端                        │
       └──────────────┬──────────────────────┘
                      │ HTTP (Iceberg REST spec)
     ┌────────────────┼────────────────┐
     │                │                │
┌────┴────┐     ┌────┴────┐     ┌────┴────┐
│ Node A  │     │ Node B  │     │ Node C  │   ← 各节点配置完全独立
│ 独立配置 │     │ 独立配置 │     │ 独立配置 │      只约定 uri+warehouse
└────┬────┘     └────┬────┘     └────┬────┘
     │ 直接读写数据文件 │                │
     └────────────────┼────────────────┘
                      ▼
       ┌─────────────────────────────────────┐
       │  S3 / MinIO (数据文件 .parquet)       │
       └─────────────────────────────────────┘
```

**关键**：catalog 只管元数据 + 凭证；数据文件由各节点客户端**直接**读写 S3
（不经过 catalog 服务，无带宽瓶颈）。

## 快速开始（本地开发）

### 1. 启动 Lakekeeper + Postgres + MinIO

```bash
# 起 Lakekeeper + Postgres
docker compose -f scripts/lakekeeper/docker-compose.yml up -d

# MinIO 若未运行:
docker run -d --name nexora-minio-test -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# bootstrap Lakekeeper + 创建 warehouse "nexora"
./scripts/lakekeeper/bootstrap.sh
```

### 2. 启动 Nexora（REST 后端）

```bash
nexora-app \
  --event-store-backend rest \
  --event-store-rest-uri http://localhost:8181/catalog \
  --event-store-rest-warehouse nexora \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin \
  --event-store-s3-path-style
```

多节点时，**每个节点用相同的上述参数即可**（`--event-store-s3-endpoint`
换成各节点网络可达的 S3 地址）。无需共享任何本地文件。

## CLI 参数

| 参数 | 说明 | REST 必填 |
|-----|------|:---:|
| `--event-store-backend rest` | 选择 REST 后端 | ✓ |
| `--event-store-rest-uri` | catalog endpoint | ✓ |
| `--event-store-rest-warehouse` | warehouse 名 | ✓ |
| `--event-store-s3-endpoint` | 客户端读写数据文件用 | ✓ |
| `--event-store-s3-access-key` | S3 key（或 `AWS_ACCESS_KEY_ID`） | ✓ |
| `--event-store-s3-secret-key` | S3 secret（或 `AWS_SECRET_ACCESS_KEY`） | ✓ |
| `--event-store-s3-region` | 默认 `us-east-1` | |
| `--event-store-s3-path-style` | MinIO 需要 | |

## 代码集成

`StorageConfig::rest()` 构造 REST 配置：

```rust
use nexora_eventlog::{EventLogStore, StorageConfig};

let config = StorageConfig::rest(
    "http://localhost:8181/catalog", // REST uri
    "nexora",                        // warehouse
    "http://localhost:9000",         // s3 endpoint (客户端数据文件 IO)
    "us-east-1",                     // region
    "minioadmin",                    // access key
    "minioadmin",                    // secret key
    true,                            // path_style (MinIO)
);
let store = EventLogStore::new_with_config(config).await?;
```

`EventLogStore` 内部按后端分支选 `RestCatalogBuilder` / `SqlCatalogBuilder`，
上层 `append` / `read_table_batches` 等逻辑完全不变（都基于 `Arc<dyn Catalog>`）。

## 附带收益：修掉了 S3 endpoint bug

`iceberg-catalog-sql 0.9.1` 在 `SqlCatalog::new` 里 `FileIOBuilder::new(factory).build()`
**没传 props**，导致 S3 endpoint 丢失（我们用 `S3PropsInjectingFactory` workaround 绕过）。

`iceberg-catalog-rest 0.9.1` 在同一处正确调用了 `.with_props(props)`：

```rust
// iceberg-catalog-rest-0.9.1/src/catalog.rs:443
let file_io = FileIOBuilder::new(factory).with_props(props).build();  // ✅
```

所以 REST 后端**不需要** workaround，S3 配置直接生效。

## 测试验证

`crates/nexora-eventlog/tests/lakekeeper_rest_distributed_test.rs` —
**每个节点用完全独立的配置**（不共享任何本地文件），验证真正的多节点共享：

| 测试 | 验证 | 结果 |
|-----|------|------|
| `test_rest_basic_cross_node_read` | node-a 写，node-b/c 独立配置读到 | ✅ |
| `test_rest_concurrent_writes` | 3 独立节点并发写 65 行 | ✅ |
| `test_rest_persistence_across_restart` | drop 节点后新节点读到 7 行 | ✅ |
| `test_rest_snapshot_visibility` | 跨节点 snapshot 可见性 10→25 | ✅ |

运行：
```bash
# 前提: docker compose up + bootstrap.sh 已执行
cargo test --test lakekeeper_rest_distributed_test \
  --features olap --package nexora-eventlog -- --ignored --nocapture
```

对比 `minio_distributed_test.rs`（S3 + SQLite 后端）：那套测试必须让所有节点
**共享同一个 SQLite catalog 文件**才能互相看到数据；REST 后端则完全不需要。

## 生产部署要点

1. **Lakekeeper HA**：Postgres 用主从/托管服务；Lakekeeper 无状态可多副本。
2. **鉴权**：本地 dev 关了鉴权；生产启用 OAuth/OIDC（见 Lakekeeper 文档）。
3. **凭证下发**：可让 Lakekeeper 用 STS 下发临时凭证，客户端不再持有长期 S3 key。
4. **加密 key**：`LAKEKEEPER__PG_ENCRYPTION_KEY` 换成安全值并妥善保管。

## 参考

- [Lakekeeper GitHub](https://github.com/lakekeeper/lakekeeper)
- [Lakekeeper 文档](https://docs.lakekeeper.io/)
- [Iceberg REST Catalog spec](https://iceberg.apache.org/concepts/catalog/)
- 选型分析：本次调研对比了 Lakekeeper / Apache Polaris / Nessie，
  选 Lakekeeper 因其 Rust 原生、与 nexora 运维模型一致、drop-in 兼容 0.9.1 栈。
