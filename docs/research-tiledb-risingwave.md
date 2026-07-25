# TileDB & RisingWave 架构研究 — Nexora-RS 融合增强方案

> 研究日期: 2026-06-28 | 源码版本: TileDB-2.30.1, RisingWave-3.0.0

## 一、两个系统概况

| 维度 | TileDB-2.30.1 | RisingWave-3.0.0 |
|------|--------------|------------------|
| 语言 | C++ | Rust |
| 定位 | 嵌入式多维数组存储引擎 | 流式 SQL 数据库 (Postgres 兼容) |
| 核心创新 | Tile 分块 + Fragment 版本化 + 云原生 VFS | Hummock LSM-Tree + 物化视图 + Barrier 检查点 |
| 存储模型 | 多维数组 (稠密/稀疏) | 共享存储 LSM-Tree (S3-backed) |
| 查询模型 | 多维子数组范围查询 | 物化视图增量维护 |
| 架构风格 | 嵌入式库 (无服务器进程) | 计算-存储分离 (Meta+Compute+Compactor) |
| 许可证 | MIT | Apache 2.0 |

---

## 二、与 Nexora-RS 架构设计文档的映射

### 2.1 TileDB → Nexora-RS 阶段 D (分层存储)

TileDB 提供了一整套**云原生存储引擎**的设计参考：

| TileDB 模式 | Nexora-RS 等价场景 | 融合方案 |
|------------|-----------------|---------|
| Fragment = 不可变时间批次 | 图事件 Journal 按时间分片 | **图 Fragment** 替代当前 flat journal Vec |
| Tile = 多维分块 | 按 (NexoraId, EventTime) 分块存储 | **Tile-based Graph Storage** 替代 RocksDB key-value |
| 时间戳命名 `__t1_t2_uuid` | 图快照版本化 | **时间旅行查询**: 按时间戳打开历史图状态 |
| R-Tree 空间索引 | 节点 ID 范围查询 | **碎片跳过**: 只读包含目标节点的 Fragment |
| Filter Pipeline | 按属性选择压缩方案 | **属性级压缩**: edge 数据用 zstd, property 用 lz4 |
| Consolidation | Journal 压缩和去重 | **后台 Compaction**: 合并小 Fragment, 去重事件 |
| VFS 抽象 | S3/Iceberg 后端 | **统一存储接口**: `StorageBackend` trait (local/S3/GCS/Azure) |
| Tile Metadata (min/max/sum) | 聚合查询优化 | **元数据加速**: count/sum/avg 直接从 tile metadata 回答 |

### 2.2 RisingWave → Nexora-RS 阶段 C (分布式) + 阶段 D (分层存储)

RisingWave 提供了**Rust 原生流式计算引擎**的完整参考实现：

| RisingWave 模式 | Nexora-RS 等价场景 | 融合方案 |
|---------------|-----------------|---------|
| Hummock LSM-Tree | 图状态持久化 | **三层读**: memtable → staging SST → committed SST |
| Barrier + Epoch | WAL checkpoint | **Epoch 屏障**: 替代当前每节点 WAL，全局一致性检查点 |
| MaterializeExecutor | Standing Query 物化 | **增量 SQ 物化**: 每个 SQ = 一个物化视图 |
| ChainExecutor (snapshot→stream) | 节点唤醒 (历史→实时) | **Backfill 模式**: 从快照加载基线，然后增量更新 |
| `StateTable` 抽象 | GraphShard 状态管理 | **通用状态表**: 支持 Insert/Delete/Update 的 KV 接口 |
| `StreamChunk` (列式批次) | 事件批处理 | **列式批处理**: 属性列+边列分开存储，压缩率更高 |
| `StreamKind` (Append/Retract/Upsert) | 事件类型优化 | **流类型标记**: Append 流无去重开销 |
| Dispatch 策略 (hash/broadcast/simple) | 分片路由 | **更丰富的分发策略**: 超越简单的 NexoraId hash |
| MergeIterator (多源合并) | 多 Fragment 读 | **合并读取**: 从多个时间分片合并图事件 |
| 计算-存储分离 | 集群架构 | **无状态 CN + 共享存储**: compute 节点故障秒级恢复 |
| Compaction 分离 | Journal 压缩 | **独立 Compactor 节点**: 不占用图计算资源 |

---

## 三、具体融合增强方案

### 3.1 增强 1: Fragment-based Graph Storage (借鉴 TileDB)

**当前 Nexora-RS**: RocksDB key-value 存储所有事件，按时间排序但无分片概念

**融合后设计**:
```
nexora-data/
  __schema/           # Array schema (graph schema versioned)
  __fragments/
    __1700000000_1700003600_uuid1/   # 1小时时间窗口的图事件
      nodes.tdb        # 节点数据 tile (按 NexoraId hash 分块)
      edges.tdb        # 边数据 tile
      props.tdb        # 属性数据 tile (列式)
      __fragment_meta  # R-Tree + tile min/max
    __1700003600_1700007200_uuid2/
      ...
  __commits/
    .wrt               # 原子提交标记
  __consolidated/      # 压缩后的合并 Fragment
```

**关键收益**:
- **时间旅行**: 打开 `as_of(timestamp)` 获取历史图快照
- **碎片跳过**: R-Tree 索引定位只包含目标 NexoraId 的 fragments
- **独立压缩**: nodes/edges/props 用不同压缩方案
- **S3 原生**: 每个 fragment 是一个 S3 prefix，tile 级别 range GET

### 3.2 增强 2: Hummock-style Epoch Barrier (借鉴 RisingWave)

**当前 Nexora-RS**: 每个 NodeTask 独立 WAL，无全局一致性点

**融合后设计**:
```
Meta 服务定期注入 Barrier:
  Barrier { epoch: N }
    → 流经所有 Shard
    → 每个 Shard: flush memtable → upload SST → 报告完成
    → 所有 Shard 完成: epoch N committed
    → 全局一致性快照点
```

**WAL 简化为**:
```
当前 (复杂):  256个独立 WAL 文件 + 各自 fsync + 各自恢复
融合后 (简化): 全局 Barrier → 批量 flush → Hummock SST upload → 单一一致性点
```

### 3.3 增强 3: Tile-based Property Storage (借鉴 TileDB)

**当前 Nexora-RS**: 属性按节点为 BTreeMap<String, PropertyValue>，JSON 序列化

**融合后设计**:
```
Tile Extent: (NexoraId range = 1000, EventTime range = 1小时)
每 Tile 预计算:
  - min/max EventTime → 时间范围查询跳过整个 tile
  - min/max property value → 聚合查询从 metadata 回答
  - null count → IS NULL 查询跳过无 null 的 tile
  - bloom filter → 快速判断属性值是否存在
```

**存储量对比**:
```
当前: {"speed": 15.0, "zone": "A", "name": "FL-001"}
融合: speed.tdb (f64 tile) + zone.tdb (string dict tile) + name.tdb (string tile)
      每列独立压缩: speed=bitwidth_reduce+zstd, zone=dictionary+zstd, name=lz4
```

### 3.4 增强 4: 增量 Standing Query 物化 (借鉴 RisingWave)

**当前 Nexora-RS**: SQ 评估是全量匹配 (`on_property_change` → `evaluate()` → 检查所有 SQ)

**融合后设计**:
```
每个 SQ = RisingWave Materialized View:
  SQ "speed > 100" → MaterializedView {
    state_table: StateTable<NexoraId, bool>  // 节点→匹配状态
    incremental:  当 speed 变更 → 检查 → 更新 state_table → 仅发送 delta
  }

Chain Backfill:
  1. 从快照加载所有节点的 speed 属性
  2. 批量计算匹配状态 → 写入 state_table
  3. 切换到流式增量更新
  4. 下游收到: INSERT (新匹配) / DELETE (取消匹配)
```

**性能提升**:
```
当前: 每次属性变更 O(SQ数量 × 节点数) 全量扫描
融合后: 每次属性变更 O(SQ数量) 增量更新 (仅处理变更节点)
```

### 3.5 增强 5: 计算-存储分离 (借鉴 RisingWave)

**当前 Nexora-RS**: 单机所有组件耦合

**融合后设计**:
```
┌─────────────────────────────────────────────┐
│                Meta Service                 │
│  ShardMap管理 / Barrier调度 / Epoch提交     │
└─────────────────────────────────────────────┘
          │                    │
    ┌─────┴──────┐      ┌─────┴──────┐
    │ Compute-1  │      │ Compute-2  │
    │ NodeTask   │      │ NodeTask   │
    │ SQ Eval    │      │ SQ Eval    │
    │ MemTable   │      │ MemTable   │
    └─────┬──────┘      └─────┬──────┘
          │                    │
          └────────┬───────────┘
                   ▼
         ┌─────────────────┐
         │  Hummock SSTs   │
         │  (S3/Iceberg)   │
         └─────────────────┘
                   │
         ┌─────────┴─────────┐
         │ Compactor Nodes   │
         │ Fragment合并/去重  │
         └───────────────────┘
```

**关键收益**:
- **秒级故障恢复**: CN 故障 → 新 CN 加载最新 HummockVersion → 立即开始服务
- **弹性伸缩**: 增加 CN 无需数据迁移
- **独立 Compaction**: 不影响图计算性能

### 3.6 增强 6: Filter Pipeline (借鉴 TileDB)

**当前 Nexora-RS**: 事件编解码是 EventCodec (当前 JSON，设计目标 FlatBuffers)，但无压缩

**融合后设计**:
```rust
pub struct FilterPipeline {
    filters: Vec<Box<dyn Filter>>,  // 有序过滤器链
}

impl FilterPipeline {
    // 写入时: 正向依次应用
    fn apply_forward(&self, data: &[u8]) -> FilteredData;
    // 读取时: 反向依次解除
    fn apply_reverse(&self, filtered: &[u8]) -> Vec<u8>;
}

// 图事件按属性分开压缩:
// edge 数据: byteshuffle → zstd (列式数值数据)
// property 数据: dictionary → lz4 (重复字符串多)
// 时间戳: delta → rle (单调递增数值)
```

---

## 四、优先级建议与文件路径

### 立即可做 (对当前代码改动最小)

| # | 增强项 | 借鉴来源 | 涉及 Nexora-RS 文件 | 工作量 |
|---|--------|---------|------------------|--------|
| 1 | Tile Metadata (min/max/sum/nullcount) | TileDB `tile_metadata_generator.h` | `nexora-persistor-rocksdb/persistor.rs` | 3天 |
| 2 | Fragment 命名 `__t1_t2_uuid` | TileDB `timestamped_name.md` | `nexora-core/src/persistor.rs` | 2天 |
| 3 | Epoch Barrier (简化 WAL) | RisingWave `barrier/` | `nexora-core/src/wal/` | 5天 |
| 4 | 列式属性存储 | TileDB per-attribute tiles | `nexora-core/src/graph/node_task.rs` | 5天 |

### 阶段 C/D 可做 (需要新建 crate)

| # | 增强项 | 借鉴来源 | 涉及 Nexora-RS crate | 工作量 |
|---|--------|---------|-------------------|--------|
| 5 | VFS 抽象 (S3/GCS/Azure) | TileDB `vfs.h` | 新建 `nexora-vfs` | 2周 |
| 6 | Hummock-style 共享存储 | RisingWave `hummock/` | 新建 `nexora-storage` | 4周 |
| 7 | Consolidation/Compaction | TileDB `consolidator/` | `nexora-core` | 3周 |
| 8 | 增量 SQ 物化 (MV 模式) | RisingWave `MaterializeExecutor` | `nexora-standing-query` | 3周 |

### 已在 TileDB/RisingWave 源码中的关键参考文件

**TileDB (可直接阅读实现细节)**:
```
/Users/frank/aiCoding/TileDB-2.30.1/
  format_spec/fragment.md              ← Fragment 存储格式
  format_spec/timestamped_name.md      ← 时间戳命名
  format_spec/tile.md                  ← Tile 格式
  format_spec/filter_pipeline.md       ← Filter Pipeline
  tiledb/sm/filter/filter_pipeline.h   ← 可组合过滤器链
  tiledb/sm/rtree/rtree.h             ← R-Tree 空间索引
  tiledb/sm/filesystem/vfs.h          ← VFS 抽象接口
  tiledb/sm/consolidator/             ← 后台合并
```

**RisingWave (可直接阅读实现细节)**:
```
/Users/frank/aiCoding/risingwave-3.0.0/
  src/storage/src/hummock/             ← Hummock LSM-Tree
  src/storage/src/hummock/store/version.rs  ← 三层读合并
  src/storage/src/hummock/store/local_hummock_storage.rs ← 写路径
  src/storage/src/hummock/iterator/merge_inner.rs  ← MergeIterator
  src/stream/src/common/table/state_table.rs   ← StateTable
  src/stream/src/executor/mview/materialize.rs  ← 物化视图
  src/stream/src/executor/chain.rs    ← Snapshot→Stream
  src/meta/src/barrier/               ← Barrier 调度
  src/frontend/src/stream_fragmenter/ ← Fragment 图→Actor
```

---

## 五、总结

两个系统在各自领域都是顶级开源项目。TileDB 在**存储引擎设计**上极为成熟（tile/chunk/fragment/pipeline/vfs），RisingWave 在**流式计算引擎**上极为成熟（Hummock/barrier/MV/StateTable）。

**Nexora-RS 的独特定位**（流式图计算）恰好是两个系统的交叉领域：
- 从 TileDB 借鉴：**如何高效存储和查询图事件**
- 从 RisingWave 借鉴：**如何增量维护图查询结果**

最关键的两个模式融合：
1. **Fragment + Barrier = 图时间旅行**：TileDB 的 Fragment 版本化 + RisingWave 的 Epoch 屏障 → 图状态任意时间点查询
2. **Tile-DB Columnar + Hummock LSM = 图存储引擎**：TileDB 的列式 Tile + RisingWave 的共享存储 LSM → 真正云原生的图存储

---

## 六、RisingWave 高级 Feature 详细分析

### 6.1 Vector / HNSW 搜索 ⭐⭐⭐

**状态**: 完整原生实现 (Flat + HNSW, 4 种距离类型, pgvector 兼容语法)

**DeepStreaming 映射**: HNSW 向量索引作为流式状态后端，实现实时相似搜索——"当前事件 embedding 与已知 fraud vectors top-5 相似时告警"

**关键文件**:
```
src/storage/hummock_sdk/src/vector_index.rs    ← HNSW/Flat 索引定义
src/expr/impl/src/scalar/vector.rs              ← L2/Cosine/InnerProduct 距离函数
src/frontend/src/optimizer/plan_node/batch_vector_search.rs ← 向量搜索执行
src/storage/src/table/batch_table/vector_index_reader.rs    ← 向量索引读取
```

### 6.2 Time-Travel 查询 (AsOf) ⭐⭐⭐

**状态**: 完整实现 (FOR SYSTEM_TIME AS OF / FOR SYSTEM_VERSION AS OF)

**DeepStreaming 映射**: "当前流与 24h 前的状态做对比" → 流式异常检测的核心能力

**关键文件**: `src/sqlparser/src/ast/mod.rs:3864` (AsOf enum), `src/frontend/src/optimizer/plan_node/batch_seq_scan.rs`

### 6.3 弹性伸缩 ⭐⭐⭐

**状态**: 生产级 (ALTER PARALLELISM adaptive/fixed/bounded/ratio, 无 shuffle 优化, 增量状态重平衡)

**DeepStreaming 映射**: Actor 重调度框架直接可复用

**关键文件**: `src/meta/src/stream/scale.rs` (核心缩放编排), `src/frontend/src/handler/alter_parallelism.rs`

### 6.4 Schema Evolution / CDC ⭐⭐⭐

**状态**: 深度支持 (ADD/DROP/ALTER column + REFRESH SCHEMA from Schema Registry)

**DeepStreaming 映射**: 表替换模式——生成新流图，原子性替换，在线 schema 迁移

**关键文件**: `src/frontend/src/handler/alter_table_column.rs`, `src/frontend/src/handler/alter_table_with_sr.rs`

### 6.5 Stream AI/ML ⭐⭐⭐

**状态**: 多种嵌入 UDF (Python, JavaScript/QuickJS, WASM/Rust, Arrow Flight 外部 UDF, OpenAI Embedding)

**DeepStreaming 映射**: UDF 框架直接可用。关键机会：在流式查询中使 UDF 成为一等公民且具有 exactly-once 语义

**关键文件**: `src/expr/impl/src/scalar/ai_model.rs`, `src/expr/impl/src/udf/`

### 6.6 多租户 (Resource Groups) ⭐⭐

**状态**: 生产级 (ALTER MATERIALIZED VIEW SET RESOURCE_GROUP, worker node labeling)

**DeepStreaming 映射**: 资源感知调度——相同的集群隔离生产/测试流式作业

### 6.7 异地容灾 (Backup/Restore) ⭐⭐

**状态**: 完整的备份/恢复，但**无**实时 geo-replication

**DeepStreaming 映射**: 元数据备份框架直接可用

### 6.8 代价优化 (CBO) ⭐⭐

**状态**: 索引选择代价矩阵 + 基数范围追踪，但**无**直方图统计/ANALYZE

**DeepStreaming 映射**: 代价矩阵模式可直接用于 DeepStreaming 的访问路径选择

### 6.9 Recursive CTE / 图遍历 ⭐⭐ (RisingWave 不支持！)

**状态**: **完全不存在**

**DeepStreaming 映射**: **这是 DeepStreaming 可以填补的空白！** 流式不动点算子可以处理迭代计算——"当新边到达时持续重新计算可达性"

### 6.10 Data Quality / Validation ⭐⭐

**状态**: NOT NULL + generated columns 已支持；CHECK/FK/UNIQUE 解析但**不强制执行**

**DeepStreaming 映射**: Generated column 基础设施可扩展为流式数据质量监视器

---

## 七、TileDB 高级 Feature 详细分析

### 7.1 Serverless / REST API ⭐⭐⭐

**状态**: 生产级 (完整 Cap'n Proto 序列化 + REST client, `tiledb://` URI 协议)

**DeepStreaming 映射**: 可直接采用 capnp 序列化模式用于 DeepStreaming 的云/远程接口

**关键文件**: `tiledb/sm/rest/`, `tiledb/sm/serialization/tiledb-rest.capnp`

### 7.2 维度标签 / 二级索引 ⭐⭐⭐

**状态**: 实验 (Dimension Label API——字符串名称查询替代原始维度坐标)

**DeepStreaming 映射**: 直接适用！DeepStreaming 可对实体 ID 字符串进行索引，同时保留整数时间戳作为主维度

**关键文件**: `tiledb/sm/array_schema/dimension_label.*`, `tiledb/sm/query/readers/ordered_dim_label_reader.cc`

### 7.3 加密静态数据 ⭐⭐⭐

**状态**: 生产级 (AES-256-GCM, 独立 per-tile-part IV + GCM tag, 密钥零化析构)

**DeepStreaming 映射**: 作为 Pipeline 的一个 stage——加密仅仅是一个 filter，与其他 filters 组合

**关键文件**: `tiledb/sm/filter/encryption_aes256gcm_filter.*`, `tiledb/sm/crypto/encryption_key.*`

### 7.4 谓词下推 + DataFusion 集成 ⭐⭐⭐

**状态**: 生产级 (完整 AST 谓词系统 + DataFusion LogicalExpr 双向转换 + Arrow C Data Interface)

**DeepStreaming 映射**: 可用于流式过滤谓词；DataFusion 集成路径作为 SQL filter pushdown 的模板

**关键文件**: `tiledb/sm/query/ast/query_ast.h`, `tiledb/oxidize/expr/src/`, `tiledb/oxidize/arrow/src/`

### 7.5 数据版本化 / 时间旅行 ⭐⭐⭐

**状态**: 生产级 (Fragment 时间戳命名 + 按时间过滤)

**DeepStreaming 映射**: `__t1_t2_uuid` 命名可作为 DeepStreaming 图事件日志的 Fragment 命名约定

**关键文件**: `tiledb/sm/fragment/fragment_identifier.*`, `tiledb/sm/consolidator/`

### 7.6 TileDB 中没有但需求极高的功能 ⭐⭐⭐

| 功能 | 需求 | DeepStreaming 机会 |
|------|------|-------------------|
| Delta Lake / Iceberg 集成 | 非常高 | DeepStreaming 可实现自己的开放格式兼容层 |
| CDC / Event Notifications | 高 | DeepStreaming 的原生事件系统 |
| Cross-array Joins | 中 | DeepStreaming 的图引擎本身就解决这个问题 |
| UDF (非仅聚合) | 非常高 | DeepStreaming 可扩展 WASM/Python UDF |
| Branching / Tags | 高 | DeepStreaming 可原生支持图快照命名 |

---

## 八、Top 10 DeepStreaming 增强建议 (融合 TileDB + RisingWave + 原始设计)

| # | 功能 | 借鉴来源 | 优势 | 优先级 |
|---|------|---------|------|--------|
| 1 | **HNSW 向量搜索** | RisingWave | AI embedding 实时相似搜索——每个图节点可以有 embedding | P1 |
| 2 | **Time-Travel 图查询** | RisingWave + TileDB | `MATCH (n) AS OF '2026-06-01' RETURN n` | P1 |
| 3 | **Fragment 时间分片存储** | TileDB | 图事件按时间分片不可变存储，R-Tree 索引跳过 | P1 |
| 4 | **UDF 框架 (WASM/Python/JS)** | RisingWave | 流式图计算中嵌入自定义逻辑 | P2 |
| 5 | **流式不动点算子 (Recursive CTE)** | 填补空白 | 可达性/传递闭包/图遍历——RisingWave 和 TileDB 都没有！ | P2 |
| 6 | **弹性 Actor 伸缩** | RisingWave | 无 shuffle 优化 + 增量状态重平衡 | P2 |
| 7 | **AES-256-GCM 加密** | TileDB | Pipeline 中的一个 stage，与压缩组合 | P2 |
| 8 | **DataFusion SQL 双向集成** | TileDB oxidize | 图查询 → SQL 表达式；SQL → 图查询 | P3 |
| 9 | **Schema Registry 同步** | RisingWave | 上游 schema 变更自动同步到图 | P3 |
| 10 | **Cap'n Proto 序列化** | TileDB | 替代 JSON/FlatBuffers，零拷贝，云原生 | P3 |
