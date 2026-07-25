# Nexora-RS 项目修复与优化报告

日期：2026-06-27

## 概述

本次修改针对全项目代码审查中发现的持久化、WAL、Cypher、Standing Query、HTTP API、解析器和数据摄取问题进行了修复，并补充了相应的回归测试。

## 核心持久化与 WAL

### 已修复

- 所有节点变更现在都会先写入共享 WAL，再更新内存状态。
- WAL 使用单条批量记录保存一次原子提交，避免只恢复部分操作。
- durable 模式使用 `Always` 同步策略，提交成功前会执行 WAL flush 和 `fsync`。
- WAL 恢复只重放最后一个 checkpoint 之后的事件。
- 恢复成功后写入新的 checkpoint，避免重复恢复。
- 普通属性和边操作不再绕过 WAL。
- 节点落盘失败时不再提前清空内存 journal。
- 快照、journal 和 checkpoint 按安全顺序写入。
- LRU 驱逐失败会向调用方返回错误，不再静默超过内存限制。

### 优雅关闭

- HTTP 服务首先停止接收请求并等待正在执行的请求结束。
- 随后逐分片持久化所有活跃节点。
- 最后显式 flush RocksDB。
- 修复了此前 Ctrl+C 正常退出后仍会丢失数据的问题。

## RocksDB

### 已修复

- journal key 增加事件序号，同一时间戳的多个事件不再相互覆盖。
- namespace 被纳入 RocksDB key，不同 namespace 的数据实现隔离。
- 命名 namespace 使用带版本标记的 key 前缀。
- 默认 namespace 保持与旧版磁盘 key 的兼容性。
- 读取损坏事件时返回明确错误，不再静默跳过。
- shutdown 阶段显式执行数据库 flush。
- 对超长 namespace 和 NexoraId 增加校验。

## 快照

- 快照序列化和反序列化改为返回明确的错误。
- 损坏快照不再被当作空节点加载。
- 从快照恢复时只重放快照时间之后的事件。
- 快照时间会覆盖本次 journal 中的最新事件时间。

## Cypher

### 已修复

- Cypher 快照同时枚举：
  - 活跃节点；
  - 仅存在 journal 的节点；
  - 仅存在 snapshot 的节点。
- 刚写入且尚未休眠的节点现在可以立即被查询。
- 节点枚举顺序固定，查询快照具有确定性。
- 超过 100,000 个节点时返回明确错误，不再静默截断结果。
- 正确处理 incoming half-edge，并对重复的两侧 half-edge 去重。
- 删除基于字符串搜索的写语句判断，避免属性值中出现关键字时被误判。
- 浮点属性不再被静默截断为整数。

### 当前限制

当前使用的 `cypher-parser` 执行器没有浮点数值类型。查询涉及浮点属性时，系统会返回明确的“不支持”错误，以避免产生错误结果。

## Standing Query

- 属性变化后使用节点的完整属性集合重新计算匹配。
- 修复修改无关属性导致已有匹配被错误取消的问题。
- LabelFilter 按文档语义要求节点包含全部指定标签。
- 评估前复制查询列表，避免长时间持有查询注册表读锁。

## HTTP API

- 属性写入失败会返回对应的 HTTP 错误状态，不再始终返回 200。
- 非法 NexoraId 返回 `400 Bad Request`。
- edge direction 仅接受 `in` 或 `out`，其他值返回 400。
- 增加 HTTP 请求 tracing 中间件。
- `--num-shards` 和 `--max-nodes-per-shard` 必须大于零。
- `--wal-dir` 必须与 `--rocksdb-path` 一起使用。
- 启动日志会准确区分内存、RocksDB 和 RocksDB + WAL 模式。

## Cypher 解析器

- 修复 `STARTS WITH` 和 `ENDS WITH`。
- 修复 `REMOVE n.property` 和 `REMOVE n:Label`。
- 修复 `SET n += {...}` 的 `+=` token。
- 支持查询末尾分号。
- 支持 XOR 表达式。
- 支持无向边模式。
- 支持 `*..N` 开放范围路径。
- 未闭合的块注释现在会返回 lexer 错误。
- map literal 可作为表达式使用。

## 数据摄取

- 文件不存在时，`start()` 立即返回错误，状态不会错误地变成 Running。
- 文件读取完成后状态自动变成 Completed。
- 文件读取错误会记录为 Error 状态。
- `stop()` 会终止后台任务。
- 防止同一 source 被重复启动。

## 序列化

- PackedCodec 会拒绝截断或带多余数据的输入。
- snapshot codec 不再通过空值掩盖序列化错误。
- 生成的 FlatBuffers 代码告警已在模块边界正确隔离。

## 工程配置

- 项目最低 Rust 版本从 1.75 更新为 1.88，与实际代码和依赖要求一致。
- 删除未使用的 `tracing-appender`。
- 缩减 `tower-http` 到实际使用的 feature。
- 全项目已执行 `rustfmt`。
- 全项目通过严格 Clippy 检查，所有 warning 均视为错误。

## 测试结果

执行并通过：

```text
cargo test --workspace --all-targets
199 passed, 0 failed

cargo test --workspace --doc
通过；1 个示例按原设计 ignored

cargo fmt --all -- --check
通过

cargo clippy --workspace --all-targets -- -D warnings
通过

cargo audit
扫描 196 个依赖，未发现安全漏洞
```

## 新增或强化的回归场景

- 正常关闭持久化活跃节点。
- 未 sleep 时通过 WAL 恢复属性。
- 在线节点立即参与 Cypher 查询。
- 浮点属性不会被截断。
- SQ 修改无关属性后保持匹配。
- HTTP 错误状态和 edge direction 校验。
- RocksDB 相同时间戳事件不覆盖。
- RocksDB namespace 隔离。
- 旧版默认 namespace key 兼容读取。
- 损坏快照返回错误。
- PackedCodec 截断输入检测。
- parser 字符串谓词、REMOVE、分号、无向边、开放路径范围和 map merge。
- ingest 文件缺失与完成状态。

## 黑盒验证

使用真实 HTTP 服务和 RocksDB + WAL 完成了以下验证：

1. 注册 Label Standing Query。
2. 写入在线 Person 节点。
3. 修改无关属性后确认 SQ 仍保持匹配。
4. 未 sleep 时执行 Cypher，确认节点立即可见。
5. 通过 Ctrl+C 关闭服务。
6. 使用同一 RocksDB 和 WAL 目录重启。
7. 确认属性和 Cypher 查询结果均成功恢复。

