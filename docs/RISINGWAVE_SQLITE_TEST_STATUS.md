# RisingWave SQLite 持久化测试状态

**日期**: 2026-07-28  
**状态**: 🟡 编译阶段 - 等待 RisingWave 二进制

---

## ✅ 已完成

### 1. 代码实现（100%）

**修改文件**:
- `crates/nexora-risingwave/src/distributed.rs` (256-283 行)
- `crates/nexora-risingwave/src/embedded_process.rs` (104-109, 253-276 行)

**核心变更**:
```rust
// 分布式集群：每个节点使用独立的 SQLite
.arg("--backend").arg("sql")
.arg("--sql-endpoint").arg(format!("sqlite://{}/meta.db", risingwave_dir))
.arg("--state-store").arg(format!("hummock+fs://{}/state", risingwave_dir))
```

### 2. Nexora 编译（100%）

```bash
✅ nexora-risingwave 库编译通过
✅ nexora 主程序编译通过（target/release/nexora, 42MB）
✅ 包含 risingwave,embedded 特性
✅ 编译时间：31.07 秒
```

### 3. 测试脚本（100%）

```bash
✅ scripts/test-sqlite-persistence.sh
   - 自动启动 3 节点集群
   - 创建表并插入数据
   - 验证 SQLite 文件存在
   - 重启并验证数据持久化
```

---

## 🟡 进行中

### RisingWave 二进制编译

**问题**: Nexora 需要 RisingWave 独立二进制文件来启动集群

**当前状态**:
```
后台任务 ID: b9219nfik
命令: cd vendor/risingwave && cargo build --release --bin risingwave
预计耗时: 20-40 分钟
输出日志: /private/tmp/claude-501/.../b9219nfik.output
```

**目标产物**:
```
vendor/risingwave/target/release/risingwave
```

**安装后位置**（代码会自动查找）:
1. `RISINGWAVE_BIN` 环境变量
2. `bin/risingwave-embedded`
3. `target/release/risingwave`
4. `vendor/risingwave/target/release/risingwave`
5. 系统 PATH (`which risingwave`)

---

## ⏳ 待完成

### 集成测试流程

编译完成后执行：

```bash
# 1. 验证二进制文件存在
ls -lh vendor/risingwave/target/release/risingwave

# 2. 设置环境变量（可选）
export RISINGWAVE_BIN=$PWD/vendor/risingwave/target/release/risingwave

# 3. 运行自动化测试
./scripts/test-sqlite-persistence.sh
```

**预期测试输出**:
```
========================================
  SQLite 持久化验证测试
========================================

✅ 依赖检查通过
🚀 启动 Nexora (3 节点 RisingWave 集群)...
⏳ 等待服务就绪...
✅ RisingWave Frontend 就绪

========================================
测试 1: 创建表并插入数据
========================================
CREATE TABLE
INSERT 0 3
 id |  name   |        created_at
----+---------+---------------------------
  1 | Alice   | 2026-07-28 02:10:00.123456
  2 | Bob     | 2026-07-28 02:10:00.234567
  3 | Charlie | 2026-07-28 02:10:00.345678
✅ 测试 1 通过

========================================
验证 SQLite 文件存在
========================================

节点 1:
  ✅ meta.db 存在 (大小: 128K)
  ✅ state/ 目录存在

节点 2:
  ✅ meta.db 存在 (大小: 128K)
  ✅ state/ 目录存在

节点 3:
  ✅ meta.db 存在 (大小: 128K)
  ✅ state/ 目录存在

✅ 所有节点的 SQLite 文件都存在

========================================
测试 2: 重启并验证数据持久化
========================================
🛑 停止 Nexora...
🚀 重新启动 Nexora...
⏳ 等待服务就绪...
✅ RisingWave Frontend 就绪

🔍 验证数据是否持久化...
  查询结果: 3 行
✅ 数据持久化成功！

完整数据:
 id |  name   |        created_at
----+---------+---------------------------
  1 | Alice   | 2026-07-28 02:10:00.123456
  2 | Bob     | 2026-07-28 02:10:00.234567
  3 | Charlie | 2026-07-28 02:10:00.345678

========================================
  🎉 所有测试通过！
========================================

SQLite 持久化验证成功：
  ✅ 3 节点集群启动正常
  ✅ SQLite 文件已创建
  ✅ 数据写入成功
  ✅ 重启后数据完整
```

---

## 📊 数据目录结构

测试将创建以下结构：

```
/tmp/nexora-risingwave-test-XXXXXXXX/
├── meta-1/
│   └── .risingwave/
│       ├── meta.db              ← SQLite 元数据（DDL, schema）
│       └── state/               ← Hummock 状态（数据）
├── meta-2/
│   └── .risingwave/
│       ├── meta.db              ← Raft 同步，与 meta-1 一致
│       └── state/
├── meta-3/
│   └── .risingwave/
│       ├── meta.db              ← Raft 同步，与 meta-1 一致
│       └── state/
├── frontend-1/
├── compute-1/
└── nexora.log                   ← 主进程日志
```

---

## 🚀 手动测试命令（快速验证）

如果自动化脚本失败，可手动执行：

### 1. 启动集群

```bash
RISINGWAVE_DATA_DIR=/tmp/test-rw ./target/release/nexora \
  --enable-event-streams \
  --embedded-event-streams \
  --event-streams-cluster \
  --port 8080 \
  --rocksdb-path /tmp/test-rw/graph
```

### 2. 连接并创建表

```bash
psql -h 127.0.0.1 -p 4566 -d dev <<EOF
CREATE TABLE test_persistence (
    id INT PRIMARY KEY,
    name VARCHAR,
    created_at TIMESTAMP
);

INSERT INTO test_persistence VALUES
    (1, 'Alice', NOW()),
    (2, 'Bob', NOW()),
    (3, 'Charlie', NOW());

SELECT * FROM test_persistence ORDER BY id;
EOF
```

### 3. 验证 SQLite 文件

```bash
ls -lh /tmp/test-rw/meta-1/.risingwave/meta.db
ls -lh /tmp/test-rw/meta-2/.risingwave/meta.db
ls -lh /tmp/test-rw/meta-3/.risingwave/meta.db
```

### 4. 重启验证

```bash
# Ctrl+C 停止进程

# 重新启动（使用相同的 RISINGWAVE_DATA_DIR）
RISINGWAVE_DATA_DIR=/tmp/test-rw ./target/release/nexora \
  --enable-event-streams \
  --embedded-event-streams \
  --event-streams-cluster \
  --port 8080 \
  --rocksdb-path /tmp/test-rw/graph

# 查询验证
psql -h 127.0.0.1 -p 4566 -d dev -c "SELECT * FROM test_persistence;"
```

---

## 📋 检查清单

- [x] 代码实现完成
- [x] Nexora 编译成功
- [x] 测试脚本编写
- [ ] RisingWave 二进制编译（进行中）
- [ ] 运行集成测试
- [ ] 验证 SQLite 持久化
- [ ] 验证重启恢复
- [ ] 更新文档

---

## 🐛 已知问题

### Issue 1: RisingWave 二进制缺失

**错误信息**:
```
ERROR nexora: Failed to start distributed event streams cluster: 
  RisingWave binary not found. Please set RISINGWAVE_BIN or install RisingWave.
```

**解决方案**:
- 正在从源码编译：`vendor/risingwave/target/release/risingwave`
- 或通过 Homebrew 安装：`brew install risingwavelabs/risingwave/risingwave`

---

## 📖 相关文档

- [实现完成总结](SQLITE_PERSISTENCE_IMPLEMENTATION.md)
- [嵌入式持久化方案](EMBEDDED_PERSISTENCE_SOLUTION.md)
- [RisingWave 集成计划](RISINGWAVE_INTEGRATION_PLAN.md)

---

**最后更新**: 2026-07-28 02:11 UTC  
**下一步**: 等待 RisingWave 编译完成 → 运行集成测试
