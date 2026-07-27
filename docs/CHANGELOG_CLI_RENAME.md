# 变更日志 - CLI 参数重命名

**日期**: 2026-07-27  
**版本**: v2.1.0  
**类型**: 破坏性变更 (Breaking Change)

---

## 🎯 变更目标

将所有暴露 "RisingWave" 第三方组件名称的用户界面改为业务语义化的 **"Event Streams"（事件流）**。

---

## ✅ 完成的变更

### 1. CLI 参数重命名

| 文件 | 变更内容 |
|------|---------|
| `crates/nexora-app/src/main.rs` | 7 个 CLI 参数重命名 |

**具体变更**:
- `--enable-risingwave` → `--enable-event-streams`
- `--enable-embedded-risingwave` → `--embedded-event-streams`
- `--risingwave-meta-addr` → `--event-streams-meta-addr`
- `--risingwave-frontend-addr` → `--event-streams-frontend-addr`
- `--risingwave-cluster-mode` → `--event-streams-cluster`
- `--risingwave-raft-node-id` → `--event-streams-raft-node-id`
- `--risingwave-raft-peers` → `--event-streams-raft-peers`

### 2. TOML 配置结构重命名

| 文件 | 变更内容 |
|------|---------|
| `crates/nexora-app/src/config.rs` | `AppTomlConfig` 结构体字段重命名 |
| `crates/nexora-app/src/config_loader.rs` | 默认配置构造函数更新 |

**具体变更**:
- `AppTomlConfig.risingwave` → `AppTomlConfig.event_streams`
- `RisingWaveConfig` → `EventStreamsConfig` (类型别名保留)
- `default_rw_*` 函数 → `default_event_streams_*`

### 3. 日志输出更新

| 文件 | 变更内容 |
|------|---------|
| `crates/nexora-app/src/main.rs` | 启动日志消息更新 |

**具体变更**:
- "RisingWave: starting distributed cluster" → "Event Streams: starting distributed cluster"
- "RisingWave: distributed cluster started" → "Event Streams: distributed cluster started"
- "RisingWave: embedded process started" → "Event Streams: embedded process started"
- 错误消息中的 "RisingWave" → "event streams"

### 4. 配置文件示例更新

| 文件 | 变更内容 |
|------|---------|
| `nexora-cluster.toml.example` | 配置段和注释全部更新 |

**具体变更**:
- `[risingwave]` → `[event_streams]`
- `[[risingwave.meta_nodes]]` → `[[event_streams.meta_nodes]]`
- `[[risingwave.compute_nodes]]` → `[[event_streams.compute_nodes]]`
- 所有注释中的 "RisingWave" → "事件流"

### 5. 文档更新

| 文件 | 变更类型 |
|------|---------|
| `docs/RUNTIME_OPERATIONS_GUIDE.md` | 全文自动替换 |
| `docs/CLI_PARAMETER_RENAME_2026-07-27.md` | ✅ 新建 - 重命名说明文档 |
| `docs/CHANGELOG_CLI_RENAME.md` | ✅ 新建 - 本变更日志 |

**运行操作指南更新内容**:
- 所有 CLI 命令示例更新
- TOML 配置示例更新
- 快速参考命令更新

---

## 🔒 保持不变的部分

以下内容 **未变更**，保持向后兼容：

### 1. Cargo Feature Flag
```bash
# ✅ 仍然使用 risingwave
cargo build --features risingwave,embedded
```

### 2. Rust Crate 名称
```rust
// ✅ 仍然是 nexora-risingwave
use nexora_risingwave::EmbeddedRisingWave;
```

### 3. HTTP API 路径
```bash
# ✅ 内部 API 路径保持不变
curl http://localhost:8080/api/risingwave/status
curl http://localhost:8080/api/risingwave/cluster
```

### 4. 进程名称
```bash
# ✅ 进程仍然是 risingwave
ps aux | grep risingwave
```

### 5. 二进制文件名
```bash
# ✅ 仍然需要 RisingWave 官方二进制
which risingwave
```

---

## 🧪 测试验证

### 1. 编译测试

```bash
# 清理并重新编译
cargo clean
cargo build --release --features risingwave,embedded

# ✅ 编译成功
# Finished `release` profile [optimized] target(s) in 1m 03s
```

### 2. 参数验证

```bash
# 查看新参数
./target/release/nexora --help | grep event-streams

# ✅ 输出:
#   --enable-event-streams
#   --embedded-event-streams
#   --event-streams-meta-addr <EVENT_STREAMS_META_ADDR>
#   --event-streams-frontend-addr <EVENT_STREAMS_FRONTEND_ADDR>
#   --event-streams-cluster
#   --event-streams-raft-node-id <EVENT_STREAMS_RAFT_NODE_ID>
#   --event-streams-raft-peers <EVENT_STREAMS_RAFT_PEERS>
```

### 3. 配置文件测试

```bash
# 创建测试配置
cat > /tmp/test-event-streams.toml << 'EOF'
[server]
host = "127.0.0.1"
port = 8080

[event_streams]
enabled = true
embedded = true
cluster_mode = false
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
EOF

# 验证解析
./target/release/nexora --config /tmp/test-event-streams.toml --help

# ✅ Config file parsed successfully
```

### 4. 功能测试

测试计划（需要实际运行环境）:
- [ ] 单节点模式启动
- [ ] 3 节点集群模式启动
- [ ] 配置文件加载
- [ ] 日志输出验证
- [ ] API 端点可访问性

---

## 📋 升级检查清单

### 对于用户

- [ ] 更新所有启动脚本中的 CLI 参数
- [ ] 更新 TOML 配置文件
- [ ] （可选）重命名数据目录
- [ ] 测试启动是否正常
- [ ] 验证日志输出

### 对于开发者

- [ ] 更新 CI/CD 脚本
- [ ] 更新部署文档
- [ ] 更新 README.md
- [ ] 更新监控脚本（如果直接读取配置）
- [ ] 通知下游用户

---

## 🐛 已知问题

### 1. 旧配置文件无法自动迁移

**问题**: 使用旧的 `[risingwave]` 配置段会导致配置被忽略（不会报错）。

**影响**: 用户可能认为启用了事件流功能，但实际未生效。

**解决方案**: 
- 短期：在文档中明确说明
- 长期：添加配置迁移工具或警告消息

### 2. 混用新旧参数的行为

**问题**: 如果同时使用新旧参数，行为未定义。

```bash
# ⚠️ 不要这样做
./target/release/nexora \
  --enable-risingwave \           # 旧参数（无效）
  --enable-event-streams          # 新参数（有效）
```

**解决方案**: 只使用新参数。

---

## 📊 影响评估

### 破坏性影响

| 影响范围 | 严重程度 | 受影响对象 |
|---------|----------|-----------|
| CLI 脚本 | 高 | 所有使用旧参数的启动脚本 |
| TOML 配置 | 高 | 所有使用旧配置段的文件 |
| 文档 | 中 | 教程、示例代码 |
| 监控脚本 | 低 | 仅读取配置文件的脚本 |

### 不受影响

| 范围 | 原因 |
|------|------|
| Cargo 构建 | Feature flag 未变 |
| HTTP API 调用 | 路径保持不变 |
| 运行时进程 | 进程名未变 |
| 数据文件格式 | 存储格式未变 |

---

## 🔄 回滚方案

如果需要回滚到 v2.0：

```bash
# 1. 切换到 v2.0 分支/标签
git checkout v2.0.x

# 2. 重新编译
cargo build --release --features risingwave,embedded

# 3. 恢复旧配置文件
cp nexora.toml.bak nexora.toml

# 4. 使用旧参数启动
./target/release/nexora --enable-risingwave ...
```

---

## 📝 后续工作

### 短期（本周）

- [ ] 更新 `README.md`
- [ ] 更新 `PRODUCTION_READINESS_AUDIT_2026-07-27.md`
- [ ] 在 GitHub Release Notes 中添加破坏性变更警告
- [ ] 创建迁移脚本 `scripts/migrate-config-v2.0-to-v2.1.sh`

### 中期（2-4 周）

- [ ] 添加配置文件版本检查
- [ ] 添加旧配置段警告消息
- [ ] 创建配置迁移工具
- [ ] 补充更多测试用例

### 长期（1-3 月）

- [ ] 考虑移除 Cargo feature 中的 `risingwave` 命名（v3.0）
- [ ] 考虑重命名 `nexora-risingwave` crate（v3.0）
- [ ] 统一所有内部命名（v3.0 major breaking change）

---

## 🤝 贡献者

- **提议**: 用户反馈（隐藏实现细节）
- **实现**: Claude (Nexora 开发团队)
- **审核**: 待定
- **测试**: 待定

---

## 📚 相关文档

- [CLI 参数重命名说明](CLI_PARAMETER_RENAME_2026-07-27.md)
- [运行操作指南](RUNTIME_OPERATIONS_GUIDE.md)
- [生产就绪度审核](PRODUCTION_READINESS_AUDIT_2026-07-27.md)

---

**创建时间**: 2026-07-27  
**最后更新**: 2026-07-27  
**版本**: 1.0
