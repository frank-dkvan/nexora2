# 文档维护指南

本文档提供 Nexora 2.0 文档维护的最佳实践和自动化工具。

---

## 📋 文档结构

```
nexora2/
├── README.md                          # 项目主页（中英双语）
├── QUICKSTART.md                      # 快速开始
├── CHANGELOG.md                       # 变更日志
├── docs/
│   ├── architecture/                  # 架构文档
│   │   ├── STORAGE_ARCHITECTURE.md   # 存储架构详解 ⭐ NEW
│   │   ├── API_ENDPOINT_REFERENCE.md # API 端点参考 ⭐ NEW
│   │   └── DISTRIBUTED_EVENT_WRITE_DESIGN.md
│   ├── api-tutorial.md               # API 使用教程
│   ├── cluster-ops.md                # 集群运维
│   ├── backup-restore.md             # 备份恢复
│   └── ops/
│       ├── RUNBOOK.md                # 运维手册
│       └── SECURITY.md               # 安全配置
├── scripts/
│   └── validate-docs.sh              # 文档验证脚本 ⭐ NEW
└── .github/workflows/
    └── validate-docs.yml             # CI 文档验证 ⭐ NEW
```

---

## 🔄 文档同步规则

### 规则 1: API 路径迁移完成

✅ **已完成**: 所有 `/api/v2/*` 路径已迁移至 `/api/*`

**检查方法**:
```bash
# 不应有任何输出
grep -r "api/v2" . --include="*.md" | grep -v ".git"
```

### 规则 2: CLI 参数与代码一致

📍 **真实来源**: `crates/nexora-app/src/main.rs` 中的 `#[arg(long)]` 定义

**常见参数**:
- `--host`, `--port` - 基础配置
- `--storage-backend` - 图数据存储 (memory/local/s3)
- `--event-store-backend` - 事件日志存储 (local/s3/rest)
- `--s3-endpoint`, `--s3-bucket` - S3 配置
- `--event-store-s3-*` - 事件日志专用 S3 配置
- `--cluster`, `--node-id` - 集群模式

**验证方法**:
```bash
# 提取代码中的参数
grep '#\[arg(long' crates/nexora-app/src/main.rs -A 1 | grep -E '^\s*[a-z_]+:' | sed 's/^\s*//; s/:.*$//'

# 对比文档中提到的参数
grep -rho -- '--[a-z][-a-z0-9]*' README.md | sort -u
```

### 规则 3: Feature Flags 准确性

📍 **真实来源**: `crates/nexora-app/Cargo.toml` 的 `[features]` 节

**已定义的 features**:
```toml
[features]
default = []
event-first = ["dep:nexora-eventlog", "nexora-eventlog/olap", "nexora-pgwire/event-first"]
kafka = ["nexora-stream/kafka"]
mqtt = ["nexora-stream/mqtt"]
websocket = ["nexora-stream/websocket"]
kinesis = ["nexora-stream/kinesis", "rocksdb-offsets"]
zenoh = ["nexora-stream/zenoh"]
otel = ["dep:opentelemetry", ...]
```

**文档要求**:
- ✅ `event-first` 必须在 README.md 中说明
- ✅ 编译示例必须包含 `cargo build --features event-first`
- ✅ 区分默认功能 vs 可选功能

### 规则 4: Rust 版本一致性

📍 **真实来源**: `Cargo.toml` 中的 `rust-version = "1.75"`

**必须同步的位置**:
- `README.md` - Badge 和先决条件
- `CONTRIBUTING.md` - 开发环境要求
- `docs/api-tutorial.md` - 先决条件
- `docs/cluster-ops.md` - Dockerfile

**检查方法**:
```bash
# 提取 Cargo.toml 版本
grep 'rust-version' Cargo.toml | head -1

# 检查所有文档中的版本
grep -rn "1\.[0-9][0-9]+" --include="*.md" | grep -i rust
```

---

## 🛠️ 自动化工具

### 1. 本地验证脚本

**脚本**: `scripts/validate-docs.sh`

**功能**:
- ✅ 检查废弃的 `/api/v2/` 路径
- ✅ 验证文档中的 API 端点存在于代码中
- ✅ 验证 CLI 参数一致性
- ⚠️ 警告文档中提到但代码不存在的参数

**使用方法**:
```bash
./scripts/validate-docs.sh
```

**预期输出**:
```
==================================================
  Nexora 2.0 Documentation Consistency Checker
==================================================

📡 [1/5] Extracting API endpoints from code...
   Found 77 unique routes
📚 [2/5] Scanning documented API endpoints...
   Found 125 documented endpoints
🔍 [3/5] Checking for deprecated /api/v2/ paths...
✅ No /api/v2/ paths found
🔗 [4/5] Validating documented endpoints against code...
✅ All documented endpoints exist in code
⚙️  [5/5] Validating CLI parameters...
   Code defines 45 CLI parameters
   Docs mention 38 CLI parameters
✅ All documented CLI parameters are valid

==================================================
                   SUMMARY
==================================================

✅ All checks passed!
```

### 2. GitHub Actions CI

**工作流**: `.github/workflows/validate-docs.yml`

**触发条件**:
- Pull Request 修改了文档或 `main.rs`
- Push 到 main 分支

**检查项**:
1. 检查废弃的 `/api/v2/` 路径
2. 验证核心 API 端点已文档化
3. 验证核心 CLI 参数已文档化
4. 验证 Rust 版本一致性
5. 验证 feature flags 一致性

**查看结果**:
在 GitHub PR 页面查看 "Validate Documentation" 检查状态。

---

## 📝 文档编写规范

### API 端点文档

**必须包含**:
1. HTTP 方法和路径
2. 请求示例（包含完整的 curl 命令）
3. 响应示例（JSON 格式）
4. 认证要求（如需要）

**示例**:
```markdown
### 创建 Standing Query

\`\`\`http
POST /api/standing-queries
Content-Type: application/json

{
  "name": "high-value-users",
  "query": "MATCH (u:User) WHERE u.value > 1000 RETURN u"
}
\`\`\`

**响应**:
\`\`\`json
{
  "id": "sq-abc123",
  "name": "high-value-users",
  "status": "active"
}
\`\`\`

**认证**: 需要 `admin` 角色
```

### CLI 参数文档

**必须包含**:
1. 参数名称（使用 `--kebab-case` 格式）
2. 参数说明
3. 默认值（如有）
4. 示例

**示例**:
```markdown
### Event Store Backend

配置事件日志存储后端。

\`\`\`bash
--event-store-backend <TYPE>
\`\`\`

**可用值**:
- `local` (默认) - 本地文件系统 + SQLite catalog
- `s3` - S3 存储 + 本地 SQLite catalog
- `rest` - REST catalog + S3（推荐生产使用）

**示例**:
\`\`\`bash
./nexora --event-store-backend rest \
  --event-store-rest-uri http://lakekeeper:8181/catalog
\`\`\`
```

### 架构文档

**必须包含**:
1. 清晰的架构图（ASCII 或 Mermaid）
2. 各组件的职责说明
3. 数据流向
4. 配置示例

**示例**: 参考 `docs/architecture/STORAGE_ARCHITECTURE.md`

---

## 🔍 常见问题处理

### Q1: 添加新的 API 端点后如何更新文档？

**步骤**:
1. 在 `docs/architecture/API_ENDPOINT_REFERENCE.md` 中添加新端点
2. 在 `docs/api-tutorial.md` 相应章节添加使用示例
3. 如果是核心功能，在 `README.md` 中提及
4. 运行 `./scripts/validate-docs.sh` 验证

### Q2: 修改 CLI 参数名称如何避免破坏文档？

**步骤**:
1. 先在代码中修改 `main.rs`
2. 全局搜索旧参数名：`grep -r "old-param-name" . --include="*.md"`
3. 批量替换：`find . -name "*.md" -exec sed -i '' 's/old-param-name/new-param-name/g' {} \;`
4. 运行验证脚本确认

### Q3: 如何确保多个文档中的示例保持一致？

**建议**:
- 对于复杂的多步骤示例，在一个地方（如 `docs/api-tutorial.md`）详细说明
- 其他文档引用该章节，而不是重复示例
- 使用统一的示例数据（如固定的用户名、节点 ID）

---

## 🎯 修复文档不一致的流程

### 发现不一致

**触发方式**:
1. CI 检查失败
2. 运行 `validate-docs.sh` 报错
3. 用户报告文档错误

### 修复步骤

1. **定位真实来源**
   ```bash
   # API 端点
   grep -n "\.route(" crates/nexora-app/src/main.rs
   
   # CLI 参数
   grep -n "#\[arg(long" crates/nexora-app/src/main.rs
   
   # Feature flags
   grep -A 20 "^\[features\]" crates/nexora-app/Cargo.toml
   ```

2. **批量修复文档**
   ```bash
   # 示例：替换所有文档中的错误参数
   find . -name "*.md" -exec sed -i '' 's/--old-param/--new-param/g' {} \;
   ```

3. **验证修复**
   ```bash
   ./scripts/validate-docs.sh
   ```

4. **提交修复**
   ```bash
   git add .
   git commit -m "docs: Fix inconsistency between docs and code
   
   - Update API endpoint paths
   - Correct CLI parameter names
   - Sync Rust version requirements"
   ```

---

## 📊 文档健康度指标

### 当前状态 (2026-07-25)

| 指标 | 状态 | 说明 |
|------|------|------|
| API 端点覆盖率 | ✅ 95% | 77 个端点，72 个已文档化 |
| CLI 参数一致性 | ✅ 100% | 所有参数与代码匹配 |
| Rust 版本一致性 | ✅ 100% | 所有文档使用 1.75 |
| Feature flags 一致性 | ✅ 100% | event-first 等已准确说明 |
| `/api/v2/` 清理 | ✅ 100% | 无遗留的旧路径 |

### 维护目标

- 🎯 保持 API 端点覆盖率 > 90%
- 🎯 所有 PR 必须通过文档验证 CI
- 🎯 每月审查一次文档健康度
- 🎯 用户报告的文档问题 24 小时内修复

---

## 🤝 贡献文档

### Pull Request 检查清单

提交文档修改的 PR 前，请确认：

- [ ] 运行 `./scripts/validate-docs.sh` 通过
- [ ] API 端点使用 `/api/` 而非 `/api/v2/`
- [ ] CLI 参数名称与 `main.rs` 一致
- [ ] 示例代码可以直接运行
- [ ] Rust 版本号与 `Cargo.toml` 一致
- [ ] 如果修改了架构，更新了架构文档

### 审查重点

审查文档 PR 时，重点检查：

1. **准确性** - 文档描述与代码实现一致
2. **完整性** - 新功能有完整的使用说明和示例
3. **可维护性** - 避免重复内容，使用引用
4. **用户友好** - 示例清晰，步骤具体

---

## 📚 相关资源

- [文档一致性报告](../DOCUMENTATION_CONSISTENCY_REPORT.md) - 最新验证结果
- [存储架构](architecture/STORAGE_ARCHITECTURE.md) - 双层存储详解
- [API 端点参考](architecture/API_ENDPOINT_REFERENCE.md) - 完整端点列表
- [变更日志](../CHANGELOG.md) - API 变更历史