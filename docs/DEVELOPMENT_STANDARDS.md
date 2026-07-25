# Nexora 开发规范与纪律

> **目标**: 防止代码质量退化，避免屎山积累，保持生产级可维护性

**强制执行**: 所有规范通过自动化工具检查，CI 失败 → PR 不可合并

---

## 🚦 代码质量门禁 (CI 强制)

### 1. Formatting (零容忍)
```bash
# 本地开发
cargo fmt --all

# CI 检查 (必须通过)
cargo fmt --all --check
```

**规则**:
- ❌ **禁止** 提交未格式化代码
- ✅ 推荐配置 IDE 保存时自动 fmt (见 `.vscode/settings.json`)

---

### 2. Clippy (警告即错误)
```bash
# 本地开发 (提交前必跑)
cargo clippy --all-targets -- -D warnings

# 修复建议 (非破坏性)
cargo clippy --fix --allow-dirty --all-targets
```

**规则**:
- ❌ **禁止** 有任何 clippy warnings (等同编译错误)
- ✅ 允许的例外必须显式标注且注释说明原因:
  ```rust
  #[allow(dead_code)] // PG-wire auth stub, reserved for SCRAM-SHA-256
  struct UuidSalt(uuid::Uuid);
  ```
- ⚠️ **禁止滥用 `#[allow]`** — 每个 allow 必须有合理理由

**常见 clippy 违规及修复** (基于本次清理):

| 违规类型 | ❌ 错误写法 | ✅ 正确写法 |
|---------|-----------|-----------|
| Assertion | `assert_eq!(x, true)` | `assert!(x)` |
| File create | `.create(true).write(true)` | `.create(true).truncate(true).write(true)` |
| Error handling | `if x.is_err() { x.unwrap_err() }` | `if let Err(e) = x { e.to_string() }` |
| Test 位置 | 测试模块在文件中间 | 测试模块在文件末尾 |
| 复杂类型 | `Arc<RwLock<HashMap<K, (V1, V2)>>>` | `type MyMap = HashMap<K, (V1, V2)>;` 再用 `Arc<RwLock<MyMap>>` |

---

### 3. 测试覆盖 (分层验证)

#### 最小门禁 (CI 必须通过)
```bash
# 所有 lib 测试必须通过
cargo test --workspace --lib --no-fail-fast

# 集成测试必须通过 (feature-gated)
cargo test --workspace --test '*' --no-fail-fast
```

**规则**:
- ❌ **禁止** 提交导致测试失败的代码
- ❌ **禁止** 注释掉失败的测试 (修复或删除功能)
- ✅ 新功能必须有对应测试 (见测试分层要求)

#### 测试分层要求

| 层级 | 类型 | 覆盖范围 | 示例 |
|------|------|---------|------|
| **L1 单元** | `#[test]` in `src/` | 纯函数、数据结构、算法 | `node_task.rs::test_event_time_lww()` |
| **L2 集成** | `tests/*.rs` | 跨模块协作、端到端流程 | `tests/wal_crash_recovery.rs` |
| **L3 回归** | 生产 bug 复现 | 每个 P0/P1 bug 必有测试 | `tests/test_issue_123_applied_index.rs` |

**强制要求**:
1. **新功能** → 至少 L1 + L2 测试
2. **Bug 修复** → 必须有 L3 回归测试 (先写失败测试,再修复)
3. **重构** → 测试通过率不得下降

---

## 📝 提交规范 (Conventional Commits)

### 格式
```
<type>(<scope>): <subject>

<body>

<footer>
```

### Type 分类 (严格执行)

| Type | 用途 | 示例 |
|------|------|------|
| `feat` | 新功能 | `feat(stream): add event-time watermark support` |
| `fix` | Bug 修复 | `fix(raft): persist applied_index before ack` |
| `perf` | 性能优化 | `perf(wal): batch fsync in group commit` |
| `refactor` | 重构 (无功能变更) | `refactor(shard): extract wake_node logic` |
| `test` | 测试相关 | `test(core): add regression test for #123` |
| `style` | 格式/风格 | `style: fix clippy warnings` |
| `docs` | 文档 | `docs: update ROADMAP with A1.2 progress` |
| `chore` | 构建/工具 | `chore: update Cargo dependencies` |

**规则**:
- ✅ Subject 用**现在时**祈使句 ("add" 而非 "added")
- ✅ Subject **不超过 72 字符**
- ✅ Body 说明 **为什么** 改 (what/how 看 diff)
- ✅ Footer 关联 issue (`Closes #123`, `Refs #456`)
- ❌ **禁止** 无意义 message ("fix", "update", "wip")

### Commit 粒度

**✅ 一个 commit = 一个原子变更**
```bash
# 好例子 (原子,可回滚)
git log --oneline
a1b2c3d feat(lww): add per-property event_time tracking
b2c3d4e feat(lww): implement event-time LWW in commit_operations  
c3d4e5f test(lww): add out-of-order write regression tests

# ❌ 坏例子 (杂糅,难回滚)
x1y2z3w feat: add lww and fix clippy and update docs
```

---

## 🏗️ 架构约束 (代码审查重点)

### 1. 错误处理 (零 panic 生产代码)

**规则**:
- ❌ **禁止** 在 `src/` 里用 `.unwrap()` / `.expect()`
  - 例外: 测试代码 (`#[cfg(test)]`)
  - 例外: 启动时配置验证 (main.rs init 阶段)
- ✅ 用 `Result<T, E>` + `?` 传播错误
- ✅ 不可恢复错误用 `thiserror::Error`

```rust
// ❌ 生产代码禁止
let value = map.get(&key).unwrap(); 

// ✅ 正确写法
let value = map.get(&key)
    .ok_or_else(|| NodeError::KeyNotFound(key.clone()))?;
```

### 2. 并发安全 (Actor 模型纪律)

**Nexora 采用 Actor 模型** (每个 NodeTask 独占状态):
- ✅ **只通过消息** 修改节点状态 (`NodeCommand`)
- ❌ **禁止** 跨 actor 直接访问可变状态
- ✅ 共享只读投影用 `Arc<DashMap<..., Arc<NodeReadState>>>`

**并发原语使用规范**:
- `Mutex<T>` — 临界区 **< 1ms** (不可跨 `.await`)
- `RwLock<T>` — 读多写少,读不阻塞读
- `DashMap<K, V>` — 高并发 map (免锁热路径)
- `Arc<AtomicU64>` — 计数器/metrics

### 3. 异步纪律 (避免阻塞 Runtime)

**规则**:
- ❌ **禁止** 在 async 函数里调用阻塞 IO (`std::fs`, `std::net`)
- ✅ 用 `tokio::fs` / `tokio::net`
- ❌ **禁止** CPU 密集计算占住 async task
- ✅ CPU 密集 → `tokio::task::spawn_blocking`

```rust
// ❌ 阻塞 runtime
async fn bad() {
    let data = std::fs::read("file.txt").unwrap(); // 💥
}

// ✅ 非阻塞
async fn good() {
    let data = tokio::fs::read("file.txt").await?;
}
```

### 4. 依赖管理

**新增依赖的审查清单**:
- [ ] 是否有更轻量替代? (避免重复功能)
- [ ] 是否活跃维护? (最近 6 个月有更新)
- [ ] License 兼容? (Apache-2.0 / MIT / BSD)
- [ ] 安全审计? (`cargo audit` 无高危漏洞)

**禁止依赖**:
- ❌ Unmaintained crates (>1 年无更新)
- ❌ 单一作者无 bus factor
- ❌ GPL/AGPL (传染性 license)

---

## 🔍 Code Review Checklist (每个 PR 必查)

### 提交前自查 (作者责任)

```bash
# 1. 格式化
cargo fmt --all

# 2. Clippy
cargo clippy --all-targets -- -D warnings

# 3. 测试
cargo test --workspace

# 4. 本地 CI (全量验证)
./scripts/local-ci.sh

# 5. 提交信息
git log --oneline -1  # 检查格式
```

### Review 关注点 (Reviewer 责任)

#### 🛡️ 正确性 (P0)
- [ ] 并发安全: 无数据竞争、无死锁
- [ ] 错误处理: 所有 Result 被处理,无 unwrap
- [ ] 边界条件: 空集合、零值、溢出
- [ ] 资源清理: File/Socket/Task 正确关闭

#### 🎯 测试 (P0)
- [ ] 新功能有测试 (L1+L2)
- [ ] Bug 修复有回归测试 (L3)
- [ ] 测试覆盖 happy path + edge cases

#### 📐 设计 (P1)
- [ ] 符合现有架构模式 (Actor/Async)
- [ ] 模块边界清晰,职责单一
- [ ] 无循环依赖

#### 🧹 可维护性 (P1)
- [ ] 命名清晰 (函数名说明意图)
- [ ] 注释说明 **为什么** (不说明 what)
- [ ] 复杂逻辑有文档/示例

---

## 🤖 自动化配置 (开箱即用)

### `.vscode/settings.json` (推荐)
```json
{
  "rust-analyzer.checkOnSave.command": "clippy",
  "rust-analyzer.checkOnSave.extraArgs": ["--all-targets", "--", "-D", "warnings"],
  "editor.formatOnSave": true,
  "editor.defaultFormatter": "rust-lang.rust-analyzer"
}
```

### `clippy.toml` (工作区级)
```toml
# 见根目录 clippy.toml
# 已配置: 禁止 unwrap, 强制文档注释等
```

### `.editorconfig`
```ini
root = true

[*]
end_of_line = lf
insert_final_newline = true
charset = utf-8

[*.rs]
indent_style = space
indent_size = 4
```

### Git hooks (推荐安装)
```bash
# 安装 pre-commit hook (提交前自动检查)
cp scripts/git-hooks/pre-commit .git/hooks/
chmod +x .git/hooks/pre-commit
```

---

## 📊 质量监控 (定期回顾)

### 周度指标 (Team Review)
- 新增 warnings 数量 (应为 0)
- 测试覆盖率变化
- clippy fixes 数量 (应递减)
- 平均 PR review 轮次

### 月度审计
- `cargo audit` 安全漏洞检查
- 依赖更新 (`cargo outdated`)
- Dead code 清理 (`cargo +nightly udeps`)

---

## 🚨 红线 (触犯直接拒绝)

以下行为 **PR 直接 Close,不予 Review**:

1. ❌ CI 失败 (fmt/clippy/test 任一不通过)
2. ❌ 生产代码含 `unwrap()` 无 `#[cfg(test)]` 保护
3. ❌ 提交信息不符合 Conventional Commits
4. ❌ 无测试的新功能
5. ❌ 回归 bug 无回归测试
6. ❌ 大规模无关改动 (格式化应独立 PR)

---

## 📚 学习资源

- **Rust 最佳实践**: [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- **Async 编程**: [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- **错误处理**: [thiserror book](https://docs.rs/thiserror)
- **测试策略**: [Rust Testing](https://doc.rust-lang.org/book/ch11-00-testing.html)

---

## 🔄 规范演进

本文档由团队共同维护:
- 提案修改 → 提 PR 到本文档
- 重大变更 → Team 讨论后合并
- 每季度回顾一次

**当前版本**: v1.0 (2026-07-18)  
**下次回顾**: 2026-10-18
