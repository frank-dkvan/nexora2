# ✅ Nexora 开发 Checklist

> **打印出来贴在显示器旁边** 📌

---

## 🔥 提交前必查 (3 步,每次必做)

```bash
# 1. 格式化
cargo fmt --all

# 2. 零警告
cargo clippy --all-targets -- -D warnings

# 3. 测试通过
cargo test --workspace --lib
```

**红线**: 任何一项失败 → **不允许提交**

---

## 📝 Commit Message 格式

```
<type>(<scope>): <subject>

<body>

Closes #123
```

**Type**: `feat` | `fix` | `perf` | `refactor` | `test` | `style` | `docs` | `chore`

**示例**:
```
feat(raft): add snapshot compression

Reduces snapshot size by 60% using zstd.

Closes #456
```

---

## 🚫 绝对禁止

- ❌ 生产代码用 `.unwrap()` (测试可以)
- ❌ Async 函数里用 `std::fs` (用 `tokio::fs`)
- ❌ 无测试的新功能
- ❌ 提交 "fix", "update" 之类的无意义 message
- ❌ CI 失败就推送

---

## 📊 Code Review 必查

### 作者自查
- [ ] 所有 `Result` 被处理 (无 unwrap)
- [ ] 新功能有测试 (L1+L2)
- [ ] Bug 修复有回归测试
- [ ] 提交信息符合规范

### Reviewer 查
- [ ] 并发安全 (无数据竞争)
- [ ] 错误处理正确
- [ ] 测试覆盖充分
- [ ] 符合现有架构

---

## 🛠️ 工具安装 (一次性)

```bash
# 1. Git hook (自动检查)
cp scripts/git-hooks/pre-commit .git/hooks/
chmod +x .git/hooks/pre-commit

# 2. VSCode 配置 (自动格式化)
# 已有 .vscode/settings.json,打开项目自动生效
```

---

## 📚 速查文档

| 问题 | 文档 |
|------|------|
| 开发规范全文 | [DEVELOPMENT_STANDARDS.md](DEVELOPMENT_STANDARDS.md) |
| 新人上手 | [DEVELOPER_QUICKSTART.md](DEVELOPER_QUICKSTART.md) |
| 架构理解 | [ARCHITECTURE.md](ARCHITECTURE.md) |
| 贡献流程 | [CONTRIBUTING.md](../CONTRIBUTING.md) |

---

## 🆘 CI 失败怎么办?

```bash
# 本地完整 CI (复现问题)
./scripts/local-ci.sh

# 查看具体错误
# - fmt 失败 → cargo fmt --all
# - clippy 失败 → cargo clippy --all-targets -- -D warnings
# - test 失败 → cargo test <test_name> -- --nocapture
```

---

**记住**: 质量门禁是保护,不是障碍 🛡️  
**目标**: 永远保持 main 分支可发布 ✨
