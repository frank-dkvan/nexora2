# 新开发者快速上手

> 5 分钟从零到第一个 PR

## 🚀 环境准备

### 必需工具
```bash
# 1. Rust (1.88+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. 基础组件
rustup component add rustfmt clippy

# 3. Git hooks (推荐)
cp scripts/git-hooks/pre-commit .git/hooks/
chmod +x .git/hooks/pre-commit
```

### IDE 配置 (VSCode 推荐)
```bash
# 安装扩展
code --install-extension rust-lang.rust-analyzer
code --install-extension tamasfe.even-better-toml

# VSCode 会自动读取 .vscode/settings.json (已配置好)
```

---

## 📝 第一次提交

### 1. 创建分支
```bash
git checkout -b feat/my-feature
# 或: fix/bug-123, refactor/cleanup-foo
```

### 2. 修改代码
```rust
// 示例: 添加一个工具函数
pub fn my_function() -> Result<(), Error> {
    // 实现...
    Ok(())
}

// ✅ 记得加测试
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_function() {
        assert!(my_function().is_ok());
    }
}
```

### 3. 提交前检查 (必须)
```bash
# 自动格式化
cargo fmt --all

# 检查警告 (必须零警告)
cargo clippy --all-targets -- -D warnings

# 跑测试
cargo test --workspace --lib

# (可选) 完整 CI
./scripts/local-ci.sh
```

### 4. 提交
```bash
git add .
git commit -m "feat(core): add my_function utility

This function does X to solve Y problem.

Closes #123"

# 如果安装了 pre-commit hook,会自动检查
```

### 5. 推送并创建 PR
```bash
git push origin feat/my-feature

# 使用 gh CLI (推荐)
gh pr create --fill

# 或访问 GitHub 页面手动创建
```

---

## 🎯 常见任务速查

### 修复 Clippy 警告
```bash
# 查看详细警告
cargo clippy --all-targets -- -D warnings

# 自动修复 (部分)
cargo clippy --fix --allow-dirty --all-targets

# 逐个 crate 修复
cargo clippy -p nexora-core -- -D warnings
```

### 运行特定测试
```bash
# 单个测试
cargo test test_my_function

# 单个 crate
cargo test -p nexora-core

# 集成测试
cargo test --test integration_test_name

# 显示 println! 输出
cargo test -- --nocapture
```

### 添加依赖
```bash
# 添加到 workspace 依赖
vim Cargo.toml  # 编辑 [workspace.dependencies]

# 在具体 crate 引用
cd crates/nexora-core
cargo add serde --features derive
```

### 查看文档
```bash
# 生成并打开本地文档
cargo doc --open --no-deps
```

---

## 🔍 代码审查常见问题

### ❌ 常见错误

#### 1. Unwrap in 生产代码
```rust
// ❌ 错误
let value = map.get(&key).unwrap();

// ✅ 正确
let value = map.get(&key)
    .ok_or_else(|| Error::KeyNotFound(key.clone()))?;
```

#### 2. 阻塞 async runtime
```rust
// ❌ 错误 (阻塞)
async fn bad() {
    let data = std::fs::read("file.txt").unwrap();
}

// ✅ 正确 (非阻塞)
async fn good() {
    let data = tokio::fs::read("file.txt").await?;
}
```

#### 3. 无测试的新功能
```rust
// ❌ 只写实现,不写测试

// ✅ 实现 + 测试
pub fn new_feature() { ... }

#[cfg(test)]
mod tests {
    #[test]
    fn test_new_feature() { ... }
}
```

#### 4. 提交信息不规范
```bash
# ❌ 错误
git commit -m "fix"
git commit -m "update code"

# ✅ 正确
git commit -m "fix(raft): persist applied_index before ack

Raft applied_index was not persisted, causing replay
of already-applied entries after restart.

Closes #123"
```

---

## 📚 必读文档

1. **[DEVELOPMENT_STANDARDS.md](DEVELOPMENT_STANDARDS.md)** — 开发规范 (10 分钟)
2. **[ARCHITECTURE.md](ARCHITECTURE.md)** — 架构概览 (30 分钟)
3. **[CONTRIBUTING.md](../CONTRIBUTING.md)** — 贡献指南 (5 分钟)

---

## 💡 开发技巧

### 快速定位代码
```bash
# 搜索函数定义
rg "fn my_function"

# 搜索结构体
rg "struct NodeTask"

# 搜索测试
rg "#\[test\]" --type rust
```

### Debug 技巧
```rust
// 打印调试
dbg!(variable);

// 条件断点 (配合 IDE)
if some_condition {
    println!("Debug: {:?}", value);
}

// 使用 tracing
tracing::debug!(node_id = %qid, "Processing mutation");
```

### 性能分析
```bash
# Benchmark
cargo bench

# Flamegraph
cargo install flamegraph
cargo flamegraph --bin nexora
```

---

## 🆘 遇到问题?

### CI 失败
1. 本地跑 `./scripts/local-ci.sh` 复现
2. 查看具体失败的 job (fmt/clippy/test)
3. 逐个修复

### 测试失败
1. 本地复现: `cargo test test_name -- --nocapture`
2. 查看测试日志
3. 如果是并发问题,试试 `cargo test -- --test-threads=1`

### Merge 冲突
```bash
# 拉取最新 main
git fetch origin main

# Rebase (推荐,保持线性历史)
git rebase origin/main

# 解决冲突后
git add .
git rebase --continue
```

---

## ✨ 进阶主题

- **Actor 模型**: 阅读 `nexora-core/src/graph/node_task.rs`
- **WAL 机制**: 阅读 `nexora-core/src/wal/`
- **Raft 共识**: 阅读 `nexora-zenoh/src/raft_handler.rs`
- **查询优化**: 阅读 `nexora-cypher/src/optimizer/`

---

**准备好了?** 去 [GitHub Issues](https://github.com/frank-dkvan/nexora/issues) 挑一个 `good-first-issue` 开始吧! 🚀
