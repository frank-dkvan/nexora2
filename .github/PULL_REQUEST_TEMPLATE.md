## 📋 变更类型
<!-- 选择一个主要类型 -->
- [ ] `feat`: 新功能
- [ ] `fix`: Bug 修复
- [ ] `perf`: 性能优化
- [ ] `refactor`: 重构(无功能变更)
- [ ] `test`: 测试相关
- [ ] `style`: 代码风格/格式化
- [ ] `docs`: 文档
- [ ] `chore`: 构建/工具

## 🎯 改动概要
<!-- 一句话说明本 PR 做了什么 -->

## 🔗 关联 Issue
<!-- 如有关联 issue,使用 Closes #123 或 Refs #456 -->

## 📝 详细说明
<!-- 解释为什么需要这个改动,而不是怎么改的(代码已经说明了 how) -->

### 改动前
<!-- 当前的问题或限制 -->

### 改动后
<!-- 改动后的行为 -->

## ✅ 测试验证

### 本地验证清单
- [ ] `cargo fmt --all --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo test --workspace --lib` 通过
- [ ] 新功能有对应测试 (或注明 N/A)
- [ ] Bug 修复有回归测试 (或注明 N/A)

### 测试场景
<!-- 描述如何验证本 PR,手动测试步骤或自动化测试覆盖 -->

## 🚨 破坏性变更
<!-- 如果是 breaking change,说明影响范围和迁移方案 -->
- [ ] 无破坏性变更
- [ ] 有破坏性变更 (下方说明):

## 📸 截图/日志
<!-- 如有 UI 变化或关键日志,贴在这里 -->

## 🧐 Review 重点
<!-- 提示 reviewer 重点关注的部分 -->

## 📚 补充资料
<!-- 设计文档、RFC、相关讨论链接 -->

---

<!-- 自动化检查会验证以下项(CI 必须通过): -->
- CI: Format, Clippy, Tests, Security Audit
- Commit Message: Conventional Commits 格式
- PR Title: 与 Commit Message 一致

**提交前请确认**: 所有复选框已勾选,CI 全绿 ✅
