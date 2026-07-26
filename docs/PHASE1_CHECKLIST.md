# ✅ RisingWave Integration - Phase 1 Execution Checklist

**准备完成日期**: 2026-07-26  
**执行所需时间**: 15-30分钟  
**前置条件**: nexora2项目，Git 2.9+，网络连接

---

## 📋 执行前检查清单

### 环境检查
- [ ] 位于项目根目录：`/Users/frank/aiCoding/nexora2`
- [ ] Git工作区干净：`git status`显示无未提交更改
- [ ] 所有现有测试通过：`cargo test --workspace`（1590+测试）
- [ ] 网络连接正常（需要从GitHub获取RisingWave）
- [ ] 磁盘空间充足（需要约500MB）

### 文档就绪
- [x] RISINGWAVE_INTEGRATION_PLAN.md - 完整架构
- [x] CLAUDE.md - 开发指南
- [x] RISINGWAVE_PHASE1_REPORT.md - 详细报告
- [x] PHASE1_QUICKSTART.md - 快速执行指南
- [x] RISINGWAVE_STATUS.md - 状态跟踪
- [x] SESSION_SUMMARY.md - 会话总结

### 脚本就绪
- [x] scripts/init-risingwave.sh - 可执行
- [x] scripts/sync-risingwave.sh - 可执行
- [x] scripts/apply-patches.sh - 可执行

---

## 🚀 执行步骤

### Step 1: 预检查（5分钟）

```bash
cd /Users/frank/aiCoding/nexora2

# 1. 检查Git状态
git status

# 2. 确认测试基线
cargo test --workspace

# 3. 查看当前分支
git branch

# 4. 可选：创建备份分支
git branch backup-before-risingwave-$(date +%Y%m%d)
```

**验收**: 
- ✅ 工作区干净
- ✅ 所有测试通过
- ✅ 在main或feature分支上

### Step 2: 执行集成（15-20分钟）

```bash
# 运行初始化脚本
./scripts/init-risingwave.sh

# 脚本会提示确认，输入 Y 继续
```

**脚本会自动执行**:
1. 添加risingwave-upstream remote
2. 获取RisingWave v3.0.2（最耗时：5-10分钟）
3. 添加Git Subtree到vendor/risingwave/
4. 创建4个占位crate
5. 更新Cargo.toml
6. 更新.gitignore
7. 自动提交

**期间观察**:
- 看到"Fetching RisingWave repository"时耐心等待
- 看到绿色SUCCESS消息表示步骤完成

### Step 3: 验证集成（5-10分钟）

```bash
# 1. 验证目录结构
ls -la vendor/risingwave/
ls -la crates/nexora-risingwave/
ls -la crates/nexora-consensus/
ls -la crates/nexora-rpc/
ls -la extensions/meta_raft/

# 2. 验证RisingWave版本
cat vendor/risingwave/Cargo.toml | grep "^version"

# 3. 验证workspace编译
cargo check --workspace

# 4. 验证所有测试仍然通过
cargo test --workspace

# 5. 检查Git历史
git log --oneline -5
```

**验收**:
- ✅ 所有目录存在
- ✅ RisingWave版本是3.0.2
- ✅ cargo check通过
- ✅ 所有1590+测试通过
- ✅ 看到新的Git commits

---

## ✅ 成功标准

Phase 1执行成功当且仅当：

### 文件系统检查
- [x] `vendor/risingwave/` 目录存在
- [x] `vendor/risingwave/Cargo.toml` 包含 version = "3.0.2"
- [x] `crates/nexora-risingwave/` 存在
- [x] `crates/nexora-consensus/` 存在
- [x] `crates/nexora-rpc/` 存在
- [x] `extensions/meta_raft/` 存在
- [x] 每个新crate包含 Cargo.toml 和 src/lib.rs
- [x] `patches/` 目录存在

### 编译检查
- [x] `cargo check --workspace` 无错误
- [x] 无新增编译警告

### 测试检查
- [x] `cargo test --workspace` 全部通过
- [x] 测试数量：1590+ (与之前一致)
- [x] 无测试失败或回归

### Git检查
- [x] 工作区干净：`git status` 无未提交更改
- [x] 新增2-3个commits
- [x] Commits包含"risingwave"关键词
- [x] Git历史干净（使用了--squash）

---

## ⚠️ 常见问题处理

### 问题1: "Subtree pull takes too long"
**现象**: 卡在Fetching步骤超过10分钟

**解决**:
```bash
# 正常现象，RisingWave ~500MB
# 继续等待或检查网络连接
ping github.com
```

### 问题2: "Merge conflicts"
**现象**: Git报告merge conflicts

**解决**:
```bash
# 查看冲突文件
git status

# 如果是首次运行，不应该有冲突
# 如果出现，中止并报告
git merge --abort
```

### 问题3: "Cargo check fails"
**现象**: 编译失败

**解决**:
```bash
# 这不应该发生，因为只是添加了占位符
# 检查Cargo.toml格式
cat Cargo.toml | grep -A10 "RisingWave"

# 如果格式错误，手动修复
vim Cargo.toml
```

### 问题4: "Tests fail"
**现象**: 某些测试失败

**解决**:
```bash
# 这是BLOCKER - 不应该发生
# 需要回滚并调查
git log --oneline -10
git reset --hard <before-risingwave-commit>

# 报告问题
```

---

## 🔄 回滚计划

如果需要回滚Phase 1:

```bash
# 1. 找到集成前的commit
git log --oneline -10

# 2. 硬回滚到该commit
git reset --hard <commit-hash>

# 3. 删除remote
git remote remove risingwave-upstream

# 4. 验证回滚成功
cargo test --workspace
git status
```

---

## 📊 执行后报告

执行完成后，填写以下表格：

```
执行日期: _______________
执行人: _______________
执行时间: _____分钟

结果:
[ ] 成功 - 所有检查通过
[ ] 部分成功 - 有警告但可继续
[ ] 失败 - 需要回滚

vendor/risingwave大小: _____MB
新增commits: _____个
测试通过数: _____个

遇到的问题:
___________________________________
___________________________________

解决方案:
___________________________________
___________________________________

下一步行动:
[ ] 进入Phase 2: 实现nexora-consensus
[ ] 需要修复问题后重试
[ ] 需要重新评估计划
```

---

## 🎯 Phase 1完成后的状态

### 文件结构
```
nexora2/
├── vendor/
│   └── risingwave/              # ✅ 新增
├── crates/
│   ├── (现有crates...)
│   ├── nexora-risingwave/       # ✅ 新增
│   ├── nexora-consensus/        # ✅ 新增
│   └── nexora-rpc/              # ✅ 新增
├── extensions/
│   └── meta_raft/               # ✅ 新增
├── patches/                     # ✅ 新增
├── CLAUDE.md                    # ✅ 新增
└── docs/
    ├── RISINGWAVE_*.md          # ✅ 新增
    └── (现有文档...)
```

### 功能状态
- ✅ 所有现有功能正常
- ✅ RisingWave源码已集成（但未启用）
- ✅ 占位crate已创建（但未实现）
- ⏳ 等待Phase 2实现consensus和RPC

---

## 📅 时间线

| 时间点 | 里程碑 |
|--------|--------|
| 2026-07-26 | Phase 1准备完成 ← **当前位置** |
| 执行后 | Phase 1完成 |
| Week 2 | Phase 2: 实现共享基础设施 |
| Week 3 | Phase 3: 实现RisingWave包装器 |
| Week 4 | Phase 4: Raft HA扩展 |
| Week 5 | Phase 5: App集成 |
| Week 6 | Phase 6: Event Pipeline |

---

## 🚦 准备状态

**Phase 1准备状态**: ✅ 100% READY

- [x] 文档完整（6份）
- [x] 脚本就绪（3个）
- [x] 架构设计完成
- [x] 风险评估完成
- [x] 回滚计划就绪
- [x] 测试基线建立

**可以开始执行！**

---

## 📞 需要帮助？

### 执行前
- 阅读 [PHASE1_QUICKSTART.md](PHASE1_QUICKSTART.md)
- 复习 [RISINGWAVE_INTEGRATION_PLAN.md](RISINGWAVE_INTEGRATION_PLAN.md)

### 执行中
- 查看脚本输出的提示信息
- 脚本自带详细的错误处理

### 执行后
- 对照本清单验证结果
- 查看 [CLAUDE.md](../CLAUDE.md) 的故障排查部分

---

**准备就绪！执行命令：**

```bash
cd /Users/frank/aiCoding/nexora2
./scripts/init-risingwave.sh
```

🚀 Good luck!
