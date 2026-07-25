# 开发规范落地指南 (Team Onboarding)

> **给团队 Leader / 技术负责人**: 如何在团队中推广这套规范

---

## 📅 推广计划 (2 周落地)

### Week 1: 工具配置 + 培训

#### Day 1-2: 基础设施部署
- [ ] 合并 PR #8 (clippy 修复 + 规范文档)
- [ ] 启用 GitHub branch protection rules:
  - ✅ Require status checks to pass (CI jobs)
  - ✅ Require branches to be up to date
  - ✅ Require conversation resolution
- [ ] 配置 GitHub Actions secrets (如需要)

#### Day 3: 团队培训 (1 小时)
**会议议程**:
1. **为什么需要规范** (10 分钟)
   - 回顾最近的质量问题 (本次 30+ clippy warnings)
   - 屎山代码的维护成本
   - 生产事故案例 (如 applied_index 未落盘)

2. **规范速览** (20 分钟)
   - 演示: 提交前 3 步检查 (fmt/clippy/test)
   - 演示: pre-commit hook 自动拦截
   - 演示: CI 失败 → PR blocked

3. **工具安装** (15 分钟)
   - 全员安装 pre-commit hook
   - VSCode 配置检查 (自动格式化)
   - 第一次 clippy 修复演练

4. **Q&A** (15 分钟)

**培训后任务**:
- [ ] 每人安装 pre-commit hook
- [ ] 每人提交一个符合规范的 test PR
- [ ] Code review 互审,熟悉 PR 模板

#### Day 4-5: 试运行
- 所有 PR 必须填写 PR 模板
- Reviewer 使用 Review Checklist
- 收集反馈,调整细节

### Week 2: 严格执行

#### Day 6-10: 全员执行
- **红线**: CI 不过 → PR 直接 Close (不 Review)
- 每日站会回顾: 昨日 CI 失败次数 (目标递减)
- 表扬符合规范的优秀 PR

#### Day 11-12: 复盘
- 统计数据:
  - 平均每个 PR 的 CI 失败次数
  - Clippy warnings 新增数 (应为 0)
  - 平均 Review 轮次 (应下降)
- 识别高频问题,补充到规范文档

#### Day 13-14: 固化
- 更新 CONTRIBUTING.md,明确红线
- 记录常见问题到 FAQ
- 制定激励机制 (月度优秀 PR 奖)

---

## 🎯 关键指标 (KPI)

### 质量指标
| 指标 | 目标 | 监控方式 |
|------|------|---------|
| Clippy warnings | **0** | CI 强制 |
| 测试覆盖率 | 不下降 | `cargo tarpaulin` (可选) |
| CI 失败率 | < 10% | GitHub Insights |
| 平均 Review 轮次 | < 2 | PR 统计 |

### 过程指标
| 指标 | 目标 | 监控方式 |
|------|------|---------|
| Pre-commit hook 安装率 | 100% | 团队自查 |
| PR 模板填写完整率 | 100% | Review 时检查 |
| Commit message 规范率 | > 95% | 周度 Review |

---

## 🛡️ 执行纪律

### 红线 (不可妥协)
1. ❌ **CI 失败不允许合并** — 无例外
2. ❌ **生产代码无 unwrap** — Code review 必查
3. ❌ **新功能无测试不合并** — 定义明确

### 灰度 (逐步收紧)
- Week 1: CI 失败 → 提醒修复
- Week 2: CI 失败 → 要求说明原因
- Week 3+: CI 失败 → 直接 Close

### 例外流程
**紧急 Hotfix** (生产事故):
1. 可以 bypass pre-commit hook (`git commit --no-verify`)
2. **但** CI 必须过,或提供 skip 原因
3. 事后 24 小时内补测试

---

## 📋 每日/每周 Checklist

### 每日站会 (2 分钟)
- [ ] 昨日 CI 失败次数 (公示,鼓励改进)
- [ ] 积压 PR 中是否有 CI 失败的 (提醒修复)

### 周度 Review (30 分钟)
- [ ] Review 上周 merged PR 质量抽查
- [ ] 更新规范 FAQ (如有新问题)
- [ ] 表扬优秀 PR (示范作用)

### 月度审计 (1 小时)
- [ ] `cargo audit` 安全漏洞扫描
- [ ] 依赖更新 (`cargo outdated`)
- [ ] Dead code 清理
- [ ] 规范文档回顾 (是否需要调整)

---

## 🎓 培训资料清单

### 必读 (新人 onboarding)
1. **[DEV_CHECKLIST.md](DEV_CHECKLIST.md)** — 5 分钟,打印贴桌上
2. **[DEVELOPER_QUICKSTART.md](DEVELOPER_QUICKSTART.md)** — 15 分钟,第一次提交前读
3. **[DEVELOPMENT_STANDARDS.md](DEVELOPMENT_STANDARDS.md)** — 30 分钟,详细规范

### 进阶 (按需)
- Actor 模型 → `nexora-core/src/graph/node_task.rs`
- WAL 原理 → `nexora-core/src/wal/`
- Raft 共识 → `nexora-zenoh/src/raft_handler.rs`

---

## 🚨 常见阻力及应对

### 阻力 1: "检查太严格,影响开发效率"
**应对**:
- 数据说话: 统计修复低质量代码的时间成本
- 长期收益: 减少 Code review 往返次数
- 自动化: pre-commit hook 自动修复大部分问题

### 阻力 2: "CI 太慢,等不起"
**应对**:
- 优化 CI: 并行 jobs,缓存依赖 (已配置 `rust-cache`)
- 本地先跑: `./scripts/local-ci.sh` 提前发现问题
- 增量检查: 只检查变更的 crate (可优化)

### 阻力 3: "历史代码不合规,不知道怎么改"
**应对**:
- 隔离策略: 新代码必须合规,老代码逐步改
- Pair programming: 老手带新手修一遍
- 集中清理: 每月一次 "Code Quality Day"

---

## 🎁 激励机制 (可选)

### 个人奖励
- **月度质量之星**: CI 一次过的 PR 最多
- **最佳 Reviewer**: Review 最细致,发现关键问题
- **重构英雄**: 清理最多 dead code/warnings

### 团队目标
- 连续 2 周 CI 失败率 < 5% → 团建聚餐
- 季度末 Clippy warnings = 0 → 团队奖金

---

## 📞 支持渠道

### 遇到问题?
1. **查文档**: [DEVELOPMENT_STANDARDS.md](DEVELOPMENT_STANDARDS.md) FAQ 部分
2. **问团队**: Slack #dev-standards 频道
3. **提 Issue**: 规范本身有问题 → 提 PR 改进

### 规范迭代
- 规范不是一成不变的
- 发现不合理之处 → 提案讨论 → PR 修改
- 每季度团队回顾一次

---

## ✅ 落地完成标志

当满足以下条件时,视为规范成功落地:

- ✅ 全员安装 pre-commit hook
- ✅ 连续 2 周 CI 失败率 < 10%
- ✅ 所有 PR 填写完整模板
- ✅ Clippy warnings 保持为 0
- ✅ 团队成员能独立按规范开发

---

**记住**: 规范的目的是**提升效率**,而非增加负担。  
通过自动化工具,让规范成为习惯,而非枷锁 🚀
