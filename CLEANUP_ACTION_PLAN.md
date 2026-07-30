# Nexora 2.0 文档清理行动计划

**生成日期**: 2026-07-27  
**目标**: 消除误导性文档，澄清实际架构

---

## 🎯 清理目标

- 删除 35+ 个误导性 RisingWave 文档
- 精简 CLAUDE.md 从 605 行到 <200 行
- 决定 vendor/risingwave 去留（50MB 未使用代码）
- 修复编译错误
- 澄清实际集成方式：**外部进程，非源码集成**

---

## 📋 阶段 1: 删除误导性文档（P0）

### 要删除的文件（16 个）

```bash
# Phase 1-6 虚假报告
rm docs/RISINGWAVE_INTEGRATION_PLAN.md
rm docs/RISINGWAVE_PHASE1_REPORT.md
rm docs/RISINGWAVE_PHASE2_REPORT.md
rm docs/RISINGWAVE_PHASE3_REPORT.md
rm docs/RISINGWAVE_PHASE4_REPORT.md
rm docs/RISINGWAVE_PHASE5_REPORT.md
rm docs/RISINGWAVE_PHASE6_PLAN.md
rm docs/RISINGWAVE_PHASE6_REPORT.md

# 过时的计划文档
rm docs/RISINGWAVE_PHASE7_PLAN.md
rm docs/RISINGWAVE_PHASE7_BLOCKERS.md
rm docs/RISINGWAVE_EMBEDDED_ANALYSIS.md
rm docs/RISINGWAVE_PGWIRE_ARCHITECTURE.md

# 重复的实施报告
rm docs/RISINGWAVE_PHASE7.1_REPORT.md
rm docs/RISINGWAVE_PHASE7.3_DONE.md
rm docs/RISINGWAVE_PHASE7.5_REPORT.md
rm docs/RISINGWAVE_PHASE7.6_DONE.md
```

**理由**: 这些文档描述了从未实现的"深度集成"计划

---

## 📋 阶段 2: 重命名保留文档（P0）

```bash
# 单节点实现
mv docs/RISINGWAVE_PHASE7.1_SUMMARY.md docs/RISINGWAVE_SINGLE_NODE_SUMMARY.md
mv docs/RISINGWAVE_PHASE7.1_VERIFICATION.md docs/RISINGWAVE_SINGLE_NODE_VERIFICATION.md
mv docs/RISINGWAVE_PHASE7.5_QUICKREF.md docs/RISINGWAVE_SINGLE_NODE_QUICKREF.md

# 集群实现
mv docs/RISINGWAVE_PHASE8_FINAL_SUMMARY.md docs/RISINGWAVE_CLUSTER_HA_SUMMARY.md
mv docs/RISINGWAVE_PHASE8_QUICKREF.md docs/RISINGWAVE_CLUSTER_HA_QUICKREF.md
mv docs/RISINGWAVE_PHASE8_API_COMPLETE.md docs/RISINGWAVE_API_REFERENCE.md

# 保留有用文档
# - docs/RISINGWAVE_USER_GUIDE.md
# - docs/RISINGWAVE_STATUS.md
# - README_DISTRIBUTED_RISINGWAVE.md
```

---

## 📋 阶段 3: 精简 CLAUDE.md（P0）

### 当前问题
- 605 行，充满"深度集成"虚假描述
- 详细的 Git Subtree 管理说明（实际未使用）
- Phase 1-6 实施时间表（虚假里程碑）

### 新版本结构（<200 行）

```markdown
# Nexora 2.0 开发指南

## 项目概述
- 事件优先的流式图数据库
- 核心功能列表

## 可选功能：RisingWave 集成
- 通过外部进程启动 RisingWave
- 需要用户自行下载二进制文件
- 支持单节点和 3 节点 HA 集群

## 开发工作流
- 构建命令
- 测试命令
- 代码风格

## 项目结构
- 核心 crates
- 可选 crates

## 贡献指南
- Git 规范
- PR 检查清单
```

---

## 📋 阶段 4: 处理 vendor/risingwave（P1）

### 选项 A: 删除（推荐）

```bash
git rm -rf vendor/risingwave
# 修改 Cargo.toml 移除 exclude 条目
git commit -m "chore: remove unused vendor/risingwave (50MB)"
```

**优点**:
- 减少仓库 50MB
- 消除误导
- 更快克隆

**缺点**:
- 如果将来需要真正集成，需重新添加

### 选项 B: 保留并添加说明

```bash
cat > vendor/risingwave/README_NEXORA.md <<EOF
# ⚠️ 重要说明

此目录通过 Git Subtree 添加，但**当前未被 Nexora 使用**。

## Nexora 如何使用 RisingWave

Nexora 通过**外部进程**方式集成 RisingWave：
- 不编译此目录中的源码
- 通过 std::process::Command 启动独立进程
- 需要用户自行下载 RisingWave v3.0.2 二进制文件

## 获取 RisingWave

https://github.com/risingwavelabs/risingwave/releases/tag/v3.0.2

## 为什么保留此目录

为未来可能的源码级集成预留，当前仅作参考。
EOF
```

---

## 📋 阶段 5: 修复编译错误（P0）

### 问题

```bash
$ cargo build --workspace
error[E0433]: failed to resolve: use of unresolved crate `nexora_risingwave`
```

### 修复步骤

1. 检查 `crates/nexora-app/Cargo.toml`:
```toml
[dependencies]
nexora-risingwave = { path = "../nexora-risingwave", optional = true }

[features]
risingwave = ["nexora-risingwave", "nexora-risingwave/embedded"]
```

2. 检查 `crates/nexora-risingwave/Cargo.toml`:
```toml
[features]
default = []
embedded = []  # 确保此特性存在
```

3. 统一特性标志使用:
- 文档中使用 `--features risingwave`
- 代码中实际使用 `--features embedded`
- 建议统一为 `--features risingwave`

---

## 📋 阶段 6: 更新主要文档（P1）

### README.md

**删除**:
- "Phase 1-6 完整集成" 描述
- "深度集成 RisingWave 内部" 声称

**添加**:
- 明确说明：外部进程集成
- 用户需自行下载 RisingWave 二进制

### nexora.toml.example

**添加注释**:
```toml
# RisingWave 集成（可选）
# 注意：需要自行下载 RisingWave v3.0.2 二进制文件
# 下载地址：https://github.com/risingwavelabs/risingwave/releases
[risingwave]
enabled = false
cluster_mode = false
```

---

## ✅ 执行检查清单

### 阶段 1-2: 文档清理
- [ ] 删除 16 个误导性文档
- [ ] 重命名 9 个有用文档
- [ ] 验证保留的文档准确性

### 阶段 3: CLAUDE.md
- [ ] 备份当前版本
- [ ] 创建精简版本（<200 行）
- [ ] 移除所有"深度集成"描述

### 阶段 4: vendor/
- [ ] 决定删除或保留
- [ ] 如果保留，添加 README_NEXORA.md
- [ ] 提交更改

### 阶段 5: 编译修复
- [ ] 修复条件编译配置
- [ ] 统一特性标志命名
- [ ] 验证 `cargo build --workspace` 通过
- [ ] 验证 `cargo test --workspace` 通过

### 阶段 6: 主文档更新
- [ ] 更新 README.md
- [ ] 更新 nexora.toml.example
- [ ] 更新 RISINGWAVE_USER_GUIDE.md

---

## 📊 预期效果

| 指标 | 当前 | 清理后 | 改善 |
|------|------|--------|------|
| 误导性文档数 | 16 | 0 | -100% |
| CLAUDE.md 行数 | 605 | <200 | -67% |
| vendor/ 大小 | 50MB | 0 或明确标注 | -100% or 0% |
| 编译成功率 | 失败 | 100% | ✅ |
| 新贡献者理解时间 | ~4 小时 | <30 分钟 | -87% |

---

## 🚀 执行建议

### 优先级顺序

1. **立即执行**（今天）
   - 阶段 1: 删除误导性文档（5 分钟）
   - 阶段 2: 重命名文档（3 分钟）
   - 阶段 5: 修复编译错误（30 分钟）

2. **本周完成**
   - 阶段 3: 重写 CLAUDE.md（1 小时）
   - 阶段 4: 决定 vendor/ 处理（15 分钟）

3. **下周完成**
   - 阶段 6: 更新主文档（1 小时）

---

**生成工具**: Claude Code  
**审查依据**: 完整源码分析 + Git 历史  
**建议**: 立即开始阶段 1-2，只需 10 分钟
