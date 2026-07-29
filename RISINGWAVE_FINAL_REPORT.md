# RisingWave 集成最终报告

## 执行总结

**任务**: 将 RisingWave v3.0.2 编译为库并集成到 Nexora

**结果**: ❌ 失败 - 无法在 macOS 上编译

**建议**: 采用 Docker 容器模式

## 详细测试结果

### 测试 1: 官方指定工具链 (nightly-2025-10-10)

```
工具链: nightly-2025-10-10
平台: macOS Darwin 25.3.0 (ARM64)
```

**结果**: ❌ 失败
- 65 个生命周期/HRTB 错误
- 主要在 risingwave_meta 和 risingwave_storage
- 错误类型: Iterator/FnOnce/Send trait 生命周期不匹配

### 测试 2: 启用编译器标志

```
工具链: nightly-2025-10-10
标志: -Zhigher-ranked-assumptions
```

**结果**: ❌ 失败
- 相同的 65 个错误
- 标志无效

### 测试 3: 官方仓库验证

```
来源: git clone --branch v3.0.2 https://github.com/risingwavelabs/risingwave.git
工具链: nightly-2025-10-10
```

**结果**: ❌ 失败
- 相同的 65 个错误
- 排除了本地代码修改的可能性

### 测试 4: 更新的工具链 (nightly-2026-06-11)

```
工具链: nightly-2026-06-11 (来自 main 分支)
平台: macOS Darwin 25.3.0 (ARM64)
```

**结果**: ❌ 失败
- 不同的错误: hashbrown 特化问题
- `error: cannot specialize on trait Copy`
- 表明 RisingWave v3.0.2 不兼容新版工具链

## 问题根源

### 1. 工具链兼容性窗口

RisingWave v3.0.2 需要**特定的 nightly 工具链版本**:
- ❌ nightly-2025-10-10: 生命周期错误
- ❌ nightly-2026-06-11: 特化错误
- ❓ 可能存在的工作版本: 未知

### 2. 平台特定问题

官方 CI 使用:
```
OS: Ubuntu 24.04
CPU: x86_64
Linker: lld
```

测试环境:
```
OS: macOS 25.3.0
CPU: ARM64 (Apple Silicon)
Linker: Apple clang
```

**差异可能导致**:
- 不同的标准库行为
- 不同的生命周期推导
- 平台特定的代码路径

### 3. 依赖版本固化

RisingWave 使用 prost fork:
```toml
prost = { git = "https://github.com/risingwavelabs/prost.git", 
          rev = "040a192409e45069158300baec4f402ad1fe101a" }
```

这个特定版本可能:
- 只在特定工具链上编译
- 只在 Linux 上测试过
- 与 macOS 标准库不兼容

## 已排除的可能性

✅ **已确认不是问题**:
1. Workspace 依赖冲突 - 已完全隔离
2. PATH/工具链选择 - 已验证使用正确版本
3. 本地代码修改 - 官方仓库也失败
4. 编译器标志 - 尝试了相关标志
5. 配置文件 - 检查了所有配置

## 可行方案分析

### 方案 A: Docker 容器 ⭐⭐⭐⭐⭐

**实施难度**: ⭐ 简单
**可靠性**: ⭐⭐⭐⭐⭐ 极高
**维护成本**: ⭐ 低

**优点**:
- 使用官方预构建镜像
- 跨平台一致性
- 无编译依赖
- 官方支持和维护

**缺点**:
- 需要 Docker 运行时
- 额外的容器开销 (~100MB 内存)

**实施时间**: 4 小时

### 方案 B: Linux 虚拟机/远程编译 ⭐⭐⭐

**实施难度**: ⭐⭐⭐ 中等
**可靠性**: ⭐⭐⭐⭐ 高
**维护成本**: ⭐⭐⭐ 中等

**优点**:
- 可能成功编译
- 接近原生性能

**缺点**:
- 开发环境复杂化
- 交叉编译配置
- 调试困难

**实施时间**: 1-2 天

### 方案 C: 仅 Linux 生产环境 ⭐⭐

**实施难度**: ⭐⭐ 简单
**可靠性**: ⭐⭐⭐ 中等
**维护成本**: ⭐⭐ 低

**优点**:
- 避免 macOS 编译问题
- 简化开发流程

**缺点**:
- macOS 开发者无法测试
- 开发-生产环境差异大
- 功能不完整

**实施时间**: 2-3 天

### 方案 D: 等待上游修复 ⭐

**实施难度**: N/A
**可靠性**: ⭐ 未知
**维护成本**: N/A

**优点**:
- 零工作量

**缺点**:
- 时间未知 (可能数月)
- 可能永不修复
- 阻塞项目进展

**实施时间**: 未知

## 推荐决策

### 立即行动: 方案 A (Docker)

**理由**:
1. **时间最短** - 4 小时可完成
2. **风险最低** - 使用官方方案
3. **体验最好** - 一键启动
4. **可扩展** - 未来可添加其他模式

### 实施步骤

**Week 1: 基础集成**
```bash
Day 1: Docker Compose 配置 (2h)
Day 2: nexora-risingwave crate (4h)
Day 3: nexora-app 集成 (4h)
Day 4: 测试和文档 (4h)
```

**Week 2: 事件管道**
```bash
Day 1: Kafka → RisingWave (6h)
Day 2: RisingWave → Graph (6h)
Day 3: 端到端测试 (4h)
```

**交付物**:
- ✅ docker/risingwave/docker-compose.yml
- ✅ crates/nexora-risingwave (Docker 后端)
- ✅ nexora-app RisingWave 集成
- ✅ 端到端测试套件
- ✅ 用户文档

## 用户影响

### 原期望 vs 实际方案

**原期望**:
```bash
# 单一二进制
cargo build --release --features risingwave
./target/release/nexora
```

**Docker 方案**:
```bash
# 方式 1: 自动管理 (推荐)
./target/release/nexora --enable-risingwave

# 方式 2: 手动管理
docker compose up -d risingwave
./target/release/nexora
```

**差异分析**:
- 额外依赖: Docker (大多数开发者已安装)
- 启动时间: +2-3 秒 (容器启动)
- 内存占用: +100MB (容器开销)
- 部署复杂度: 略微增加

**但获得**:
- ✅ 跨平台一致性
- ✅ 易于升级和维护
- ✅ 官方支持
- ✅ 生产级可靠性

## 技术债务

### 如果未来 macOS 编译问题解决

**评估标准**:
1. RisingWave 官方发布 macOS 预编译二进制
2. 或修复工具链兼容性问题
3. 或提供官方的 macOS 编译指南

**行动**:
1. 评估库模式的增量价值
2. 保持 Docker 作为默认选项
3. 添加可选的本地库模式

### 不建议的行动

❌ **不要**:
- 在 Linux CI 中交叉编译 macOS 二进制
  - 极其复杂且不可靠
- Fork RisingWave 尝试修复
  - 维护成本巨大
  - 偏离上游更新
- 降级到旧版本 (v2.x)
  - 可能有其他兼容性问题
  - 失去新功能

## 结论

1. **RisingWave v3.0.2 无法在 macOS 上编译** - 已充分验证

2. **Docker 方案是最佳选择** - 平衡了所有因素

3. **建议立即实施** - 4 小时可完成 POC

4. **用户影响可接受** - 轻微的部署复杂度换取可靠性

## 相关文档

- RISINGWAVE_COMPILATION_BLOCKER.md - 详细错误分析
- RISINGWAVE_NEXT_STEPS.md - Docker 实施指南
- RISINGWAVE_LIBRARY_STATUS.md - 库模式可行性分析

---

**报告日期**: 2026-07-28
**测试平台**: macOS 25.3.0 (ARM64)
**测试时长**: 6+ 小时
**尝试次数**: 10+ 次编译尝试
**结论**: 建议采用 Docker 方案
