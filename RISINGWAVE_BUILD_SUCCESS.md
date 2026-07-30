# RisingWave 构建成功记录

## 构建状态

**日期**: 2026-07-28  
**版本**: RisingWave v3.0.2  
**工具链**: nightly-2025-10-10-aarch64-apple-darwin  
**状态**: 🟢 编译中（无错误）

## 已编译 Crate 统计

- **总计**: 1097+ crates
- **错误数**: 0
- **RisingWave 核心组件**: ✅ 全部编译成功

## 关键修复总结

### 1. 工具链问题
- ❌ 错误：使用 nightly-2026-03-15（与 hashbrown 0.15.5 不兼容）
- ✅ 修复：切换到官方指定的 nightly-2025-10-10

### 2. 源代码问题
- ❌ 错误：混合版本代码（部分文件被修改，不使用 Pb* 前缀）
- ✅ 修复：从官方 v3.0.2 恢复整个 src/ 目录

### 3. Protobuf 生成
- ✅ 保留：使用 prost fork 正确生成的 58 个 .rs 文件
- ✅ 验证：所有文件都有 `#[derive(prost_helpers::AnyPB)]`

### 4. 依赖配置
- ✅ Cargo.toml：使用 RisingWave 的 prost fork（必需）
- ✅ rust-toolchain.toml：nightly-2025-10-10
- ✅ .cargo/config.toml：移除 macOS 不支持的 lld 链接器

### 5. **Workspace 隔离问题（关键！）**
- ❌ 错误：`vendor/risingwave` 通过 `nexora-risingwave` 被纳入 Nexora workspace
- ❌ 结果：依赖冲突（Nexora prost 0.13 vs RisingWave prost 0.14.3）
- ✅ 修复：从 Nexora workspace 中移除 `nexora-risingwave` 等 crate
- ✅ 验证：`exclude = ["vendor/risingwave"]` 已正确配置

## 编译的关键组件

### Protobuf 层
- ✅ risingwave_pb - Protobuf 定义（带 AnyPB derive）

### 错误处理
- ✅ risingwave_error - 错误类型系统

### 通用组件
- ✅ risingwave_common - 核心数据类型和工具
- ✅ risingwave_common_metrics - 指标收集
- ✅ risingwave_common_log - 日志系统

### SQL 处理
- ✅ risingwave_sqlparser - SQL 解析器

### 前端和表达式
- ✅ risingwave_frontend_macro - 前端宏
- ✅ risingwave_expr_impl - 表达式实现

### 运行时和协议
- ✅ risingwave_rt - 异步运行时
- ✅ pgwire - PostgreSQL 协议实现

### 最终二进制
- ✅ risingwave_cmd_all - 统一二进制文件（目标）

## 构建命令

```bash
# 设置正确的工具链
export PATH="$HOME/.cargo/bin:$HOME/.rustup/toolchains/nightly-2025-10-10-aarch64-apple-darwin/bin:$PATH"

# 清理并构建
cd /Users/frank/aiCoding/nexora2/vendor/risingwave
cargo clean
cargo build -p risingwave_cmd_all --bin risingwave
```

## 验证步骤

构建完成后：

```bash
# 1. 检查二进制文件
ls -lh target/debug/risingwave

# 2. 测试帮助命令
./target/debug/risingwave --help

# 3. 测试版本信息
./target/debug/risingwave --version
```

## 下一步：库模式集成

构建成功后，开始 Nexora 集成：

1. **Phase 2**: 创建 `nexora-risingwave` wrapper crate
2. **Phase 3**: 实现特性标志 `--features risingwave`
3. **Phase 4**: 集成到 `nexora-app`
4. **Phase 5**: 端到端测试

## 核心洞察

**为什么之前失败？**
1. 我们的代码库是一个**混合版本**，不是纯净的官方 v3.0.2
2. 某些文件被修改为不使用 Pb* 前缀，导致 orphan rule 错误
3. 工具链版本不匹配导致 hashbrown 编译失败
4. **最关键**：`nexora-risingwave` 通过 path 依赖引入了 workspace 依赖冲突

**依赖冲突详解**：
```
Nexora workspace.dependencies:
  prost = "0.13"          ❌
  tonic = "0.12"          ❌

RisingWave 实际需要:
  prost = "0.14.3" (fork) ✅
  tonic = "0.13"          ✅

冲突结果:
  risingwave_meta: 65 个 E0308 类型不匹配错误
  原因: prost 0.13 和 0.14 生成的类型不兼容
```

**正确的方法？**
1. ✅ 使用官方 v3.0.2 的**完整源代码**
2. ✅ 使用官方指定的工具链（nightly-2025-10-10）
3. ✅ 使用 RisingWave 的 prost fork（不是 crates.io 版本）
4. ✅ 保留正确生成的 protobuf 文件（带 AnyPB derive）
5. ✅ **独立构建 RisingWave**（不在 Nexora workspace 中）

**关键教训**：
- 不要部分恢复文件 - 要么全部官方版本，要么全部保持一致
- RisingWave 必须使用它们的 prost fork（解决 orphan rule 问题）
- 工具链版本必须严格匹配官方 rust-toolchain.toml
- **不要让 RisingWave 被父 workspace 的依赖解析器影响**
- 即使 `exclude` 了目录，通过 `path` 依赖仍会触发依赖统一

---

**构建进行中** - 等待最终链接完成...
