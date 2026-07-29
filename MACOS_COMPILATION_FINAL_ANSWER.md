# macOS ARM64 编译 RisingWave - 最终答案（已实测更正）

## ✅ 结论：最新版本可以在 macOS ARM64 上成功编译

**实测日期**: 2026-07-28
**测试平台**: macOS ARM64 (Apple Silicon)

之前"macOS 无法编译"的结论**仅适用于 v3.0.2**，最新 main 分支已修复。

---

## 实测结果

### 编译成功

| 项目 | 结果 |
|------|------|
| 版本 | RisingWave 3.1.0-alpha (commit 99b9287) |
| 工具链 | nightly-2026-06-11 |
| 平台 | macOS ARM64 (Mach-O 64-bit executable arm64) |
| 完整二进制 | ✅ 709MB `risingwave` |
| 编译时间 | 19分46秒 (release 全量) |
| 错误数 | **0** |

### 运行验证

```
$ ./target/release/risingwave --version
risingwave 3.1.0-alpha (99b9287)

$ file target/release/risingwave
target/release/risingwave: Mach-O 64-bit executable arm64
```

可用运行模式：
- single-node - 单进程启动所有服务（最适合嵌入 Nexora）
- standalone - 独立模式
- meta / frontend / compute / compactor - 分布式组件
- ctl (risectl) - 运维工具

---

## 为什么之前失败，现在成功？

### v3.0.2 的问题（已成为历史）

- 工具链: nightly-2025-10-10
- 错误: 65 个 HRTB（Higher-Rank Trait Bounds）生命周期错误
- 主要位置: risingwave_meta（47个）、risingwave_storage（18个）
- 典型错误: implementation of Iterator is not general enough

这是 v3.0.2 代码 + nightly-2025-10-10 组合在 macOS 上触发的编译器生命周期推断 bug。

### main 分支已修复

- 工具链: nightly-2026-06-11（更新的编译器）
- 代码: 相关 tokio::spawn + 迭代器生命周期代码已重构
- 结果: risingwave_meta 单独编译 7分49秒，0 错误

### 关于"hashbrown 错误"的更正

之前报告的 cannot specialize on trait Copy 错误经复测为误判：
- 单独测试 hashbrown 0.15.5 + nightly-2026-06-11，2.83秒正常编译
- 该错误只在特定 feature 组合下出现，非默认构建路径

---

## 复现步骤

```bash
# 1. 确保有正确的工具链
rustup toolchain install nightly-2026-06-11

# 2. 克隆最新代码
cd /tmp
git clone --depth 1 https://github.com/risingwavelabs/risingwave.git
cd risingwave

# 3. 锁定工具链（仓库已指定 nightly-2026-06-11）
rustup override set nightly-2026-06-11

# 4. 编译完整二进制
cargo build --release --bin risingwave -p risingwave_cmd_all

# 5. 验证
./target/release/risingwave --version
```

预期: 约 20 分钟完成，生成 ~709MB 的 arm64 二进制。

---

## 对 Nexora 集成的影响

现在有两条可行路径：

### 路径 A: 库模式集成（原始目标，现已可行）

由于最新版本能在 macOS 编译，可以重新评估将 RisingWave 作为库/子进程嵌入 Nexora 单一二进制的方案。

注意事项:
- 需要将 vendor/risingwave 更新到 main 分支（3.1.0-alpha）而非 v3.0.2
- 工具链需切换到 nightly-2026-06-11
- 需重新验证与 Nexora 现有依赖的兼容性（prost 等）

### 路径 B: 独立二进制 + 进程管理

编译出的 risingwave 二进制可由 Nexora 以 single-node 模式启动为子进程，无需 Docker。

---

## 下一步

请选择：

1. 重新尝试库模式集成（原始目标）
   - 更新 vendor 到 main 分支
   - 切换工具链到 nightly-2026-06-11
   - 重新测试工作区编译

2. 使用编译好的二进制 + 子进程模式
   - Nexora 内嵌管理 risingwave 二进制
   - single-node 模式启动

3. 进一步测试
   - 编译并运行完整 single-node 集群验证功能

---

状态: ✅ 已实测确认最新版本可编译
更正: 推翻此前"macOS 不可编译"的结论
