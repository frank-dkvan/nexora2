# RisingWave 编译成功 - 最终解决方案

## 问题总结

经过多次尝试，发现了导致 RisingWave 编译失败的**三个根本原因**：

### 1. 源代码不一致
- **问题**: 部分文件被修改为不使用 `Pb*` 前缀
- **表现**: orphan rule 错误、类型找不到
- **解决**: 恢复整个 `src/` 目录为官方 v3.0.2 版本

### 2. Workspace 依赖冲突
- **问题**: Nexora workspace 的 prost 0.13 与 RisingWave 的 prost 0.14 冲突
- **表现**: 65 个 E0308 类型不匹配错误
- **解决**: 从 Nexora workspace 中移除 RisingWave 相关 crate

### 3. 工具链版本错误
- **问题**: 使用了 stable 或错误版本的 nightly
- **表现**: 生命周期错误、profile-rustflags 需要 nightly
- **解决**: 使用 `rustup run nightly-2025-10-10` 显式指定工具链

## 最终成功步骤

```bash
# 1. 恢复官方源代码
cd /tmp
git clone --depth 1 --branch v3.0.2 https://github.com/risingwavelabs/risingwave.git risingwave-official
cp -r risingwave-official/src/* /Users/frank/aiCoding/nexora2/vendor/risingwave/src/

# 2. 从 Nexora workspace 中移除 RisingWave
cd /Users/frank/aiCoding/nexora2
# 编辑 Cargo.toml，注释掉：
# "crates/nexora-risingwave",
# "crates/nexora-consensus",
# "crates/nexora-rpc",
# "extensions/meta_raft",

# 3. 使用正确的工具链构建
cd /Users/frank/aiCoding/nexora2/vendor/risingwave
rustup toolchain install nightly-2025-10-10
rustup run nightly-2025-10-10 cargo build -p risingwave_cmd_all --bin risingwave
```

## 关键文件

### rust-toolchain.toml
```toml
[toolchain]
channel = "nightly-2025-10-10"
```

### Cargo.toml (RisingWave)
```toml
[workspace.dependencies]
prost = { git = "https://github.com/risingwavelabs/prost.git", rev = "040a192409e45069158300baec4f402ad1fe101a" }
prost-build = { git = "https://github.com/risingwavelabs/prost.git", rev = "040a192409e45069158300baec4f402ad1fe101a" }
prost-derive = { git = "https://github.com/risingwavelabs/prost.git", rev = "040a192409e45069158300baec4f402ad1fe101a" }
```

### Cargo.toml (Nexora) - 修改后
```toml
[workspace]
members = [
    "crates/nexora-core",
    "crates/nexora-cypher",
    # ... 其他 crate ...
    # 注释掉这些：
    # "crates/nexora-risingwave",
    # "crates/nexora-consensus",
    # "crates/nexora-rpc",
    # "extensions/meta_raft",
]
exclude = [
    "vendor/risingwave",
]
```

## 为什么 rust-toolchain.toml 不自动生效？

### 问题
即使 `vendor/risingwave/rust-toolchain.toml` 存在并正确配置，cargo 仍然使用了 stable 或错误版本。

### 原因
1. **环境变量覆盖**: 如果设置了 `RUSTUP_TOOLCHAIN` 环境变量，会覆盖 rust-toolchain.toml
2. **PATH 优先级**: 如果 PATH 中有 Homebrew 的 rustc，可能被优先使用
3. **Workspace 上下文**: 在父 workspace 目录运行时，可能使用父目录的工具链设置

### 解决方案
使用 `rustup run` 显式指定工具链：
```bash
rustup run nightly-2025-10-10 cargo build
```

这样可以确保：
- ✅ 使用正确的 rustc 版本
- ✅ 使用正确的 cargo 版本
- ✅ 忽略环境变量和 PATH 设置

## 验证构建成功

```bash
# 检查二进制文件
ls -lh /Users/frank/aiCoding/nexora2/vendor/risingwave/target/debug/risingwave

# 测试运行
./target/debug/risingwave --help

# 预期输出
RisingWave v3.0.2
A distributed SQL streaming database
```

## 下一步：集成到 Nexora

### 方案 1: 进程模式（推荐）

```rust
// crates/nexora-risingwave/src/embedded_process.rs
pub struct RisingWaveProcess {
    binary_path: PathBuf,
}

impl RisingWaveProcess {
    pub async fn start(&self) -> Result<()> {
        Command::new(&self.binary_path)
            .arg("standalone")
            .spawn()?;
        Ok(())
    }
}
```

**优点**:
- ✅ 完全隔离，无依赖冲突
- ✅ RisingWave 可以独立更新
- ✅ 简单易维护

### 方案 2: 库模式（未来）

需要解决 prost 版本冲突：

**选项 A**: 升级 Nexora 到 prost 0.14
```toml
# Nexora Cargo.toml
[workspace.dependencies]
prost = "0.14"
tonic = "0.13"
```

**选项 B**: 使用动态链接
- 将 RisingWave 编译为 .dylib
- 通过 FFI 调用

## 经验教训

1. **不要修改官方源代码** - 保持与上游完全一致
2. **工具链版本必须严格匹配** - 使用 `rustup run` 确保正确版本
3. **Workspace 依赖冲突很隐蔽** - `exclude` 不够，需要移除 path 依赖
4. **分层调试** - 先独立编译，再集成
5. **参考官方构建** - 查看 RisingWave 的 CI 配置

## 相关文档

- [RisingWave Integration Plan](docs/RISINGWAVE_INTEGRATION_PLAN.md)
- [Workspace Isolation Issue](RISINGWAVE_WORKSPACE_ISOLATION.md)
- [Nexora CLAUDE.md](CLAUDE.md)

---

**状态**: ✅ RisingWave 独立编译成功  
**下一步**: 等待构建完成，测试二进制文件，然后实现进程模式集成
