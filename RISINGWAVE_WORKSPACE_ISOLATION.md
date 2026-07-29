# RisingWave Workspace 隔离问题解决方案

## 问题描述

**症状**: 在 Nexora 项目根目录构建 RisingWave 时，出现 65 个类型不匹配错误（E0308）

**根本原因**: Cargo workspace 依赖统一机制导致的版本冲突

## 问题分析

### Cargo Workspace 依赖解析机制

当一个项目包含多个 crate 时，Cargo 会尝试统一所有 crate 的依赖版本：

```toml
# Nexora 根目录 Cargo.toml
[workspace]
members = [
    "crates/nexora-risingwave",  # 引用了 vendor/risingwave
    ...
]
exclude = [
    "vendor/risingwave",         # ❌ exclude 不够！
]

[workspace.dependencies]
prost = "0.13"    # Nexora 使用的版本
tonic = "0.12"
```

### 冲突链条

```
1. nexora-risingwave/Cargo.toml:
   risingwave_cmd_all = { path = "../../vendor/risingwave/src/cmd_all" }
   
2. Cargo 依赖解析器发现:
   - nexora-risingwave 依赖 vendor/risingwave 的子 crate
   - 这些子 crate 需要 prost 0.14.3 (fork)
   
3. Workspace 依赖统一:
   - 尝试统一 prost 版本
   - Nexora workspace.dependencies 定义了 prost 0.13
   - 部分 RisingWave crate 被迫使用 prost 0.13
   
4. 类型不兼容:
   - risingwave_pb 用 prost 0.14.3 生成的类型
   - risingwave_meta 用 prost 0.13 编译
   - 类型签名不匹配 → 65 个 E0308 错误
```

## 解决方案

### 方案 1: 临时隔离（当前采用）

从 Nexora workspace 中移除 RisingWave 相关 crate：

```toml
# Nexora Cargo.toml
[workspace]
members = [
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

**优点**:
- ✅ RisingWave 可以独立构建
- ✅ 不影响现有 Nexora 代码
- ✅ 快速验证 RisingWave 编译

**缺点**:
- ❌ 暂时无法构建 nexora-risingwave wrapper
- ❌ 需要后续解决集成问题

### 方案 2: 升级 Nexora 的 prost（未来）

升级 Nexora 的 prost 到 0.14：

```toml
# Nexora Cargo.toml
[workspace.dependencies]
prost = "0.14"         # 从 0.13 升级
prost-build = "0.14"
tonic = "0.13"         # 从 0.12 升级
tonic-build = "0.13"
```

**优点**:
- ✅ 彻底解决版本冲突
- ✅ 可以正常集成 RisingWave

**缺点**:
- ⚠️  需要重新生成所有 protobuf 文件
- ⚠️  可能破坏现有 Nexora 代码
- ⚠️  需要测试所有 1590+ 测试用例

### 方案 3: RisingWave 作为独立二进制（Phase 7.1）

不集成源码，只启动 RisingWave 进程：

```rust
// nexora-risingwave/src/embedded_process.rs
pub struct RisingWaveProcess {
    binary_path: PathBuf,  // 独立编译的 risingwave 二进制
    // ...
}

impl RisingWaveProcess {
    pub async fn start(&self) -> Result<()> {
        Command::new(&self.binary_path)
            .arg("standalone")
            .spawn()?;
    }
}
```

**优点**:
- ✅ 完全隔离，无依赖冲突
- ✅ RisingWave 独立更新
- ✅ 易于部署和测试

**缺点**:
- ❌ 不是真正的"库模式"
- ❌ 多进程管理复杂度

### 方案 4: 动态链接（Phase 8 未来）

将 RisingWave 编译为动态库：

```toml
# vendor/risingwave/Cargo.toml
[lib]
crate-type = ["cdylib"]  # 或 "dylib"
```

**优点**:
- ✅ 真正的隔离
- ✅ 可以独立编译

**缺点**:
- ⚠️  需要定义 C ABI 接口
- ⚠️  RisingWave 不是为 FFI 设计的
- ⚠️  复杂度极高

## 推荐路径

### Phase 1-6: 独立构建（当前）

1. ✅ 从 workspace 移除 nexora-risingwave
2. ✅ 独立构建 vendor/risingwave
3. ✅ 验证 RisingWave 二进制可用

### Phase 7: 进程模式集成

```bash
# 1. 构建 RisingWave 二进制
cd vendor/risingwave
cargo build --release --bin risingwave

# 2. nexora-risingwave 启动进程
# 不需要源码依赖，只需二进制路径
```

### Phase 8: 库模式集成（可选）

如果必须要库模式：

1. 创建专门的 workspace 给 RisingWave：
```
nexora2/
├── Cargo.toml              # Nexora workspace (prost 0.13)
└── vendor/
    └── risingwave/
        ├── Cargo.toml      # 独立 workspace (prost 0.14)
        └── ...
```

2. 使用 features 和条件编译避免冲突

## 验证步骤

### 1. 验证隔离构建

```bash
cd /Users/frank/aiCoding/nexora2/vendor/risingwave
cargo build --bin risingwave
./target/debug/risingwave --help
```

预期输出：
```
RisingWave v3.0.2
A distributed SQL streaming database
```

### 2. 验证 Nexora 构建

```bash
cd /Users/frank/aiCoding/nexora2
cargo test --workspace
```

预期：所有 1590+ 测试通过

### 3. 验证集成（Phase 7）

```bash
cd /Users/frank/aiCoding/nexora2
cargo run --bin nexora-app -- \
  --risingwave-binary ./vendor/risingwave/target/debug/risingwave
```

## 经验教训

### 1. `exclude` 不够

```toml
exclude = ["vendor/risingwave"]  # ❌ 不阻止 path 依赖
```

即使 exclude 了目录，通过 `path = "../../vendor/risingwave/..."` 依然会触发依赖解析。

### 2. Workspace 依赖统一是全局的

- Cargo 会统一整个依赖图中的所有版本
- 不仅仅是 `[workspace.dependencies]`
- 包括所有 `path` 依赖的传递依赖

### 3. prost 版本不兼容

- prost 0.13 和 0.14 生成的代码**不兼容**
- 不能混用不同版本生成的类型
- RisingWave 的 prost fork 进一步增加了差异

### 4. 最佳实践

对于大型外部项目（如 RisingWave）：

1. ✅ 保持独立的 workspace
2. ✅ 使用二进制集成，而非源码集成
3. ✅ 如果必须源码集成，确保依赖版本完全一致
4. ✅ 考虑使用 git submodule 而非 subtree

## 参考资料

- [Cargo Workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html)
- [Cargo Resolver](https://doc.rust-lang.org/cargo/reference/resolver.html)
- [prost Migration Guide](https://github.com/tokio-rs/prost/blob/master/CHANGELOG.md)

---

**状态**: ✅ 方案 1 实施中  
**下一步**: 等待独立构建完成，验证二进制文件
