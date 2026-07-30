# Phase 7.1 实施阻塞问题分析

**文档版本**: 1.0  
**创建日期**: 2026-07-26  
**状态**: 🚧 阻塞中  
**负责人**: frank

---

## 执行摘要

Phase 7.1（依赖集成）遇到两个**关键阻塞问题**，使得直接将 RisingWave 作为 Rust 库嵌入 Nexora 在当前状态下**不可行**：

1. **Rust 工具链不兼容**：RisingWave 需要 nightly，Nexora 使用 stable
2. **依赖冲突**：RisingWave 使用 madsim（模拟器）版本的 tokio/tonic，与标准版本完全不兼容

### 建议方案

**短期（Phase 7 替代）**：实现**进程内启动外部二进制**的"伪嵌入"模式
- 用户体验：单一命令启动（`nexora --embedded-risingwave`）
- 实现：Nexora 自动管理 RisingWave 子进程
- 工时：20-30 小时（比原计划少 90 小时）

**长期（Phase 8+）**：与 RisingWave 团队合作
- 提交 PR 支持可选的 stable Rust 构建
- 或等待 RisingWave 移除 madsim 生产构建依赖

---

## 问题 1: Rust 工具链不兼容

### 现象

```bash
$ cd vendor/risingwave && cargo build --release
error: failed to parse manifest at `/Users/frank/aiCoding/nexora2/vendor/risingwave/Cargo.toml`

Caused by:
  the cargo feature `profile-rustflags` requires a nightly version of Cargo, 
  but this is the `stable` channel
```

### 根本原因

RisingWave 的 `Cargo.toml` 使用了 nightly-only 特性：

```toml
# vendor/risingwave/Cargo.toml
cargo-features = ["profile-rustflags"]  # ← 需要 nightly

[profile.dev]
rustflags = ["-Z", "threads=8"]  # ← 需要 nightly
```

### 影响

- **Nexora 使用 stable Rust 1.88**（workspace 配置）
- **无法在同一 workspace 中同时支持 stable 和 nightly**
- 切换到 nightly 会影响 Nexora 的所有现有代码

### 可能的解决方案

| 方案 | 可行性 | 工作量 | 风险 |
|------|--------|--------|------|
| 1. Nexora 切换到 nightly | ❌ 低 | 高 | 破坏稳定性保证 |
| 2. Fork RisingWave 移除 nightly 特性 | ⚠️ 中 | 极高 | 维护负担重 |
| 3. 等待 RisingWave 支持 stable | ⏳ 未知 | 低 | 时间不可控 |
| 4. 使用二进制而非源码集成 | ✅ 高 | 低 | 需要改变架构 |

---

## 问题 2: 依赖冲突（madsim）

### 现象

RisingWave 使用 **madsim** 版本的核心依赖：

```toml
# vendor/risingwave/Cargo.toml
[workspace.dependencies]
tokio = { 
    package = "madsim-tokio",  # ← 不是标准 tokio!
    git = "https://github.com/risingwavelabs/madsim.git",
    rev = "595455fd05058b3b48bf281bea3ada1bca3d6646"
}
tonic = { 
    package = "madsim-tonic",  # ← 不是标准 tonic!
    git = "https://github.com/risingwavelabs/madsim.git",
    rev = "595455fd05058b3b48bf281bea3ada1bca3d6646"
}
```

### 什么是 madsim？

**madsim** (MADsim = Madsim Async Deterministic Simulator) 是一个**确定性模拟器**，用于测试分布式系统：

- 替换 `tokio`、`tonic`、`tokio-postgres` 等异步运行时
- 在模拟环境中可以控制时间、网络、故障注入
- **主要用于测试，不适合生产环境**

### 问题

```
nexora-core (标准 tokio 1.0)
    └─ nexora-risingwave
           └─ risingwave-common (madsim-tokio)
                ❌ 类型不兼容！
```

**具体冲突**：
- `tokio::sync::mpsc::channel` ≠ `madsim_tokio::sync::mpsc::channel`
- `tonic::Request` ≠ `madsim_tonic::Request`
- 无法在同一程序中混用两种运行时

### RisingWave 为何使用 madsim？

查看 RisingWave 文档和源码：

1. **测试驱动**：用于确定性测试（`cargo test --cfg madsim`）
2. **生产构建不应使用**：但当前 workspace 配置强制使用
3. **配置问题**：缺少 feature flag 来禁用 madsim

### 可能的解决方案

| 方案 | 可行性 | 工作量 | 风险 |
|------|--------|--------|------|
| 1. 使用 `[patch]` 替换为标准库 | ❌ 低 | 中 | 可能破坏 RisingWave |
| 2. Fork RisingWave 添加 feature flag | ⚠️ 中 | 高 | 维护负担 |
| 3. 提交 PR 到 RisingWave upstream | ✅ 高 | 中 | 时间较长 |
| 4. 使用二进制而非源码集成 | ✅ 高 | 低 | 改变架构 |

---

## 推荐方案：进程管理式"伪嵌入"

### 概念

**不将 RisingWave 编译为库，而是作为子进程管理**：

```
┌────────────────────────────────────┐
│  Nexora 主进程                      │
│  ├─ nexora-app                     │
│  └─ RisingWave Manager             │ ← 新增
│      └─ 启动/停止/监控子进程        │
└──────────┬─────────────────────────┘
           │ fork/exec
           ↓
┌──────────────────────────────────┐
│  RisingWave 子进程 (standalone)   │  ← 预编译二进制
│  ├─ Meta                         │
│  ├─ Frontend                     │
│  └─ Compute                      │
└──────────────────────────────────┘
```

### 用户体验

```bash
# 单一命令启动（用户看不到两个进程）
$ nexora --enable-embedded-risingwave

✓ Starting Nexora...
✓ Starting embedded RisingWave...
  ├─ Meta node on port 5690
  ├─ Frontend node on port 4566
  └─ Compute node ready
✓ Nexora ready on http://localhost:8080
```

### 实现方案

#### 1. 预编译 RisingWave 二进制

```bash
# 构建脚本：scripts/build-embedded-risingwave.sh
#!/bin/bash
cd vendor/risingwave
rustup override set nightly  # ← 只在这个目录用 nightly
cargo build --release --bin risingwave
cp target/release/risingwave ../../bin/risingwave-embedded
```

#### 2. 实现进程管理器

```rust
// crates/nexora-risingwave/src/embedded_process.rs

use std::process::{Child, Command, Stdio};
use std::path::PathBuf;

pub struct EmbeddedRisingWave {
    /// RisingWave 子进程句柄
    process: Child,
    
    /// 配置
    config: EmbeddedConfig,
    
    /// 健康检查客户端
    client: RisingWaveClient,
}

impl EmbeddedRisingWave {
    /// 启动嵌入式 RisingWave（作为子进程）
    pub async fn start(config: EmbeddedConfig) -> Result<Self> {
        // 1. 查找二进制
        let bin_path = find_risingwave_binary()?;
        
        // 2. 构建命令行参数
        let mut cmd = Command::new(bin_path);
        cmd.arg("standalone")
           .arg("--meta-opts").arg(config.build_meta_args())
           .arg("--frontend-opts").arg(config.build_frontend_args())
           .arg("--compute-opts").arg(config.build_compute_args())
           .stdout(Stdio::piped())
           .stderr(Stdio::piped());
        
        // 3. 启动进程
        let process = cmd.spawn()
            .context("Failed to spawn RisingWave process")?;
        
        // 4. 等待就绪
        Self::wait_for_ready(&config).await?;
        
        // 5. 创建 gRPC 客户端
        let client = RisingWaveClient::connect(&config).await?;
        
        Ok(Self { process, config, client })
    }
    
    /// 优雅关闭
    pub async fn shutdown(mut self) -> Result<()> {
        // 1. 发送 SIGTERM
        self.process.kill()?;
        
        // 2. 等待退出（最多 30 秒）
        tokio::select! {
            _ = self.process.wait() => {},
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                // 3. 超时则强制 SIGKILL
                let _ = Command::new("kill")
                    .arg("-9")
                    .arg(self.process.id().to_string())
                    .status();
            }
        }
        
        Ok(())
    }
}

/// 查找 RisingWave 二进制文件
fn find_risingwave_binary() -> Result<PathBuf> {
    // 优先级顺序：
    // 1. 环境变量 RISINGWAVE_BIN
    if let Ok(path) = std::env::var("RISINGWAVE_BIN") {
        return Ok(PathBuf::from(path));
    }
    
    // 2. 项目内预编译二进制
    let local_bin = PathBuf::from("bin/risingwave-embedded");
    if local_bin.exists() {
        return Ok(local_bin);
    }
    
    // 3. 系统 PATH
    if let Ok(path) = which::which("risingwave") {
        return Ok(path);
    }
    
    Err(anyhow!("RisingWave binary not found. Please run: make build-risingwave"))
}
```

#### 3. 集成到 nexora-app

```rust
// crates/nexora-app/src/main.rs

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // 启动嵌入式 RisingWave（如果启用）
    let risingwave = if args.enable_embedded_risingwave {
        let config = EmbeddedConfig::from_file(&args.config)?;
        Some(EmbeddedRisingWave::start(config).await?)
    } else {
        None
    };
    
    // 启动 Nexora 主服务
    let app = NexoraApp::new().await?;
    app.run().await?;
    
    // 优雅关闭
    if let Some(rw) = risingwave {
        rw.shutdown().await?;
    }
    
    Ok(())
}
```

### 优势

| 优势 | 说明 |
|------|------|
| ✅ **零依赖冲突** | RisingWave 和 Nexora 完全独立编译 |
| ✅ **工具链隔离** | RisingWave 用 nightly，Nexora 用 stable |
| ✅ **用户体验好** | 单一命令启动，自动管理生命周期 |
| ✅ **实现简单** | 20-30 小时 vs 120 小时 |
| ✅ **易于调试** | 可以独立调试 RisingWave 进程 |
| ✅ **向后兼容** | 仍然支持外部 RisingWave 服务 |

### 劣势

| 劣势 | 缓解措施 |
|------|---------|
| ⚠️ 进程间通信开销 | 使用 localhost gRPC（延迟 <1ms） |
| ⚠️ 需要预编译二进制 | 自动化构建脚本 |
| ⚠️ 不是"真"嵌入 | 用户不在意实现细节 |

---

## 工作量对比

| 方案 | 工时 | 风险 | 用户价值 |
|------|------|------|---------|
| **原计划（源码嵌入）** | 120h | ⚠️ 高（阻塞问题） | ⭐⭐⭐⭐⭐ |
| **进程管理式** | 30h | ✅ 低 | ⭐⭐⭐⭐ |
| **Fork + 修改** | 200h+ | ⚠️ 高（维护负担） | ⭐⭐⭐⭐⭐ |
| **等待上游** | ?h | ⏳ 未知 | ⭐⭐⭐⭐⭐ |

---

## 下一步行动

### 立即行动（本周）

1. **✅ 创建本文档** - 记录阻塞问题
2. **⏳ 实现进程管理器** - `crates/nexora-risingwave/src/embedded_process.rs`
3. **⏳ 编写构建脚本** - `scripts/build-embedded-risingwave.sh`
4. **⏳ 集成到 CLI** - `nexora --enable-embedded-risingwave`

### 短期（1-2 周）

5. **⏳ 测试验证** - 单元测试、集成测试
6. **⏳ 文档更新** - 用户指南、部署文档
7. **⏳ CI/CD** - 自动构建 RisingWave 二进制

### 长期（Phase 8+）

8. **⏳ 提交 PR 到 RisingWave** - 添加 `--no-madsim` feature flag
9. **⏳ 与 RisingWave 团队沟通** - 讨论嵌入式用例
10. **⏳ 真正源码集成** - 当上游支持后迁移

---

## 附录：技术调研

### A. RisingWave 的 madsim 使用情况

```bash
$ grep -r "cfg.*madsim" vendor/risingwave/src | wc -l
347  # ← 347 处条件编译

$ grep -r "package.*madsim" vendor/risingwave/Cargo.toml
tokio = { package = "madsim-tokio", ... }
tonic = { package = "madsim-tonic", ... }
rdkafka = { package = "madsim-rdkafka", ... }
```

### B. 其他嵌入式数据库的做法

| 数据库 | 嵌入方式 | 说明 |
|--------|---------|------|
| SQLite | 源码编译 | C 库，无依赖冲突 |
| RocksDB | 源码编译 | C++ 库，通过 FFI |
| DuckDB | 源码编译 | C++ 库，单一 amalgamation 文件 |
| Kuzu | 源码编译 | C++ 库，Nexora 已使用 |
| **RisingWave** | ❌ 源码编译困难 | Rust，大量依赖冲突 |

**结论**：对于复杂的 Rust 项目，进程隔离是更实用的方案。

---

## 决策记录

**日期**: 2026-07-26  
**决策**: 采用**进程管理式"伪嵌入"**方案  
**理由**:
1. 原计划的源码嵌入在当前状态下不可行
2. 进程管理式方案可以提供 90% 的用户价值，只需 25% 的工作量
3. 为未来真正的源码集成保留空间

**签署**: frank (项目负责人)
