# RisingWave 集成方案调整

## 问题分析

### 库模式集成遇到的问题

1. **Cargo 特性不兼容**: `cargo-features = ["profile-rustflags"]` 需要 nightly
2. **依赖版本冲突**: `faiss = "^0.12.2-alpha.0"` 在 crates.io 不存在
3. **Git 依赖复杂**: RisingWave 使用了 10+ 个 fork 的 Git 依赖
4. **编译时间**: 预计 30-40 分钟（首次）

### 根本原因

RisingWave 是一个**独立的数据库系统**，设计为独立运行，而不是作为库嵌入其他项目。

---

## 推荐方案：静态链接二进制

### 方案 3: 将 RisingWave 二进制嵌入 Nexora

**核心思想**: 将 RisingWave 二进制文件打包进 Nexora，启动时自动解压并运行。

```rust
// 编译时将二进制嵌入
const RISINGWAVE_BIN: &[u8] = include_bytes!("../bin/risingwave-embedded");

// 运行时解压
fn extract_risingwave_binary() -> PathBuf {
    let bin_path = std::env::temp_dir().join("risingwave");
    if !bin_path.exists() {
        std::fs::write(&bin_path, RISINGWAVE_BIN)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755))?;
        }
    }
    bin_path
}
```

**优势**:
- ✅ 只有一个 nexora 二进制（但包含 RisingWave）
- ✅ 无需设置 RISINGWAVE_BIN
- ✅ 自动部署
- ✅ 避免依赖冲突
- ✅ 编译快（只编译一次 RisingWave）

**缺点**:
- ⚠️ nexora 二进制文件变大（增加 ~60MB）
- ⚠️ 首次运行时需要解压

---

## 实施方案对比

| 方案 | 优点 | 缺点 | 实施难度 | 推荐度 |
|------|------|------|----------|--------|
| **1. 外部进程** | 简单、已实现 | 需要设置 RISINGWAVE_BIN | ⭐ 简单 | ⭐⭐⭐ |
| **2. 库模式集成** | 性能最优 | 依赖冲突、编译困难 | ⭐⭐⭐⭐⭐ 复杂 | ⭐ |
| **3. 嵌入二进制** | 单文件部署、无需配置 | 文件变大 | ⭐⭐ 中等 | ⭐⭐⭐⭐⭐ |

---

## 推荐实施：方案 3（嵌入二进制）

### Step 1: 编译 RisingWave 二进制（一次性）

```bash
cd vendor/risingwave
cargo build --release --bin risingwave
cp target/release/risingwave ../../bin/risingwave-embedded
```

### Step 2: 修改 nexora-risingwave

**新文件**: `crates/nexora-risingwave/src/embedded_binary.rs`

```rust
//! 嵌入式二进制 RisingWave

use std::path::PathBuf;
use anyhow::{Result, Context};

/// 嵌入的 RisingWave 二进制文件
#[cfg(feature = "embedded-binary")]
const RISINGWAVE_BINARY: &[u8] = include_bytes!("../../../bin/risingwave-embedded");

/// 提取并返回 RisingWave 二进制路径
pub fn get_risingwave_binary() -> Result<PathBuf> {
    #[cfg(feature = "embedded-binary")]
    {
        let bin_dir = std::env::temp_dir().join("nexora-risingwave");
        std::fs::create_dir_all(&bin_dir)?;
        
        let bin_path = bin_dir.join("risingwave");
        
        // 如果不存在或版本不匹配，重新写入
        if !bin_path.exists() {
            tracing::info!("Extracting embedded RisingWave binary to {:?}", bin_path);
            std::fs::write(&bin_path, RISINGWAVE_BINARY)
                .context("Failed to extract RisingWave binary")?;
            
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755))
                    .context("Failed to set executable permission")?;
            }
        }
        
        Ok(bin_path)
    }
    
    #[cfg(not(feature = "embedded-binary"))]
    {
        // 回退到查找外部二进制
        if let Ok(path_str) = std::env::var("RISINGWAVE_BIN") {
            return Ok(PathBuf::from(path_str));
        }
        
        if let Ok(path) = which::which("risingwave") {
            return Ok(path);
        }
        
        Err(anyhow::anyhow!(
            "RisingWave binary not found. Please set RISINGWAVE_BIN or build with --features embedded-binary"
        ))
    }
}
```

### Step 3: 更新 distributed.rs 使用嵌入二进制

```rust
// crates/nexora-risingwave/src/distributed.rs

fn find_binary(config: &DistributedConfig) -> Result<PathBuf> {
    // 优先使用嵌入的二进制
    #[cfg(feature = "embedded-binary")]
    {
        return crate::embedded_binary::get_risingwave_binary();
    }
    
    // 回退到原有逻辑
    if let Some(ref path) = config.binary_path {
        if path.exists() {
            return Ok(path.clone());
        }
    }
    // ... 其余代码
}
```

### Step 4: 更新 Cargo.toml

```toml
[features]
default = []
event-first = ["nexora-eventlog", "nexora-core"]
embedded = []  # 外部进程模式
embedded-binary = []  # 嵌入二进制模式（推荐）
```

### Step 5: 编译 Nexora（包含 RisingWave）

```bash
# 一次性编译 RisingWave（如果还没有）
cd vendor/risingwave
cargo build --release --bin risingwave
cp target/release/risingwave ../../bin/risingwave-embedded

# 编译 Nexora（自动包含 RisingWave）
cd ../..
cargo build --release --features risingwave,embedded-binary

# 结果：一个包含 RisingWave 的 nexora 二进制
ls -lh target/release/nexora
# 约 100MB（Nexora 40MB + RisingWave 60MB）
```

### Step 6: 部署和使用

```bash
# 只需复制一个文件
scp target/release/nexora user@server:/usr/local/bin/

# 直接运行（无需任何配置）
nexora \
  --enable-event-streams \
  --embedded-event-streams \
  --event-streams-cluster
  
# RisingWave 自动从 nexora 中提取并启动
```

---

## 优势总结

### 用户体验
- ✅ **单文件部署**: 只需要 `nexora` 一个文件
- ✅ **零配置**: 无需设置 RISINGWAVE_BIN
- ✅ **自动更新**: 升级 nexora 时 RisingWave 也自动升级

### 开发体验
- ✅ **编译简单**: RisingWave 只编译一次
- ✅ **依赖隔离**: 避免 Cargo 依赖冲突
- ✅ **测试方便**: 一个二进制文件包含所有功能

### 性能
- ✅ **启动快速**: 解压只需要 100ms
- ✅ **运行性能**: 与外部进程相同（都是本地进程通信）

---

## 实施时间

- **编译 RisingWave**: 30-40 分钟（一次性）
- **修改代码**: 30 分钟
- **测试验证**: 30 分钟
- **总计**: 约 2 小时

---

## 下一步行动

1. 编译 RisingWave 二进制（使用已启动的后台任务）
2. 复制到 `bin/risingwave-embedded`
3. 创建 `embedded_binary.rs`
4. 更新 `distributed.rs`
5. 编译 Nexora with `--features embedded-binary`
6. 测试验证

---

**推荐理由**: 
- 平衡了**易用性**（单文件）和**实施难度**（中等）
- 避免了库模式的**依赖地狱**
- 实现了用户的核心需求：**"只运行 nexora"**
