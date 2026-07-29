# RisingWave 静态编译进 Nexora - 实施进度

**目标**: 将 RisingWave 作为库静态编译进 Nexora，实现"只运行 nexora"

---

## ✅ 已解决的问题

### 1. Cargo nightly 特性问题
**问题**: `cargo-features = ["profile-rustflags"]` 需要 nightly
**解决**: 注释掉该行
```toml
# cargo-features = ["profile-rustflags"]  # Commented out for compatibility
```

### 2. faiss 依赖冲突
**问题**: `faiss = "0.12.2-alpha.0"` 在 crates.io 不存在
**解决**: 注释掉版本号定义，只保留 Git 版本
```toml
# faiss = { version = "0.12.2-alpha.0", features = ["static"] }  # Use Git version
faiss = { git = "https://github.com/risingwavelabs/faiss-rs.git", rev = "f6f0acb" }  # ✅
```

---

## 🟡 进行中

### 编译 nexora-risingwave with library 特性

```bash
后台任务: b28neervt
命令: cargo build --release --features library -p nexora-risingwave
状态: 进行中
预计时间: 30-40 分钟（首次编译 RisingWave）
```

**编译内容**:
```
nexora-risingwave (库)
├── risingwave_cmd_all
├── risingwave_common
├── risingwave_meta_node
├── risingwave_frontend
└── risingwave_compute
```

---

## 📋 已完成的配置

### 1. nexora-risingwave/Cargo.toml

**添加的依赖**:
```toml
[dependencies]
# RisingWave 库集成
risingwave_cmd_all = { path = "../../vendor/risingwave/src/cmd_all", optional = true }
risingwave_common = { path = "../../vendor/risingwave/src/common", optional = true }
risingwave_meta_node = { path = "../../vendor/risingwave/src/meta/node", optional = true }
risingwave_frontend = { path = "../../vendor/risingwave/src/frontend", optional = true }
risingwave_compute = { path = "../../vendor/risingwave/src/compute", optional = true }

[features]
library = [
    "risingwave_cmd_all",
    "risingwave_common",
    "risingwave_meta_node",
    "risingwave_frontend",
    "risingwave_compute"
]
```

### 2. vendor/risingwave/Cargo.toml

**修复的问题**:
- ✅ 注释掉 `cargo-features = ["profile-rustflags"]`
- ✅ 注释掉 `faiss = { version = "0.12.2-alpha.0" }`
- ✅ 保留 Git 版本的 faiss 依赖

---

## ⏳ 待完成（编译成功后）

### Phase 1: 创建库模式实现

**新文件**: `crates/nexora-risingwave/src/library.rs`

```rust
use risingwave_cmd_all::standalone;
use tokio::task::JoinHandle;

pub struct LibraryRisingWave {
    meta_handle: JoinHandle<()>,
    frontend_handle: JoinHandle<()>,
    compute_handle: JoinHandle<()>,
}

impl LibraryRisingWave {
    pub async fn start(config: EmbeddedConfig) -> Result<Self> {
        // 直接在进程内启动各节点
        let meta_handle = tokio::spawn(async {
            risingwave_meta_node::start(...).await
        });
        
        let frontend_handle = tokio::spawn(async {
            risingwave_frontend::start(...).await
        });
        
        let compute_handle = tokio::spawn(async {
            risingwave_compute::start(...).await
        });
        
        Ok(Self { meta_handle, frontend_handle, compute_handle })
    }
    
    pub async fn shutdown(self) -> Result<()> {
        self.compute_handle.abort();
        self.frontend_handle.abort();
        self.meta_handle.abort();
        Ok(())
    }
}
```

### Phase 2: 更新 lib.rs

```rust
#[cfg(feature = "library")]
pub mod library;

#[cfg(feature = "library")]
pub use library::LibraryRisingWave;
```

### Phase 3: 集成到 nexora-app

**文件**: `crates/nexora-app/Cargo.toml`
```toml
[dependencies]
nexora-risingwave = { path = "../nexora-risingwave", features = ["library"], optional = true }

[features]
risingwave = ["nexora-risingwave"]
```

**文件**: `crates/nexora-app/src/handlers/risingwave.rs`
```rust
#[cfg(feature = "risingwave")]
use nexora_risingwave::LibraryRisingWave;

pub async fn start_risingwave(config: &Config) -> Result<LibraryRisingWave> {
    let rw_config = EmbeddedConfig {
        data_dir: PathBuf::from("/tmp/nexora-risingwave"),
        // ... SQLite 配置
    };
    
    LibraryRisingWave::start(rw_config).await
}
```

### Phase 4: 最终编译

```bash
# 编译包含 RisingWave 的 Nexora
cargo build --release --features risingwave

# 结果：一个包含 RisingWave 的 nexora 二进制
ls -lh target/release/nexora
# 预计大小：100-150MB（Nexora 40MB + RisingWave 60-110MB）
```

### Phase 5: 测试验证

```bash
# 直接运行（无需任何配置）
./target/release/nexora \
  --enable-event-streams \
  --event-streams-cluster

# 连接测试
psql -h 127.0.0.1 -p 4566 -d dev -c "CREATE TABLE test (id INT);"
```

---

## 🎯 最终效果

### 编译
```bash
# 只需一次编译（约 40 分钟首次）
cargo build --release --features risingwave
```

### 部署
```bash
# 只需复制一个文件
scp target/release/nexora user@server:/usr/local/bin/
```

### 运行
```bash
# 只需运行一个命令（RisingWave 自动启动）
nexora --enable-event-streams --event-streams-cluster
```

---

## 📊 方案对比

| 维度 | 外部进程 | 库模式（当前） |
|------|---------|--------------|
| 二进制文件数 | 2 个 | 1 个 ✅ |
| 配置复杂度 | 需要 RISINGWAVE_BIN | 零配置 ✅ |
| 编译次数 | 2 次 | 1 次 ✅ |
| 编译时间 | 40 + 40 = 80 分钟 | 40 分钟 ✅ |
| 部署复杂度 | 中等 | 简单 ✅ |
| 进程间通信 | 需要 | 不需要 ✅ |
| 内存占用 | 独立进程 | 共享内存 ✅ |
| 性能 | 良好 | 更好 ✅ |

---

## 🚀 下一步

1. **等待编译完成**（进行中，任务 b28neervt）
2. **创建 library.rs**（30 分钟）
3. **集成到 nexora-app**（20 分钟）
4. **编译完整的 Nexora**（10 分钟）
5. **测试验证**（30 分钟）

**总预计时间**: 编译完成后 1.5 小时

---

**关键优势**:
- ✅ 真正的静态链接
- ✅ 零运行时依赖
- ✅ 单一二进制文件
- ✅ 最优性能（无进程间通信）
- ✅ 最简部署（一个文件）

**实施时间**: 2026-07-28  
**状态**: 编译中...
