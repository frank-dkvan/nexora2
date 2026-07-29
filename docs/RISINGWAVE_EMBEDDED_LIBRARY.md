# RisingWave 嵌入式集成方案

## 目标

将 RisingWave 作为**库**直接编译进 Nexora，用户只需运行一个 `nexora` 二进制文件。

---

## 方案对比

### 当前方案（进程模式）❌

```rust
// nexora-risingwave/src/distributed.rs
Command::new("risingwave")  // 需要外部二进制
    .arg("meta-node")
    .spawn()?
```

**问题**:
- ❌ 需要单独编译 RisingWave 二进制（30+ 分钟）
- ❌ 需要设置 `RISINGWAVE_BIN` 环境变量
- ❌ 部署时需要携带两个二进制文件

### 目标方案（库模式）✅

```rust
// nexora-risingwave/src/embedded.rs
use risingwave_cmd_all::{standalone, MetaNodeOpts};

// 直接在进程内启动
tokio::spawn(async move {
    standalone::start_meta_node(opts).await
});
```

**优势**:
- ✅ 只编译一次 Nexora
- ✅ 只有一个二进制文件
- ✅ 部署简单
- ✅ 性能更好（无进程间通信）

---

## 实施步骤

### Phase 1: 添加依赖

**文件**: `crates/nexora-risingwave/Cargo.toml`

```toml
[dependencies]
# 新增：RisingWave 核心库
risingwave_cmd_all = { path = "../../vendor/risingwave/src/cmd_all", optional = true }
risingwave_common = { path = "../../vendor/risingwave/src/common", optional = true }
risingwave_meta_node = { path = "../../vendor/risingwave/src/meta/node", optional = true }
risingwave_frontend = { path = "../../vendor/risingwave/src/frontend", optional = true }
risingwave_compute = { path = "../../vendor/risingwave/src/compute", optional = true }

# 已有依赖保持不变
which = "7"
num_cpus = "1"
# ...

[features]
default = []
event-first = ["nexora-eventlog", "nexora-core"]

# Phase 7: 外部进程模式（保留用于对比测试）
embedded-process = []

# Phase 8: 库模式（推荐）
embedded-library = [
    "risingwave_cmd_all",
    "risingwave_common",
    "risingwave_meta_node",
    "risingwave_frontend",
    "risingwave_compute"
]
```

---

### Phase 2: 创建库模式实现

**新文件**: `crates/nexora-risingwave/src/embedded_library.rs`

```rust
//! 嵌入式 RisingWave - 库模式实现
//!
//! 将 RisingWave 作为库直接编译进 Nexora，无需外部二进制文件。

use std::path::PathBuf;
use std::time::Duration;
use anyhow::{Context, Result};
use tracing::{info, debug};
use tokio::task::JoinHandle;

use risingwave_cmd_all::standalone::{ParsedStandaloneOpts, StandaloneOpts};
use risingwave_common::config::MetaBackend;
use risingwave_meta_node::MetaNodeOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_compute::ComputeNodeOpts;

/// 嵌入式 RisingWave（库模式）
pub struct EmbeddedLibraryRisingWave {
    /// Meta 节点任务句柄
    meta_handle: Option<JoinHandle<()>>,
    
    /// Frontend 节点任务句柄
    frontend_handle: Option<JoinHandle<()>>,
    
    /// Compute 节点任务句柄
    compute_handle: Option<JoinHandle<()>>,
    
    /// 配置
    config: EmbeddedConfig,
}

/// 嵌入式配置
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    /// 数据目录
    pub data_dir: PathBuf,
    
    /// Meta 监听地址
    pub meta_listen_addr: String,
    
    /// Frontend 监听地址
    pub frontend_listen_addr: String,
    
    /// Compute 并行度
    pub compute_parallelism: usize,
    
    /// 元数据后端（SQLite/Postgres/Memory）
    pub meta_backend: MetaBackendConfig,
}

#[derive(Debug, Clone)]
pub enum MetaBackendConfig {
    Memory,
    Postgres { uri: String },
    Sqlite { path: PathBuf },
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/tmp/nexora-risingwave"),
            meta_listen_addr: "127.0.0.1:5690".to_string(),
            frontend_listen_addr: "127.0.0.1:4566".to_string(),
            compute_parallelism: num_cpus::get(),
            meta_backend: MetaBackendConfig::Sqlite {
                path: PathBuf::from("/tmp/nexora-risingwave/meta.db"),
            },
        }
    }
}

impl EmbeddedLibraryRisingWave {
    /// 启动嵌入式 RisingWave（库模式）
    pub async fn start(config: EmbeddedConfig) -> Result<Self> {
        info!("Starting embedded RisingWave (library mode)...");
        
        // 1. 创建数据目录
        std::fs::create_dir_all(&config.data_dir)
            .context("Failed to create data directory")?;
        
        // 2. 构建 Meta 节点配置
        let meta_opts = Self::build_meta_opts(&config)?;
        
        // 3. 构建 Frontend 节点配置
        let frontend_opts = Self::build_frontend_opts(&config)?;
        
        // 4. 构建 Compute 节点配置
        let compute_opts = Self::build_compute_opts(&config)?;
        
        // 5. 启动 Meta 节点
        info!("Starting Meta node at {}...", config.meta_listen_addr);
        let meta_handle = tokio::spawn(async move {
            if let Err(e) = risingwave_meta_node::start(meta_opts).await {
                tracing::error!("Meta node failed: {}", e);
            }
        });
        
        // 等待 Meta 节点就绪
        tokio::time::sleep(Duration::from_secs(3)).await;
        
        // 6. 启动 Frontend 节点
        info!("Starting Frontend at {}...", config.frontend_listen_addr);
        let frontend_handle = tokio::spawn(async move {
            if let Err(e) = risingwave_frontend::start(frontend_opts).await {
                tracing::error!("Frontend failed: {}", e);
            }
        });
        
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // 7. 启动 Compute 节点
        info!("Starting Compute node...");
        let compute_handle = tokio::spawn(async move {
            if let Err(e) = risingwave_compute::start(compute_opts).await {
                tracing::error!("Compute node failed: {}", e);
            }
        });
        
        info!("✓ Embedded RisingWave (library mode) started successfully");
        
        Ok(Self {
            meta_handle: Some(meta_handle),
            frontend_handle: Some(frontend_handle),
            compute_handle: Some(compute_handle),
            config,
        })
    }
    
    /// 构建 Meta 配置
    fn build_meta_opts(config: &EmbeddedConfig) -> Result<MetaNodeOpts> {
        let backend = match &config.meta_backend {
            MetaBackendConfig::Memory => MetaBackend::Mem,
            MetaBackendConfig::Postgres { uri } => {
                MetaBackend::Sql {
                    endpoint: uri.clone(),
                }
            }
            MetaBackendConfig::Sqlite { path } => {
                MetaBackend::Sql {
                    endpoint: format!("sqlite://{}", path.display()),
                }
            }
        };
        
        Ok(MetaNodeOpts {
            listen_addr: config.meta_listen_addr.clone(),
            backend,
            state_store: format!("hummock+fs://{}/state", config.data_dir.display()),
            data_directory: config.data_dir.clone(),
            ..Default::default()
        })
    }
    
    /// 构建 Frontend 配置
    fn build_frontend_opts(config: &EmbeddedConfig) -> Result<FrontendOpts> {
        Ok(FrontendOpts {
            listen_addr: config.frontend_listen_addr.clone(),
            meta_addr: config.meta_listen_addr.clone(),
            ..Default::default()
        })
    }
    
    /// 构建 Compute 配置
    fn build_compute_opts(config: &EmbeddedConfig) -> Result<ComputeNodeOpts> {
        Ok(ComputeNodeOpts {
            meta_address: config.meta_listen_addr.clone(),
            parallelism: config.compute_parallelism,
            ..Default::default()
        })
    }
    
    /// 优雅关闭
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down embedded RisingWave (library mode)...");
        
        // 逆序关闭：Compute -> Frontend -> Meta
        if let Some(handle) = self.compute_handle.take() {
            handle.abort();
        }
        
        if let Some(handle) = self.frontend_handle.take() {
            handle.abort();
        }
        
        if let Some(handle) = self.meta_handle.take() {
            handle.abort();
        }
        
        info!("✓ Embedded RisingWave (library mode) stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_embedded_library_startup() {
        let config = EmbeddedConfig::default();
        let rw = EmbeddedLibraryRisingWave::start(config).await.unwrap();
        
        // 等待服务就绪
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        // 测试连接
        let result = tokio_postgres::connect(
            "host=127.0.0.1 port=4566 dbname=dev user=root",
            tokio_postgres::NoTls,
        ).await;
        
        assert!(result.is_ok(), "Failed to connect to RisingWave");
        
        rw.shutdown().await.unwrap();
    }
}
```

---

### Phase 3: 更新 lib.rs

**文件**: `crates/nexora-risingwave/src/lib.rs`

```rust
//! RisingWave 集成模块

#[cfg(feature = "embedded-process")]
pub mod embedded_process;

#[cfg(feature = "embedded-library")]
pub mod embedded_library;

pub mod distributed;

// 导出统一接口
#[cfg(feature = "embedded-library")]
pub use embedded_library::{EmbeddedLibraryRisingWave, EmbeddedConfig};

#[cfg(feature = "embedded-process")]
pub use embedded_process::{EmbeddedRisingWave, EmbeddedConfig as ProcessConfig};
```

---

### Phase 4: 更新 Nexora 主程序

**文件**: `crates/nexora-app/Cargo.toml`

```toml
[dependencies]
# RisingWave 集成（库模式）
nexora-risingwave = { path = "../nexora-risingwave", features = ["embedded-library"], optional = true }

[features]
# 推荐：库模式
risingwave = ["nexora-risingwave"]

# 旧方式：进程模式（保留用于对比）
risingwave-process = ["nexora-risingwave/embedded-process"]
```

**文件**: `crates/nexora-app/src/handlers/risingwave.rs`

```rust
#[cfg(feature = "risingwave")]
pub async fn start_embedded_risingwave(
    config: &Config,
) -> Result<nexora_risingwave::EmbeddedLibraryRisingWave> {
    use nexora_risingwave::{EmbeddedLibraryRisingWave, EmbeddedConfig, MetaBackendConfig};
    use std::path::PathBuf;
    
    let rw_config = EmbeddedConfig {
        data_dir: PathBuf::from(
            std::env::var("RISINGWAVE_DATA_DIR")
                .unwrap_or_else(|_| "/tmp/nexora-risingwave".to_string())
        ),
        meta_listen_addr: "127.0.0.1:5690".to_string(),
        frontend_listen_addr: "127.0.0.1:4566".to_string(),
        compute_parallelism: num_cpus::get(),
        meta_backend: MetaBackendConfig::Sqlite {
            path: PathBuf::from("/tmp/nexora-risingwave/meta.db"),
        },
    };
    
    EmbeddedLibraryRisingWave::start(rw_config).await
}
```

---

## 编译与测试

### 1. 编译 Nexora（包含 RisingWave）

```bash
cd /Users/frank/aiCoding/nexora2

# 清理旧的构建产物
cargo clean

# 编译（包含 RisingWave 库）
cargo build --release --features risingwave

# 预计耗时：首次 30-40 分钟，增量 5-10 分钟
```

### 2. 运行测试

```bash
# 启动 Nexora（RisingWave 自动内嵌启动）
RISINGWAVE_DATA_DIR=/tmp/test-rw ./target/release/nexora \
  --enable-event-streams \
  --port 8080 \
  --rocksdb-path /tmp/test-rw/graph

# 连接测试
psql -h 127.0.0.1 -p 4566 -d dev <<EOF
CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR);
INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM test;
EOF

# 验证持久化
ls -lh /tmp/test-rw/meta.db
ls -lh /tmp/test-rw/state/
```

---

## 效果对比

| 维度 | 进程模式 | 库模式 |
|------|---------|--------|
| **编译次数** | 2 次（nexora + risingwave） | 1 次（nexora only） |
| **编译时间** | 40 + 40 = 80 分钟 | 40 分钟 |
| **二进制文件** | 2 个（nexora + risingwave） | 1 个（nexora） |
| **部署复杂度** | 需要设置 RISINGWAVE_BIN | 直接运行 |
| **启动方式** | std::process::Command | tokio::spawn |
| **进程数** | 4+ 个进程 | 1 个进程（多线程） |
| **性能** | 进程间通信开销 | 直接内存调用 |
| **内存占用** | 每个进程独立内存 | 共享内存 |

---

## 下一步

1. **实施 Phase 1-4**（预计 2-3 小时）
2. **首次编译**（30-40 分钟）
3. **测试验证** SQLite 持久化功能
4. **更新文档**

---

**优势总结**:
- ✅ **一次编译**：只需编译 Nexora
- ✅ **一个二进制**：部署只需 `target/release/nexora`
- ✅ **零配置**：无需 RISINGWAVE_BIN
- ✅ **更高性能**：无进程间通信开销
- ✅ **更低内存**：共享内存池

**实施时间**: 2026-07-28  
**预计完成**: 2026-07-28（编译 + 测试）
