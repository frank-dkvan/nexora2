//! 嵌入式 RisingWave - 进程管理实现
//!
//! 通过子进程方式运行 RisingWave，提供类似嵌入式库的用户体验。
//!
//! # 架构
//!
//! ```text
//! ┌──────────────────────────────┐
//! │  Nexora 主进程                │
//! │  └─ EmbeddedEventStreaming       │
//! └──────────┬───────────────────┘
//!            │ fork/exec
//!            ↓
//! ┌──────────────────────────────┐
//! │  RisingWave 子进程            │
//! │  (standalone 模式)            │
//! └──────────────────────────────┘
//! ```
//!
//! # 示例
//!
//! ```no_run
//! use nexora_risingwave::{EmbeddedEventStreaming, EmbeddedConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = EmbeddedConfig::default();
//!     let rw = EmbeddedEventStreaming::start(config).await?;
//!
//!     // 使用 RisingWave...
//!
//!     rw.shutdown().await?;
//!     Ok(())
//! }
//! ```

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use anyhow::{Context, Result, anyhow};
use tracing::{info, warn, debug};

/// 嵌入式 RisingWave 进程管理器
pub struct EmbeddedEventStreaming {
    /// RisingWave 子进程句柄
    process: Option<Child>,

    /// 进程 ID
    pid: u32,

    /// 配置
    config: EmbeddedConfig,

    /// 状态
    state: EmbeddedState,
}

/// 嵌入式状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedState {
    /// 未启动
    NotStarted,
    /// 启动中
    Starting,
    /// 运行中
    Running,
    /// 停止中
    Stopping,
    /// 已停止
    Stopped,
}

/// 嵌入式配置
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    /// RisingWave 二进制路径（可选，自动查找）
    pub binary_path: Option<PathBuf>,

    /// 数据目录
    pub data_dir: PathBuf,

    /// Meta 配置
    pub meta: MetaConfig,

    /// Frontend 配置
    pub frontend: FrontendConfig,

    /// Compute 配置
    pub compute: ComputeConfig,

    /// 启动超时（秒）
    pub startup_timeout_secs: u64,

    /// 关闭超时（秒）
    pub shutdown_timeout_secs: u64,
}

#[derive(Debug, Clone)]
pub struct MetaConfig {
    pub listen_addr: String,
    pub backend: MetaBackend,
}

#[derive(Debug, Clone)]
pub enum MetaBackend {
    Memory,
    Postgres { uri: String },
    Sqlite { path: PathBuf },
}

#[derive(Debug, Clone)]
pub struct FrontendConfig {
    pub listen_addr: String,
}

#[derive(Debug, Clone)]
pub struct ComputeConfig {
    pub parallelism: usize,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            binary_path: None,
            data_dir: PathBuf::from("/tmp/nexora-risingwave"),
            meta: MetaConfig {
                listen_addr: "127.0.0.1:5690".to_string(),
                backend: MetaBackend::Memory,
            },
            frontend: FrontendConfig {
                listen_addr: "127.0.0.1:4566".to_string(),
            },
            compute: ComputeConfig {
                parallelism: num_cpus::get(),
            },
            startup_timeout_secs: 60,
            shutdown_timeout_secs: 30,
        }
    }
}

impl EmbeddedEventStreaming {
    /// 启动嵌入式 RisingWave
    ///
    /// # 错误
    ///
    /// - 找不到 RisingWave 二进制文件
    /// - 进程启动失败
    /// - 启动超时
    pub async fn start(config: EmbeddedConfig) -> Result<Self> {
        info!("Starting embedded RisingWave...");

        // 1. 查找二进制文件
        let binary_path = Self::find_binary(&config)?;
        info!("Using RisingWave binary: {}", binary_path.display());

        // 2. 创建数据目录
        std::fs::create_dir_all(&config.data_dir)
            .context("Failed to create data directory")?;

        // 3. 构建命令
        let mut cmd = Command::new(&binary_path);
        cmd.arg("standalone");

        // Meta 配置
        let meta_opts = Self::build_meta_opts(&config);
        cmd.arg("--meta-opts").arg(&meta_opts);

        // Frontend 配置
        let frontend_opts = Self::build_frontend_opts(&config);
        cmd.arg("--frontend-opts").arg(&frontend_opts);

        // Compute 配置
        let compute_opts = Self::build_compute_opts(&config);
        cmd.arg("--compute-opts").arg(&compute_opts);

        // 重定向输出
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        debug!("Command: {:?}", cmd);

        // 4. 启动进程
        let process = cmd.spawn()
            .context("Failed to spawn RisingWave process")?;

        let pid = process.id();
        info!("RisingWave process started with PID: {}", pid);

        // 5. 等待就绪
        let mut instance = Self {
            process: Some(process),
            pid,
            config: config.clone(),
            state: EmbeddedState::Starting,
        };

        instance.wait_for_ready().await?;
        instance.state = EmbeddedState::Running;

        info!("✓ Embedded RisingWave is ready");
        Ok(instance)
    }

    /// 查找 RisingWave 二进制文件
    fn find_binary(config: &EmbeddedConfig) -> Result<PathBuf> {
        // 1. 显式指定的路径
        if let Some(ref path) = config.binary_path {
            if path.exists() {
                return Ok(path.clone());
            }
            return Err(anyhow!(
                "Specified RisingWave binary not found: {}",
                path.display()
            ));
        }

        // 2. 环境变量
        if let Ok(path_str) = std::env::var("RISINGWAVE_BIN") {
            let path = PathBuf::from(path_str);
            if path.exists() {
                return Ok(path);
            }
        }

        // 3. 项目内预编译二进制
        let local_paths = [
            "bin/risingwave-embedded",
            "target/release/risingwave",
            "../vendor/risingwave/target/release/risingwave",
        ];

        for path_str in &local_paths {
            let path = PathBuf::from(path_str);
            if path.exists() {
                return Ok(path);
            }
        }

        // 4. 系统 PATH
        if let Ok(path) = which::which("risingwave") {
            return Ok(path);
        }

        Err(anyhow!(
            "RisingWave binary not found. Please:\n\
             1. Set RISINGWAVE_BIN environment variable, or\n\
             2. Run: make build-risingwave, or\n\
             3. Install RisingWave to system PATH"
        ))
    }

    /// 构建 Meta 配置参数
    #[doc(hidden)]
    pub fn build_meta_opts(config: &EmbeddedConfig) -> String {
        let mut opts = Vec::new();

        opts.push(format!("--listen-addr {}", config.meta.listen_addr));

        match &config.meta.backend {
            MetaBackend::Memory => {
                opts.push("--backend mem".to_string());
                opts.push(format!("--state-store hummock+memory"));
            }
            MetaBackend::Postgres { uri } => {
                opts.push(format!("--backend postgres --sql-endpoint {}", uri));
                opts.push(format!("--state-store hummock+fs://{}/hummock",
                                 config.data_dir.display()));
            }
            MetaBackend::Sqlite { path } => {
                opts.push(format!("--backend sql --sql-endpoint sqlite://{}", path.display()));
                opts.push(format!("--state-store hummock+fs://{}/hummock",
                                 config.data_dir.display()));
            }
        }

        opts.join(" ")
    }

    /// 构建 Frontend 配置参数
    #[doc(hidden)]
    pub fn build_frontend_opts(config: &EmbeddedConfig) -> String {
        format!(
            "--listen-addr {} --meta-addr {}",
            config.frontend.listen_addr,
            config.meta.listen_addr
        )
    }

    /// 构建 Compute 配置参数
    #[doc(hidden)]
    pub fn build_compute_opts(config: &EmbeddedConfig) -> String {
        format!(
            "--meta-addr {} --parallelism {}",
            config.meta.listen_addr,
            config.compute.parallelism
        )
    }

    /// 等待 RisingWave 就绪
    async fn wait_for_ready(&self) -> Result<()> {
        info!("Waiting for RisingWave to become ready...");

        let timeout = Duration::from_secs(self.config.startup_timeout_secs);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(anyhow!(
                    "RisingWave startup timeout after {}s",
                    self.config.startup_timeout_secs
                ));
            }

            // 检查进程是否还在运行
            if let Some(_process) = &self.process {
                // 这里我们简化实现，实际应该检查 Frontend 端口
                // TODO: 实现 TCP 健康检查
            }

            // 尝试连接 Frontend 端口
            if Self::check_port_open(&self.config.frontend.listen_addr).await {
                info!("Frontend port is open");
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// 检查端口是否打开
    async fn check_port_open(addr: &str) -> bool {
        tokio::net::TcpStream::connect(addr).await.is_ok()
    }

    /// 优雅关闭 RisingWave
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down embedded RisingWave...");
        self.state = EmbeddedState::Stopping;

        if let Some(mut process) = self.process.take() {
            // 1. 发送 SIGTERM
            #[cfg(unix)]
            {
                use nix::sys::signal::{self, Signal};
                use nix::unistd::Pid;

                let pid = Pid::from_raw(self.pid as i32);
                if let Err(e) = signal::kill(pid, Signal::SIGTERM) {
                    warn!("Failed to send SIGTERM: {}", e);
                }
            }

            #[cfg(not(unix))]
            {
                if let Err(e) = process.kill() {
                    warn!("Failed to kill process: {}", e);
                }
            }

            // 2. 等待退出（带超时）
            let timeout = Duration::from_secs(self.config.shutdown_timeout_secs);

            tokio::select! {
                result = tokio::task::spawn_blocking(move || process.wait()) => {
                    match result {
                        Ok(Ok(status)) => {
                            info!("RisingWave exited with status: {}", status);
                        }
                        Ok(Err(e)) => {
                            warn!("Error waiting for process: {}", e);
                        }
                        Err(e) => {
                            warn!("Join error: {}", e);
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    warn!("Shutdown timeout, force killing process");

                    // 3. 超时则强制 SIGKILL
                    #[cfg(unix)]
                    {
                        use nix::sys::signal::{self, Signal};
                        use nix::unistd::Pid;

                        let pid = Pid::from_raw(self.pid as i32);
                        let _ = signal::kill(pid, Signal::SIGKILL);
                    }
                }
            }
        }

        self.state = EmbeddedState::Stopped;
        info!("✓ Embedded RisingWave stopped");
        Ok(())
    }

    /// 获取当前状态
    pub fn state(&self) -> EmbeddedState {
        self.state
    }

    /// 获取进程 ID
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// 获取配置
    pub fn config(&self) -> &EmbeddedConfig {
        &self.config
    }
}

impl Drop for EmbeddedEventStreaming {
    fn drop(&mut self) {
        if self.state == EmbeddedState::Running {
            warn!("EmbeddedEventStreaming dropped while still running, attempting cleanup");

            if let Some(mut process) = self.process.take() {
                let _ = process.kill();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = EmbeddedConfig::default();
        assert_eq!(config.meta.listen_addr, "127.0.0.1:5690");
        assert_eq!(config.frontend.listen_addr, "127.0.0.1:4566");
    }

    #[test]
    fn test_build_meta_opts() {
        let config = EmbeddedConfig::default();
        let opts = EmbeddedEventStreaming::build_meta_opts(&config);
        assert!(opts.contains("--listen-addr"));
        assert!(opts.contains("--backend mem"));
    }
}
