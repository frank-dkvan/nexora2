//! In-process (library mode) RisingWave entry point.
//!
//! Unlike the process-based [`crate::embedded_process`] wrapper, this module
//! links RisingWave's crates directly into the Nexora binary and starts a
//! full RisingWave instance **inside the current process** using RisingWave's
//! `single-node` deployment (all of meta, compute, frontend and compactor in
//! one address space).
//!
//! This is only compiled with the `library` feature, which path-depends on the
//! vendored RisingWave crates. The single binary produced by `nexora-app
//! --features risingwave` embeds RisingWave with no external `risingwave`
//! executable required.
//!
//! # Relationship to `risingwave` binary
//!
//! RisingWave's own `risingwave single-node` binary does, in `main`:
//! `init logger` → `main_okk(|shutdown| standalone(opts, shutdown))`.
//! `main_okk` owns the process (it builds the top-level runtime, installs the
//! logger and never returns). We cannot use it when embedding: Nexora already
//! owns `main` and installs its own `tracing` subscriber. Instead we replicate
//! only the process-global setup that `standalone` actually needs (the rustls
//! crypto provider and the server start time), then drive `standalone` as a
//! task on Nexora's existing Tokio runtime. Each RisingWave service still spawns
//! its own isolated runtime internally via `Service::spawn`.

use clap::Parser;
use risingwave_cmd_all::{map_single_node_opts_to_standalone_opts, standalone, SingleNodeOpts};
use risingwave_common::util::tokio_util::sync::CancellationToken;
use std::path::PathBuf;
use tokio::task::JoinHandle;
use tracing::info;

use crate::error::{Result, EventStreamingError};

/// Configuration for an embedded, in-process RisingWave single-node instance.
#[derive(Debug, Clone)]
pub struct EmbeddedLibraryConfig {
    /// Frontend (Postgres wire protocol) listen address, e.g. `127.0.0.1:4566`.
    pub frontend_listen_addr: String,
    /// Persist meta store and object store under this directory. Ignored when
    /// [`Self::in_memory`] is `true`.
    pub store_directory: Option<PathBuf>,
    /// Run entirely in memory. Data is lost on shutdown; for tests and demos
    /// only, never for production.
    pub in_memory: bool,
    /// Optional RisingWave TOML config file path (`--config-path`).
    pub config_path: Option<PathBuf>,
    /// Optional Prometheus metrics listen address.
    pub prometheus_listener_addr: Option<String>,
    /// Maximum idle time (seconds) before meta service auto-shuts down when no
    /// workers are connected. `None` = never shut down due to idleness (recommended
    /// for embedded use). Only set `Some(...)` for short-lived test scenarios.
    pub max_idle_secs: Option<u64>,
}

impl Default for EmbeddedLibraryConfig {
    fn default() -> Self {
        Self {
            frontend_listen_addr: "127.0.0.1:4566".to_string(),
            store_directory: None,
            in_memory: false,
            config_path: None,
            prometheus_listener_addr: None,
            max_idle_secs: None,
        }
    }
}

impl EmbeddedLibraryConfig {
    /// Create a config with default single-node settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the frontend (pgwire) listen address.
    pub fn with_frontend_listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.frontend_listen_addr = addr.into();
        self
    }

    /// Persist data under `dir` (turns off in-memory mode).
    pub fn with_store_directory(mut self, dir: impl Into<PathBuf>) -> Self {
        self.store_directory = Some(dir.into());
        self.in_memory = false;
        self
    }

    /// Run entirely in memory (test/demo only).
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        self.store_directory = None;
        self
    }

    /// Render this config into the `single-node` CLI argument vector that
    /// [`SingleNodeOpts`] parses. `SingleNodeOpts` fields are private, so
    /// constructing it via its `clap::Parser` impl is the supported path (the
    /// same one `SingleNodeOpts::new_for_playground` uses).
    fn to_args(&self) -> Vec<String> {
        // argv[0] placeholder; clap ignores it.
        let mut args: Vec<String> = vec!["single-node".to_string()];

        if self.in_memory {
            args.push("--in-memory".to_string());
        } else if let Some(dir) = &self.store_directory {
            args.push("--store-directory".to_string());
            args.push(dir.to_string_lossy().to_string());
        }

        // Frontend listen address (flattened NodeSpecificOpts::listen_addr).
        args.push("--listen-addr".to_string());
        args.push(self.frontend_listen_addr.clone());

        if let Some(path) = &self.config_path {
            args.push("--config-path".to_string());
            args.push(path.to_string_lossy().to_string());
        }

        if let Some(addr) = &self.prometheus_listener_addr {
            args.push("--prometheus-listener-addr".to_string());
            args.push(addr.clone());
        }

        if let Some(secs) = self.max_idle_secs {
            args.push("--max-idle-secs".to_string());
            args.push(secs.to_string());
        }

        args
    }
}

/// Handle to a running in-process RisingWave instance.
///
/// Dropping the handle does **not** stop RisingWave; call [`Self::shutdown`]
/// for a graceful stop, or [`Self::abort`] to force-cancel.
pub struct EmbeddedLibrary {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    frontend_listen_addr: String,
}

impl EmbeddedLibrary {
    /// Start an embedded RisingWave single-node instance on the current Tokio
    /// runtime. Returns once the instance has been spawned; RisingWave becomes
    /// query-ready asynchronously (poll the frontend port to confirm).
    ///
    /// Must be called from within a Tokio runtime context.
    pub fn start(config: EmbeddedLibraryConfig) -> Result<Self> {
        install_process_globals();

        let args = config.to_args();
        info!(?args, "starting embedded RisingWave (single-node, library mode)");

        // Parse into SingleNodeOpts via clap, then lower to standalone opts.
        let single_opts = SingleNodeOpts::try_parse_from(&args).map_err(|e| {
            EventStreamingError::MetaStartFailed(format!(
                "failed to build RisingWave single-node options: {e}"
            ))
        })?;
        let standalone_opts = map_single_node_opts_to_standalone_opts(single_opts);

        let shutdown = CancellationToken::new();
        let task_token = shutdown.clone();
        let task = tokio::spawn(async move {
            standalone(standalone_opts, task_token).await;
        });

        Ok(Self {
            shutdown,
            task,
            frontend_listen_addr: config.frontend_listen_addr,
        })
    }

    /// The frontend (pgwire) listen address this instance was started with.
    pub fn frontend_listen_addr(&self) -> &str {
        &self.frontend_listen_addr
    }

    /// Signal RisingWave to shut down gracefully and wait for it to finish.
    pub async fn shutdown(self) -> Result<()> {
        info!("shutting down embedded RisingWave (library mode)");
        self.shutdown.cancel();
        self.task.await.map_err(|e| {
            EventStreamingError::MetaStartFailed(format!("RisingWave task join error: {e}"))
        })
    }

    /// Force-abort the RisingWave task without waiting for graceful shutdown.
    pub fn abort(self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

/// Install the process-global state that RisingWave's `standalone` relies on,
/// but which is normally set up by the `risingwave` binary's `main_okk`.
///
/// Both operations are idempotent / best-effort so this is safe to call more
/// than once and safe to call alongside a host process that may have already
/// installed a rustls provider.
fn install_process_globals() {
    // RisingWave uses rustls (object store over HTTPS, TLS connectors). The
    // process needs a default crypto provider installed exactly once; ignore
    // the error if the host already installed one.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Used by RisingWave's system catalog (`rw_catalog`) for uptime.
    risingwave_variables::init_server_start_time();
}
