//! RisingWave initialization for Phase 5.2
//!
//! This module handles RisingWave Meta node initialization in single-node
//! and distributed (Raft HA) modes.

#![cfg(feature = "event-streaming")]

use crate::config::{AppTomlConfig, EventStreamingMode};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;

/// Initialize RisingWave based on CLI args and configuration
pub async fn init_event_streaming(
    cli: &crate::Cli,
    config: &Option<AppTomlConfig>,
) -> Result<Option<Arc<nexora_risingwave::EventStreamingModule>>> {
    let es_config = config.as_ref().and_then(|c| c.event_streaming.as_ref());
    let enabled = cli.enable_event_streaming || es_config.map_or(false, |c| c.enabled);

    if !enabled {
        return Ok(None);
    }

    // Determine mode: CLI flag > config file > default (single)
    let mode = parse_event_streaming_mode(&cli.event_streaming_mode, es_config)?;

    match mode {
        EventStreamingMode::Single => init_single_node(cli, es_config).await,
        EventStreamingMode::Distributed => init_distributed_node(cli, es_config).await,
    }
}

/// Parse event streaming mode from CLI or config
fn parse_event_streaming_mode(
    cli_mode: &str,
    config: Option<&crate::config::EventStreamingConfig>,
) -> Result<EventStreamingMode> {
    // CLI takes precedence
    match cli_mode.to_lowercase().as_str() {
        "single" => Ok(EventStreamingMode::Single),
        "distributed" => Ok(EventStreamingMode::Distributed),
        other => {
            // Fallback to config file
            if let Some(cfg) = config {
                Ok(cfg.mode)
            } else {
                anyhow::bail!("Invalid event streaming mode: '{}'. Expected 'single' or 'distributed'", other)
            }
        }
    }
}

/// Initialize single-node RisingWave Meta
async fn init_single_node(
    cli: &crate::Cli,
    config: Option<&crate::config::EventStreamingConfig>,
) -> Result<Option<Arc<nexora_risingwave::EventStreamingModule>>> {
    tracing::info!("Starting Event Streaming in single-node mode");

    let meta_addr = cli
        .event_streaming_meta_addr
        .as_ref()
        .or_else(|| config.map(|c| &c.meta_addr))
        .context("Meta address required for single-node mode")?;

    let frontend_addr = cli
        .event_streaming_frontend_addr
        .as_ref()
        .or_else(|| config.map(|c| &c.frontend_addr))
        .context("Frontend address required for single-node mode")?;

    let meta_socket: SocketAddr = meta_addr
        .parse()
        .context(format!("Invalid meta address: {}", meta_addr))?;

    let frontend_socket: SocketAddr = frontend_addr
        .parse()
        .context(format!("Invalid frontend address: {}", frontend_addr))?;

    let rw_config = nexora_risingwave::EventStreamingConfig::new()
        .with_meta_addr(meta_socket)
        .with_frontend_addr(frontend_socket);

    let module = nexora_risingwave::EventStreamingModule::start(rw_config)
        .await
        .context("Failed to start single-node Event Streaming")?;

    tracing::info!(
        "Event Streaming started (single-node, meta={}, frontend={})",
        meta_addr,
        frontend_addr
    );

    Ok(Some(Arc::new(module)))
}

/// Distributed multi-node HA event-streaming init.
///
/// NOTE: This dead-code path is superseded by the runtime cluster bootstrap in
/// `main.rs` (`nexora_risingwave::start_distributed_library_cluster`). The old
/// HA bridge here referenced RisingWave/consensus APIs that no longer exist
/// (`EventStreamingModule::start_with_meta`, `nexora_consensus::RaftMode`,
/// `nexora_consensus::RaftElectionClient`). It is stubbed to keep the build
/// green; single-node embedded mode (`init_single_node`) is fully functional.
async fn init_distributed_node(
    _cli: &crate::Cli,
    _config: Option<&crate::config::EventStreamingConfig>,
) -> Result<Option<Arc<nexora_risingwave::EventStreamingModule>>> {
    anyhow::bail!(
        "Distributed HA mode is not available via this entry point; \
         configure [event_streaming.distributed] which is bootstrapped in main.rs"
    )
}
