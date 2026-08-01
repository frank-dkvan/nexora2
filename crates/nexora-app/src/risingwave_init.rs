//! RisingWave initialization for Phase 5.2
//!
//! This module handles RisingWave Meta node initialization in single-node
//! and distributed (Raft HA) modes.

#![cfg(feature = "event-streaming")]

use crate::config::{AppTomlConfig, EventStreamingMode};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
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

/// Initialize distributed RisingWave Meta with Raft HA
#[cfg(feature = "library")]
async fn init_distributed_node(
    cli: &crate::Cli,
    config: Option<&crate::config::EventStreamingConfig>,
) -> Result<Option<Arc<nexora_risingwave::EventStreamingModule>>> {
    tracing::info!("Starting Event Streaming in distributed mode with Raft HA");

    let dist_config = config
        .and_then(|c| c.distributed.as_ref())
        .context("Distributed configuration required for distributed mode")?;

    if !dist_config.enabled {
        anyhow::bail!("Distributed mode enabled but distributed.enabled=false in config");
    }

    // Create Raft consensus client
    let raft_config = nexora_consensus::RaftConfig::new(
        dist_config.raft_node_id,
        dist_config.meta.listen_addr.parse()?,
    )
    .mode(nexora_consensus::RaftMode::MultiNode)
    .data_dir(PathBuf::from(
        dist_config
            .consensus
            .as_ref()
            .map(|c| c.data_dir.as_str())
            .unwrap_or("./nexora-data/raft"),
    ));

    // Add peers
    let raft_config = dist_config
        .meta
        .peers
        .iter()
        .fold(raft_config, |cfg, peer| {
            cfg.add_peer(peer.node_id, peer.addr.parse().unwrap())
        });

    // Set heartbeat and election timeout
    let raft_config = if let Some(consensus) = &dist_config.consensus {
        raft_config
            .heartbeat_interval(consensus.heartbeat_interval_secs * 1000)
            .election_timeout(
                consensus.election_timeout_secs * 1000,
                consensus.election_timeout_secs * 2000,
            )
    } else {
        raft_config
    };

    tracing::info!(
        "Initializing Raft consensus (node_id={}, peers={})",
        dist_config.raft_node_id,
        dist_config.meta.peers.len()
    );

    let consensus = nexora_consensus::RaftConsensusClient::new(raft_config)
        .await
        .context("Failed to create Raft consensus client")?;

    // Create Raft election client
    let election_config = extensions_meta_raft::RaftElectionConfig {
        node_id: dist_config.node_id.clone(),
        raft_node_id: dist_config.raft_node_id,
        peer_node_ids: dist_config.meta.peers.iter().map(|p| p.node_id).collect(),
        heartbeat_interval_secs: dist_config
            .consensus
            .as_ref()
            .map(|c| c.heartbeat_interval_secs)
            .unwrap_or(1),
        election_timeout_secs: dist_config
            .consensus
            .as_ref()
            .map(|c| c.election_timeout_secs)
            .unwrap_or(5),
    };

    let election_client = extensions_meta_raft::RaftElectionClient::new(election_config)
        .await
        .context("Failed to create Raft election client")?;

    // Create election adapter
    let election_adapter = Arc::new(RaftElectionAdapter::new(election_client));

    // Create Meta node with HA
    let meta_addr: SocketAddr = dist_config
        .meta
        .listen_addr
        .parse()
        .context("Invalid Meta listen address")?;

    let meta_node =
        nexora_risingwave::meta_wrapper::MetaNode::with_election(meta_addr, election_adapter);

    // Start Meta node
    meta_node
        .start()
        .await
        .context("Failed to start Meta node")?;

    tracing::info!(
        "Meta node started (node_id={}, addr={}, leader={})",
        dist_config.node_id,
        dist_config.meta.listen_addr,
        meta_node.is_leader().await
    );

    // Create EventStreamingModule with HA-enabled Meta
    let frontend_addr: SocketAddr = cli
        .event_streaming_frontend_addr
        .as_ref()
        .or_else(|| config.map(|c| &c.frontend_addr))
        .context("Frontend address required")?
        .parse()
        .context("Invalid frontend address")?;

    let rw_config = nexora_risingwave::EventStreamingConfig::new()
        .with_meta_addr(meta_addr)
        .with_frontend_addr(frontend_addr);

    let module = nexora_risingwave::EventStreamingModule::start_with_meta(rw_config, meta_node)
        .await
        .context("Failed to start Event Streaming with HA Meta")?;

    tracing::info!(
        "Event Streaming started (distributed HA, {} Meta nodes)",
        dist_config.meta.peers.len() + 1
    );

    Ok(Some(Arc::new(module)))
}

/// Fallback for non-library builds
#[cfg(not(feature = "library"))]
async fn init_distributed_node(
    _cli: &crate::Cli,
    _config: Option<&crate::config::EventStreamingConfig>,
) -> Result<Option<Arc<nexora_risingwave::EventStreamingModule>>> {
    anyhow::bail!("Distributed mode requires --features library")
}

/// Adapter to implement ElectionClientTrait for RaftElectionClient
struct RaftElectionAdapter {
    inner: extensions_meta_raft::RaftElectionClient,
}

impl RaftElectionAdapter {
    fn new(client: extensions_meta_raft::RaftElectionClient) -> Self {
        Self { inner: client }
    }
}

#[async_trait::async_trait]
impl nexora_risingwave::meta_wrapper::ElectionClientTrait for RaftElectionAdapter {
    async fn init(&self) -> nexora_risingwave::Result<()> {
        self.inner
            .init()
            .await
            .map_err(|e| nexora_risingwave::EventStreamingError::MetaStartFailed(e.to_string()))
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    fn id(&self) -> nexora_risingwave::Result<String> {
        self.inner
            .id()
            .map_err(|e| nexora_risingwave::EventStreamingError::MetaStartFailed(e.to_string()))
    }

    async fn shutdown(&self) -> nexora_risingwave::Result<()> {
        self.inner
            .shutdown()
            .await
            .map_err(|e| nexora_risingwave::EventStreamingError::MetaStartFailed(e.to_string()))
    }
}
