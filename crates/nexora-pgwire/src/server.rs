//! PostgreSQL protocol listener and connection lifecycle management.

use std::io::{BufReader, Cursor};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pgwire::tokio::process_socket;
use pgwire::tokio::tokio_rustls;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

use crate::auth::NexoraStartupHandler;
use crate::config::{PgConfig, PgConfigError};
use crate::extended_query::NexoraExtendedQueryHandler;
use crate::session::{ActivityState, ConnectionContext};
use crate::simple_query::NexoraSimpleQueryHandler;
use crate::PgAppState;

static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Error)]
pub enum PgServerError {
    #[error(transparent)]
    Config(#[from] PgConfigError),
    #[error("failed to bind PostgreSQL listener at {address}: {source}")]
    Bind {
        address: String,
        #[source]
        source: std::io::Error,
    },
    #[error("PostgreSQL listener failed: {0}")]
    Accept(#[source] std::io::Error),
    #[error("failed to read PG TLS certificate {path}: {source}")]
    ReadTlsCertificate {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read PG TLS private key {path}: {source}")]
    ReadTlsKey {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("PG TLS private key file {0} contains no supported key")]
    MissingTlsKey(String),
    #[error("invalid PG TLS certificate or private key: {0}")]
    InvalidTls(#[source] tokio_rustls::rustls::Error),
    #[error("PostgreSQL server task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub struct PgServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<Result<(), PgServerError>>,
}

impl PgServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(self) -> Result<(), PgServerError> {
        let _ = self.shutdown_tx.send(true);
        self.task.await?
    }
}

pub struct NexoraPgWireHandlers {
    simple_handler: Arc<NexoraSimpleQueryHandler>,
    extended_handler: Arc<NexoraExtendedQueryHandler>,
    startup_handler: Arc<NexoraStartupHandler>,
}

impl pgwire::api::PgWireServerHandlers for NexoraPgWireHandlers {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        self.simple_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.startup_handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(pgwire::api::NoopHandler)
    }
}

pub async fn run_pg_server(
    state: PgAppState,
    config: PgConfig,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), PgServerError> {
    config.validate()?;
    let tls_acceptor = load_tls_acceptor(&config)?;
    let bind_addr = bind_endpoint(&config.bind_addr, config.port);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(|source| PgServerError::Bind {
            address: bind_addr,
            source,
        })?;
    serve(listener, Arc::new(state), config, tls_acceptor, shutdown_rx).await
}

pub async fn spawn_pg_server(
    graph: Arc<nexora_core::GraphService>,
    mv_manager: Arc<nexora_core::materialized_view::MaterializedViewManager>,
    sq_manager: Option<Arc<nexora_standing_query::StandingQueryManager>>,
    query_pool: Arc<nexora_core::query_pool::QueryPool>,
    config: PgConfig,
) -> Result<PgServerHandle, PgServerError> {
    spawn_pg_server_with_router(
        graph,
        mv_manager,
        sq_manager,
        None,
        None,
        query_pool,
        #[cfg(feature = "event-first")]
        None,
        config,
    )
    .await
}

/// Like [`spawn_pg_server`], but also wires a cross-node [`HybridRouter`] so
/// cluster deployments route whole-graph queries to shard owners. Pass `None`
/// for `router` to get the single-node behaviour (identical to `spawn_pg_server`).
///
/// [`HybridRouter`]: nexora_zenoh::router::HybridRouter
pub async fn spawn_pg_server_with_router(
    graph: Arc<nexora_core::GraphService>,
    mv_manager: Arc<nexora_core::materialized_view::MaterializedViewManager>,
    sq_manager: Option<Arc<nexora_standing_query::StandingQueryManager>>,
    router: Option<Arc<nexora_zenoh::router::HybridRouter>>,
    replication_progress: Option<Arc<nexora_zenoh::ReplicationProgress>>,
    query_pool: Arc<nexora_core::query_pool::QueryPool>,
    #[cfg(feature = "event-first")] event_store: Option<Arc<nexora_eventlog::EventLogStore>>,
    config: PgConfig,
) -> Result<PgServerHandle, PgServerError> {
    config.validate()?;
    let users = if config.trust {
        Arc::new(std::collections::HashMap::new())
    } else {
        Arc::new(config.load_users()?)
    };
    let state = Arc::new(PgAppState {
        graph,
        mv_manager,
        sq_manager,
        router,
        // C1: a node-level read-after-write tracker shared by all connections on
        // this PG server. Writes note their seq here; later reads consult it so a
        // failover read never serves data older than a write this node accepted.
        // (Node-level is a safe superset of strict per-session read-your-writes.)
        session_tracker: Arc::new(nexora_zenoh::SessionReadTracker::new()),
        // C2: replication progress from the ClusterManager. The writer records
        // owner write seqs and follower acks; distributed reads gate Majority
        // concern on replicas caught up to quorum commit_index. None in single-
        // node mode or when passed as None (degrades Majority to owner-first).
        replication_progress,
        query_pool,
        trust_auth: config.trust,
        users,
        server_version: config.server_version.clone(),
        #[cfg(feature = "event-first")]
        event_store,
    });
    let tls_acceptor = load_tls_acceptor(&config)?;
    let bind_addr = bind_endpoint(&config.bind_addr, config.port);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(|source| PgServerError::Bind {
            address: bind_addr,
            source,
        })?;
    let local_addr = listener.local_addr().map_err(PgServerError::Accept)?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(serve(listener, state, config, tls_acceptor, shutdown_rx));
    Ok(PgServerHandle {
        local_addr,
        shutdown_tx,
        task,
    })
}

async fn serve(
    listener: TcpListener,
    state: Arc<PgAppState>,
    config: PgConfig,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), PgServerError> {
    let local_addr = listener.local_addr().map_err(PgServerError::Accept)?;
    tracing::info!(
        addr = %local_addr,
        trust_auth = config.trust,
        tls = tls_acceptor.is_some(),
        max_connections = config.max_connections,
        idle_timeout_secs = config.idle_timeout_secs,
        "PostgreSQL wire protocol server started"
    );

    let permits = Arc::new(Semaphore::new(config.max_connections));
    let (connection_shutdown_tx, connection_shutdown_rx) = watch::channel(false);
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    tracing::warn!(%error, "PG connection task panicked");
                }
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted.map_err(PgServerError::Accept)?;
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    tracing::warn!(peer = %peer_addr, "PG connection rejected: max connections reached");
                    tokio::spawn(reject_too_many_connections(stream, config.max_connections));
                    continue;
                };

                let state = state.clone();
                let tls_acceptor = tls_acceptor.clone();
                let connection_shutdown_rx = connection_shutdown_rx.clone();
                let idle_timeout = Duration::from_secs(config.idle_timeout_secs);
                connections.spawn(async move {
                    let _permit = permit;
                    let _active_guard = ActiveConnectionGuard::new();
                    let (context, activity_rx) =
                        ConnectionContext::new(&state.server_version);
                    let handlers = NexoraPgWireHandlers {
                        simple_handler: Arc::new(NexoraSimpleQueryHandler {
                            state: state.clone(),
                            context: context.clone(),
                        }),
                        extended_handler: Arc::new(NexoraExtendedQueryHandler),
                        startup_handler: Arc::new(NexoraStartupHandler::new(
                            &state,
                            context,
                        )),
                    };
                    process_connection(
                        stream,
                        peer_addr,
                        tls_acceptor,
                        handlers,
                        connection_shutdown_rx,
                        activity_rx,
                        idle_timeout,
                    )
                    .await;
                });
            }
        }
    }

    tracing::info!(
        active_connections = ACTIVE_CONNECTIONS.load(Ordering::Relaxed),
        "PG server stopping active connections"
    );
    let _ = connection_shutdown_tx.send(true);
    let grace = Duration::from_secs(config.shutdown_grace_secs);
    if tokio::time::timeout(grace, async {
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                tracing::warn!(%error, "PG connection task panicked during shutdown");
            }
        }
    })
    .await
    .is_err()
    {
        tracing::warn!(
            ?grace,
            "PG shutdown grace period expired; aborting connections"
        );
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    tracing::info!("PostgreSQL wire protocol server stopped");
    Ok(())
}

async fn process_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    handlers: NexoraPgWireHandlers,
    mut shutdown_rx: watch::Receiver<bool>,
    activity_rx: watch::Receiver<ActivityState>,
    idle_timeout: Duration,
) {
    let protocol = process_socket(stream, tls_acceptor, handlers);
    tokio::pin!(protocol);
    let idle = wait_for_idle(activity_rx, idle_timeout);
    tokio::pin!(idle);

    tokio::select! {
        result = &mut protocol => match result {
            Ok(()) => tracing::debug!(peer = %peer_addr, "PG connection closed"),
            Err(error) => tracing::debug!(peer = %peer_addr, %error, "PG connection error"),
        },
        _ = &mut idle => {
            tracing::info!(peer = %peer_addr, ?idle_timeout, "PG connection closed after idle timeout");
        }
        _ = shutdown_rx.changed() => {
            tracing::debug!(peer = %peer_addr, "PG connection closed for server shutdown");
        }
    }
}

async fn wait_for_idle(mut activity_rx: watch::Receiver<ActivityState>, timeout: Duration) {
    loop {
        let state = *activity_rx.borrow_and_update();
        if state.busy {
            if activity_rx.changed().await.is_err() {
                return;
            }
            continue;
        }
        tokio::select! {
            _ = tokio::time::sleep(timeout) => return,
            changed = activity_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

async fn reject_too_many_connections(mut stream: TcpStream, max: usize) {
    let message = format!("too many connections (maximum {max})");
    let mut payload = Vec::new();
    for (field, value) in [
        (b'S', "FATAL"),
        (b'V', "FATAL"),
        (b'C', "53300"),
        (b'M', message.as_str()),
    ] {
        payload.push(field);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
    }
    payload.push(0);

    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(b'E');
    frame.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    let _ = stream.write_all(&frame).await;
    let _ = stream.shutdown().await;
}

fn load_tls_acceptor(
    config: &PgConfig,
) -> Result<Option<pgwire::tokio::TlsAcceptor>, PgServerError> {
    let (Some(cert_path), Some(key_path)) = (&config.tls_cert, &config.tls_key) else {
        return Ok(None);
    };
    let cert_pem =
        std::fs::read(cert_path).map_err(|source| PgServerError::ReadTlsCertificate {
            path: cert_path.clone(),
            source,
        })?;
    let key_pem = std::fs::read(key_path).map_err(|source| PgServerError::ReadTlsKey {
        path: key_path.clone(),
        source,
    })?;
    let certificates = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(cert_pem)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| PgServerError::ReadTlsCertificate {
            path: cert_path.clone(),
            source,
        })?;
    let private_key = rustls_pemfile::private_key(&mut BufReader::new(Cursor::new(key_pem)))
        .map_err(|source| PgServerError::ReadTlsKey {
            path: key_path.clone(),
            source,
        })?
        .ok_or_else(|| PgServerError::MissingTlsKey(key_path.clone()))?;
    // The workspace enables more than one rustls provider through different
    // crates. Select the provider used by pgwire explicitly.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(PgServerError::InvalidTls)?;
    Ok(Some(tokio_rustls::TlsAcceptor::from(Arc::new(
        server_config,
    ))))
}

struct ActiveConnectionGuard;

impl ActiveConnectionGuard {
    fn new() -> Self {
        ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn active_connection_count() -> usize {
    ACTIVE_CONNECTIONS.load(Ordering::Relaxed)
}

fn bind_endpoint(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}
