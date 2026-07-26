//! Ontology API handlers — CRUD for domain-package (schema) definitions.
//!
//! Stage 6: exposes the [`OntologyManager`](nexora_core::ontology_manager::OntologyManager)
//! over HTTP. Definitions are persisted to the control-plane store and restored
//! on startup, so ontologies survive restarts.
#![allow(dead_code)]

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use super::AppState;
use nexora_core::domain_package::DomainPackage;
use nexora_core::ontology_manager::OntologyError;

// ============================================================
// Error mapping
// ============================================================

/// Map an [`OntologyError`] to an HTTP status + JSON body.
fn error_response(err: OntologyError) -> (StatusCode, Json<serde_json::Value>) {
    let status = match &err {
        OntologyError::Validation(_) | OntologyError::Serialization(_) => StatusCode::BAD_REQUEST,
        OntologyError::NotFound(_) => StatusCode::NOT_FOUND,
        OntologyError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": err.to_string() })))
}

// ============================================================
// API Handlers
// ============================================================

/// POST /api/v2/ontologies — Create or update an ontology from a domain package.
///
/// Accepts a JSON body matching [`DomainPackage`]:
/// ```json
/// {
///   "schema": {
///     "domain": "iot",
///     "version": "1.0",
///     "labels": [],
///     "edge_types": [],
///     "constraints": [],
///     "indexes": []
///   },
///   "mappings": [],
///   "standing_queries": [],
///   "materialized_views": []
/// }
/// ```
pub async fn create_ontology(
    State(state): State<AppState>,
    Json(pkg): Json<DomainPackage>,
) -> impl IntoResponse {
    let domain = pkg.schema.domain.clone();
    // Validate before proposing (Raft or local).
    if let Err(e) = state.ontology_manager.validate(&pkg).await {
        return error_response(e).into_response();
    }
    // Raft mode: propose via consensus; every node activates via its apply callback.
    if let Some(ref cm) = state.cluster_manager {
        if cm.is_raft_enabled() {
            let pkg_json = match serde_json::to_vec(&pkg) {
                Ok(j) => j,
                Err(e) => {
                    return error_response(OntologyError::Serialization(e.to_string()))
                        .into_response()
                }
            };
            match cm.propose_ontology_put(&domain, pkg_json).await {
                Ok(true) => {
                    return (
                        StatusCode::CREATED,
                        Json(json!({
                            "domain": domain,
                            "status": "created",
                        })),
                    )
                        .into_response();
                }
                Ok(false) => {} // fallthrough to broadcast
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("Raft propose failed: {e}")})),
                    )
                        .into_response();
                }
            }
        }
    }
    // Fallback: local create + best-effort broadcast (single-node or Raft disabled).
    match state.ontology_manager.create(pkg).await {
        Ok(domain) => {
            activate_ontology(&state, &domain).await;
            broadcast_ontology(&state, &domain).await;
            (
                StatusCode::CREATED,
                Json(json!({
                    "domain": domain,
                    "status": "created",
                })),
            )
                .into_response()
        }
        Err(e) => error_response(e).into_response(),
    }
}

/// POST /api/v2/ontologies/yaml — Create or update an ontology from a YAML body.
///
/// The request body is the raw YAML text of a domain package.
pub async fn create_ontology_yaml(
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    // Parse YAML to domain package.
    let pkg: DomainPackage = match serde_yaml::from_str(&body) {
        Ok(p) => p,
        Err(e) => {
            return error_response(OntologyError::Serialization(format!("YAML parse: {e}")))
                .into_response()
        }
    };
    let domain = pkg.schema.domain.clone();
    // Validate.
    if let Err(e) = state.ontology_manager.validate(&pkg).await {
        return error_response(e).into_response();
    }
    // Raft mode: propose.
    if let Some(ref cm) = state.cluster_manager {
        if cm.is_raft_enabled() {
            let pkg_json = match serde_json::to_vec(&pkg) {
                Ok(j) => j,
                Err(e) => {
                    return error_response(OntologyError::Serialization(e.to_string()))
                        .into_response()
                }
            };
            match cm.propose_ontology_put(&domain, pkg_json).await {
                Ok(true) => {
                    return (
                        StatusCode::CREATED,
                        Json(json!({
                            "domain": domain,
                            "status": "created",
                        })),
                    )
                        .into_response();
                }
                Ok(false) => {}
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("Raft propose failed: {e}")})),
                    )
                        .into_response();
                }
            }
        }
    }
    // Fallback.
    match state.ontology_manager.create(pkg).await {
        Ok(domain) => {
            activate_ontology(&state, &domain).await;
            broadcast_ontology(&state, &domain).await;
            (
                StatusCode::CREATED,
                Json(json!({
                    "domain": domain,
                    "status": "created",
                })),
            )
                .into_response()
        }
        Err(e) => error_response(e).into_response(),
    }
}

/// Activate a just-created/updated ontology in the event-first plane: ensure each
/// mapped topic's Iceberg event table exists and hot-update the shared router so
/// the topic double-writes (Both) immediately — no restart. App-layer mediation:
/// `OntologyManager` lives in nexora-core, which cannot depend on nexora-eventlog,
/// so the store/router coupling is done here where both are in scope. No-op when
/// event-first is off or the store failed to initialize.
#[allow(unused_variables)]
async fn activate_ontology(state: &AppState, domain: &str) {
    #[cfg(feature = "event-first")]
    {
        let (Some(store), Some(router)) = (&state.event_store, &state.event_router) else {
            return;
        };
        let Some(pkg) = state.ontology_manager.get(domain).await else {
            return;
        };
        for m in &pkg.mappings {
            if let Err(e) = store.ensure_table_from_domain(&m.source, &pkg).await {
                tracing::warn!(
                    "Failed to ensure event table '{}' for domain '{}': {}",
                    m.source,
                    domain,
                    e
                );
            }
        }
        router.apply_domain_package(&pkg);

        // Schedule the package's materialized views for periodic refresh.
        #[cfg(feature = "event-first")]
        if let Some(scheduler) = &state.refresh_scheduler {
            schedule_domain_views(scheduler, &pkg).await;
        }

        tracing::info!("Ontology '{}' activated in event-first plane", domain);
    }
}

/// Broadcast a just-created/updated ontology to every peer node so each registers
/// the schema locally (creates its event tables + updates its router). This makes
/// a cross-node event query work no matter which node the ontology was created on
/// and which node the query lands on. No-op in single-node mode (no router) or
/// when the router has no remote client. Best-effort per peer: a peer that fails
/// to apply is logged but does not fail the originating request — the ontology is
/// persisted in the control plane and replayed on that peer's next restart.
#[allow(unused_variables)]
async fn broadcast_ontology(state: &AppState, domain: &str) {
    let Some(router) = state.router.as_ref() else {
        return; // single-node
    };
    let Some(client) = router.remote_client_arc() else {
        return;
    };
    let Some(pkg) = state.ontology_manager.get(domain).await else {
        return;
    };
    let pkg_json = match serde_json::to_string(&pkg) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("broadcast_ontology: serialize '{}' failed: {}", domain, e);
            return;
        }
    };
    let local_id = router.local_node_id().await;
    for node_id in router.all_node_ids().await {
        if node_id == local_id {
            continue;
        }
        let op = nexora_zenoh::GraphOperation::ApplyOntology {
            pkg_json: pkg_json.clone(),
        };
        match client.execute(&node_id, op).await {
            Ok(_) => tracing::info!("Broadcast ontology '{}' to node {}", domain, node_id),
            Err(e) => tracing::warn!(
                "broadcast_ontology: node {} failed to apply '{}': {} (will replay on its restart)",
                node_id,
                domain,
                e
            ),
        }
    }
}

/// Broadcast an ontology *removal* to every peer so each drops the domain's
/// routing rules locally (symmetric with [`broadcast_ontology`]). Event tables
/// are kept on every node (append-only source of truth). Best-effort per peer.
#[allow(unused_variables)]
async fn broadcast_ontology_removal(state: &AppState, domain: &str) {
    let Some(router) = state.router.as_ref() else {
        return; // single-node
    };
    let Some(client) = router.remote_client_arc() else {
        return;
    };
    let local_id = router.local_node_id().await;
    for node_id in router.all_node_ids().await {
        if node_id == local_id {
            continue;
        }
        let op = nexora_zenoh::GraphOperation::RemoveOntology {
            domain: domain.to_string(),
        };
        match client.execute(&node_id, op).await {
            Ok(_) => tracing::info!(
                "Broadcast ontology removal '{}' to node {}",
                domain,
                node_id
            ),
            Err(e) => tracing::warn!(
                "broadcast_ontology_removal: node {} failed to remove '{}': {}",
                node_id,
                domain,
                e
            ),
        }
    }
}

/// Schedule every materialized view in a domain package.
///
/// - Pull → periodic full refresh (`schedule`).
/// - Push / Hybrid(push_enabled) → periodic incremental refresh (`schedule_incremental`):
///   only the source's new Iceberg-snapshot files are read and merged into the prior
///   state; a no-op when nothing changed.
///
/// Boundary worth knowing: `MaterializedView::from_domain_mv` always yields a
/// *SQL-transform* view, and incremental refresh for SQL views currently falls back
/// to a full recompute (true incremental only applies to structured `Aggregate`
/// views built via `MaterializedView::aggregate`). So a push-mode DomainMV here gets
/// a short-interval full refresh — correct, just not yet delta-only. Aggregate-style
/// DomainMV parsing is future work.
///
/// Conversion failures (a view whose query is not single-table SQL) are logged and
/// skipped — one bad view never blocks the rest. Shared by ontology activation and
/// startup replay (main.rs) so both paths schedule identically.
#[cfg(feature = "event-first")]
pub(crate) async fn schedule_domain_views(
    scheduler: &nexora_eventlog::RefreshScheduler,
    pkg: &DomainPackage,
) {
    use nexora_eventlog::{MaterializedView, RefreshMode};

    /// Push 视图的增量刷新周期(秒)。
    const PUSH_INTERVAL_SECS: u64 = 5;

    for dmv in &pkg.materialized_views {
        let view = match MaterializedView::from_domain_mv(dmv) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Skipping materialized view '{}': {}", dmv.name, e);
                continue;
            }
        };
        let scheduled = match view.refresh_mode {
            RefreshMode::Pull { interval_secs } => scheduler
                .schedule(view.clone(), interval_secs)
                .await
                .map(|_| {
                    tracing::info!(
                        "Scheduled materialized view '{}' (full refresh every {}s)",
                        view.name,
                        interval_secs
                    );
                }),
            RefreshMode::Hybrid {
                push_enabled: false,
                pull_interval_secs,
            } => scheduler
                .schedule(view.clone(), pull_interval_secs)
                .await
                .map(|_| {
                    tracing::info!(
                        "Scheduled materialized view '{}' (full refresh every {}s)",
                        view.name,
                        pull_interval_secs
                    );
                }),
            RefreshMode::Push
            | RefreshMode::Hybrid {
                push_enabled: true, ..
            } => scheduler
                .schedule_incremental(view.clone(), PUSH_INTERVAL_SECS)
                .await
                .map(|_| {
                    tracing::info!(
                        "Scheduled materialized view '{}' (incremental every {}s)",
                        view.name,
                        PUSH_INTERVAL_SECS
                    );
                }),
        };
        if let Err(e) = scheduled {
            tracing::warn!("Failed to schedule view '{}': {}", view.name, e);
        }
    }
}

/// GET /api/v2/ontologies — List all registered ontologies plus aggregate stats.
pub async fn list_ontologies(State(state): State<AppState>) -> impl IntoResponse {
    let domains = state.ontology_manager.list().await;
    let (domain_count, total_labels, total_edge_types) = state.ontology_manager.stats().await;

    (
        StatusCode::OK,
        Json(json!({
            "domains": domains,
            "stats": {
                "domain_count": domain_count,
                "total_labels": total_labels,
                "total_edge_types": total_edge_types,
            },
        })),
    )
        .into_response()
}

/// GET /api/v2/ontologies/{domain} — Fetch a single ontology definition.
pub async fn get_ontology(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> impl IntoResponse {
    match state.ontology_manager.get(&domain).await {
        Some(pkg) => (StatusCode::OK, Json(pkg)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("domain not found: {}", domain) })),
        )
            .into_response(),
    }
}

/// DELETE /api/v2/ontologies/{domain} — Remove an ontology definition.
pub async fn delete_ontology(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> impl IntoResponse {
    // Raft mode: propose deletion via consensus; every node deactivates via apply.
    if let Some(ref cm) = state.cluster_manager {
        if cm.is_raft_enabled() {
            match cm.propose_ontology_delete(&domain).await {
                Ok(true) => {
                    return (
                        StatusCode::OK,
                        Json(json!({
                            "domain": domain,
                            "status": "removed",
                        })),
                    )
                        .into_response();
                }
                Ok(false) => {} // fallthrough
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("Raft propose failed: {e}")})),
                    )
                        .into_response();
                }
            }
        }
    }
    // Fallback: local remove + best-effort broadcast.
    // Capture the package before removal so we can unmap its router rules. The
    // event *table* is intentionally NOT dropped: it is an append-only source of
    // truth and must survive an ontology deletion (also iceberg 0.9.1 lacks a
    // table-drop path). Deleting an ontology only stops NEW routing to it.
    #[cfg(feature = "event-first")]
    let pkg_before = state.ontology_manager.get(&domain).await;

    match state.ontology_manager.remove(&domain).await {
        Ok(()) => {
            #[cfg(feature = "event-first")]
            if let (Some(router), Some(pkg)) = (&state.event_router, pkg_before) {
                router.remove_domain_package(&pkg);
                tracing::info!("Ontology '{}' routing rules removed", domain);
            }
            broadcast_ontology_removal(&state, &domain).await;
            (
                StatusCode::OK,
                Json(json!({
                    "domain": domain,
                    "status": "removed",
                })),
            )
                .into_response()
        }
        Err(e) => error_response(e).into_response(),
    }
}

/// POST /api/v2/ontologies/validate — Dry-run validate a domain package.
///
/// Does not register or persist anything; only reports whether the package
/// would pass validation.
pub async fn validate_ontology(
    State(state): State<AppState>,
    Json(pkg): Json<DomainPackage>,
) -> impl IntoResponse {
    match state.ontology_manager.validate(&pkg).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "domain": pkg.schema.domain,
                "valid": true,
            })),
        )
            .into_response(),
        Err(e) => error_response(e).into_response(),
    }
}
