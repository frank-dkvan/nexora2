//! PostgreSQL simple-query protocol implementation.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use nexora_id::{NexoraId, PropertyValue};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error_mapping;
use crate::session::ConnectionContext;
use crate::PgAppState;

#[derive(Clone)]
pub struct NexoraSimpleQueryHandler {
    pub state: Arc<PgAppState>,
    pub context: ConnectionContext,
}

#[async_trait]
impl SimpleQueryHandler for NexoraSimpleQueryHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.context.begin_query();
        let result = self.execute_query(query).await;
        self.context.end_query();
        result
    }
}

impl NexoraSimpleQueryHandler {
    async fn execute_query(&self, query: &str) -> PgWireResult<Vec<Response>> {
        let statements = match Parser::parse_sql(&GenericDialect {}, query) {
            Ok(statements) => statements,
            Err(error) => {
                return Ok(vec![error_response(
                    error_mapping::SQLSTATE_SYNTAX_ERROR,
                    format!("parse error: {error}"),
                )]);
            }
        };
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            let sql = statement.to_string();
            tracing::debug!(%sql, "PG simple query");

            if let Some(response) =
                crate::catalog::try_handle_compatibility_query(&self.state, &self.context, &sql)
                    .await
            {
                let response = response?;
                let is_error = matches!(response, Response::Error(_));
                responses.push(response);
                if is_error {
                    break;
                }
                continue;
            }

            // P0.2: Check if this is a SELECT query against a materialized view
            if let Statement::Query(ref query_ast) = statement {
                if let Some(view_name) = extract_view_name_from_query(query_ast) {
                    if self.state.mv_manager.view_exists(&view_name).await {
                        tracing::debug!(view_name = %view_name, "Routing query to materialized view");
                        match crate::mv_handler::query_materialized_view(
                            &self.state.mv_manager,
                            &view_name,
                            query_ast,
                        )
                        .await
                        {
                            Ok(response) => {
                                responses.push(response);
                                continue;
                            }
                            Err(error) => {
                                responses.push(Response::Error(Box::new(ErrorInfo::new(
                                    "ERROR".to_string(),
                                    "58000".to_string(),
                                    format!("materialized view query error: {}", error),
                                ))));
                                break;
                            }
                        }
                    }
                }
            }

            // Event-first: Check if this is a SELECT query against an event table
            if let Some(event_response) =
                crate::event_table_handler::try_query_event_table(&self.state, &statement).await
            {
                match event_response {
                    Ok(response) => {
                        responses.push(response);
                        continue;
                    }
                    Err(error) => {
                        responses.push(Response::Error(Box::new(ErrorInfo::new(
                            "ERROR".to_string(),
                            "58000".to_string(),
                            format!("event table query error: {}", error),
                        ))));
                        break;
                    }
                }
            }

            if self.is_readonly_session().await && is_write_statement(&statement) {
                responses.push(error_response(
                    "42501",
                    "permission denied: readonly users cannot execute write statements".to_owned(),
                ));
                break;
            }

            // Expand `SELECT *` / `SELECT t.*` on a node table into explicit
            // property columns. A bare `*` translates to Cypher `RETURN n`, which
            // serializes each node as a single JSON object under one column named
            // `n` — GUI clients (DBeaver) then show one opaque column and can even
            // NPE on the untyped object. By sampling the label's property names
            // and rewriting to `SELECT id, name, speed, … FROM t`, the client gets
            // real per-property typed columns. Only rewrites plain node SELECTs;
            // anything else is left untouched.
            let sql = self.expand_select_star(&statement, &sql).await;
            // Re-parse if we rewrote the SQL so downstream sees the new projection.
            let statement = if sql != statement.to_string() {
                match parse_single_statement(&sql) {
                    Some(s) => s,
                    None => statement, // parse failed: fall back to original
                }
            } else {
                statement
            };

            // Cluster mode: route whole-graph statements through the distributed
            // planner instead of this node's local shards. On a real multi-node
            // cluster, running SQL against only the local graph would return
            // incomplete / misrouted results, so anything the distributed planner
            // cannot prove mergeable is refused with an explicit error rather than
            // silently falling back to local execution. Single-node (router None or
            // all-local) returns None here and the normal local path runs unchanged.
            if let Some(dist) = self.try_cluster_route(&statement, &sql).await {
                match dist {
                    Ok(response) => {
                        responses.push(response);
                        continue;
                    }
                    Err(response) => {
                        responses.push(response);
                        break;
                    }
                }
            }

            // For UPDATE/DELETE, try to extract node IDs from WHERE clause first.
            // If that fails (complex WHERE), query before execution to find matching nodes.
            let pre_execution_node_ids = if matches!(
                statement,
                Statement::Update { .. } | Statement::Delete(_)
            ) {
                // First try the fast path: extract IDs directly from WHERE clause
                let where_ids = extract_node_ids_from_where_clause_for_statement(&statement);

                if !where_ids.is_empty() {
                    // Fast path: WHERE has explicit ID conditions
                    tracing::info!(
                        "Fast path: extracted {} node IDs from WHERE clause",
                        where_ids.len()
                    );
                    Some(where_ids)
                } else {
                    // Slow path: complex WHERE clause, need to query
                    tracing::info!("Slow path: querying to find matching nodes");
                    match self.extract_pre_execution_node_ids(&statement).await {
                        Ok(ids) => {
                            tracing::info!("Slow path: found {} matching nodes", ids.len());
                            Some(ids)
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to extract pre-execution node IDs");
                            None
                        }
                    }
                }
            } else {
                None
            };

            // FIX C: Route SQL execution through QueryPool to enforce concurrency limit.
            // Before: PgWire bypassed the pool, allowing unbounded concurrent queries.
            let query_pool = Arc::clone(&self.state.query_pool);
            let graph = Arc::clone(&self.state.graph);
            let sql_for_exec = sql.clone();
            match query_pool
                .execute(async move { nexora_sql::execute_sql(&graph, &sql_for_exec).await })
                .await
            {
                Ok(result) => {
                    // P0 FIX: Trigger Standing Query evaluation after successful write
                    if is_write_statement(&statement) {
                        if let Err(e) = self
                            .trigger_standing_queries(&statement, &result, pre_execution_node_ids)
                            .await
                        {
                            tracing::warn!(error = %e, "Failed to trigger standing queries after write");
                        }
                    }

                    responses.push(sql_result_to_response(&statement, result)?);
                }
                Err(error) => {
                    let message = error.to_string();
                    responses.push(error_response(
                        error_mapping::classify_sql_error(&message),
                        message,
                    ));
                    // PostgreSQL skips the remainder of a simple-query message
                    // after the first error.
                    break;
                }
            }
        }
        Ok(responses)
    }

    /// Cluster-mode dispatch: route a whole-graph statement through the
    /// distributed planner instead of this node's local shards.
    ///
    /// Returns:
    /// - `None` — not applicable: single-node (no router) or every shard is
    ///   local, so the normal local execution path is correct. Also `None` for
    ///   MV-backed reads and catalog queries (handled before this is called).
    /// - `Some(Ok(response))` — the statement was executed distributively.
    /// - `Some(Err(response))` — cluster mode is active but the statement is not
    ///   in the provably-mergeable subset (or an owner was unreachable). Refused
    ///   with an explicit error rather than silently returning local/partial data.
    async fn try_cluster_route(
        &self,
        statement: &Statement,
        sql: &str,
    ) -> Option<Result<Response, Response>> {
        let router = self.state.router.as_ref()?;
        // Single-node / all-local: local execution is correct and cheaper.
        if router.all_shards_local().await {
            return None;
        }

        // Translate SQL → Cypher; the distributed planner analyzes Cypher.
        let (cypher, is_write) = match nexora_sql::translate_sql_to_cypher(sql) {
            Ok(t) => t,
            Err(e) => {
                return Some(Err(error_response(
                    error_mapping::SQLSTATE_SYNTAX_ERROR,
                    format!("cluster: cannot translate SQL to a distributed plan: {e}"),
                )));
            }
        };

        // Only the provably-mergeable subset is safe to run across owners.
        // Everything else is refused (no silent local fallback in cluster mode).
        let Some(plan) = nexora_zenoh::distributed_query::plan(&cypher) else {
            return Some(Err(error_response(
                // 0A000 = feature_not_supported
                "0A000",
                format!(
                    "cluster mode: this statement is not supported by the distributed \
                     query planner yet, and Nexora will not run it against a single \
                     node's local shards (that would return incomplete or misrouted \
                     results). Translated Cypher: {cypher}"
                ),
            )));
        };

        // For a DELETE, the affected nodes vanish once the write runs, so their
        // ids must be captured BEFORE execution (an UPDATE leaves them in place,
        // but capturing up front keeps one code path). We derive the affected-id
        // set from the write's own Cypher — turning its `MATCH … WHERE …` prefix
        // into a `RETURN id(n)` read run distributively — so it works for both
        // explicit-id and predicate WHERE clauses across every owner.
        let pre_write_ids: Vec<NexoraId> =
            if is_write && matches!(statement, Statement::Update { .. } | Statement::Delete(_)) {
                match self.distributed_write_affected_ids(router, &cypher).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        tracing::warn!(error = %e, "cluster: capturing affected ids for SQ failed");
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            };

        // C1/C2: reads run under Majority read concern with this session's
        // read-after-write tracker and the cluster's replication progress, so a
        // failover read never serves data older than a quorum holds or older than
        // the client's own last write. Writes ignore the concern (they fan to
        // owners regardless). progress is None in single-node / untracked mode,
        // which degrades Majority to owner-first failover (prior behavior).
        let concern = nexora_zenoh::ReadConcern::Majority;
        let progress = self.state.replication_progress.as_deref();
        let session = self.state.session_tracker.as_ref();
        match nexora_zenoh::distributed_query::execute(
            router,
            &plan,
            Some(concern),
            progress,
            Some(session),
        )
        .await
        {
            Ok((columns, rows)) => {
                // After a successful distributed write, evaluate Standing Queries
                // on the affected nodes. Unlike the single-node path, we must
                // fetch each node's properties through the router (the node may be
                // owned by a remote shard, so this node's local graph doesn't hold
                // it). Skip on failure rather than fail the write — SQ evaluation
                // is a side effect, not part of the write's success contract.
                if is_write {
                    if let Err(e) = self
                        .trigger_standing_queries_distributed(statement, &pre_write_ids)
                        .await
                    {
                        tracing::warn!(error = %e, "cluster: SQ trigger after distributed write failed");
                    }
                }
                Some(Ok(
                    self.distributed_result_to_response(statement, columns, rows)
                ))
            }
            Err(e) => Some(Err(distributed_router_error_response(&e))),
        }
    }

    /// Capture the node ids a distributed write will affect, by rewriting the
    /// write's Cypher into a `RETURN id(n)` read and running it distributively.
    ///
    /// The translated write always has the shape `MATCH (n…) [WHERE …] <write>`
    /// where `<write>` is `SET …` / `DETACH DELETE n` / `DELETE n` / `REMOVE …`.
    /// Truncating at the first write keyword leaves `MATCH (n…) [WHERE …]`, to
    /// which we append `RETURN id(n)` — a filtered projection read the planner
    /// distributes across owners. Returns the matched node ids (from every owner).
    async fn distributed_write_affected_ids(
        &self,
        router: &Arc<nexora_zenoh::router::HybridRouter>,
        write_cypher: &str,
    ) -> Result<Vec<NexoraId>, Box<dyn std::error::Error + Send + Sync>> {
        // Find where the write clause begins; everything before it is the read
        // prefix. Keywords are matched case-insensitively with surrounding spaces
        // so a property named e.g. "reset" can't be mistaken for REMOVE. The scan
        // ignores any keyword occurring INSIDE a quoted string literal (e.g.
        // `WHERE n.name = 'go SET go'`), which a raw substring search would
        // wrongly cut at, corrupting the read prefix.
        let Some(cut) = find_write_clause_start(write_cypher) else {
            // No write clause found — nothing to derive (shouldn't happen for a
            // write, but don't guess).
            return Ok(Vec::new());
        };
        let read_cypher = format!("{} RETURN id(n)", &write_cypher[..cut]);

        let Some(plan) = nexora_zenoh::distributed_query::plan(&read_cypher) else {
            // The affected-id read isn't distributable — surface nothing rather
            // than risk a partial/local id set. The write already committed, so
            // this is a real (if narrow) gap: its SQ/MV side effects won't fire.
            // Log loudly rather than degrade silently.
            tracing::warn!(
                read_cypher = %read_cypher,
                "cluster: affected-id capture for a committed write is not distributable; \
                 Standing Query / Materialized View side effects will NOT fire for this write"
            );
            return Ok(Vec::new());
        };
        // Affected-id capture is internal write bookkeeping (which nodes a write
        // will touch), not a client read, so default concern / no session guard.
        let (_cols, rows) =
            nexora_zenoh::distributed_query::execute(router, &plan, None, None, None).await?;

        // `RETURN id(n)` yields either a bare hex string or, depending on the
        // executor, a node object whose `"id"` field holds the hex qid. Accept
        // both so the affected-id set is captured regardless of shape.
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let hex = match row.first() {
                Some(serde_json::Value::String(s)) => Some(s.as_str()),
                Some(serde_json::Value::Object(map)) => map.get("id").and_then(|v| v.as_str()),
                _ => None,
            };
            if let Some(hex) = hex {
                if let Ok(id) = NexoraId::from_hex(hex) {
                    ids.push(id);
                }
            }
        }
        Ok(ids)
    }

    /// Trigger Standing Query evaluation after a distributed write, fetching each
    /// affected node's properties through the router so remote-owned nodes are
    /// visible (the single-node `trigger_standing_queries` reads this node's local
    /// graph, which in cluster mode does not hold remotely-owned nodes).
    ///
    /// - INSERT: affected ids come from the VALUES clause; fetch each node's
    ///   current properties and evaluate SQs (a match transition).
    /// - UPDATE: affected ids are `pre_write_ids` (captured from the write's own
    ///   MATCH/WHERE); the nodes still exist, so fetch properties and evaluate.
    /// - DELETE: affected ids are `pre_write_ids` captured BEFORE the write (the
    ///   nodes are now gone); evaluate with empty properties so a previously
    ///   matching node transitions to unmatched.
    async fn trigger_standing_queries_distributed(
        &self,
        statement: &Statement,
        pre_write_ids: &[NexoraId],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(sq_manager) = self.state.sq_manager.as_ref() else {
            return Ok(());
        };
        let Some(router) = self.state.router.as_ref() else {
            return Ok(());
        };

        // A DELETE's nodes no longer exist: signal unmatch with empty properties
        // for each id captured before the write ran.
        if matches!(statement, Statement::Delete(_)) {
            let empty: std::collections::HashMap<String, PropertyValue> =
                std::collections::HashMap::new();
            for node_id in pre_write_ids {
                sq_manager
                    .on_property_change(node_id, "_deleted", &PropertyValue::Boolean(true), &empty)
                    .await;
            }
            return Ok(());
        }

        // INSERT reads ids from the VALUES clause; UPDATE uses the pre-captured
        // affected ids (the nodes still exist for a property re-read).
        let affected: Vec<NexoraId> = if matches!(statement, Statement::Update { .. }) {
            pre_write_ids.to_vec()
        } else {
            let empty_result = nexora_sql::SqlResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                query_time_ms: 0,
                translated_cypher: String::new(),
                rows_affected: None,
            };
            extract_affected_node_ids(statement, &empty_result)?
        };

        for node_id in affected {
            // Fetch the node's full property set from whichever node owns it.
            let props = match router
                .route(
                    &node_id,
                    nexora_zenoh::GraphOperation::GetAllProperties {
                        qid: node_id.clone(),
                    },
                )
                .await
            {
                Ok(nexora_zenoh::GraphResult::Property(Some(serde_json::Value::Object(map)))) => {
                    map
                }
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!(node = %node_id, error = %e, "cluster: fetch properties for SQ failed");
                    continue;
                }
            };

            let properties: std::collections::HashMap<String, PropertyValue> = props
                .into_iter()
                .filter_map(|(k, v)| json_to_property_value(&v).map(|pv| (k, pv)))
                .collect();

            for (key, value) in &properties {
                sq_manager
                    .on_property_change(&node_id, key, value, &properties)
                    .await;
            }
        }
        Ok(())
    }

    /// Convert a distributed query result into a PG `Response`.
    ///
    /// A distributed write returns the 8 well-known stat columns (see
    /// [`nexora_zenoh::distributed_query::WRITE_STAT_COLUMNS`]) and a single
    /// summed row; that is surfaced as a command tag (INSERT/UPDATE/DELETE n),
    /// matching the single-node write response. Everything else is a row set.
    fn distributed_result_to_response(
        &self,
        statement: &Statement,
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    ) -> Response {
        if let Some(affected) = write_affected_from_stat_columns(&columns, &rows) {
            return Response::Execution(command_tag(statement, affected));
        }

        if columns.is_empty() {
            return Response::Execution(command_tag(statement, 0));
        }

        let clean_columns = columns
            .iter()
            .map(|column| column.strip_prefix("n.").unwrap_or(column).to_owned())
            .collect::<Vec<_>>();
        let schema = Arc::new(build_field_infos_from_sample(&clean_columns, &rows));
        let row_schema = schema.clone();
        let row_iter = rows.into_iter().map(move |row| {
            let mut encoder = DataRowEncoder::new(row_schema.clone());
            encode_row(&mut encoder, &row_schema, &row)?;
            Ok(encoder.take_row())
        });
        Response::Query(QueryResponse::new(schema, futures::stream::iter(row_iter)))
    }

    async fn is_readonly_session(&self) -> bool {
        if self.state.trust_auth {
            return false;
        }
        let username = self.context.session.lock().await.username.clone();
        self.state
            .users
            .get(&username)
            .is_some_and(|(_, role)| role == "readonly")
    }

    /// Trigger Standing Query evaluation after a successful write operation.
    ///
    /// This extracts the affected node IDs from the SQL statement and result,
    /// fetches their current properties, and triggers SQ evaluation on each.
    async fn trigger_standing_queries(
        &self,
        statement: &Statement,
        result: &nexora_sql::SqlResult,
        pre_execution_node_ids: Option<Vec<NexoraId>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("trigger_standing_queries called");

        // Only trigger if SQ manager is configured
        let sq_manager: &Arc<nexora_standing_query::StandingQueryManager> =
            match &self.state.sq_manager {
                Some(mgr) => mgr,
                None => {
                    tracing::debug!("No Standing Query manager configured, skipping SQ evaluation");
                    return Ok(());
                }
            };

        // Check if this is a DELETE statement
        let is_delete = matches!(statement, Statement::Delete(_));

        // Use pre-execution node IDs if available (for UPDATE/DELETE with complex WHERE),
        // otherwise extract from statement/result
        let mut affected_nodes = if let Some(ids) = pre_execution_node_ids {
            tracing::info!("Using {} pre-execution node IDs", ids.len());
            ids
        } else {
            extract_affected_node_ids(statement, result)?
        };

        // If still no IDs were found, try the old fallback logic
        if affected_nodes.is_empty() {
            affected_nodes = match statement {
                Statement::Update {
                    table, selection, ..
                } => {
                    self.query_matching_nodes_from_table(table, selection)
                        .await?
                }
                Statement::Delete(delete) => {
                    if let Some(first_table) = delete.tables.first() {
                        self.query_matching_nodes_from_name(first_table, &delete.selection)
                            .await?
                    } else {
                        Vec::new()
                    }
                }
                _ => Vec::new(),
            };
            tracing::info!(
                "Queried {} matching nodes for complex WHERE clause",
                affected_nodes.len()
            );
        }

        tracing::info!("Extracted {} affected nodes", affected_nodes.len());

        if affected_nodes.is_empty() {
            tracing::trace!("No affected nodes found for SQ evaluation");
            return Ok(());
        }

        let node_count = affected_nodes.len();
        tracing::debug!(
            node_count,
            "Triggering Standing Query evaluation for affected nodes"
        );

        let mut total_matches = 0;

        // For DELETE operations, trigger SQ with empty properties to cause unmatch
        if is_delete {
            tracing::info!(
                "DELETE operation detected, triggering SQ with empty properties for {} nodes",
                node_count
            );
            for node_id in affected_nodes {
                tracing::info!("Triggering SQ unmatch for deleted node: {}", node_id);
                // Use empty properties to signal the node no longer exists
                let empty_props = std::collections::HashMap::new();
                sq_manager
                    .on_property_change(
                        &node_id,
                        "_deleted",
                        &PropertyValue::Boolean(true),
                        &empty_props,
                    )
                    .await;
            }
            return Ok(());
        }

        // For INSERT/UPDATE operations, fetch properties and trigger on_property_change
        for node_id in affected_nodes {
            tracing::info!("Processing node: {}", node_id);

            // Fetch all current properties of this node
            let properties_btree = match self.state.graph.get_all_properties(&node_id).await {
                Ok(props) => props,
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch properties for node {}: {}, treating as deleted",
                        node_id,
                        e
                    );
                    // Node might have been deleted concurrently, trigger with empty props
                    let empty_props = std::collections::HashMap::new();
                    sq_manager
                        .on_property_change(
                            &node_id,
                            "_deleted",
                            &PropertyValue::Boolean(true),
                            &empty_props,
                        )
                        .await;
                    continue;
                }
            };

            tracing::info!("Node {} has {} properties", node_id, properties_btree.len());

            // Convert BTreeMap<Symbol, PropertyValue> to HashMap<String, PropertyValue>
            let properties: std::collections::HashMap<String, PropertyValue> = properties_btree
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.clone()))
                .collect();

            // Trigger SQ evaluation for each property change
            // (In a real scenario, we'd track which specific properties changed,
            // but for simplicity we evaluate all properties)
            for (key, value) in &properties {
                let matches = sq_manager
                    .on_property_change(&node_id, key, value, &properties)
                    .await;
                total_matches += matches;

                if matches > 0 {
                    tracing::info!("Property {} matched {} queries", key, matches);
                }
            }
        }

        if total_matches > 0 {
            tracing::info!(
                node_count,
                total_matches,
                "Standing Query evaluation completed"
            );
        }

        Ok(())
    }

    /// Extract node IDs affected by UPDATE/DELETE BEFORE execution.
    ///
    /// This queries the database to find all nodes matching the WHERE clause
    /// before the statement is executed, so we know which nodes will be affected.
    async fn extract_pre_execution_node_ids(
        &self,
        statement: &Statement,
    ) -> Result<Vec<NexoraId>, Box<dyn std::error::Error + Send + Sync>> {
        match statement {
            Statement::Update {
                table, selection, ..
            } => self.query_matching_nodes_from_table(table, selection).await,
            Statement::Delete(delete) => {
                // Extract table name from the FROM clause
                use sqlparser::ast::FromTable;
                let table_name = match &delete.from {
                    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                        if let Some(first_table) = tables.first() {
                            first_table.relation.to_string()
                        } else {
                            return Ok(Vec::new());
                        }
                    }
                };

                self.query_matching_nodes_impl(&table_name, &delete.selection)
                    .await
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Query nodes matching the given WHERE clause (for UPDATE statements).
    ///
    /// Executes a MATCH query to find all nodes that satisfy the WHERE condition.
    /// Returns the list of matching node IDs.
    async fn query_matching_nodes_from_table(
        &self,
        table: &sqlparser::ast::TableWithJoins,
        selection: &Option<sqlparser::ast::Expr>,
    ) -> Result<Vec<NexoraId>, Box<dyn std::error::Error + Send + Sync>> {
        let table_name = table.relation.to_string();
        self.query_matching_nodes_impl(&table_name, selection).await
    }

    /// Query nodes matching the given WHERE clause (for DELETE statements).
    async fn query_matching_nodes_from_name(
        &self,
        table: &sqlparser::ast::ObjectName,
        selection: &Option<sqlparser::ast::Expr>,
    ) -> Result<Vec<NexoraId>, Box<dyn std::error::Error + Send + Sync>> {
        let table_name = table.to_string();
        self.query_matching_nodes_impl(&table_name, selection).await
    }

    /// Internal implementation for querying matching nodes.
    async fn query_matching_nodes_impl(
        &self,
        table_name: &str,
        selection: &Option<sqlparser::ast::Expr>,
    ) -> Result<Vec<NexoraId>, Box<dyn std::error::Error + Send + Sync>> {
        // Build a SELECT query to find matching nodes
        let where_clause = match selection {
            Some(expr) => format!(" WHERE {}", expr),
            None => String::new(),
        };

        let query = format!("SELECT id FROM {}{}", table_name, where_clause);
        tracing::info!("Executing query to find affected nodes: {}", query);

        // Execute the query
        let result = nexora_sql::execute_sql(&self.state.graph, &query)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        tracing::info!("Query returned {} rows", result.rows.len());

        // Extract node IDs from the result
        let mut node_ids = Vec::new();
        for row in &result.rows {
            if let Some(serde_json::Value::String(id_str)) = row.first() {
                let node_id = if let Ok(id) = NexoraId::from_hex(id_str) {
                    id
                } else {
                    NexoraId::from_bytes(id_str.as_bytes().to_vec())
                };
                node_ids.push(node_id);
            }
        }

        tracing::info!("Found {} nodes matching WHERE clause", node_ids.len());
        Ok(node_ids)
    }

    /// Expand a `SELECT *` / `SELECT t.*` on a node table into an explicit
    /// column list, returning the rewritten SQL (or the original `sql` unchanged
    /// if this isn't a plain node-table star select).
    ///
    /// Why: a bare `*` compiles to Cypher `RETURN n`, which serializes each node
    /// as one JSON object in a single column named `n`. GUI clients then show one
    /// opaque column (and DBeaver NPEs opening the untyped object). Sampling the
    /// label's property names and rewriting to `SELECT id, name, speed, … FROM t`
    /// gives real per-property typed columns. Read-only, best-effort: any reason
    /// we can't confidently expand → return the SQL untouched.
    async fn expand_select_star(&self, statement: &Statement, sql: &str) -> String {
        let Statement::Query(query) = statement else {
            return sql.to_owned();
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return sql.to_owned();
        };
        // Only a single, unqualified-or-`t.*` wildcard projection; no GROUP BY /
        // aggregates / DISTINCT (those have their own translation).
        let has_group_by = !matches!(
            &select.group_by,
            sqlparser::ast::GroupByExpr::Expressions(e, _) if e.is_empty()
        );
        if select.projection.len() != 1 || select.distinct.is_some() || has_group_by {
            return sql.to_owned();
        }
        let is_star = matches!(
            select.projection[0],
            sqlparser::ast::SelectItem::Wildcard(_)
                | sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
        );
        if !is_star {
            return sql.to_owned();
        }
        // Single FROM table, no JOINs.
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return sql.to_owned();
        }
        let raw_table = select.from[0].relation.to_string();
        let label = nexora_sql::normalize_table_ident(&raw_table);
        // Edge tables and the `nodes` catch-all keep their existing behavior.
        if label.is_empty() || label.eq_ignore_ascii_case("nodes") || label.starts_with("edge_") {
            return sql.to_owned();
        }

        let columns = self.sample_label_columns(&label).await;
        if columns.is_empty() {
            return sql.to_owned();
        }

        // Rebuild: SELECT <cols> FROM <original relation + rest of query>. Reuse
        // the original text after FROM so WHERE/ORDER BY/LIMIT are preserved.
        let col_list = columns.join(", ");
        let upper = sql.to_ascii_uppercase();
        let Some(from_idx) = upper.find(" FROM ") else {
            return sql.to_owned();
        };
        format!("SELECT {} {}", col_list, &sql[from_idx + 1..])
    }

    /// Sample the union of property names across a few nodes of `label`, always
    /// including `id`. Returns `[]` if the label has no nodes (caller then leaves
    /// the query as `*`). Deterministic order: `id` first, then first-seen.
    async fn sample_label_columns(&self, label: &str) -> Vec<String> {
        const SAMPLE: usize = 20;
        let ids = self.state.graph.label_index.query(label).await;
        if ids.is_empty() {
            return Vec::new();
        }
        let mut columns: Vec<String> = vec!["id".to_owned()];
        for qid in ids.iter().take(SAMPLE) {
            if let Ok(props) = self.state.graph.get_all_properties(qid).await {
                for (k, _) in props {
                    let name = k.to_string();
                    // `label` is a reserved projection alias in our RETURN mapping
                    // and `id` is already present; skip dupes.
                    if name != "id" && !columns.contains(&name) {
                        columns.push(name);
                    }
                }
            }
        }
        columns
    }
}

/// Parse exactly one SQL statement, returning it only if the input is a single
/// well-formed statement. Used to re-parse a rewritten query.
fn parse_single_statement(sql: &str) -> Option<Statement> {
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if stmts.len() == 1 {
        Some(stmts.remove(0))
    } else {
        None
    }
}

/// Extract node IDs affected by a write statement.
///
/// For INSERT: parses the node ID from the VALUES clause
/// For UPDATE: extracts from the WHERE clause (if present)
/// For DELETE: extracts from the WHERE clause (if present)
fn extract_affected_node_ids(
    statement: &Statement,
    result: &nexora_sql::SqlResult,
) -> Result<Vec<NexoraId>, Box<dyn std::error::Error + Send + Sync>> {
    let mut node_ids = Vec::new();

    match statement {
        Statement::Insert(insert) => {
            // For INSERT, try to extract node IDs from the VALUES clause
            if let Some(source) = &insert.source {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    for row in &values.rows {
                        // First column is typically the node ID
                        if let Some(expr) = row.first() {
                            if let Some(id_str) = extract_string_literal(expr) {
                                // Try hex first, then fall back to raw bytes
                                let node_id = if let Ok(id) = NexoraId::from_hex(&id_str) {
                                    id
                                } else {
                                    // Treat as raw string bytes (for test IDs like "v001", "s001")
                                    NexoraId::from_bytes(id_str.as_bytes().to_vec())
                                };
                                node_ids.push(node_id);
                            }
                        }
                    }
                }
            }
        }
        Statement::Update {
            table: _,
            selection,
            ..
        } => {
            // Extract node IDs from UPDATE's WHERE clause
            node_ids = extract_node_ids_from_where_clause(selection);

            // Also check if the result contains node IDs in the first column
            if node_ids.is_empty() && !result.rows.is_empty() {
                for row in &result.rows {
                    if let Some(serde_json::Value::String(id_str)) = row.first() {
                        let node_id = if let Ok(id) = NexoraId::from_hex(id_str) {
                            id
                        } else {
                            NexoraId::from_bytes(id_str.as_bytes().to_vec())
                        };
                        node_ids.push(node_id);
                    }
                }
            }

            tracing::debug!("UPDATE: extracted {} node IDs", node_ids.len());
        }
        Statement::Delete(delete) => {
            // Extract node IDs from DELETE's WHERE clause
            node_ids = extract_node_ids_from_where_clause(&delete.selection);

            tracing::debug!(
                "DELETE: extracted {} node IDs from WHERE clause",
                node_ids.len()
            );
        }
        _ => {}
    }

    Ok(node_ids)
}

/// Extract a string literal from an SQL expression
fn extract_string_literal(expr: &sqlparser::ast::Expr) -> Option<String> {
    match expr {
        sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s))
        | sqlparser::ast::Expr::Value(sqlparser::ast::Value::DoubleQuotedString(s)) => {
            Some(s.clone())
        }
        _ => None,
    }
}

/// Extract node IDs from a WHERE clause (for UPDATE/DELETE statements).
///
/// This function looks for conditions like:
/// - `WHERE id = 'node_id'`
/// - `WHERE id IN ('id1', 'id2', ...)`
/// - `WHERE node_id = 'value'` (if column is "id" or "node_id")
fn extract_node_ids_from_where_clause(selection: &Option<sqlparser::ast::Expr>) -> Vec<NexoraId> {
    let mut node_ids = Vec::new();

    let Some(expr) = selection else {
        return node_ids;
    };

    extract_ids_recursive(expr, &mut node_ids);
    node_ids
}

/// Extract node IDs from WHERE clause of UPDATE/DELETE statements.
fn extract_node_ids_from_where_clause_for_statement(statement: &Statement) -> Vec<NexoraId> {
    match statement {
        Statement::Update { selection, .. } => extract_node_ids_from_where_clause(selection),
        Statement::Delete(delete) => extract_node_ids_from_where_clause(&delete.selection),
        _ => Vec::new(),
    }
}

/// Recursively extract node IDs from an SQL expression tree
fn extract_ids_recursive(expr: &sqlparser::ast::Expr, node_ids: &mut Vec<NexoraId>) {
    use sqlparser::ast::{BinaryOperator, Expr};

    match expr {
        // Handle: id = 'value'
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::Eq => {
                    // Check if left side is an ID column
                    if is_id_column(left) {
                        if let Some(id) = extract_node_id_from_value(right) {
                            node_ids.push(id);
                        }
                    }
                    // Also check reversed: 'value' = id
                    else if is_id_column(right) {
                        if let Some(id) = extract_node_id_from_value(left) {
                            node_ids.push(id);
                        }
                    }
                }
                // Handle AND/OR - recurse into both sides
                BinaryOperator::And | BinaryOperator::Or => {
                    extract_ids_recursive(left, node_ids);
                    extract_ids_recursive(right, node_ids);
                }
                _ => {}
            }
        }
        // Handle: id IN ('id1', 'id2', ...)
        Expr::InList { expr, list, .. } => {
            if is_id_column(expr) {
                for item in list {
                    if let Some(id) = extract_node_id_from_value(item) {
                        node_ids.push(id);
                    }
                }
            }
        }
        // Handle nested expressions
        Expr::Nested(inner) => {
            extract_ids_recursive(inner, node_ids);
        }
        _ => {}
    }
}

/// Check if an expression refers to an ID column (id, node_id, or similar)
fn is_id_column(expr: &sqlparser::ast::Expr) -> bool {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            name == "id" || name == "node_id" || name == "_id"
        }
        sqlparser::ast::Expr::CompoundIdentifier(parts) => {
            // Handle qualified names like "n.id" or "table.id"
            parts
                .last()
                .map(|ident| {
                    let name = ident.value.to_lowercase();
                    name == "id" || name == "node_id" || name == "_id"
                })
                .unwrap_or(false)
        }
        _ => false,
    }
}

/// Extract a NexoraId from an SQL value expression
fn extract_node_id_from_value(expr: &sqlparser::ast::Expr) -> Option<NexoraId> {
    if let Some(id_str) = extract_string_literal(expr) {
        // Try hex first, then fall back to raw bytes
        if let Ok(id) = NexoraId::from_hex(&id_str) {
            Some(id)
        } else {
            // Treat as raw string bytes (for test IDs like "v001", "v002")
            Some(NexoraId::from_bytes(id_str.as_bytes().to_vec()))
        }
    } else {
        None
    }
}

/// Extract the table/view name from a SELECT query
fn extract_view_name_from_query(query: &sqlparser::ast::Query) -> Option<String> {
    if let SetExpr::Select(select) = query.body.as_ref() {
        if let Some(table_with_joins) = select.from.first() {
            if let sqlparser::ast::TableFactor::Table { name, .. } = &table_with_joins.relation {
                // Get the last part of the name (handles schema-qualified names)
                return name.0.last().map(|ident| ident.value.clone());
            }
        }
    }
    None
}

fn is_write_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_)
    )
}

fn sql_result_to_response(
    statement: &Statement,
    result: nexora_sql::SqlResult,
) -> PgWireResult<Response> {
    if result.columns.is_empty() {
        return Ok(Response::Execution(command_tag(
            statement,
            result.rows_affected.unwrap_or(0),
        )));
    }

    let clean_columns = result
        .columns
        .iter()
        .map(|column| column.strip_prefix("n.").unwrap_or(column).to_owned())
        .collect::<Vec<_>>();
    let schema = Arc::new(build_field_infos_from_sample(&clean_columns, &result.rows));
    let row_schema = schema.clone();
    let rows = result.rows.into_iter().map(move |row| {
        let mut encoder = DataRowEncoder::new(row_schema.clone());
        encode_row(&mut encoder, &row_schema, &row)?;
        Ok(encoder.take_row())
    });
    Ok(Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(rows),
    )))
}

fn command_tag(statement: &Statement, affected: usize) -> Tag {
    match statement {
        Statement::Insert(insert) => {
            let rows = insert
                .source
                .as_ref()
                .and_then(|source| match source.body.as_ref() {
                    SetExpr::Values(values) => Some(values.rows.len()),
                    _ => None,
                })
                .unwrap_or(affected);
            Tag::new("INSERT").with_oid(0).with_rows(rows)
        }
        Statement::Update { assignments, .. } => {
            let rows = affected / assignments.len().max(1);
            Tag::new("UPDATE").with_rows(rows)
        }
        Statement::Delete(_) => Tag::new("DELETE").with_rows(affected),
        _ => Tag::new("OK"),
    }
}

pub fn build_field_infos_from_sample(
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
) -> Vec<FieldInfo> {
    columns
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let mut inferred = None;
            let mut mixed = false;
            for value in rows.iter().filter_map(|row| row.get(index)) {
                if value.is_null() {
                    continue;
                }
                let value_type = infer_pg_type(value);
                if inferred
                    .as_ref()
                    .is_some_and(|current| current != &value_type)
                {
                    mixed = true;
                    break;
                }
                inferred = Some(value_type);
            }
            FieldInfo::new(
                name.clone(),
                None,
                None,
                if mixed {
                    Type::TEXT
                } else {
                    inferred.unwrap_or(Type::TEXT)
                },
                FieldFormat::Text,
            )
        })
        .collect()
}

fn infer_pg_type(value: &serde_json::Value) -> Type {
    match value {
        serde_json::Value::Bool(_) => Type::BOOL,
        serde_json::Value::Number(number) if number.is_f64() => Type::FLOAT8,
        serde_json::Value::Number(_) => Type::INT8,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Type::JSONB,
        serde_json::Value::Null | serde_json::Value::String(_) => Type::TEXT,
    }
}

fn encode_row(
    encoder: &mut DataRowEncoder,
    schema: &[FieldInfo],
    row: &[serde_json::Value],
) -> PgWireResult<()> {
    if row.len() != schema.len() {
        return Err(user_error(
            "XX000",
            format!(
                "query returned {} values for {} columns",
                row.len(),
                schema.len()
            ),
        ));
    }
    for (field, value) in schema.iter().zip(row) {
        encode_value(encoder, field.datatype(), value)?;
    }
    Ok(())
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    data_type: &Type,
    value: &serde_json::Value,
) -> PgWireResult<()> {
    if value.is_null() {
        return encoder.encode_field_with_type_and_format(
            &Option::<String>::None,
            data_type,
            FieldFormat::Text,
            &Default::default(),
        );
    }
    let text = match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Bool(value) => {
            if *value {
                "t".to_owned()
            } else {
                "f".to_owned()
            }
        }
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => serde_json::to_string(value)
            .map_err(|error| {
                user_error(
                    "XX000",
                    format!("failed to serialize result value: {error}"),
                )
            })?,
        serde_json::Value::Null => unreachable!(),
    };
    encoder.encode_field_with_type_and_format(
        &text,
        data_type,
        FieldFormat::Text,
        &Default::default(),
    )
}

fn error_response(code: &str, message: String) -> Response {
    Response::Error(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

/// If a distributed result is a write-stats row (the 8 `WRITE_STAT_COLUMNS`),
/// return the total number of rows affected (created + deleted, node + rel).
/// Returns `None` for ordinary row sets, which are surfaced as query results.
fn write_affected_from_stat_columns(
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
) -> Option<usize> {
    let expected = nexora_zenoh::distributed_query::WRITE_STAT_COLUMNS;
    if columns.len() != expected.len() || !columns.iter().zip(expected).all(|(c, e)| c == e) {
        return None;
    }
    let row = rows.first()?;
    // Columns: nodes_created, nodes_deleted, rels_created, rels_deleted,
    // properties_set, properties_removed, labels_added, labels_removed.
    // Report the mutation count the way command_tag expects (affected rows).
    let sum_at = |idx: usize| -> i64 { row.get(idx).and_then(|v| v.as_i64()).unwrap_or(0) };
    let affected = sum_at(0) + sum_at(1) + sum_at(2) + sum_at(3);
    Some(affected.max(0) as usize)
}

/// Map a cross-node routing failure to an explicit PG error. A routing failure
/// is not this node's internal fault — the shard's owner is unreachable or
/// rejected the op. Surfacing it (rather than returning local/partial data)
/// lets clients tell "owner is down" apart from a real query error.
fn distributed_router_error_response(e: &nexora_zenoh::RouterError) -> Response {
    use nexora_zenoh::RouterError;
    let (code, kind) = match e {
        // 57P01/08006-class: shard owner not currently serviceable.
        RouterError::NodeNotFound(_) => ("08006", "shard_owner_unavailable"),
        RouterError::Timeout => ("08006", "shard_owner_timeout"),
        RouterError::Remote(_) | RouterError::Serialization(_) => {
            ("08000", "remote_execution_failed")
        }
        RouterError::QuorumFailed { acked, required } => {
            return error_response(
                "08006",
                format!(
                    "cluster: write quorum failed (acked={acked}, required={required}). \
                     Not enough replicas acknowledged the write."
                ),
            );
        }
    };
    error_response(
        code,
        format!(
            "cluster: cross-node routing failed ({kind}): {e}. This shard is owned by \
             another node; with replication factor 1 an owner being down makes the \
             shard unavailable."
        ),
    )
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

/// Convert a JSON value (as returned by the router's `GetAllProperties`) into a
/// `PropertyValue` for Standing Query evaluation. Mirrors the reverse
/// `property_to_json` used by the graph adapter: integers stay integers, other
/// numbers become floats, objects become maps.
fn json_to_property_value(v: &serde_json::Value) -> Option<PropertyValue> {
    Some(match v {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(f)
            } else {
                return None;
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            PropertyValue::List(arr.iter().filter_map(json_to_property_value).collect())
        }
        serde_json::Value::Object(map) => {
            let m: std::collections::BTreeMap<String, PropertyValue> = map
                .iter()
                .filter_map(|(k, v)| json_to_property_value(v).map(|pv| (k.clone(), pv)))
                .collect();
            PropertyValue::Map(m)
        }
    })
}

/// Find the byte offset where a write clause (` SET `/` DETACH DELETE `/
/// ` DELETE `/` REMOVE `) begins in a translated write Cypher, scanning only
/// text OUTSIDE single-quoted string literals. A naive substring search would
/// mis-cut a query like `MATCH (n) WHERE n.name = 'go SET go' SET n.x = 1` at
/// the literal's ` SET `, producing a malformed read prefix. Cypher string
/// literals use single quotes with `''` as an escaped quote; we track quote
/// state and only match keywords in unquoted regions. Returns `None` if no
/// write clause is found outside a literal.
fn find_write_clause_start(cypher: &str) -> Option<usize> {
    const KEYWORDS: [&str; 4] = [" SET ", " DETACH DELETE ", " DELETE ", " REMOVE "];
    let bytes = cypher.as_bytes();
    let upper = cypher.to_uppercase();
    let upper_bytes = upper.as_bytes();
    let mut in_quote = false;
    let mut i = 0;
    while i < bytes.len() {
        if in_quote {
            // Inside a single-quoted literal. `''` is an escaped quote (stay in);
            // a lone `'` closes it.
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_quote = false;
            }
            i += 1;
            continue;
        }
        if bytes[i] == b'\'' {
            in_quote = true;
            i += 1;
            continue;
        }
        // Unquoted region: does a write keyword start here?
        for kw in KEYWORDS {
            if upper_bytes[i..].starts_with(kw.as_bytes()) {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_handles_comments_and_quoted_semicolons() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT 'a;b'; /* ; */ SELECT 2; -- ;\nSELECT 3",
        )
        .unwrap();
        assert_eq!(statements.len(), 3);
    }

    #[test]
    fn find_write_clause_start_ignores_keywords_in_literals() {
        // Keyword inside a WHERE string literal must NOT be the cut point; the
        // real ` SET ` after the literal is.
        let cy = "MATCH (n:Person) WHERE n.name = 'go SET go' SET n.flag = true";
        let cut = find_write_clause_start(cy).expect("must find the real SET");
        assert_eq!(&cy[cut..], " SET n.flag = true");

        // DELETE inside a literal is ignored; the real DETACH DELETE is found.
        let cy = "MATCH (n:Person) WHERE n.note = 'please DELETE me' DETACH DELETE n";
        let cut = find_write_clause_start(cy).expect("must find DETACH DELETE");
        assert_eq!(&cy[cut..], " DETACH DELETE n");

        // Escaped quote ('') inside the literal keeps quote tracking correct.
        let cy = "MATCH (n) WHERE n.x = 'a''b SET c' REMOVE n.y";
        let cut = find_write_clause_start(cy).expect("must find REMOVE");
        assert_eq!(&cy[cut..], " REMOVE n.y");

        // No write clause → None (a pure read).
        assert!(find_write_clause_start("MATCH (n:Person) RETURN n").is_none());
    }

    #[test]
    fn schema_is_inferred_per_column_and_skips_nulls() {
        let columns = vec!["name".to_owned(), "age".to_owned()];
        let rows = vec![
            vec![serde_json::Value::Null, serde_json::json!(30)],
            vec![serde_json::json!("Alice"), serde_json::json!(25)],
        ];
        let fields = build_field_infos_from_sample(&columns, &rows);
        assert_eq!(fields[0].datatype(), &Type::TEXT);
        assert_eq!(fields[1].datatype(), &Type::INT8);
    }

    #[test]
    fn mixed_column_types_fall_back_to_text() {
        let fields = build_field_infos_from_sample(
            &["value".to_owned()],
            &[vec![serde_json::json!(1)], vec![serde_json::json!("one")]],
        );
        assert_eq!(fields[0].datatype(), &Type::TEXT);
    }
}
