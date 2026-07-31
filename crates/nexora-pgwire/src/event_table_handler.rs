//! Event table query handler — routes SELECT queries on Iceberg event tables
//! through DataFusion when event-first mode is active.
//!
//! When a SELECT targets a table registered in the EventLogStore's Iceberg
//! catalog, every node's local rows for that table are gathered (this node
//! directly, peers via a `ScanEventTable` fan-out over the graph transport),
//! unioned into an in-memory DataFusion table, and the original SQL is run over
//! the merged rows. The Arrow result is streamed back over the PG wire protocol
//! as text-format rows. Running the SQL over the union (rather than merging
//! per-node results) keeps aggregates and GROUP BY correct across the cluster.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
use sqlparser::ast::Statement;
// These are only used by the event-first query path below.
#[cfg(feature = "event-first")]
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse};
#[cfg(feature = "event-first")]
use pgwire::api::Type;
#[cfg(feature = "event-first")]
use pgwire::error::{ErrorInfo, PgWireError};
#[cfg(feature = "event-first")]
use sqlparser::ast::{Query, SetExpr, TableFactor};
#[cfg(feature = "event-first")]
use std::sync::Arc;

/// Check if a SELECT query targets an event table and route to DataFusion if so.
///
/// Returns:
/// - `Some(Ok(response))` — query executed against an event table via DataFusion
/// - `Some(Err(error))` — event table query failed
/// - `None` — not an event table query, caller continues normal routing
#[cfg(feature = "event-first")]
pub async fn try_query_event_table(
    state: &crate::PgAppState,
    statement: &Statement,
) -> Option<PgWireResult<Response>> {
    // Only handle SELECT queries
    let query_ast = match statement {
        Statement::Query(q) => q,
        _ => return None,
    };

    // Extract table name; bail if not a simple single-table SELECT
    let table_name = extract_table_name_from_query(query_ast)?;

    // Check if it's an event table (exists in the EventLogStore catalog)
    let event_store = state.event_store.as_ref()?;
    let event_tables = event_store.list_tables().await.ok()?;
    if !event_tables.contains(&table_name) {
        return None; // Not an event table — normal routing applies
    }

    tracing::debug!(table_name = %table_name, "Routing query to event table via DataFusion");

    let sql = statement.to_string();
    Some(execute_event_query(state, &table_name, &sql).await)
}

#[cfg(not(feature = "event-first"))]
pub async fn try_query_event_table(
    _state: &crate::PgAppState,
    _statement: &Statement,
) -> Option<PgWireResult<Response>> {
    None
}

/// Execute a SQL query over an event table, gathering rows from every node in
/// the cluster (each node owns an independent local Iceberg store) and running
/// the SQL over the merged rows.
///
/// Steps:
/// 1. Scan this node's local event table → RecordBatches.
/// 2. In cluster mode, fan a `ScanEventTable` op out to every *other* node via
///    the router's remote client; each returns its rows as hex-encoded Arrow IPC
///    bytes, which we decode back into RecordBatches (schema + types preserved).
/// 3. Register the union of all batches as an in-memory table under the bare
///    table name and run the original SQL over it — so WHERE / GROUP BY /
///    aggregates all compute correctly over the whole cluster's data, on this
///    coordinator, rather than incorrectly summing per-node partial aggregates.
#[cfg(feature = "event-first")]
async fn execute_event_query(
    state: &crate::PgAppState,
    table_name: &str,
    sql: &str,
) -> PgWireResult<Response> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    let event_store = state
        .event_store
        .as_ref()
        .ok_or_else(|| user_error("event store not available".to_string()))?;

    // 1. Local rows.
    let mut all_batches = event_store
        .read_table_batches(table_name)
        .await
        .map_err(|e| user_error(format!("Failed to scan local event table: {e}")))?;

    // 2. Peer rows (cluster mode only).
    if let Some(router) = state.router.as_ref() {
        if let Some(client) = router.remote_client_arc() {
            let local_id = router.local_node_id().await;
            let node_ids = router.all_node_ids().await;
            for node_id in node_ids.iter().filter(|n| **n != local_id) {
                let op = nexora_zenoh::GraphOperation::ScanEventTable {
                    table: table_name.to_string(),
                };
                match client.execute(node_id, op).await {
                    Ok(nexora_zenoh::GraphResult::Property(Some(serde_json::Value::String(
                        hex_str,
                    )))) => {
                        if hex_str.is_empty() {
                            continue; // peer has no such table / no rows
                        }
                        match decode_ipc_batches(&hex_str) {
                            Ok(mut batches) => all_batches.append(&mut batches),
                            Err(e) => {
                                return Err(user_error(format!(
                                    "Failed to decode event rows from node {node_id}: {e}"
                                )));
                            }
                        }
                    }
                    Ok(_) => {} // unexpected shape — treat as no contribution
                    Err(e) => {
                        // A peer being unreachable makes the answer incomplete;
                        // fail loudly rather than silently return partial rows.
                        return Err(user_error(format!(
                            "Failed to scan event table on node {node_id}: {e}"
                        )));
                    }
                }
            }
        }
    }

    // 3. Determine a schema. Prefer a non-empty batch; if the whole cluster is
    // empty, fall back to the local table's Iceberg schema so an empty result
    // still reports the right columns.
    let schema = if let Some(b) = all_batches.first() {
        b.schema()
    } else {
        match event_store.load_table(table_name).await {
            Ok(table) => {
                let iceberg_schema = table.metadata().current_schema();
                match arrow_schema::Schema::try_from(iceberg_schema.as_ref()) {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        return Err(user_error(format!(
                            "Failed to derive schema for empty event table: {e}"
                        )))
                    }
                }
            }
            Err(e) => return Err(user_error(format!("Failed to load event table: {e}"))),
        }
    };

    let ctx = SessionContext::new();
    let mem_table = MemTable::try_new(schema.clone(), vec![all_batches])
        .map_err(|e| user_error(format!("Failed to build in-memory table: {e}")))?;
    ctx.register_table(table_name, Arc::new(mem_table))
        .map_err(|e| user_error(format!("Failed to register table: {e}")))?;

    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| user_error(format!("DataFusion query error: {e}")))?;
    let result_batches = df
        .collect()
        .await
        .map_err(|e| user_error(format!("Failed to collect results: {e}")))?;

    arrow_batches_to_response(result_batches)
}

/// Decode hex-encoded Arrow IPC stream bytes back into RecordBatches.
#[cfg(feature = "event-first")]
fn decode_ipc_batches(hex_str: &str) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("hex decode: {e}"))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| format!("IPC reader: {e}"))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| format!("IPC batch: {e}"))?);
    }
    Ok(batches)
}

/// Extract the table name from a single-table SELECT query.
#[cfg(feature = "event-first")]
fn extract_table_name_from_query(query: &Query) -> Option<String> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    if select.from.len() != 1 {
        return None;
    }
    match &select.from[0].relation {
        TableFactor::Table { name, .. } => {
            // Use the last identifier segment as the bare table name
            name.0.last().map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

#[cfg(feature = "event-first")]
fn user_error(msg: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "58000".to_string(),
        msg,
    )))
}

/// Convert Arrow RecordBatches into a PG-wire text-format QueryResponse.
#[cfg(feature = "event-first")]
fn arrow_batches_to_response(
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> PgWireResult<Response> {
    if batches.is_empty() {
        let schema = Arc::new(vec![FieldInfo::new(
            "(no columns)".into(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )]);
        return Ok(Response::Query(QueryResponse::new(
            schema,
            futures::stream::empty(),
        )));
    }

    let arrow_schema = batches[0].schema();

    // Build PG field info from the Arrow schema.
    let mut fields = Vec::with_capacity(arrow_schema.fields().len());
    for field in arrow_schema.fields() {
        fields.push(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            arrow_type_to_pg(field.data_type()),
            FieldFormat::Text,
        ));
    }
    let schema = Arc::new(fields);
    let schema_for_rows = schema.clone();

    // Pre-render every cell to text so the row stream owns no Arrow borrows.
    let mut text_rows: Vec<Vec<Option<String>>> = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                row.push(arrow_cell_to_text(batch.column(col_idx), row_idx));
            }
            text_rows.push(row);
        }
    }

    let row_stream = futures::stream::iter(
        text_rows
            .into_iter()
            .map(move |row| encode_text_row(row, schema_for_rows.clone())),
    );

    Ok(Response::Query(QueryResponse::new(schema, row_stream)))
}

/// Encode a pre-rendered text row into a PG DataRow.
#[cfg(feature = "event-first")]
fn encode_text_row(
    row: Vec<Option<String>>,
    schema: Arc<Vec<FieldInfo>>,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    let mut encoder = DataRowEncoder::new(schema);
    for cell in row {
        encoder.encode_field(&cell).map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "failed to encode field: {e}"
            ))))
        })?;
    }
    Ok(encoder.take_row())
}

/// Map an Arrow DataType to the closest PG type OID for wire reporting.
#[cfg(feature = "event-first")]
fn arrow_type_to_pg(dt: &arrow_schema::DataType) -> Type {
    use arrow_schema::DataType;
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Type::INT8,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Type::INT8,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Type::FLOAT8,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        _ => Type::TEXT,
    }
}

/// Render one Arrow cell as PG text format, or None for NULL.
#[cfg(feature = "event-first")]
fn arrow_cell_to_text(array: &arrow::array::ArrayRef, row: usize) -> Option<String> {
    use arrow::array::Array;
    if array.is_null(row) {
        return None;
    }
    // arrow's display formatter renders every supported type in a readable way.
    arrow::util::display::array_value_to_string(array, row).ok()
}
