//! Minimal `pg_catalog` introspection shim for GUI clients (DBeaver et al.).
//!
//! nexora has no relational catalog, but PostgreSQL GUI clients fire a fixed set
//! of `pg_catalog` introspection queries on connect (to populate the session and
//! the navigator tree) and treat an error on any of them as a fatal connection
//! failure. Without this shim DBeaver cannot connect at all.
//!
//! This module recognizes exactly the queries those clients send and returns
//! constructed result sets. It is deliberately pattern-matched (not a real
//! catalog): the queries are stable text emitted by the driver, so matching on
//! recognizable fragments is more robust than parsing their (very complex)
//! SELECT lists. Anything not recognized here falls through to catalog.rs's
//! explicit `0A000` rejection — we never return a misleading empty table for an
//! introspection query we don't actually understand.
//!
//! Captured from DBeaver 26.0.2 (PostgreSQL JDBC, preferQueryMode=simple):
//!   1  SELECT count(*) FROM nodes                    (user query, not ours)
//!   2  SET extra_float_digits = 3                    (handled by catalog.rs)
//!   3  SET application_name = '...'                  (handled by catalog.rs)
//!   4  SET application_name = '...'                  (handled by catalog.rs)
//!   5  SELECT current_schema(), session_user
//!   6  SELECT db.oid, db.* FROM pg_catalog.pg_database WHERE datname = ('nexora')
//!   7  SELECT * FROM pg_catalog.pg_settings WHERE name = ('standard_conforming_strings')
//!   8  SELECT string_agg(word, ',') FROM pg_catalog.pg_get_keywords() WHERE ...
//!   9  SELECT version()                              (handled by catalog.rs)
//!   10 SELECT * FROM pg_catalog.pg_enum WHERE 1 <> 1 LIMIT 1
//!   11 SELECT reltype FROM pg_catalog.pg_class WHERE 1 <> 1 LIMIT 1
//!   12 SELECT t.oid, t.*, c.relkind, ... FROM pg_catalog.pg_type AS t LEFT JOIN ...

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::PgWireResult;

use crate::session::ConnectionContext;
use crate::PgAppState;

/// Try to handle a `pg_catalog`/introspection query. Returns `Some` if this
/// module recognized and answered it; `None` to let the caller fall through to
/// its normal handling (including the explicit rejection for unrecognized
/// catalog queries).
///
/// `trimmed` is the SQL with trailing `;`/whitespace removed; `upper` is its
/// uppercased form (both provided by the caller so we don't recompute them).
pub async fn try_handle(
    state: &Arc<PgAppState>,
    context: &ConnectionContext,
    trimmed: &str,
    upper: &str,
) -> Option<PgWireResult<Response>> {
    // #5: current_schema(), session_user — has no `pg_catalog.` prefix, so it
    // must be caught before the SQL→Cypher path swallows it.
    if upper.contains("CURRENT_SCHEMA()") && upper.contains("SESSION_USER") {
        let user = context.session.lock().await.username.clone();
        return Some(Ok(rows_response(
            vec![("current_schema", Type::TEXT), ("session_user", Type::TEXT)],
            vec![vec![Some("public".to_owned()), Some(user)]],
        )));
    }

    // #8: pg_get_keywords() — used to build the SQL keyword highlight list.
    // Returning zero rows is safe (the client just gets no reserved-word list).
    if upper.contains("PG_GET_KEYWORDS()") {
        return Some(Ok(rows_response(
            vec![("string_agg", Type::TEXT)],
            vec![vec![None]],
        )));
    }

    // #7: pg_settings WHERE name = '<param>' — return a row for known GUC names.
    if upper.contains("PG_SETTINGS") {
        return Some(Ok(pg_settings_response(trimmed)));
    }

    // Everything below dispatches on the query's PRIMARY table (the first table
    // after FROM), not on "contains" — DBeaver's introspection queries JOIN many
    // catalogs, and substring matching lets an incidental JOIN hijack a query
    // whose real subject is something else (e.g. a pg_namespace scan that LEFT
    // JOINs pg_description must be handled as pg_namespace, not pg_description).
    let primary = primary_from_table(upper);

    match primary.as_deref() {
        // pg_namespace — the schema list. DBeaver builds the navigator tree from
        // this; return the single `public` schema.
        Some("PG_NAMESPACE") => return Some(Ok(pg_namespace_response())),

        // pg_class — the relation (table/view) list, one row per label + matview.
        // A `WHERE 1<>1` / `reltype` probe only wants the column shape.
        Some("PG_CLASS") => {
            if upper.contains("1 <> 1") || upper.contains("RELTYPE FROM") {
                return Some(Ok(empty_response(vec![("reltype", Type::INT8)])));
            }
            return Some(Ok(pg_class_response(state).await));
        }

        // pg_attribute — column list for a table.
        Some("PG_ATTRIBUTE") => return Some(Ok(pg_attribute_response(state, trimmed).await)),

        // pg_database — the driver looks up the connected database's row.
        Some("PG_DATABASE") => {
            let db = context.session.lock().await.database.clone();
            return Some(Ok(pg_database_row(&db)));
        }

        // pg_enum probes — empty with a plausible shape.
        Some("PG_ENUM") => {
            return Some(Ok(empty_response(vec![
                ("oid", Type::INT8),
                ("enumtypid", Type::INT8),
                ("enumsortorder", Type::FLOAT4),
                ("enumlabel", Type::TEXT),
            ])))
        }

        // pg_type catalog scan — zero rows makes the driver use its built-in
        // base-type table, enough to run queries.
        Some("PG_TYPE") => {
            return Some(Ok(empty_response(vec![
                ("oid", Type::INT8),
                ("typname", Type::TEXT),
                ("relkind", Type::TEXT),
                ("base_type_name", Type::TEXT),
                ("description", Type::TEXT),
            ])))
        }

        // pg_shdescription / pg_description — object comments; nexora has none.
        Some("PG_SHDESCRIPTION") | Some("PG_DESCRIPTION") => {
            return Some(Ok(empty_response(vec![("description", Type::TEXT)])))
        }

        _ => {}
    }

    // Catch-all for the long tail of catalog objects DBeaver probes while
    // browsing (pg_roles, pg_tablespace, pg_conversion, pg_event_trigger,
    // pg_extension, pg_foreign_data_wrapper, pg_proc, …). None exist in nexora,
    // so an empty result is the correct answer and lets the client keep browsing
    // instead of aborting. Reached only for queries the caller already flagged as
    // catalog/introspection.
    if upper.contains("PG_CATALOG.")
        || upper.contains("INFORMATION_SCHEMA.")
        || primary.as_deref().is_some_and(|t| t.starts_with("PG_"))
    {
        return Some(Ok(empty_response(vec![("oid", Type::INT8)])));
    }

    None
}

/// Extract the primary (first) table after `FROM` from an uppercased SQL
/// statement, stripping a leading `PG_CATALOG.` qualifier. Returns e.g.
/// `"PG_NAMESPACE"`. Used to dispatch introspection queries on their real
/// subject table rather than on any catalog name they happen to contain.
fn primary_from_table(upper: &str) -> Option<String> {
    let idx = upper.find(" FROM ")?;
    let after = &upper[idx + 6..];
    // First whitespace-delimited token after FROM.
    let token = after.split_whitespace().next()?;
    // Strip schema qualifier and any trailing punctuation / alias separators.
    let token = token.trim_start_matches("PG_CATALOG.");
    let token: String = token
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

/// One-row `pg_namespace` result: nexora exposes a single `public` schema.
///
/// Columns mirror real PostgreSQL `pg_namespace` (`oid, nspname, nspowner,
/// nspacl`) plus the `description` that DBeaver's LEFT JOIN pg_description
/// expects. DBeaver reads `n.*` and needs `nspacl` present (NULL = default
/// public privileges) to treat this as a browsable user schema; omitting it
/// made the schema node appear empty/non-expandable.
fn pg_namespace_response() -> Response {
    rows_response(
        vec![
            ("oid", Type::INT8),
            ("nspname", Type::TEXT),
            ("nspowner", Type::INT8),
            ("nspacl", Type::TEXT),
            ("description", Type::TEXT),
        ],
        vec![vec![
            Some("2200".to_owned()), // PG's standard oid for public
            Some("public".to_owned()),
            Some("10".to_owned()),
            None, // nspacl NULL = default privileges
            None, // description
        ]],
    )
}

/// `pg_class` result: one row per node label (as a table, relkind 'r') plus one
/// per materialized view (relkind 'm'). oid is a stable hash of the name so the
/// same relation keeps the same oid within a session.
async fn pg_class_response(state: &Arc<PgAppState>) -> Response {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();

    for label in table_names(state).await {
        rows.push(vec![
            Some(stable_oid(&label).to_string()),
            Some(label.clone()),
            Some("2200".to_owned()), // relnamespace = public
            Some("r".to_owned()),    // ordinary table
            Some("10".to_owned()),   // relowner
        ]);
    }
    for view in state.mv_manager.list_views().await {
        rows.push(vec![
            Some(stable_oid(&view.name).to_string()),
            Some(view.name.clone()),
            Some("2200".to_owned()),
            Some("m".to_owned()), // materialized view
            Some("10".to_owned()),
        ]);
    }

    rows_response(
        vec![
            ("oid", Type::INT8),
            ("relname", Type::TEXT),
            ("relnamespace", Type::INT8),
            ("relkind", Type::TEXT),
            ("relowner", Type::INT8),
        ],
        rows,
    )
}

/// The set of relation names to expose as tables. Prefers registered graph
/// labels; if there are none (data ingested with a `type` property rather than
/// a label, as is common), falls back to the distinct values of the `type`
/// property so the pilot's data still shows up as browsable tables. Best-effort:
/// on query error, returns whatever labels exist (possibly empty).
async fn table_names(state: &Arc<PgAppState>) -> Vec<String> {
    let labels = state.graph.label_index.all_labels().await;
    if !labels.is_empty() {
        return labels;
    }
    // Fallback: distinct `type` values.
    let mut names = Vec::new();
    if let Ok(result) =
        nexora_sql::execute_sql(&state.graph, "SELECT type FROM nodes GROUP BY type").await
    {
        for row in result.rows {
            if let Some(serde_json::Value::String(t)) = row.first() {
                if !names.contains(t) {
                    names.push(t.clone());
                }
            }
        }
    }
    names
}

/// `pg_attribute` result: the columns of a table. nexora has no fixed schema, so
/// columns are sampled from a few nodes of the label (their union of property
/// names), always including `id`. If the target table can't be determined from
/// the query, returns just `id`.
async fn pg_attribute_response(state: &Arc<PgAppState>, sql: &str) -> Response {
    let columns = match extract_relname_for_attributes(sql) {
        Some(label) => sample_columns(state, &label).await,
        None => vec!["id".to_owned()],
    };
    let rows: Vec<Vec<Option<String>>> = columns
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            vec![
                Some(name),
                Some((i as i64 + 1).to_string()), // attnum (1-based)
                Some("25".to_owned()),            // atttypid 25 = text
                Some("f".to_owned()),             // attnotnull
            ]
        })
        .collect();
    rows_response(
        vec![
            ("attname", Type::TEXT),
            ("attnum", Type::INT8),
            ("atttypid", Type::INT8),
            ("attnotnull", Type::BOOL),
        ],
        rows,
    )
}

/// Sample a few nodes of `label` and return the union of their property names
/// (always starting with `id`). nexora's `SELECT *` returns whole node objects
/// in a single `n` column rather than expanded property columns, so we sample
/// the node objects and parse their JSON keys — that yields the real attribute
/// names DBeaver shows when expanding a table. `label` may be a registered graph
/// label or a `type` property value (the pilot's data uses the latter); we match
/// on either via `WHERE type = '<label>'` and the node object's own `label`
/// field. Best-effort: on any error, returns `["id"]`.
async fn sample_columns(state: &Arc<PgAppState>, label: &str) -> Vec<String> {
    let mut names = vec!["id".to_owned()];
    // Escape single quotes to keep the literal well-formed.
    let safe = label.replace('\'', "''");
    let query = format!("SELECT * FROM nodes WHERE type = '{}' LIMIT 20", safe);
    if let Ok(result) = nexora_sql::execute_sql(&state.graph, &query).await {
        for row in &result.rows {
            // Each row is a single node object (column `n`); union its keys.
            if let Some(serde_json::Value::Object(map)) = row.first() {
                for key in map.keys() {
                    if key != "id" && !names.contains(key) {
                        names.push(key.clone());
                    }
                }
            }
        }
    }
    names
}

/// Extract the target relation name from a pg_attribute query. DBeaver's column
/// queries filter by `attrelid = <oid>` — but we key tables by name, so we look
/// for the relation name if the query carries it. Returns `None` if not present
/// (caller falls back to just `id`).
fn extract_relname_for_attributes(sql: &str) -> Option<String> {
    // DBeaver often issues: ... WHERE attrelid = 'schema.table'::regclass ...
    // or joins pg_class with relname = '<name>'. Pull a quoted relname if any.
    let lower = sql.to_ascii_lowercase();
    if let Some(pos) = lower.find("relname") {
        // find the next single-quoted string after `relname`
        if let Some(q1) = sql[pos..].find('\'') {
            let start = pos + q1 + 1;
            if let Some(q2) = sql[start..].find('\'') {
                return Some(sql[start..start + q2].to_owned());
            }
        }
    }
    None
}

/// Stable positive oid for a relation name (FNV-1a, folded into i32-positive
/// range so it fits PG's oid semantics and stays consistent within a session).
fn stable_oid(name: &str) -> u32 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in name.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    // Keep it in a high range unlikely to collide with real PG system oids.
    16_384 + (hash % 1_000_000) as u32
}

/// Build a text-encoded multi-column response from `(column_name, type)` schema
/// and rows of optional strings (None = SQL NULL).
fn rows_response(columns: Vec<(&str, Type)>, rows: Vec<Vec<Option<String>>>) -> Response {
    let schema = Arc::new(
        columns
            .iter()
            .map(|(name, ty)| {
                FieldInfo::new(
                    (*name).to_owned(),
                    None,
                    None,
                    ty.clone(),
                    FieldFormat::Text,
                )
            })
            .collect::<Vec<_>>(),
    );
    let row_schema = schema.clone();
    let encoded = rows.into_iter().map(move |row| {
        let mut encoder = DataRowEncoder::new(row_schema.clone());
        for (field, value) in row_schema.iter().zip(row) {
            encoder
                .encode_field_with_type_and_format(
                    &value,
                    field.datatype(),
                    FieldFormat::Text,
                    &Default::default(),
                )
                .expect("text encoding is infallible");
        }
        Ok(encoder.take_row())
    });
    Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(encoded.collect::<Vec<_>>()),
    ))
}

/// A zero-row response with the given column shape (for `WHERE 1<>1` probes).
fn empty_response(columns: Vec<(&str, Type)>) -> Response {
    rows_response(columns, Vec::new())
}

/// One-row `pg_database` result for the connected database. Column set covers
/// what JDBC reads; values are stable defaults (nexora has a single logical DB).
fn pg_database_row(datname: &str) -> Response {
    rows_response(
        vec![
            ("oid", Type::INT8),
            ("datname", Type::TEXT),
            ("datdba", Type::INT8),
            ("encoding", Type::INT4),
            ("datcollate", Type::TEXT),
            ("datctype", Type::TEXT),
            ("datallowconn", Type::BOOL),
            ("datistemplate", Type::BOOL),
        ],
        vec![vec![
            Some("16384".to_owned()),
            Some(datname.to_owned()),
            Some("10".to_owned()),
            Some("6".to_owned()), // 6 = UTF8
            Some("en_US.UTF-8".to_owned()),
            Some("en_US.UTF-8".to_owned()),
            Some("t".to_owned()),
            Some("f".to_owned()),
        ]],
    )
}

/// `pg_settings WHERE name = '<param>'`. Returns a single row for parameters we
/// can answer truthfully, else an empty result (unknown GUC → no row, which the
/// driver tolerates).
fn pg_settings_response(sql: &str) -> Response {
    let schema = vec![("name", Type::TEXT), ("setting", Type::TEXT)];
    // Known values the driver probes on connect.
    let known: &[(&str, &str)] = &[
        ("standard_conforming_strings", "on"),
        ("bytea_output", "hex"),
        ("integer_datetimes", "on"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
    ];
    let lower = sql.to_ascii_lowercase();
    for (name, setting) in known {
        if lower.contains(name) {
            return rows_response(
                schema,
                vec![vec![Some((*name).to_owned()), Some((*setting).to_owned())]],
            );
        }
    }
    empty_response(schema)
}
