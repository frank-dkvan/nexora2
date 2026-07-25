//! Output rendering — `--json` emits machine-readable JSON to stdout; otherwise
//! a compact human format. All rendering goes to stdout; errors go to stderr
//! (handled by the caller via exit codes).

use nexora_client::{
    BulkIngestResponse, HealthResponse, ListStandingQueriesResponse, SqlResponse,
};

/// Print a value as pretty JSON, falling back to a debug-ish string on the
/// (practically impossible) serialization failure.
fn print_json<T: serde::Serialize>(value: &T) {
    match serde_json::to_string_pretty(value) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("error: serialize output: {e}"),
    }
}

/// Cypher / query rows.
pub fn rows(columns: &[String], rows: &[Vec<serde_json::Value>], json: bool) {
    if json {
        print_json(&serde_json::json!({ "columns": columns, "rows": rows }));
        return;
    }
    if columns.is_empty() && rows.is_empty() {
        println!("(no results)");
        return;
    }
    // Header.
    println!("{}", columns.join("\t"));
    for row in rows {
        let cells: Vec<String> = row.iter().map(render_cell).collect();
        println!("{}", cells.join("\t"));
    }
    println!("({} row{})", rows.len(), if rows.len() == 1 { "" } else { "s" });
}

/// Render one cell: unquote plain strings, compact-JSON everything else.
fn render_cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".into(),
        other => other.to_string(),
    }
}

/// SQL response. Its `rows` are a flat array of JSON values (one per result
/// row, already shaped server-side), unlike the Cypher `Vec<Vec<Value>>`.
pub fn sql(resp: &SqlResponse, json: bool) {
    if json {
        print_json(&serde_json::json!({
            "columns": resp.columns,
            "rows": resp.rows,
            "error": resp.error,
        }));
        return;
    }
    if let Some(err) = &resp.error {
        // Non-fatal: surfaced here, exit code stays 0 unless caller decides.
        println!("error: {err}");
        return;
    }
    if !resp.columns.is_empty() {
        println!("{}", resp.columns.join("\t"));
    }
    for row in &resp.rows {
        println!("{}", render_cell(row));
    }
    println!(
        "({} row{})",
        resp.row_count,
        if resp.row_count == 1 { "" } else { "s" }
    );
}

/// Bulk ingest stats.
pub fn bulk(resp: &BulkIngestResponse, json: bool) {
    if json {
        print_json(&serde_json::json!({
            "status": resp.status,
            "ingested": resp.ingested,
            "nodes": resp.nodes,
            "skipped": resp.skipped,
        }));
        return;
    }
    println!(
        "ingested {} record(s) into {} node(s); {} skipped",
        resp.ingested, resp.nodes, resp.skipped
    );
}

/// Standing query list.
pub fn sq_list(resp: &ListStandingQueriesResponse, json: bool) {
    if json {
        print_json(&serde_json::json!({ "standing_queries": sq_json(resp) }));
        return;
    }
    if resp.standing_queries.is_empty() {
        println!("(no standing queries)");
        return;
    }
    println!("ID\tNAME\tMATCHES\tCREATED");
    for sq in &resp.standing_queries {
        println!(
            "{}\t{}\t{}\t{}",
            sq.id, sq.name, sq.match_count, sq.created_at
        );
    }
}

fn sq_json(resp: &ListStandingQueriesResponse) -> Vec<serde_json::Value> {
    resp.standing_queries
        .iter()
        .map(|sq| {
            serde_json::json!({
                "id": sq.id,
                "name": sq.name,
                "match_count": sq.match_count,
                "created_at": sq.created_at,
            })
        })
        .collect()
}

/// Health.
pub fn health(resp: &HealthResponse, json: bool) {
    if json {
        print_json(&serde_json::json!({ "status": resp.status }));
        return;
    }
    println!("status: {}", resp.status);
}

/// A generic status line (e.g. after a delete).
pub fn status(msg: &str, json: bool) {
    if json {
        print_json(&serde_json::json!({ "status": msg }));
        return;
    }
    println!("{msg}");
}
