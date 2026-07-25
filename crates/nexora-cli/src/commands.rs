//! Command handlers — each maps a subcommand to a `nexora-client` call and
//! renders the result via [`crate::output`].

use crate::{output, AppError};
use nexora_client::NexoraClient;

/// `nex query <cypher>`
pub async fn query(client: &NexoraClient, q: &str, json: bool) -> Result<(), AppError> {
    let resp = client.execute_cypher(q).await?;
    if let Some(err) = &resp.error {
        return Err(AppError::Runtime(format!("query error: {err}")));
    }
    output::rows(&resp.columns, &resp.rows, json);
    Ok(())
}

/// `nex sql <sql>`
pub async fn sql(client: &NexoraClient, q: &str, json: bool) -> Result<(), AppError> {
    let resp = client.execute_sql(q).await?;
    output::sql(&resp, json);
    Ok(())
}

/// `nex ingest <file|-> [--id-field]`
pub async fn ingest(
    client: &NexoraClient,
    file: &str,
    id_field: &str,
    json: bool,
) -> Result<(), AppError> {
    let raw = read_input(file)?;
    let records = parse_records(&raw)?;
    if records.is_empty() {
        return Err(AppError::Usage("no records to ingest".into()));
    }
    let resp = client.bulk_ingest(records, id_field).await?;
    output::bulk(&resp, json);
    Ok(())
}

/// `nex sq list`
pub async fn sq_list(client: &NexoraClient, json: bool) -> Result<(), AppError> {
    let resp = client.list_standing_queries().await?;
    output::sq_list(&resp, json);
    Ok(())
}

/// `nex sq delete <id>`
pub async fn sq_delete(client: &NexoraClient, id: &str, json: bool) -> Result<(), AppError> {
    client.delete_standing_query(id).await?;
    output::status(&format!("deleted standing query {id}"), json);
    Ok(())
}

/// `nex health`
pub async fn health(client: &NexoraClient, json: bool) -> Result<(), AppError> {
    let resp = client.health().await?;
    output::health(&resp, json);
    Ok(())
}

// ---------------------------------------------------------------------------
// input helpers
// ---------------------------------------------------------------------------

/// Read the ingest input: a file path, or stdin when the path is "-".
fn read_input(file: &str) -> Result<String, AppError> {
    use std::io::Read;
    if file == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| AppError::Usage(format!("read stdin: {e}")))?;
        Ok(buf)
    } else {
        std::fs::read_to_string(file).map_err(|e| AppError::Usage(format!("read {file}: {e}")))
    }
}

/// Parse ingest input as either a JSON array of objects or JSON Lines (one
/// object per non-empty line). Tries array first, then falls back to JSONL.
fn parse_records(raw: &str) -> Result<Vec<serde_json::Value>, AppError> {
    let trimmed = raw.trim_start();
    if trimmed.starts_with('[') {
        let arr: serde_json::Value = serde_json::from_str(trimmed)
            .map_err(|e| AppError::Usage(format!("parse JSON array: {e}")))?;
        match arr {
            serde_json::Value::Array(items) => Ok(items),
            _ => Err(AppError::Usage("expected a JSON array".into())),
        }
    } else {
        // JSON Lines.
        let mut records = Vec::new();
        for (i, line) in raw.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| AppError::Usage(format!("parse line {}: {e}", i + 1)))?;
            records.push(v);
        }
        Ok(records)
    }
}
