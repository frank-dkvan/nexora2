//! SQL DDL parser for materialized view management.
//!
//! Provides SQL-like syntax as syntactic sugar over the REST API:
//!
//! ```sql
//! CREATE MATERIALIZED VIEW high_speed_forklifts AS
//!   MATCH (f:Forklift) WHERE f.speed > 100 RETURN f.id, f.speed, f.zone;
//! ```

use nexora_core::materialized_view::{ColumnDef, DataType, RefreshMode};
use serde::{Deserialize, Serialize};

/// Parsed CREATE MATERIALIZED VIEW statement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateMaterializedViewStatement {
    pub view_name: String,
    pub query: String,
    pub refresh_mode: RefreshMode,
    pub schema: Vec<ColumnDef>,
}

/// Parse SQL DDL for materialized views (simple string-based parser)
pub fn parse_create_mv(input: &str) -> Result<CreateMaterializedViewStatement, String> {
    let input = input.trim();
    let upper = input.to_uppercase();

    // Check for CREATE MATERIALIZED VIEW
    if !upper.starts_with("CREATE MATERIALIZED VIEW") {
        return Err("Expected 'CREATE MATERIALIZED VIEW'".to_string());
    }

    // Extract after "CREATE MATERIALIZED VIEW"
    let after_create = &input[24..].trim_start();

    // Extract view name (first word)
    let view_name = after_create
        .split_whitespace()
        .next()
        .ok_or("Missing view name")?
        .to_string();

    // Find "AS" keyword
    let as_pos = after_create
        .to_uppercase()
        .find(" AS ")
        .ok_or("Missing 'AS' keyword")?;

    // Check for optional "WITH INCREMENTAL" or "WITH MANUAL"
    let between = &after_create[view_name.len()..as_pos];
    let refresh_mode = if between.to_uppercase().contains("WITH INCREMENTAL") {
        RefreshMode::Incremental
    } else if between.to_uppercase().contains("WITH MANUAL") {
        RefreshMode::Manual
    } else {
        RefreshMode::Incremental // default
    };

    // Extract query (everything after AS)
    let query = after_create[as_pos + 4..]
        .trim()
        .trim_end_matches(';')
        .to_string();

    // Infer schema from RETURN clause
    let schema = infer_schema_from_query(&query);

    Ok(CreateMaterializedViewStatement {
        view_name,
        query,
        refresh_mode,
        schema,
    })
}

/// Infer schema from Cypher RETURN clause
fn infer_schema_from_query(query: &str) -> Vec<ColumnDef> {
    // Simple heuristic: extract column names from RETURN clause
    if let Some(return_pos) = query.to_uppercase().rfind("RETURN") {
        let return_clause = &query[return_pos + 6..];
        let columns: Vec<_> = return_clause
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|col| {
                // Extract final identifier (e.g., "f.speed" → "speed")
                let name = col
                    .split('.')
                    .next_back()
                    .unwrap_or(col)
                    .split_whitespace()
                    .next()
                    .unwrap_or(col)
                    .to_string();

                ColumnDef {
                    name,
                    data_type: DataType::String, // Default to string
                }
            })
            .collect();

        return columns;
    }

    vec![]
}

/// Parse DROP MATERIALIZED VIEW
pub fn parse_drop_mv(input: &str) -> Result<String, String> {
    let input = input.trim();
    let upper = input.to_uppercase();

    // Check for DROP MATERIALIZED VIEW
    if !upper.starts_with("DROP MATERIALIZED VIEW") {
        return Err("Expected 'DROP MATERIALIZED VIEW'".to_string());
    }

    // Extract after "DROP MATERIALIZED VIEW"
    let after_drop = input[22..].trim();

    // Extract view name (first word)
    let view_name = after_drop
        .split_whitespace()
        .next()
        .ok_or("Missing view name")?
        .trim_end_matches(';')
        .to_string();

    Ok(view_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_mv_simple() {
        let sql = "CREATE MATERIALIZED VIEW high_speed AS MATCH (f:Forklift) WHERE f.speed > 100 RETURN f.id, f.speed";
        let result = parse_create_mv(sql).unwrap();

        assert_eq!(result.view_name, "high_speed");
        assert_eq!(
            result.query,
            "MATCH (f:Forklift) WHERE f.speed > 100 RETURN f.id, f.speed"
        );
        assert_eq!(result.schema.len(), 2);
        assert_eq!(result.schema[0].name, "id");
        assert_eq!(result.schema[1].name, "speed");
    }

    #[test]
    fn test_parse_create_mv_with_refresh() {
        let sql = "CREATE MATERIALIZED VIEW test WITH INCREMENTAL AS MATCH (n) RETURN n.id, n.name";
        let result = parse_create_mv(sql).unwrap();

        assert_eq!(result.view_name, "test");
        assert_eq!(result.refresh_mode, RefreshMode::Incremental);
    }

    #[test]
    fn test_parse_drop_mv() {
        let sql = "DROP MATERIALIZED VIEW high_speed";
        let result = parse_drop_mv(sql).unwrap();

        assert_eq!(result, "high_speed");
    }

    #[test]
    fn test_infer_schema() {
        let query = "MATCH (f:Forklift) RETURN f.id, f.speed, f.zone";
        let schema = infer_schema_from_query(query);

        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "speed");
        assert_eq!(schema[2].name, "zone");
    }
}
