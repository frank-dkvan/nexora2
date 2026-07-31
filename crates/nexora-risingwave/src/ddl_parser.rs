//! SQL DDL parser for RisingWave CREATE MATERIALIZED VIEW statements.
//!
//! This module parses RisingWave DDL and auto-generates Nexora DomainPackages,
//! enabling seamless integration between RisingWave's SQL layer and Nexora's
//! ontology system.

use crate::error::{EventStreamingError, Result};

/// SQL DDL parser for RisingWave
pub struct DdlParser;

/// Parsed schema from CREATE MATERIALIZED VIEW
#[derive(Debug, Clone)]
pub struct ParsedSchema {
    /// View name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Original SQL definition
    pub sql: String,
}

/// Column definition
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// SQL type (VARCHAR, INTEGER, etc.)
    pub sql_type: String,
    /// Nexora type mapping (string, integer, etc.)
    pub nexora_type: String,
    /// Whether the column is nullable
    pub nullable: bool,
}

impl DdlParser {
    /// Parse CREATE MATERIALIZED VIEW and extract schema
    ///
    /// # Phase 6 Implementation
    ///
    /// Phase 6 provides basic regex-based parsing. Full implementation will use
    /// sqlparser-rs for robust SQL parsing.
    ///
    /// # Arguments
    ///
    /// - `sql`: SQL DDL statement
    ///
    /// # Returns
    ///
    /// Parsed schema or error if the SQL is invalid.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use nexora_risingwave::ddl_parser::DdlParser;
    /// let sql = r#"
    ///     CREATE MATERIALIZED VIEW enriched_cargo AS
    ///     SELECT cargo_id, status, location, city
    ///     FROM raw_events
    ///     LEFT JOIN locations ON raw_events.location = locations.code
    /// "#;
    ///
    /// let schema = DdlParser::parse_create_mv(sql).unwrap();
    /// assert_eq!(schema.name, "enriched_cargo");
    /// ```
    pub fn parse_create_mv(sql: &str) -> Result<ParsedSchema> {
        // Phase 6: Simple regex-based parser
        // Extract view name using regex
        let view_name_re = regex::Regex::new(r"CREATE\s+MATERIALIZED\s+VIEW\s+(\w+)\s+AS")
            .map_err(|e| EventStreamingError::Internal(format!("Regex error: {}", e)))?;

        let view_name = view_name_re
            .captures(sql)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| {
                EventStreamingError::Internal("No CREATE MATERIALIZED VIEW found".to_string())
            })?;

        // Phase 6: For simplicity, extract column names from SELECT clause
        // Full implementation will use sqlparser-rs to parse the AST
        let select_re = regex::Regex::new(r"SELECT\s+(.*?)\s+FROM")
            .map_err(|e| EventStreamingError::Internal(format!("Regex error: {}", e)))?;

        let columns_str = select_re
            .captures(sql)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str())
            .ok_or_else(|| EventStreamingError::Internal("No SELECT clause found".to_string()))?;

        let columns = columns_str
            .split(',')
            .map(|col: &str| {
                let col_name = col.split_whitespace().last().unwrap_or(col.trim());
                ColumnDef {
                    name: col_name.to_string(),
                    sql_type: "VARCHAR".to_string(), // Default for Phase 6
                    nexora_type: "string".to_string(),
                    nullable: true,
                }
            })
            .collect();

        Ok(ParsedSchema {
            name: view_name,
            columns,
            sql: sql.to_string(),
        })
    }

    /// Convert SQL type to Nexora type
    ///
    /// # Arguments
    ///
    /// - `sql_type`: SQL type string (e.g., "INTEGER", "VARCHAR")
    ///
    /// # Returns
    ///
    /// Corresponding Nexora type string
    pub fn sql_type_to_nexora_type(sql_type: &str) -> String {
        let normalized = sql_type.to_uppercase();
        match normalized.as_str() {
            s if s.starts_with("INT") || s.starts_with("INTEGER") => "integer".to_string(),
            s if s.starts_with("BIGINT") => "long".to_string(),
            s if s.starts_with("FLOAT") || s.starts_with("REAL") => "float".to_string(),
            s if s.starts_with("DOUBLE") || s.starts_with("NUMERIC") => "double".to_string(),
            s if s.starts_with("VARCHAR") || s.starts_with("TEXT") || s.starts_with("CHAR") => {
                "string".to_string()
            }
            s if s.starts_with("BOOLEAN") || s.starts_with("BOOL") => "boolean".to_string(),
            s if s.starts_with("TIMESTAMP") || s.starts_with("DATE") || s.starts_with("TIME") => {
                "timestamp".to_string()
            }
            s if s.starts_with("JSON") => "json".to_string(),
            _ => "string".to_string(), // Default fallback
        }
    }

    /// Generate a Nexora DomainPackage from parsed schema
    ///
    /// # Phase 6 Implementation
    ///
    /// Phase 6 generates a basic DomainPackage structure. Full implementation
    /// will infer relationships from JOIN clauses.
    ///
    /// # Arguments
    ///
    /// - `schema`: Parsed schema from CREATE MATERIALIZED VIEW
    ///
    /// # Returns
    ///
    /// A DomainPackage ready to be registered in Nexora's OntologyManager
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::ddl_parser::DdlParser;
    /// let sql = "CREATE MATERIALIZED VIEW test AS SELECT id, name FROM events";
    /// let schema = DdlParser::parse_create_mv(sql).unwrap();
    /// let package = DdlParser::schema_to_domain_package(&schema);
    ///
    /// assert_eq!(package.schema.domain, "test");
    /// assert_eq!(package.schema.labels.len(), 1);
    /// ```
    #[cfg(feature = "event-first")]
    pub fn schema_to_domain_package(
        schema: &ParsedSchema,
    ) -> nexora_core::domain_package::DomainPackage {
        use nexora_core::domain_package::{DomainPackage, DomainSchema, LabelDef, PropertyDef};

        // Each parsed MV column becomes a property on a single label named after
        // the view. `nullable` maps to `required = !nullable`.
        let properties: Vec<PropertyDef> = schema
            .columns
            .iter()
            .map(|col| PropertyDef {
                name: col.name.clone(),
                prop_type: col.nexora_type.clone(),
                required: Some(!col.nullable),
                indexed: None,
                description: None,
                enum_values: None,
                min: None,
                max: None,
                default: None,
            })
            .collect();

        let label = LabelDef {
            name: schema.name.clone(),
            description: Some(format!(
                "Auto-generated from RisingWave MV: {}",
                schema.name
            )),
            extends: None,
            properties,
        };

        let domain_schema = DomainSchema {
            domain: schema.name.clone(),
            version: "1.0.0".to_string(),
            description: Some(format!(
                "Auto-generated from RisingWave MV: {}",
                schema.name
            )),
            extends: None,
            labels: vec![label],
            edge_types: vec![],
            constraints: vec![],
            indexes: vec![],
        };

        DomainPackage {
            schema: domain_schema,
            mappings: vec![],
            standing_queries: vec![],
            materialized_views: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_mv() {
        let sql = r#"
            CREATE MATERIALIZED VIEW enriched_cargo AS
            SELECT cargo_id, status, location, city
            FROM raw_events
        "#;

        let schema = DdlParser::parse_create_mv(sql).unwrap();
        assert_eq!(schema.name, "enriched_cargo");
        assert_eq!(schema.columns.len(), 4);
        assert_eq!(schema.columns[0].name, "cargo_id");
        assert_eq!(schema.columns[1].name, "status");
    }

    #[test]
    fn test_parse_mv_with_join() {
        let sql = r#"
            CREATE MATERIALIZED VIEW enriched AS
            SELECT e.id, e.status, l.city
            FROM events e
            LEFT JOIN locations l ON e.loc = l.code
        "#;

        let schema = DdlParser::parse_create_mv(sql).unwrap();
        assert_eq!(schema.name, "enriched");
        // Phase 6: Basic parsing extracts column aliases
        assert!(!schema.columns.is_empty());
    }

    #[test]
    fn test_parse_invalid_sql() {
        let sql = "SELECT * FROM events"; // Missing CREATE MATERIALIZED VIEW

        let result = DdlParser::parse_create_mv(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_type_mapping() {
        assert_eq!(DdlParser::sql_type_to_nexora_type("INTEGER"), "integer");
        assert_eq!(DdlParser::sql_type_to_nexora_type("BIGINT"), "long");
        assert_eq!(DdlParser::sql_type_to_nexora_type("VARCHAR"), "string");
        assert_eq!(DdlParser::sql_type_to_nexora_type("FLOAT"), "float");
        assert_eq!(DdlParser::sql_type_to_nexora_type("DOUBLE"), "double");
        assert_eq!(DdlParser::sql_type_to_nexora_type("BOOLEAN"), "boolean");
        assert_eq!(DdlParser::sql_type_to_nexora_type("TIMESTAMP"), "timestamp");
        assert_eq!(DdlParser::sql_type_to_nexora_type("JSON"), "json");
        assert_eq!(DdlParser::sql_type_to_nexora_type("UNKNOWN"), "string");
    }

    #[test]
    fn test_column_def_creation() {
        let col = ColumnDef {
            name: "user_id".to_string(),
            sql_type: "BIGINT".to_string(),
            nexora_type: "long".to_string(),
            nullable: false,
        };

        assert_eq!(col.name, "user_id");
        assert_eq!(col.sql_type, "BIGINT");
        assert_eq!(col.nexora_type, "long");
        assert!(!col.nullable);
    }

    #[cfg(feature = "event-first")]
    #[test]
    fn test_schema_to_domain_package() {
        let schema = ParsedSchema {
            name: "test_mv".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    sql_type: "INTEGER".to_string(),
                    nexora_type: "integer".to_string(),
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    sql_type: "VARCHAR".to_string(),
                    nexora_type: "string".to_string(),
                    nullable: true,
                },
            ],
            sql: "CREATE MATERIALIZED VIEW test_mv AS SELECT id, name FROM events".to_string(),
        };

        let package = DdlParser::schema_to_domain_package(&schema);

        assert_eq!(package.schema.domain, "test_mv");
        assert_eq!(package.schema.version, "1.0.0");
        assert_eq!(package.schema.labels.len(), 1);
        assert_eq!(package.schema.labels[0].name, "test_mv");
        assert_eq!(package.schema.labels[0].properties.len(), 2);
        assert_eq!(package.schema.labels[0].properties[0].name, "id");
        assert_eq!(package.schema.labels[0].properties[0].prop_type, "integer");
        assert_eq!(package.schema.labels[0].properties[0].required, Some(true));
        assert_eq!(package.schema.labels[0].properties[1].name, "name");
        assert_eq!(package.schema.labels[0].properties[1].required, Some(false));
    }
}
