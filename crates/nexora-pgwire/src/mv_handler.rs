//! Materialized View query handler for PG-Wire
//!
//! P0.2: MV 只读查询支持
//!
//! This module handles SQL queries against materialized views, making MVs
//! accessible like regular tables through the PostgreSQL protocol.
//!
//! Supported operations:
//! - SELECT * FROM mv_name
//! - WHERE filtering (in-memory)
//! - ORDER BY sorting (in-memory)
//! - LIMIT / OFFSET pagination

use nexora_core::materialized_view::{MaterializedRow, MaterializedViewManager};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sqlparser::ast::{BinaryOperator, Expr, Offset, OrderByExpr, Query, SetExpr, Value};
use std::sync::Arc;

/// Query a materialized view with optional WHERE/ORDER BY/LIMIT
pub async fn query_materialized_view(
    mv_manager: &Arc<MaterializedViewManager>,
    view_name: &str,
    query: &Query,
) -> PgWireResult<Response> {
    // 1. Find view_id by name
    let view_id = mv_manager
        .find_view_by_name(view_name)
        .await
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(),
                format!("materialized view \"{}\" does not exist", view_name),
            )))
        })?;

    // 2. Get all rows from the MV
    let mut rows = mv_manager.get_rows(&view_id).await.map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "58000".to_string(),
            format!("failed to query materialized view: {}", e),
        )))
    })?;

    // 3. Apply WHERE filtering
    if let SetExpr::Select(select) = query.body.as_ref() {
        if let Some(selection) = &select.selection {
            rows = apply_where_filter(rows, selection)?;
        }
    }

    // 4. Apply ORDER BY
    if let Some(ref order_by) = query.order_by {
        if !order_by.exprs.is_empty() {
            rows = apply_ordering(rows, &order_by.exprs)?;
        }
    }

    // 5. Apply OFFSET
    if let Some(offset) = &query.offset {
        rows = apply_offset(rows, offset)?;
    }

    // 6. Apply LIMIT
    if let Some(limit) = &query.limit {
        rows = apply_limit(rows, limit)?;
    }

    // 7. Build PostgreSQL response
    build_pg_response(rows)
}

/// Apply WHERE clause filtering (in-memory evaluation)
fn apply_where_filter(
    rows: Vec<MaterializedRow>,
    selection: &Expr,
) -> PgWireResult<Vec<MaterializedRow>> {
    let filtered: Vec<_> = rows
        .into_iter()
        .filter(|row| evaluate_condition(&row.values, selection))
        .collect();
    Ok(filtered)
}

/// Evaluate a WHERE condition against a row
fn evaluate_condition(
    row: &std::collections::HashMap<String, nexora_id::PropertyValue>,
    expr: &Expr,
) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = extract_value(row, left);
            let right_val = extract_value(row, right);

            match (left_val, right_val) {
                (Some(l), Some(r)) => match op {
                    BinaryOperator::Gt => l > r,
                    BinaryOperator::Lt => l < r,
                    BinaryOperator::GtEq => l >= r,
                    BinaryOperator::LtEq => l <= r,
                    BinaryOperator::Eq => l == r,
                    BinaryOperator::NotEq => l != r,
                    BinaryOperator::And => {
                        evaluate_condition(row, left) && evaluate_condition(row, right)
                    }
                    BinaryOperator::Or => {
                        evaluate_condition(row, left) || evaluate_condition(row, right)
                    }
                    _ => true, // Unsupported operator, pass through
                },
                _ => false, // NULL comparison
            }
        }
        Expr::Identifier(ident) => {
            // Column existence check
            row.contains_key(&ident.value)
        }
        _ => true, // Unsupported expression, pass through
    }
}

/// Extract a value from a row for comparison
fn extract_value(
    row: &std::collections::HashMap<String, nexora_id::PropertyValue>,
    expr: &Expr,
) -> Option<PropertyValueComparable> {
    match expr {
        Expr::Identifier(ident) => row
            .get(&ident.value)
            .map(|v| PropertyValueComparable(v.clone())),
        Expr::Value(Value::Number(n, _)) => n
            .parse::<i64>()
            .ok()
            .map(|i| PropertyValueComparable(nexora_id::PropertyValue::Integer(i))),
        Expr::Value(Value::SingleQuotedString(s)) => Some(PropertyValueComparable(
            nexora_id::PropertyValue::String(s.clone()),
        )),
        Expr::Value(Value::Boolean(b)) => Some(PropertyValueComparable(
            nexora_id::PropertyValue::Boolean(*b),
        )),
        _ => None,
    }
}

/// Wrapper for PropertyValue to implement Ord for comparisons
#[derive(Debug, Clone)]
struct PropertyValueComparable(nexora_id::PropertyValue);

impl PartialEq for PropertyValueComparable {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (nexora_id::PropertyValue::Integer(a), nexora_id::PropertyValue::Integer(b)) => a == b,
            (nexora_id::PropertyValue::Float(a), nexora_id::PropertyValue::Float(b)) => {
                (a - b).abs() < f64::EPSILON
            }
            (nexora_id::PropertyValue::String(a), nexora_id::PropertyValue::String(b)) => a == b,
            (nexora_id::PropertyValue::Boolean(a), nexora_id::PropertyValue::Boolean(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for PropertyValueComparable {}

impl PartialOrd for PropertyValueComparable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PropertyValueComparable {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use nexora_id::PropertyValue::*;
        match (&self.0, &other.0) {
            (Integer(a), Integer(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (String(a), String(b)) => a.cmp(b),
            (Boolean(a), Boolean(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

/// Apply ORDER BY sorting
fn apply_ordering(
    mut rows: Vec<MaterializedRow>,
    order_by: &[OrderByExpr],
) -> PgWireResult<Vec<MaterializedRow>> {
    if order_by.is_empty() {
        return Ok(rows);
    }

    // Simple single-column sort for now
    let first_order = &order_by[0];
    let column_name = match &first_order.expr {
        Expr::Identifier(ident) => &ident.value,
        _ => return Ok(rows), // Unsupported ORDER BY expression
    };

    let ascending = first_order.asc.unwrap_or(true);

    rows.sort_by(|a, b| {
        let a_val = a.values.get(column_name);
        let b_val = b.values.get(column_name);

        let ordering = match (a_val, b_val) {
            (Some(av), Some(bv)) => {
                PropertyValueComparable(av.clone()).cmp(&PropertyValueComparable(bv.clone()))
            }
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        };

        if ascending {
            ordering
        } else {
            ordering.reverse()
        }
    });

    Ok(rows)
}

/// Apply LIMIT clause
fn apply_limit(mut rows: Vec<MaterializedRow>, limit: &Expr) -> PgWireResult<Vec<MaterializedRow>> {
    if let Expr::Value(Value::Number(n, _)) = limit {
        if let Ok(limit_val) = n.parse::<usize>() {
            rows.truncate(limit_val);
        }
    }
    Ok(rows)
}

/// Apply OFFSET clause
fn apply_offset(rows: Vec<MaterializedRow>, offset: &Offset) -> PgWireResult<Vec<MaterializedRow>> {
    if let Expr::Value(Value::Number(n, _)) = &offset.value {
        if let Ok(offset_val) = n.parse::<usize>() {
            return Ok(rows.into_iter().skip(offset_val).collect());
        }
    }
    Ok(rows)
}

/// Build PostgreSQL wire protocol response from rows
fn build_pg_response(rows: Vec<MaterializedRow>) -> PgWireResult<Response> {
    if rows.is_empty() {
        // Empty result set
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

    // Infer schema from first row
    let schema = infer_schema(&rows[0]);
    let schema_clone = schema.clone();

    // Encode rows
    let row_stream = futures::stream::iter(
        rows.into_iter()
            .map(move |row| encode_row(row, schema_clone.clone())),
    );

    Ok(Response::Query(QueryResponse::new(schema, row_stream)))
}

/// Infer PostgreSQL schema from a MaterializedRow
fn infer_schema(row: &MaterializedRow) -> Arc<Vec<FieldInfo>> {
    let mut fields = Vec::new();

    // Always include the 'key' column first
    fields.push(FieldInfo::new(
        "key".into(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    ));

    // Add other columns from the row values
    for (col_name, value) in &row.values {
        let pg_type = match value {
            nexora_id::PropertyValue::Integer(_) => Type::INT8,
            nexora_id::PropertyValue::Float(_) => Type::FLOAT8,
            nexora_id::PropertyValue::String(_) => Type::TEXT,
            nexora_id::PropertyValue::Boolean(_) => Type::BOOL,
            nexora_id::PropertyValue::List(_) => Type::TEXT, // Serialize as text for now
            nexora_id::PropertyValue::Map(_) => Type::TEXT,  // Serialize as text for now
            nexora_id::PropertyValue::Null => Type::TEXT,
            _ => Type::TEXT, // Default to TEXT for other types
        };

        fields.push(FieldInfo::new(
            col_name.clone(),
            None,
            None,
            pg_type,
            FieldFormat::Text,
        ));
    }

    Arc::new(fields)
}

/// Encode a MaterializedRow into a PostgreSQL DataRow
fn encode_row(
    row: MaterializedRow,
    schema: Arc<Vec<FieldInfo>>,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());

    // Encode 'key' column first
    encoder.encode_field(&row.key).map_err(|e| {
        PgWireError::ApiError(Box::new(std::io::Error::other(format!(
            "failed to encode key: {}",
            e
        ))))
    })?;

    // Encode other columns in schema order
    for field in schema.iter().skip(1) {
        // Skip 'key' field
        if let Some(value) = row.values.get(field.name()) {
            // Use property_value_to_text from type_mapping
            let text_value = crate::type_mapping::property_value_to_text(value);
            encoder.encode_field(&text_value).map_err(|e| {
                PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                    "failed to encode value: {}",
                    e
                ))))
            })?;
        } else {
            // Column not present, encode NULL
            encoder.encode_field(&None::<String>).map_err(|e| {
                PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                    "failed to encode NULL: {}",
                    e
                ))))
            })?;
        }
    }

    Ok(encoder.take_row())
}
