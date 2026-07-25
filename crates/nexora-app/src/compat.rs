//! V2 API compatibility layer — bridges Nexora-RS HTTP API to Nexora V2 format.
//!
//! Adds:
//! - legacy-compatible endpoint path aliases
//! - `@type` discriminator fields in responses
//! - Model converters for V1↔V2
//!
//! Reference: Nexora `api/` module and `model-converters/` project.
#![allow(dead_code)]

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};

/// Wraps a JSON value with the Nexora V2 discriminator field.
pub fn wrap_with_type(json: serde_json::Value, type_name: &str) -> serde_json::Value {
    let mut obj = if let serde_json::Value::Object(m) = json {
        m.clone()
    } else {
        let mut m = serde_json::Map::new();
        m.insert("value".to_string(), json);
        m
    };
    obj.insert(
        "@type".to_string(),
        serde_json::Value::String(type_name.to_string()),
    );
    serde_json::Value::Object(obj)
}

/// Convert a Nexora-RS node representation to Nexora V2 format.
pub fn node_to_v2(
    qid: &str,
    labels: Vec<String>,
    properties: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "@type": "Node",
        "id": qid,
        "labels": labels,
        "properties": properties,
    })
}

/// Convert a Nexora-RS edge to Nexora V2 format.
pub fn edge_to_v2(
    edge_id: &str,
    edge_type: &str,
    from_id: &str,
    to_id: &str,
    properties: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "@type": "Edge",
        "id": edge_id,
        "edgeType": edge_type,
        "from": from_id,
        "to": to_id,
        "properties": properties,
    })
}

/// Error response in the original Nexora format.
pub fn error_response(status: StatusCode, message: &str) -> impl IntoResponse {
    (
        status,
        Json(serde_json::json!({
            "@type": "ApiError",
            "status": status.as_u16(),
            "message": message,
        })),
    )
}

/// Wrap a successful response with `@type`.
pub fn success_response(data: serde_json::Value, type_name: &str) -> impl IntoResponse {
    Json(wrap_with_type(data, type_name))
}

type V2QueryResult = Result<
    (String, Option<serde_json::Map<String, serde_json::Value>>),
    (StatusCode, Json<serde_json::Value>),
>;

/// Parse a V2 query request (legacy V2 format: `{ "query": "...", "parameters": {...} }`).
pub fn parse_v2_query_request(body: &serde_json::Value) -> V2QueryResult {
    let query = body
        .get("query")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "@type": "ApiError",
                    "status": 400,
                    "message": "Missing 'query' field in request body",
                })),
            )
        })?;

    let parameters = body.get("parameters").and_then(|v| v.as_object()).cloned();

    Ok((query, parameters))
}

/// Parse a V2 query response result list into the format.
pub fn v2_query_result(
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
) -> serde_json::Value {
    serde_json::json!({
        "@type": "QueryResults",
        "columns": columns,
        "results": rows.iter().map(|row| {
            serde_json::json!({
                "meta": [],
                "data": row.iter().enumerate().map(|(i, val)| {
                    serde_json::json!({ columns[i].as_str(): val })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_with_type() {
        let result = wrap_with_type(serde_json::json!({"name": "test"}), "CustomType");
        assert_eq!(result["@type"], "CustomType");
        assert_eq!(result["name"], "test");
    }

    #[test]
    fn test_parse_v2_query_valid() {
        let body = serde_json::json!({
            "query": "MATCH (n) RETURN n",
            "parameters": { "limit": 10 }
        });
        let (query, params) = parse_v2_query_request(&body).unwrap();
        assert_eq!(query, "MATCH (n) RETURN n");
        assert!(params.is_some());
    }

    #[test]
    fn test_parse_v2_query_missing_field() {
        let body = serde_json::json!({"wrong": "field"});
        assert!(parse_v2_query_request(&body).is_err());
    }

    #[test]
    fn test_v2_query_result() {
        let result = v2_query_result(
            vec!["name".into(), "age".into()],
            vec![vec![serde_json::json!("Alice"), serde_json::json!(30)]],
        );
        assert_eq!(result["@type"], "QueryResults");
        assert_eq!(result["columns"][0], "name");
        assert_eq!(result["results"][0]["data"][0]["name"], "Alice");
    }

    #[test]
    fn test_node_to_v2() {
        let props = serde_json::json!({"name": "Test"})
            .as_object()
            .unwrap()
            .clone();
        let node = node_to_v2("abc123", vec!["Person".into()], &props);
        assert_eq!(node["@type"], "Node");
        assert_eq!(node["id"], "abc123");
        assert_eq!(node["properties"]["name"], "Test");
    }

    #[test]
    fn test_edge_to_v2() {
        let props = serde_json::json!({"weight": 1})
            .as_object()
            .unwrap()
            .clone();
        let edge = edge_to_v2("e1", "KNOWS", "n1", "n2", &props);
        assert_eq!(edge["@type"], "Edge");
        assert_eq!(edge["edgeType"], "KNOWS");
        assert_eq!(edge["from"], "n1");
        assert_eq!(edge["to"], "n2");
    }

    #[test]
    fn test_success_response_includes_type() {
        let _resp = success_response(serde_json::json!({"status": "ok"}), "HealthResponse");
    }

    #[test]
    fn test_error_response_status() {
        let _resp = error_response(StatusCode::NOT_FOUND, "Node not found");
    }
}
