//! MCP tool definitions and dispatch.
//!
//! Each tool maps to a `nexora-client` call. `list()` returns the tool schemas
//! for `tools/list`; `call()` executes one for `tools/call`, returning an MCP
//! tool result (`{ content: [{type:"text", text}], isError? }`).

use nexora_client::NexoraClient;
use serde_json::{json, Value};

/// Tool schemas advertised to the agent.
pub fn list() -> Vec<Value> {
    vec![
        json!({
            "name": "cypher_query",
            "description": "Run a Cypher query against the nexora graph and return columns + rows.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "The Cypher query text." }
                },
                "required": ["query"]
            }
        }),
        json!({
            "name": "sql_query",
            "description": "Run a SQL query (auto-translated to Cypher server-side).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "The SQL query text." }
                },
                "required": ["query"]
            }
        }),
        json!({
            "name": "bulk_ingest",
            "description": "Ingest a batch of JSON record objects into the graph. Each record's \
                            non-id fields become node properties.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "records": {
                        "type": "array",
                        "items": { "type": "object" },
                        "description": "Array of record objects to ingest."
                    },
                    "id_field": {
                        "type": "string",
                        "description": "Field naming the node id in each record (default \"id\")."
                    }
                },
                "required": ["records"]
            }
        }),
        json!({
            "name": "cypher_query_paged",
            "description": "Run a Cypher query and return one page of rows. Use for large result \
                            sets: pass 'offset' and 'limit' to stream through results in chunks. \
                            The response includes 'next_offset' and 'has_more' so an agent can \
                            page until has_more is false without loading everything at once. \
                            Do NOT include SKIP/LIMIT in the query — this tool appends them.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "The Cypher query text (no SKIP/LIMIT)." },
                    "offset": { "type": "integer", "description": "Row offset to start from (default 0)." },
                    "limit": { "type": "integer", "description": "Max rows per page (default 100, max 1000)." }
                },
                "required": ["query"]
            }
        }),
        json!({
            "name": "standing_query_list",
            "description": "List registered standing queries (id, name, match count, created).",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "standing_query_subscribe",
            "description": "Subscribe to a standing query's live match events by id. Opens a \
                            real-time push stream: match events arrive as MCP notifications \
                            (notifications/message, logger 'nexora.sq') without polling. \
                            Idempotent — subscribing an already-active id is a no-op. Call \
                            standing_query_unsubscribe to stop.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Standing query id to subscribe to." }
                },
                "required": ["id"]
            }
        }),
        json!({
            "name": "standing_query_unsubscribe",
            "description": "Stop a live standing-query subscription started by \
                            standing_query_subscribe. Idempotent — unsubscribing an id with no \
                            active stream reports was_active=false rather than erroring.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Standing query id to unsubscribe." }
                },
                "required": ["id"]
            }
        }),
        json!({
            "name": "standing_query_create",
            "description": "Register a standing query. The pattern matches nodes by property \
                            condition and/or labels; matches are tracked incrementally.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Human-readable query name." },
                    "pattern_type": {
                        "type": "string",
                        "description": "Pattern kind, e.g. \"property\" or \"label\"."
                    },
                    "key": { "type": "string", "description": "Property key to match (property patterns)." },
                    "condition": {
                        "type": "object",
                        "description": "Match condition object, e.g. {\"GreaterThan\": 50.0}."
                    },
                    "labels": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Labels to match (label patterns)."
                    }
                },
                "required": ["name", "pattern_type"]
            }
        }),
        json!({
            "name": "node_get_property",
            "description": "Get a single property value of a node by id.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "qid": { "type": "string", "description": "Node id (hex or string)." },
                    "key": { "type": "string", "description": "Property key." }
                },
                "required": ["qid", "key"]
            }
        }),
        json!({
            "name": "node_set_property",
            "description": "Set a single property value on a node by id.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "qid": { "type": "string", "description": "Node id (hex or string)." },
                    "key": { "type": "string", "description": "Property key." },
                    "value": { "description": "Property value (any JSON type)." }
                },
                "required": ["qid", "key", "value"]
            }
        }),
        json!({
            "name": "node_get_edges",
            "description": "Get all edges of a node by id.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "qid": { "type": "string", "description": "Node id (hex or string)." }
                },
                "required": ["qid"]
            }
        }),
        json!({
            "name": "edge_add",
            "description": "Add an edge from a node to a target node.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "qid": { "type": "string", "description": "Source node id." },
                    "edge_type": { "type": "string", "description": "Edge type/label." },
                    "target": { "type": "string", "description": "Target node id." },
                    "direction": {
                        "type": "string",
                        "description": "Edge direction: \"out\" (default), \"in\", or \"both\"."
                    }
                },
                "required": ["qid", "edge_type", "target"]
            }
        }),
        json!({
            "name": "vector_search",
            "description": "k-NN vector similarity search; returns the nearest node ids + scores.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "vector": {
                        "type": "array",
                        "items": { "type": "number" },
                        "description": "Query embedding."
                    },
                    "k": { "type": "integer", "description": "Number of neighbours to return (default 10)." }
                },
                "required": ["vector"]
            }
        }),
        json!({
            "name": "health",
            "description": "Report nexora server health status.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
    ]
}

/// Execute a `tools/call`. `params` is the JSON-RPC params object
/// (`{ name, arguments }`). Always returns an MCP tool-result value; tool
/// failures are reported via `isError: true` rather than a JSON-RPC error, per
/// MCP convention (the model sees the error text and can react).
pub async fn call(ctx: &crate::Ctx, params: Option<&Value>) -> Value {
    let Some(params) = params else {
        return tool_error("missing params");
    };
    let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let client = &ctx.client;

    match name {
        "cypher_query" => cypher_query(client, &args).await,
        "cypher_query_paged" => cypher_query_paged(client, &args).await,
        "sql_query" => sql_query(client, &args).await,
        "bulk_ingest" => bulk_ingest(client, &args).await,
        "standing_query_list" => standing_query_list(client).await,
        // The subscribe tool needs the full context (base URL, token, notifier)
        // to spawn a background push task, not just the HTTP client.
        "standing_query_subscribe" => standing_query_subscribe(ctx, &args).await,
        "standing_query_unsubscribe" => standing_query_unsubscribe(ctx, &args).await,
        "standing_query_create" => standing_query_create(client, &args).await,
        "node_get_property" => node_get_property(client, &args).await,
        "node_set_property" => node_set_property(client, &args).await,
        "node_get_edges" => node_get_edges(client, &args).await,
        "edge_add" => edge_add(client, &args).await,
        "vector_search" => vector_search(client, &args).await,
        "health" => health(client).await,
        other => tool_error(&format!("unknown tool: {other}")),
    }
}

async fn cypher_query(client: &NexoraClient, args: &Value) -> Value {
    let Some(query) = args.get("query").and_then(|q| q.as_str()) else {
        return tool_error("cypher_query requires a string 'query'");
    };
    match client.execute_cypher(query).await {
        Ok(resp) => {
            if let Some(err) = resp.error {
                return tool_error(&format!("query error: {err}"));
            }
            tool_json(&json!({ "columns": resp.columns, "rows": resp.rows }))
        }
        Err(e) => tool_error(&e.to_string()),
    }
}

/// Paged Cypher execution. Appends `SKIP <offset> LIMIT <limit+1>` so we can
/// detect whether more rows exist beyond the page (by over-fetching one row),
/// then returns exactly `limit` rows plus paging metadata.
async fn cypher_query_paged(client: &NexoraClient, args: &Value) -> Value {
    let Some(query) = args.get("query").and_then(|q| q.as_str()) else {
        return tool_error("cypher_query_paged requires a string 'query'");
    };
    // Reject client-supplied paging clauses: we own SKIP/LIMIT here, and a
    // trailing LIMIT in the user query would shadow ours and break has_more.
    let upper = query.to_uppercase();
    if upper.contains(" LIMIT ") || upper.contains(" SKIP ") {
        return tool_error(
            "cypher_query_paged: remove SKIP/LIMIT from the query; this tool adds paging itself",
        );
    }

    let offset = args.get("offset").and_then(|o| o.as_u64()).unwrap_or(0);
    let limit = args
        .get("limit")
        .and_then(|l| l.as_u64())
        .unwrap_or(100)
        .clamp(1, 1000);

    // Over-fetch one row to learn if there's a next page without a count query.
    let paged = format!("{query} SKIP {offset} LIMIT {}", limit + 1);
    match client.execute_cypher(&paged).await {
        Ok(resp) => {
            if let Some(err) = resp.error {
                return tool_error(&format!("query error: {err}"));
            }
            let mut rows = resp.rows;
            let has_more = rows.len() as u64 > limit;
            if has_more {
                rows.truncate(limit as usize); // drop the sentinel over-fetched row
            }
            let returned = rows.len() as u64;
            tool_json(&json!({
                "columns": resp.columns,
                "rows": rows,
                "offset": offset,
                "limit": limit,
                "returned": returned,
                "has_more": has_more,
                "next_offset": if has_more { Some(offset + returned) } else { None },
            }))
        }
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn sql_query(client: &NexoraClient, args: &Value) -> Value {
    let Some(query) = args.get("query").and_then(|q| q.as_str()) else {
        return tool_error("sql_query requires a string 'query'");
    };
    match client.execute_sql(query).await {
        Ok(resp) => {
            if let Some(err) = resp.error {
                return tool_error(&format!("query error: {err}"));
            }
            tool_json(&json!({
                "columns": resp.columns,
                "rows": resp.rows,
                "row_count": resp.row_count,
            }))
        }
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn bulk_ingest(client: &NexoraClient, args: &Value) -> Value {
    let records = match args.get("records") {
        Some(Value::Array(items)) => items.clone(),
        _ => return tool_error("bulk_ingest requires an array 'records'"),
    };
    let id_field = args
        .get("id_field")
        .and_then(|f| f.as_str())
        .unwrap_or("id")
        .to_string();
    match client.bulk_ingest(records, id_field).await {
        Ok(resp) => tool_json(&json!({
            "status": resp.status,
            "ingested": resp.ingested,
            "nodes": resp.nodes,
            "skipped": resp.skipped,
        })),
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn standing_query_list(client: &NexoraClient) -> Value {
    match client.list_standing_queries().await {
        Ok(resp) => {
            let sqs: Vec<Value> = resp
                .standing_queries
                .iter()
                .map(|sq| {
                    json!({
                        "id": sq.id,
                        "name": sq.name,
                        "match_count": sq.match_count,
                        "created_at": sq.created_at,
                    })
                })
                .collect();
            tool_json(&json!({ "standing_queries": sqs }))
        }
        Err(e) => tool_error(&e.to_string()),
    }
}

/// Subscribe to a standing query's live match events. Verifies the query
/// exists (returning its current snapshot), then spawns a background task that
/// connects to the HTTP server's `/api/v2/ws/sq/{id}` WebSocket and forwards
/// each event to the MCP client as a `notifications/message` — real
/// server→client push, no polling required.
async fn standing_query_subscribe(ctx: &crate::Ctx, args: &Value) -> Value {
    let Some(id) = args.get("id").and_then(|i| i.as_str()) else {
        return tool_error("standing_query_subscribe requires a string 'id'");
    };
    // Idempotent: a live stream for this SQ already exists → no second WS
    // connection (which would double every event the agent receives).
    if ctx.subscriptions.is_active(id) {
        return tool_json(&json!({
            "status": "already_subscribed",
            "sq_id": id,
            "delivery": "notifications/message",
            "note": "A live subscription for this standing query is already active; \
                     events continue to arrive as MCP notifications (logger 'nexora.sq').",
        }));
    }
    // Confirm the SQ exists before opening a stream, so a typo'd id surfaces as
    // a clear tool error rather than a silent, never-firing subscription.
    match ctx.client.get_standing_query(id).await {
        Ok(sq) => {
            let handle = crate::subscribe::spawn_sq_subscription(
                ctx.base_url.clone(),
                ctx.token.clone(),
                id.to_string(),
                ctx.notify.clone(),
            );
            ctx.subscriptions.insert(id.to_string(), handle);
            tool_json(&json!({
                "status": "subscribed",
                "standing_query": sq,
                "delivery": "notifications/message",
                "note": "Live match events will arrive as MCP notifications (logger \
                         'nexora.sq') until the stream closes or you call \
                         standing_query_unsubscribe.",
            }))
        }
        Err(e) => tool_error(&e.to_string()),
    }
}

/// Cancel a live standing-query subscription started by
/// `standing_query_subscribe`. Idempotent: unsubscribing an id with no active
/// stream reports `was_active: false` rather than erroring.
async fn standing_query_unsubscribe(ctx: &crate::Ctx, args: &Value) -> Value {
    let Some(id) = args.get("id").and_then(|i| i.as_str()) else {
        return tool_error("standing_query_unsubscribe requires a string 'id'");
    };
    let was_active = ctx.subscriptions.cancel(id);
    tool_json(&json!({
        "status": "unsubscribed",
        "sq_id": id,
        "was_active": was_active,
    }))
}

async fn standing_query_create(client: &NexoraClient, args: &Value) -> Value {
    use nexora_client::{CreateSqRequest, SqPatternRequest};
    let Some(name) = args.get("name").and_then(|n| n.as_str()) else {
        return tool_error("standing_query_create requires a string 'name'");
    };
    let Some(pattern) = args.get("pattern") else {
        return tool_error("standing_query_create requires an object 'pattern'");
    };
    let pattern: SqPatternRequest = match serde_json::from_value(pattern.clone()) {
        Ok(p) => p,
        Err(e) => return tool_error(&format!("invalid 'pattern': {e}")),
    };
    let req = CreateSqRequest {
        name: name.to_string(),
        pattern,
    };
    match client.create_standing_query(&req).await {
        Ok(resp) => tool_json(&json!({ "id": resp.id, "name": resp.name })),
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn node_get_property(client: &NexoraClient, args: &Value) -> Value {
    let (Some(qid), Some(key)) = (
        args.get("qid").and_then(|q| q.as_str()),
        args.get("key").and_then(|k| k.as_str()),
    ) else {
        return tool_error("node_get_property requires string 'qid' and 'key'");
    };
    match client.get_property(qid, key).await {
        Ok(resp) => tool_json(&json!({ "qid": qid, "key": key, "value": resp.value })),
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn node_set_property(client: &NexoraClient, args: &Value) -> Value {
    let (Some(qid), Some(key)) = (
        args.get("qid").and_then(|q| q.as_str()),
        args.get("key").and_then(|k| k.as_str()),
    ) else {
        return tool_error("node_set_property requires string 'qid' and 'key'");
    };
    let Some(value) = args.get("value") else {
        return tool_error("node_set_property requires a 'value'");
    };
    match client.set_property(qid, key, value.clone()).await {
        Ok(resp) => tool_json(&resp),
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn node_get_edges(client: &NexoraClient, args: &Value) -> Value {
    let Some(qid) = args.get("qid").and_then(|q| q.as_str()) else {
        return tool_error("node_get_edges requires a string 'qid'");
    };
    match client.get_edges(qid).await {
        Ok(resp) => tool_json(&json!({ "qid": qid, "edges": resp.edges })),
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn edge_add(client: &NexoraClient, args: &Value) -> Value {
    let (Some(qid), Some(edge_type), Some(target)) = (
        args.get("qid").and_then(|q| q.as_str()),
        args.get("edge_type").and_then(|t| t.as_str()),
        args.get("target").and_then(|t| t.as_str()),
    ) else {
        return tool_error("edge_add requires string 'qid', 'edge_type', and 'target'");
    };
    let direction = args
        .get("direction")
        .and_then(|d| d.as_str())
        .unwrap_or("out");
    match client.add_edge(qid, edge_type, target, direction).await {
        Ok(resp) => tool_json(&resp),
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn vector_search(client: &NexoraClient, args: &Value) -> Value {
    let vector: Vec<f32> = match args.get("vector").and_then(|v| v.as_array()) {
        Some(arr) => arr
            .iter()
            .filter_map(|n| n.as_f64().map(|f| f as f32))
            .collect(),
        None => return tool_error("vector_search requires an array 'vector'"),
    };
    if vector.is_empty() {
        return tool_error("vector_search 'vector' must be a non-empty array of numbers");
    }
    let k = args.get("k").and_then(|k| k.as_u64()).unwrap_or(10) as usize;
    match client.vector_search(vector, k).await {
        Ok(resp) => tool_json(&json!({ "neighbors": resp.neighbors })),
        Err(e) => tool_error(&e.to_string()),
    }
}

async fn health(client: &NexoraClient) -> Value {
    match client.health().await {
        Ok(resp) => tool_json(&json!({
            "status": resp.status,
            "mode": resp.mode,
            "active_nodes": resp.active_nodes,
            "version": resp.version,
        })),
        Err(e) => tool_error(&e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// MCP tool-result helpers
// ---------------------------------------------------------------------------

/// A successful tool result whose text content is pretty-printed JSON.
fn tool_json(value: &Value) -> Value {
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    json!({ "content": [{ "type": "text", "text": text }] })
}

/// An error tool result. `isError` lets the agent see the failure and react,
/// rather than the call failing at the JSON-RPC layer.
fn tool_error(message: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": message }],
        "isError": true
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A client pointed at a dead port that fails fast (no retry backoff), so
    /// dispatch tests exercise the tool arms without waiting on real network.
    fn dead_client() -> NexoraClient {
        NexoraClient::builder()
            .base_url("http://127.0.0.1:1")
            .max_retries(0)
            .timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap()
    }

    /// A `Ctx` wrapping the dead client, with a notify channel whose receiver is
    /// leaked so `send` never fails during dispatch tests.
    fn dead_ctx() -> crate::Ctx {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Keep the receiver alive for the duration of the process so the sender
        // stays open; tests don't assert on pushed notifications.
        std::mem::forget(rx);
        crate::Ctx {
            client: dead_client(),
            base_url: "http://127.0.0.1:1".to_string(),
            token: None,
            notify: tx,
            subscriptions: crate::subscribe::SubscriptionRegistry::new(),
        }
    }

    /// Names advertised by `list()`, in order.
    fn advertised_names() -> Vec<String> {
        list()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn every_tool_has_a_valid_schema() {
        for tool in list() {
            let name = tool["name"].as_str().expect("tool needs a name");
            assert!(!name.is_empty(), "tool name must be non-empty");
            assert!(
                tool["description"].as_str().is_some(),
                "{name} needs a description"
            );
            assert_eq!(
                tool["inputSchema"]["type"].as_str(),
                Some("object"),
                "{name} inputSchema must be an object"
            );
        }
    }

    #[test]
    fn tool_names_are_unique() {
        let names = advertised_names();
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len(), "duplicate tool name in list()");
    }

    #[test]
    fn expected_tool_surface() {
        let names = advertised_names();
        for expected in [
            "cypher_query",
            "cypher_query_paged",
            "sql_query",
            "bulk_ingest",
            "standing_query_list",
            "standing_query_subscribe",
            "standing_query_unsubscribe",
            "standing_query_create",
            "node_get_property",
            "node_set_property",
            "node_get_edges",
            "edge_add",
            "vector_search",
            "health",
        ] {
            assert!(
                names.contains(&expected.to_string()),
                "missing tool: {expected}"
            );
        }
    }

    #[tokio::test]
    async fn paged_query_rejects_client_limit() {
        // A query that already contains LIMIT must be rejected so our paging
        // isn't shadowed.
        let ctx = dead_ctx();
        let result = call(
            &ctx,
            Some(&json!({
                "name": "cypher_query_paged",
                "arguments": { "query": "MATCH (n) RETURN n LIMIT 5" }
            })),
        )
        .await;
        assert_eq!(result["isError"], json!(true));
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains("SKIP/LIMIT"),
            "should explain paging ownership: {text}"
        );
    }

    #[tokio::test]
    async fn paged_query_requires_query_string() {
        let ctx = dead_ctx();
        let result = call(
            &ctx,
            Some(&json!({ "name": "cypher_query_paged", "arguments": {} })),
        )
        .await;
        assert_eq!(result["isError"], json!(true));
    }

    #[tokio::test]
    async fn sq_subscribe_requires_id() {
        let ctx = dead_ctx();
        let result = call(
            &ctx,
            Some(&json!({ "name": "standing_query_subscribe", "arguments": {} })),
        )
        .await;
        assert_eq!(result["isError"], json!(true));
    }

    #[tokio::test]
    async fn sq_unsubscribe_requires_id() {
        let ctx = dead_ctx();
        let result = call(
            &ctx,
            Some(&json!({ "name": "standing_query_unsubscribe", "arguments": {} })),
        )
        .await;
        assert_eq!(result["isError"], json!(true));
    }

    #[tokio::test]
    async fn sq_unsubscribe_missing_is_not_error() {
        // Unsubscribing an id that was never subscribed is idempotent: it
        // reports was_active=false, not an error.
        let ctx = dead_ctx();
        let result = call(
            &ctx,
            Some(&json!({
                "name": "standing_query_unsubscribe",
                "arguments": { "id": "never-subscribed" }
            })),
        )
        .await;
        assert!(
            result.get("isError").is_none(),
            "idempotent unsubscribe must not error"
        );
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains("\"was_active\": false"),
            "should report was_active=false: {text}"
        );
    }

    #[tokio::test]
    async fn unknown_tool_is_error() {
        let ctx = dead_ctx();
        let result = call(&ctx, Some(&json!({ "name": "does_not_exist" }))).await;
        assert_eq!(result["isError"], json!(true));
    }

    #[tokio::test]
    async fn missing_params_is_error() {
        let ctx = dead_ctx();
        let result = call(&ctx, None).await;
        assert_eq!(result["isError"], json!(true));
    }

    #[tokio::test]
    async fn advertised_tools_are_dispatched() {
        // Every advertised name must NOT fall through to the "unknown tool"
        // arm. We point at a dead port so real calls fail fast; a dispatched
        // tool either fails on the arg check or the network, but never returns
        // the "unknown tool:" sentinel.
        let ctx = dead_ctx();
        for name in advertised_names() {
            let result = call(&ctx, Some(&json!({ "name": name, "arguments": {} }))).await;
            let text = result["content"][0]["text"].as_str().unwrap_or("");
            assert!(
                !text.starts_with("unknown tool:"),
                "tool {name} advertised but not dispatched"
            );
        }
    }
}
