//! `nexora-mcp` — a Model Context Protocol server exposing nexora graph
//! operations as agent tools over stdio JSON-RPC 2.0.
//!
//! Like the `nex` CLI, this is a thin client over `nexora-client`: it talks to a
//! running nexora-app over HTTP and never links the engine. An MCP-capable agent
//! spawns this binary, performs the `initialize` handshake, discovers tools via
//! `tools/list`, and invokes them via `tools/call`.
//!
//! Config via env: `NEXORA_URL` (default http://localhost:8080), `NEXORA_TOKEN`
//! (optional bearer token).
//!
//! Protocol: line-delimited JSON-RPC on stdin/stdout. In addition to
//! request/response, the server pushes **server→client notifications** (JSON-RPC
//! messages with no `id`): tools like `standing_query_subscribe` spawn a
//! background task that streams standing-query match events from the HTTP
//! server's WebSocket and forwards them as `notifications/message`. The main
//! loop multiplexes stdin reads and outbound notifications with `tokio::select!`,
//! and all stdout writes go through a single mutex so responses and
//! notifications never interleave mid-line.

use nexora_client::NexoraClient;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

mod subscribe;
mod tools;

const PROTOCOL_VERSION: &str = "2024-11-05";

/// Shared server context handed to tool dispatch. Carries the HTTP client plus
/// the bits needed to open push subscriptions (base URL / token) and the
/// notification sender that background tasks use to reach the client.
#[derive(Clone)]
pub struct Ctx {
    pub client: NexoraClient,
    pub base_url: String,
    pub token: Option<String>,
    pub notify: mpsc::UnboundedSender<Value>,
    /// Tracks live SQ subscriptions so subscribe is idempotent and unsubscribe
    /// has something to cancel. In-memory only (see SubscriptionRegistry docs).
    pub subscriptions: subscribe::SubscriptionRegistry,
}

#[tokio::main]
async fn main() {
    let (base_url, token) = read_config();
    let client = match build_client(&base_url, token.clone()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("nexora-mcp: failed to init client: {e}");
            std::process::exit(1);
        }
    };

    // Outbound channel: responses and push notifications both funnel here so a
    // single writer task owns stdout (no interleaving).
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Value>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(msg) = out_rx.recv().await {
            if let Ok(mut s) = serde_json::to_string(&msg) {
                s.push('\n');
                if stdout.write_all(s.as_bytes()).await.is_err() {
                    break;
                }
                let _ = stdout.flush().await;
            }
        }
    });

    let ctx = Ctx {
        client,
        base_url,
        token,
        notify: out_tx.clone(),
        subscriptions: subscribe::SubscriptionRegistry::new(),
    };
    // Guard concurrent tool execution is unnecessary (client is Clone + Send),
    // but we serialize request handling to preserve MCP call ordering.
    let ctx = Arc::new(ctx);

    // Async, line-delimited stdin.
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let req: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = out_tx.send(error_response(
                            Value::Null,
                            -32700,
                            &format!("parse error: {e}"),
                        ));
                        continue;
                    }
                };

                let id = req.get("id").cloned();
                let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
                let response = handle(&ctx, method, &req).await;
                match (id, response) {
                    (Some(id), Some(result)) => {
                        let _ = out_tx.send(success_response(id, result));
                    }
                    (Some(id), None) => {
                        let _ = out_tx.send(error_response(
                            id,
                            -32601,
                            &format!("method not found: {method}"),
                        ));
                    }
                    (None, _) => { /* notification from client — no reply */ }
                }
            }
            Ok(None) => break, // EOF — client closed the pipe
            Err(e) => {
                eprintln!("nexora-mcp: stdin read error: {e}");
                break;
            }
        }
    }

    // Drop the last sender so the writer task can drain and exit.
    drop(out_tx);
    drop(ctx);
    let _ = writer.await;
}

fn read_config() -> (String, Option<String>) {
    let url = std::env::var("NEXORA_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let token = std::env::var("NEXORA_TOKEN").ok().filter(|t| !t.is_empty());
    (url, token)
}

fn build_client(url: &str, token: Option<String>) -> Result<NexoraClient, String> {
    let mut builder = NexoraClient::builder().base_url(url.to_string());
    if let Some(token) = token {
        builder = builder.bearer_token(token);
    }
    builder.build().map_err(|e| e.to_string())
}

/// Dispatch a JSON-RPC method. Returns `Some(result)` to reply with success,
/// or `None` for an unknown method (caller emits method-not-found).
async fn handle(ctx: &Ctx, method: &str, req: &Value) -> Option<Value> {
    match method {
        "initialize" => Some(json!({
            "protocolVersion": PROTOCOL_VERSION,
            // Advertise that we emit log notifications (used for SQ push).
            "capabilities": { "tools": {}, "logging": {} },
            "serverInfo": { "name": "nexora-mcp", "version": env!("CARGO_PKG_VERSION") },
        })),
        "notifications/initialized" | "notifications/cancelled" => Some(Value::Null),
        "ping" => Some(json!({})),
        "tools/list" => Some(json!({ "tools": tools::list() })),
        "tools/call" => Some(tools::call(ctx, req.get("params")).await),
        _ => None,
    }
}

fn success_response(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}
