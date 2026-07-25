//! Background standing-query subscription → MCP notification bridge.
//!
//! `standing_query_subscribe` spawns [`spawn_sq_subscription`], which connects to
//! the HTTP server's `/api/v2/ws/sq/{id}` WebSocket and forwards each match
//! event to the MCP client as a `notifications/message` (JSON-RPC notification,
//! no `id`). This is real server→client push: the agent receives events as they
//! happen without polling.
//!
//! The task is best-effort and self-contained: on connect failure or socket
//! close it emits one final notification explaining the state and exits, so a
//! dead subscription can never wedge the main loop.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

/// Derive the WebSocket URL for a standing-query stream from the HTTP base URL.
/// `http://host:port` → `ws://host:port/api/v2/ws/sq/{id}` (and https → wss).
pub fn sq_ws_url(base_url: &str, sq_id: &str) -> String {
    let ws_base = if let Some(rest) = base_url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base_url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        // Assume already a ws/wss URL or bare host.
        base_url.to_string()
    };
    let trimmed = ws_base.trim_end_matches('/');
    format!("{trimmed}/api/v2/ws/sq/{sq_id}")
}

/// Wrap an SQ event payload as an MCP `notifications/message` value.
///
/// MCP defines `notifications/message` for server-emitted log/data messages;
/// clients surface these to the model. We tag the logger as `nexora.sq` and
/// attach the raw event under `data` so the agent can react to matches.
fn as_mcp_notification(sq_id: &str, event: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "notifications/message",
        "params": {
            "level": "info",
            "logger": "nexora.sq",
            "data": {
                "sq_id": sq_id,
                "event": event,
            }
        }
    })
}

/// Spawn a background task that streams SQ match events over WebSocket and
/// forwards them as MCP notifications on `notify`. Returns the task's
/// [`JoinHandle`] so a subscription registry can track and cancel it; the task
/// runs until the socket closes, the notify channel is dropped, or it is
/// aborted via the handle.
pub fn spawn_sq_subscription(
    base_url: String,
    token: Option<String>,
    sq_id: String,
    notify: mpsc::UnboundedSender<Value>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let url = sq_ws_url(&base_url, &sq_id);

        // Build the WS handshake request, adding the bearer token if present so
        // the subscription authenticates the same way REST calls do.
        let request = match url.as_str().into_client_request() {
            Ok(mut req) => {
                if let Some(tok) = &token {
                    if let Ok(val) = format!("Bearer {tok}").parse() {
                        req.headers_mut().insert("Authorization", val);
                    }
                }
                req
            }
            Err(e) => {
                let _ = notify.send(as_mcp_notification(
                    &sq_id,
                    json!({ "error": format!("invalid ws url {url}: {e}") }),
                ));
                return;
            }
        };

        let ws_stream = match tokio_tungstenite::connect_async(request).await {
            Ok((stream, _resp)) => stream,
            Err(e) => {
                let _ = notify.send(as_mcp_notification(
                    &sq_id,
                    json!({ "error": format!("failed to connect {url}: {e}") }),
                ));
                return;
            }
        };

        let (mut write, mut read) = ws_stream.split();

        // Announce that the subscription is live.
        let _ = notify.send(as_mcp_notification(
            &sq_id,
            json!({ "status": "subscribed", "url": url }),
        ));

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let event: Value =
                        serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text }));
                    // If the outbound channel is gone, the server is shutting
                    // down — stop the task.
                    if notify.send(as_mcp_notification(&sq_id, event)).is_err() {
                        break;
                    }
                }
                Ok(Message::Ping(payload)) => {
                    let _ = write.send(Message::Pong(payload)).await;
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }

        // Final notification so the agent knows the stream ended.
        let _ = notify.send(as_mcp_notification(
            &sq_id,
            json!({ "status": "unsubscribed" }),
        ));
    })
}

/// In-memory registry of active standing-query subscriptions, keyed by SQ id.
///
/// Purpose: make `standing_query_subscribe` idempotent (one live stream per SQ)
/// and give `standing_query_unsubscribe` something to cancel. Deliberately NOT
/// persisted — an entry is a live `JoinHandle` bound to this process's tokio
/// runtime; it has no meaning across a restart, and MCP servers are per-session
/// child processes anyway (see module docs / design discussion).
///
/// Cloneable and cheap: all clones share one `Mutex<HashMap>`.
#[derive(Clone, Default)]
pub struct SubscriptionRegistry {
    inner: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, tokio::task::JoinHandle<()>>>,
    >,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// True if there is a *live* subscription for `sq_id`. Finished tasks are
    /// treated as absent (and evicted), so a stream that closed on its own can
    /// be re-subscribed without an explicit unsubscribe.
    pub fn is_active(&self, sq_id: &str) -> bool {
        let mut map = self.inner.lock().unwrap();
        match map.get(sq_id) {
            Some(handle) if handle.is_finished() => {
                map.remove(sq_id);
                false
            }
            Some(_) => true,
            None => false,
        }
    }

    /// Register a subscription's handle. Any pre-existing handle for the same
    /// id is aborted first so we never leak a task (callers should check
    /// [`is_active`] first for idempotency, but this stays safe regardless).
    pub fn insert(&self, sq_id: String, handle: tokio::task::JoinHandle<()>) {
        if let Some(old) = self.inner.lock().unwrap().insert(sq_id, handle) {
            old.abort();
        }
    }

    /// Cancel and remove the subscription for `sq_id`. Returns true if one was
    /// active (i.e. the caller actually stopped a running stream).
    pub fn cancel(&self, sq_id: &str) -> bool {
        match self.inner.lock().unwrap().remove(sq_id) {
            Some(handle) => {
                let finished = handle.is_finished();
                handle.abort();
                // Report whether it was still running when we cancelled it.
                !finished
            }
            None => false,
        }
    }

    /// Ids of all currently-tracked subscriptions (may include ones that just
    /// finished but haven't been evicted yet).
    pub fn active_ids(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_from_http() {
        assert_eq!(
            sq_ws_url("http://localhost:8080", "abc"),
            "ws://localhost:8080/api/v2/ws/sq/abc"
        );
    }

    #[test]
    fn ws_url_from_https() {
        assert_eq!(
            sq_ws_url("https://graph.example.com", "sq-1"),
            "wss://graph.example.com/api/v2/ws/sq/sq-1"
        );
    }

    #[test]
    fn ws_url_trims_trailing_slash() {
        assert_eq!(
            sq_ws_url("http://localhost:8080/", "x"),
            "ws://localhost:8080/api/v2/ws/sq/x"
        );
    }

    #[test]
    fn notification_shape_is_valid_jsonrpc() {
        let n = as_mcp_notification("sq1", json!({ "match_count": 3 }));
        assert_eq!(n["jsonrpc"], "2.0");
        assert_eq!(n["method"], "notifications/message");
        assert!(n.get("id").is_none(), "notifications must not carry an id");
        assert_eq!(n["params"]["data"]["sq_id"], "sq1");
        assert_eq!(n["params"]["data"]["event"]["match_count"], 3);
    }

    /// A handle to a task that never finishes on its own, so registry tests can
    /// observe "active" state deterministically.
    fn pending_handle() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            // Park until aborted.
            std::future::pending::<()>().await;
        })
    }

    #[tokio::test]
    async fn registry_tracks_and_reports_active() {
        let reg = SubscriptionRegistry::new();
        assert!(!reg.is_active("sq1"));
        reg.insert("sq1".to_string(), pending_handle());
        assert!(
            reg.is_active("sq1"),
            "inserted subscription should be active"
        );
        assert_eq!(reg.active_ids(), vec!["sq1".to_string()]);
    }

    #[tokio::test]
    async fn registry_cancel_aborts_and_reports() {
        let reg = SubscriptionRegistry::new();
        reg.insert("sq1".to_string(), pending_handle());
        // First cancel stops a running stream → true.
        assert!(reg.cancel("sq1"), "cancel of active sub should report true");
        assert!(
            !reg.is_active("sq1"),
            "cancelled sub must no longer be active"
        );
        // Second cancel finds nothing → false.
        assert!(
            !reg.cancel("sq1"),
            "cancel of missing sub should report false"
        );
    }

    #[tokio::test]
    async fn registry_insert_replaces_and_aborts_old() {
        let reg = SubscriptionRegistry::new();
        let old = pending_handle();
        reg.insert("sq1".to_string(), old);
        // Re-insert with a fresh handle; the old one must be aborted, not leaked.
        reg.insert("sq1".to_string(), pending_handle());
        assert!(reg.is_active("sq1"));
        assert_eq!(
            reg.active_ids().len(),
            1,
            "no duplicate entries for same id"
        );
    }

    #[tokio::test]
    async fn registry_evicts_finished_task() {
        let reg = SubscriptionRegistry::new();
        // A task that completes immediately.
        let handle = tokio::spawn(async {});
        let _ = handle.await; // ensure it's finished
                              // Re-spawn a finished handle into the registry.
        let done = tokio::spawn(async {});
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        reg.insert("sq1".to_string(), done);
        // is_active sees a finished task, evicts it, reports false.
        assert!(
            !reg.is_active("sq1"),
            "finished task should be treated as inactive"
        );
        assert!(
            reg.active_ids().is_empty(),
            "finished task should be evicted"
        );
    }
}
