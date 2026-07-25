//! Webhook sink implementation for Standing Query results
//!
//! Sends SQ match/unmatch events to HTTP webhooks

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::result::StandingQueryResult;
use crate::sink_registry::{SinkConfig, WebhookSinkConfig};
use reqwest::Client;
use serde_json::json;
use tokio::sync::broadcast;
use uuid::Uuid;

/// Webhook sink executor
pub struct WebhookSink {
    config: WebhookSinkConfig,
    client: Client,
}

impl WebhookSink {
    pub fn new(config: WebhookSinkConfig) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

        Ok(Self { config, client })
    }

    /// Send a Standing Query result to the webhook
    pub async fn send(&self, result: &StandingQueryResult) -> Result<(), String> {
        let payload = json!({
            "sq_id": result.sq_id.to_string(),
            "sq_name": result.sq_name,
            "node_id": result.qid.to_string(),
            "result_type": format!("{:?}", result.result_type),
            "matched_properties": result.matched_properties,
            "timestamp": result.timestamp.to_rfc3339(),
            "sq_version": result.sq_version,
            "hit_id": result.hit_id,
        });

        let mut retries = 0;
        loop {
            let mut request = self.client.post(&self.config.url).json(&payload);

            // Add custom headers
            for (key, value) in &self.config.headers {
                request = request.header(key, value);
            }

            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        tracing::debug!(
                            url = %self.config.url,
                            sq_id = %result.sq_id,
                            "Webhook sent successfully"
                        );
                        return Ok(());
                    } else {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();
                        tracing::warn!(
                            url = %self.config.url,
                            status = %status,
                            body = %body,
                            "Webhook returned error status"
                        );

                        if retries >= self.config.max_retries {
                            return Err(format!("Webhook failed with status {}: {}", status, body));
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        url = %self.config.url,
                        error = %e,
                        retry = retries,
                        "Webhook request failed"
                    );

                    if retries >= self.config.max_retries {
                        return Err(format!(
                            "Webhook request failed after {} retries: {}",
                            retries, e
                        ));
                    }
                }
            }

            retries += 1;
            tokio::time::sleep(Duration::from_millis(self.config.retry_delay_ms)).await;
        }
    }
}

/// Webhook sink runner - subscribes to SQ results and sends them to webhooks
pub struct WebhookSinkRunner {
    sinks: Arc<tokio::sync::RwLock<HashMap<Uuid, WebhookSink>>>,
}

impl WebhookSinkRunner {
    pub fn new() -> Self {
        Self {
            sinks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Register a webhook sink
    pub async fn register(&self, sink_id: Uuid, config: WebhookSinkConfig) -> Result<(), String> {
        let webhook = WebhookSink::new(config)?;
        self.sinks.write().await.insert(sink_id, webhook);
        Ok(())
    }

    /// Unregister a webhook sink
    pub async fn unregister(&self, sink_id: Uuid) {
        self.sinks.write().await.remove(&sink_id);
    }

    /// Start listening to SQ results and forwarding to registered webhooks
    pub async fn run(
        &self,
        mut sq_receiver: broadcast::Receiver<StandingQueryResult>,
        sink_registry: Arc<crate::sink_registry::SinkRegistry>,
    ) {
        loop {
            match sq_receiver.recv().await {
                Ok(result) => {
                    // Get sinks subscribed to this SQ
                    let sinks = sink_registry.get_sinks_for_sq(result.sq_id).await;

                    for registered_sink in sinks {
                        if let SinkConfig::Webhook(webhook_config) = &registered_sink.config {
                            let sinks_guard = self.sinks.read().await;
                            if let Some(_webhook) = sinks_guard.get(&registered_sink.id) {
                                let result_clone = result.clone();
                                let webhook_clone = WebhookSink::new(webhook_config.clone());

                                // Send in a separate task to avoid blocking
                                tokio::spawn(async move {
                                    if let Ok(wh) = webhook_clone {
                                        if let Err(e) = wh.send(&result_clone).await {
                                            tracing::error!(
                                                sink_id = %registered_sink.id,
                                                error = %e,
                                                "Failed to send to webhook"
                                            );
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped = skipped, "Webhook sink lagged behind");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!("SQ result channel closed, stopping webhook sink runner");
                    break;
                }
            }
        }
    }
}

impl Default for WebhookSinkRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_webhook_sink_creation() {
        let config = WebhookSinkConfig {
            url: "https://example.com/webhook".to_string(),
            headers: HashMap::new(),
            timeout_secs: 30,
            max_retries: 3,
            retry_delay_ms: 1000,
        };

        let sink = WebhookSink::new(config);
        assert!(sink.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_sink_runner() {
        let runner = WebhookSinkRunner::new();

        let config = WebhookSinkConfig {
            url: "https://example.com/webhook".to_string(),
            headers: HashMap::new(),
            timeout_secs: 30,
            max_retries: 3,
            retry_delay_ms: 1000,
        };

        let sink_id = Uuid::new_v4();
        let result = runner.register(sink_id, config).await;
        assert!(result.is_ok());

        runner.unregister(sink_id).await;
    }
}
