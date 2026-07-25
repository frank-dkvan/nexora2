//! HTTP Webhook output — POSTs SQ results to a URL with retry and auth support.

use crate::sink_trait::{OutputError, OutputSink, OutputStatus};
use async_trait::async_trait;
use nexora_standing_query::StandingQueryResult;
use serde::{Deserialize, Serialize};

/// Configuration for webhook output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Target URL to POST results to.
    pub url: String,
    /// HTTP status codes that are considered successful.
    #[serde(default = "default_success_codes")]
    pub success_codes: Vec<u16>,
    /// Maximum number of retries on transient failures.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Base retry backoff in milliseconds (exponential: base * 2^attempt).
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
    /// Request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Optional Bearer token for Authorization header.
    pub bearer_token: Option<String>,
    /// Optional Basic auth username.
    pub basic_user: Option<String>,
    /// Optional Basic auth password.
    pub basic_pass: Option<String>,
    /// Custom headers to include (key-value pairs).
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

fn default_success_codes() -> Vec<u16> {
    vec![200, 201, 202, 204]
}
fn default_max_retries() -> u32 {
    5
}
fn default_backoff_ms() -> u64 {
    100
}
fn default_timeout_secs() -> u64 {
    10
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:9999/webhook".into(),
            success_codes: default_success_codes(),
            max_retries: 5,
            backoff_ms: 100,
            timeout_secs: 10,
            bearer_token: None,
            basic_user: None,
            basic_pass: None,
            headers: Vec::new(),
        }
    }
}

/// Webhook output with retry and authentication support.
pub struct WebhookOutput {
    name: String,
    config: WebhookConfig,
    client: reqwest::Client,
}

impl WebhookOutput {
    /// Create with the original simple API (backward compatible).
    pub fn new(name: &str, url: &str) -> Self {
        Self::with_config(
            name,
            WebhookConfig {
                url: url.to_string(),
                ..Default::default()
            },
        )
    }

    /// Create with full configuration.
    pub fn with_config(name: &str, config: WebhookConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            name: name.to_string(),
            config,
            client,
        }
    }

    async fn send_with_retry(&self, json: &serde_json::Value) -> Result<(), OutputError> {
        let mut last_error = String::new();

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let delay = self.config.backoff_ms * 2u64.pow(attempt - 1);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                tracing::debug!(
                    name = %self.name,
                    attempt,
                    "Retrying webhook"
                );
            }

            let mut req = self.client.post(&self.config.url).json(json);

            // Auth headers
            if let Some(ref token) = self.config.bearer_token {
                req = req.header("Authorization", format!("Bearer {token}"));
            }
            if let (Some(ref user), Some(ref pass)) =
                (&self.config.basic_user, &self.config.basic_pass)
            {
                let encoded = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    format!("{user}:{pass}"),
                );
                req = req.header("Authorization", format!("Basic {encoded}"));
            }
            // Custom headers
            for (key, val) in &self.config.headers {
                req = req.header(key.as_str(), val.as_str());
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if self.config.success_codes.contains(&status) {
                        return Ok(());
                    }
                    last_error = format!("HTTP {status}");
                    // Non-retryable: client errors (except 429/408)
                    if (400..500).contains(&status) && status != 429 && status != 408 {
                        return Err(OutputError::Sink(format!("Webhook failed: HTTP {status}")));
                    }
                }
                Err(e) => {
                    last_error = format!("{e}");
                }
            }
        }

        Err(OutputError::Sink(format!(
            "Webhook failed after {} retries: {last_error}",
            self.config.max_retries
        )))
    }
}

#[async_trait]
impl OutputSink for WebhookOutput {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, result: &StandingQueryResult) -> Result<(), OutputError> {
        let json = result.to_json();
        self.send_with_retry(&json).await
    }

    fn status(&self) -> OutputStatus {
        OutputStatus::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_webhook_config_default() {
        let config = WebhookConfig::default();
        assert!(config.success_codes.contains(&200));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.timeout_secs, 10);
    }

    #[tokio::test]
    async fn test_webhook_auth_headers() {
        let config = WebhookConfig {
            bearer_token: Some("test-token".into()),
            basic_user: Some("admin".into()),
            basic_pass: Some("secret".into()),
            ..Default::default()
        };
        assert_eq!(config.bearer_token.unwrap(), "test-token");
        assert_eq!(config.basic_user.unwrap(), "admin");
    }

    #[tokio::test]
    async fn test_webhook_custom_headers() {
        let config = WebhookConfig {
            headers: vec![("X-Custom".into(), "value".into())],
            ..Default::default()
        };
        assert_eq!(
            config.headers[0],
            ("X-Custom".to_string(), "value".to_string())
        );
    }
}
