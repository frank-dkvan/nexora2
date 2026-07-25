//! Console output — logs SQ results to stdout via tracing.
//!
//! Supports both plain and pretty-printing modes.

use crate::sink_trait::{OutputError, OutputSink, OutputStatus};
use async_trait::async_trait;
use nexora_standing_query::StandingQueryResult;

pub struct ConsoleOutput {
    name: String,
    /// Whether to pretty-print the JSON output.
    pretty: bool,
    /// Whether to use colored output.
    colored: bool,
}

impl ConsoleOutput {
    /// Create a console output sink.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            pretty: false,
            colored: true,
        }
    }

    /// Create a console output sink with pretty-printing enabled.
    pub fn new_pretty(name: &str) -> Self {
        Self {
            name: name.to_string(),
            pretty: true,
            colored: true,
        }
    }

    /// Create with full options.
    pub fn with_options(name: &str, pretty: bool, colored: bool) -> Self {
        Self {
            name: name.to_string(),
            pretty,
            colored,
        }
    }
}

#[async_trait]
impl OutputSink for ConsoleOutput {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, result: &StandingQueryResult) -> Result<(), OutputError> {
        let json_val = result.to_json();
        let formatted = if self.pretty {
            serde_json::to_string_pretty(&json_val).unwrap_or_else(|_| json_val.to_string())
        } else {
            serde_json::to_string(&json_val).unwrap_or_else(|_| json_val.to_string())
        };

        let result_type_str = match result.result_type {
            nexora_standing_query::ResultType::Matched => "MATCHED",
            nexora_standing_query::ResultType::Unmatched => "UNMATCHED",
        };

        if self.colored {
            match result.result_type {
                nexora_standing_query::ResultType::Matched => {
                    tracing::info!(
                        sq = %result.sq_name,
                        node = %result.qid,
                        "SQ MATCHED: {formatted}"
                    );
                }
                nexora_standing_query::ResultType::Unmatched => {
                    tracing::debug!(
                        sq = %result.sq_name,
                        node = %result.qid,
                        "SQ unmatched: {formatted}"
                    );
                }
            }
        } else {
            let _ = result_type_str;
            tracing::info!(
                sq = %result.sq_name,
                node = %result.qid,
                result_type = result_type_str,
                "{formatted}"
            );
        }

        Ok(())
    }

    fn status(&self) -> OutputStatus {
        OutputStatus::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_id::NexoraId;
    use nexora_standing_query::ResultType;

    #[tokio::test]
    async fn test_console_output_basic() {
        let sink = ConsoleOutput::new("test-console");
        assert_eq!(sink.name(), "test-console");

        let result = StandingQueryResult::new(
            uuid::Uuid::new_v4(),
            "test",
            NexoraId::from_bytes(b"n".to_vec()),
            std::collections::HashMap::new(),
            ResultType::Matched,
            chrono::Utc::now(),
        );

        assert!(sink.process(&result).await.is_ok());
        assert!(matches!(sink.status(), OutputStatus::Active));
    }

    #[tokio::test]
    async fn test_console_output_pretty() {
        let sink = ConsoleOutput::new_pretty("pretty");
        let result = StandingQueryResult::new(
            uuid::Uuid::new_v4(),
            "pretty-test",
            NexoraId::from_bytes(b"p".to_vec()),
            std::collections::HashMap::new(),
            ResultType::Unmatched,
            chrono::Utc::now(),
        );
        assert!(sink.process(&result).await.is_ok());
    }
}
