//! Drop output — discards all SQ results (no-op sink).
//!
//! Useful for testing or when you want to register a sink but don't
//! need the results anywhere.

use crate::sink_trait::{OutputError, OutputSink, OutputStatus};
use async_trait::async_trait;
use nexora_standing_query::StandingQueryResult;

/// A no-op output sink that discards all standing query results.
pub struct DropOutput {
    name: String,
}

impl DropOutput {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl OutputSink for DropOutput {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, _result: &StandingQueryResult) -> Result<(), OutputError> {
        // Discard the result — always succeeds
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
    async fn test_drop_output_always_succeeds() {
        let sink = DropOutput::new("test-drop");
        assert_eq!(sink.name(), "test-drop");

        let result = StandingQueryResult::new(
            uuid::Uuid::new_v4(),
            "test",
            NexoraId::from_bytes(b"x".to_vec()),
            std::collections::HashMap::new(),
            ResultType::Matched,
            chrono::Utc::now(),
        );

        assert!(sink.process(&result).await.is_ok());
        assert!(matches!(sink.status(), OutputStatus::Active));
    }
}
