//! OutputSink trait.

use async_trait::async_trait;
use nexora_standing_query::StandingQueryResult;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OutputStatus {
    Active,
    Paused,
    Error(String),
}

#[async_trait]
pub trait OutputSink: Send + Sync {
    fn name(&self) -> &str;
    async fn process(&self, result: &StandingQueryResult) -> Result<(), OutputError>;
    fn status(&self) -> OutputStatus;
}

#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    #[error("sink error: {0}")]
    Sink(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
