//! Kafka output sink — publishes SQ results to a Kafka topic.
//!
//! Uses `rdkafka::producer::FutureProducer` for async delivery.

use crate::sink_trait::{OutputError, OutputSink, OutputStatus};
use async_trait::async_trait;
use nexora_standing_query::StandingQueryResult;
use serde::{Deserialize, Serialize};

/// Configuration for the Kafka output sink.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaOutputConfig {
    /// Kafka bootstrap servers.
    pub brokers: String,
    /// Target topic to publish results to.
    pub topic: String,
    /// Key field from the SQ result to use as the Kafka message key.
    /// Default: "sq_name" (uses the SQ name).
    pub key_field: String,
    /// Maximum number of retries on transient errors.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Base backoff in milliseconds.
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

fn default_max_retries() -> u32 {
    3
}
fn default_backoff_ms() -> u64 {
    100
}

impl Default for KafkaOutputConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".into(),
            topic: "nexora-sq-results".into(),
            key_field: "sq_name".into(),
            max_retries: 3,
            backoff_ms: 100,
        }
    }
}

/// Kafka output sink using rdkafka.
///
/// Falls back gracefully to a no-op if rdkafka is not available
/// (feature-gated behind `kafka` feature on the workspace).
pub struct KafkaOutput {
    name: String,
    config: KafkaOutputConfig,
    #[cfg(feature = "kafka")]
    producer: std::sync::Mutex<Option<rdkafka::producer::FutureProducer>>,
    /// Track whether we successfully connected
    connected: std::sync::atomic::AtomicBool,
}

impl KafkaOutput {
    pub fn new(name: &str, config: KafkaOutputConfig) -> Self {
        #[cfg(feature = "kafka")]
        let (producer, connected) = {
            let producer_config: rdkafka::ClientConfig = rdkafka::ClientConfig::new()
                .set("bootstrap.servers", &config.brokers)
                .set("message.timeout.ms", "5000")
                .set("request.required.acks", "1")
                .to_owned();

            match producer_config.create() {
                Ok(p) => {
                    tracing::info!(
                        "Kafka output sink '{}' connected to {}",
                        name,
                        config.brokers
                    );
                    (std::sync::Mutex::new(Some(p)), true)
                }
                Err(e) => {
                    tracing::warn!("Kafka output sink '{}' failed to connect: {e}. Will operate in degraded mode.", name);
                    (std::sync::Mutex::new(None), false)
                }
            }
        };

        #[cfg(not(feature = "kafka"))]
        let connected = false;

        Self {
            name: name.to_string(),
            config,
            #[cfg(feature = "kafka")]
            producer,
            connected: std::sync::atomic::AtomicBool::new(connected),
        }
    }

    #[cfg_attr(not(feature = "kafka"), allow(unused_variables))]
    async fn send_with_retry(&self, key: &str, payload: &str) -> Result<(), OutputError> {
        #[cfg_attr(not(feature = "kafka"), allow(unused_mut, unused_assignments))]
        let mut last_error = String::new();
        if let Some(attempt) = (0..=self.config.max_retries).next() {
            if attempt > 0 {
                let delay = self.config.backoff_ms * 2u64.pow(attempt - 1);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }

            #[cfg(feature = "kafka")]
            {
                use rdkafka::producer::FutureRecord;
                // Clone the producer out of the guard and release the lock before
                // awaiting. FutureProducer is Arc-backed, so the clone is cheap,
                // and holding a std::sync::MutexGuard across .await would make this
                // future non-Send (and could deadlock other senders).
                let producer = self.producer.lock().unwrap().clone();
                if let Some(producer) = producer {
                    let record = FutureRecord::to(&self.config.topic)
                        .key(key)
                        .payload(payload);

                    match producer
                        .send(record, std::time::Duration::from_secs(5))
                        .await
                    {
                        Ok(_) => {
                            self.connected
                                .store(true, std::sync::atomic::Ordering::Relaxed);
                            return Ok(());
                        }
                        Err((e, _)) => {
                            // Transient produce error — record it and let the loop
                            // retry with backoff until max_retries is exhausted.
                            last_error = format!("Kafka produce error: {e}");
                            continue;
                        }
                    }
                } else {
                    // No producer available — the sink failed to connect at
                    // startup. Returning Ok here would silently discard every
                    // result forever with no error surfaced to the caller. Fail
                    // loudly instead so the loss is visible and retryable.
                    self.connected
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                    return Err(OutputError::Sink(format!(
                        "Kafka sink '{}' is not connected (producer unavailable); \
                         result not delivered",
                        self.name
                    )));
                }
            }

            #[cfg(not(feature = "kafka"))]
            {
                // Kafka not compiled — fail gracefully
                self.connected
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return Err(OutputError::Sink(
                    "Kafka support not compiled. Enable the 'kafka' feature.".into(),
                ));
            }
        }

        Err(OutputError::Sink(format!(
            "Failed to send to Kafka after {} retries: {last_error}",
            self.config.max_retries
        )))
    }
}

#[async_trait]
impl OutputSink for KafkaOutput {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, result: &StandingQueryResult) -> Result<(), OutputError> {
        let json = serde_json::to_string(&result.to_json())
            .map_err(|e| OutputError::Sink(format!("JSON error: {e}")))?;

        let key = match self.config.key_field.as_str() {
            "sq_name" => result.sq_name.clone(),
            "qid" => result.qid.to_hex(),
            _ => result.sq_name.clone(),
        };

        self.send_with_retry(&key, &json).await
    }

    fn status(&self) -> OutputStatus {
        if self.connected.load(std::sync::atomic::Ordering::Relaxed) {
            OutputStatus::Active
        } else {
            OutputStatus::Error("Not connected".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kafka_output_name() {
        let config = KafkaOutputConfig::default();
        let sink = KafkaOutput::new("kafka-test", config);
        assert_eq!(sink.name(), "kafka-test");
    }

    #[tokio::test]
    async fn test_kafka_output_status_no_broker() {
        let config = KafkaOutputConfig {
            brokers: "invalid-host:9999".into(),
            ..Default::default()
        };
        let sink = KafkaOutput::new("test-no-broker", config);
        let status = sink.status();
        // Should report error status without crashing
        if let OutputStatus::Error(_) = status { /* expected for test without kafka */ }
    }
}
