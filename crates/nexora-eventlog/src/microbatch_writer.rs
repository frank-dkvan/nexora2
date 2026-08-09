//! Microbatch Writer — batches events before committing to Iceberg.
//!
//! **Problem**: Each event triggers a full Iceberg transaction (write data file + commit snapshot).
//! - Single event: 110ms (10ms write + 100ms commit)
//! - 1000 events: 110 seconds (串行)
//!
//! **Solution**: Buffer events and commit in batches.
//! - 1000 events → single commit → 110ms total
//! - Speedup: 1000x

use crate::{EventLogStore, RawEvent};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::time::{Duration, Instant};

/// Microbatch writer configuration.
#[derive(Clone, Debug)]
pub struct MicrobatchConfig {
    /// Maximum events to buffer before flushing (size trigger).
    pub max_batch_size: usize,
    /// Maximum time to buffer events before flushing (time trigger).
    pub max_delay_ms: u64,
    /// Enable adaptive batch sizing based on latency.
    pub adaptive: bool,
}

impl Default for MicrobatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000, // 1000 events or
            max_delay_ms: 100,    // 100ms timeout
            adaptive: true,       // auto-tune
        }
    }
}

/// Per-topic event buffer.
struct TopicBuffer {
    events: Vec<RawEvent>,
    first_event_time: Instant,
    waiting_senders: Vec<oneshot::Sender<Result<u64>>>,
}

/// Microbatch writer — buffers events and flushes in batches.
pub struct MicrobatchWriter {
    buffers: Arc<Mutex<HashMap<String, TopicBuffer>>>,
    store: Arc<EventLogStore>,
    config: Arc<Mutex<MicrobatchConfig>>,
}

impl MicrobatchWriter {
    /// Create a new microbatch writer.
    pub fn new(store: Arc<EventLogStore>, config: MicrobatchConfig) -> Arc<Self> {
        let writer = Arc::new(Self {
            buffers: Arc::new(Mutex::new(HashMap::new())),
            store,
            config: Arc::new(Mutex::new(config)),
        });

        // Start background flush worker
        writer.start_flush_worker();

        writer
    }

    /// Append an event (non-blocking, returns when flushed).
    pub async fn append(&self, event: RawEvent) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        let topic = event.topic.clone();

        let should_flush = {
            let mut buffers = self.buffers.lock().await;
            let config = self.config.lock().await;

            let buffer = buffers.entry(topic.clone()).or_insert_with(|| TopicBuffer {
                events: Vec::new(),
                first_event_time: Instant::now(),
                waiting_senders: Vec::new(),
            });

            buffer.events.push(event);
            buffer.waiting_senders.push(tx);

            // Check if we should flush immediately
            buffer.events.len() >= config.max_batch_size
        };

        if should_flush {
            // Flush asynchronously (don't block caller)
            let self_clone = Arc::new(self.clone_refs());
            let topic_clone = topic.clone();
            tokio::spawn(async move {
                if let Err(e) = self_clone.flush_topic(&topic_clone).await {
                    tracing::error!("Failed to flush topic '{}': {}", topic_clone, e);
                }
            });
        }

        // Wait for flush to complete
        rx.await.map_err(|_| anyhow!("Flush worker died"))?
    }

    /// Start background flush worker (timeout-based flush).
    fn start_flush_worker(self: &Arc<Self>) {
        let writer = Arc::clone(self);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(10));

            loop {
                ticker.tick().await;

                let (topics_to_flush, max_delay) = {
                    let buffers = writer.buffers.lock().await;
                    let config = writer.config.lock().await;
                    let max_delay = Duration::from_millis(config.max_delay_ms);

                    let topics: Vec<String> = buffers
                        .iter()
                        .filter(|(_, buf)| {
                            !buf.events.is_empty() && buf.first_event_time.elapsed() >= max_delay
                        })
                        .map(|(topic, _)| topic.clone())
                        .collect();

                    (topics, max_delay)
                };

                for topic in topics_to_flush {
                    if let Err(e) = writer.flush_topic(&topic).await {
                        tracing::error!("Failed to flush topic '{}': {}", topic, e);
                    }
                }
            }
        });
    }

    /// Flush a specific topic's buffer.
    async fn flush_topic(&self, topic: &str) -> Result<()> {
        let (events, senders) = {
            let mut buffers = self.buffers.lock().await;
            match buffers.remove(topic) {
                Some(buffer) => (buffer.events, buffer.waiting_senders),
                None => return Ok(()), // Already flushed by another task
            }
        };

        if events.is_empty() {
            return Ok(());
        }

        let event_count = events.len();

        // Batch write to Iceberg (single transaction)
        let start = Instant::now();
        let result = self.store.append(&events).await;
        let elapsed = start.elapsed();

        tracing::info!(
            "Flushed {} events to topic '{}' in {:?}",
            event_count,
            topic,
            elapsed
        );

        // Adjust batch size based on latency (adaptive mode)
        {
            let mut config = self.config.lock().await;
            if config.adaptive {
                Self::adjust_batch_size(&mut config, topic, elapsed, event_count);
            }
        }

        // Notify all waiting senders
        match &result {
            Ok(count) => {
                for sender in senders {
                    let _ = sender.send(Ok(*count));
                }
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                for sender in senders {
                    let _ = sender.send(Err(anyhow!("{}", err_msg)));
                }
            }
        }

        result.map(|_| ())
    }

    /// Adjust batch size based on observed latency.
    fn adjust_batch_size(
        config: &mut MicrobatchConfig,
        topic: &str,
        elapsed: Duration,
        event_count: usize,
    ) {
        let current_size = config.max_batch_size;

        if elapsed < Duration::from_millis(50) && event_count >= current_size {
            // Low latency and batch was full → increase batch size
            config.max_batch_size = (current_size * 12 / 10).min(10000);
            tracing::debug!(
                "Topic '{}': low latency ({:?}), increasing batch size {} → {}",
                topic,
                elapsed,
                current_size,
                config.max_batch_size
            );
        } else if elapsed > Duration::from_millis(200) {
            // High latency → decrease batch size for better responsiveness
            config.max_batch_size = (current_size * 8 / 10).max(100);
            tracing::debug!(
                "Topic '{}': high latency ({:?}), decreasing batch size {} → {}",
                topic,
                elapsed,
                current_size,
                config.max_batch_size
            );
        }
    }

    /// Flush all buffers (call before shutdown).
    pub async fn flush_all(&self) -> Result<()> {
        let topics: Vec<String> = {
            let buffers = self.buffers.lock().await;
            buffers.keys().cloned().collect()
        };

        for topic in topics {
            self.flush_topic(&topic).await?;
        }

        Ok(())
    }

    /// Helper to clone Arc references (for spawned tasks).
    fn clone_refs(&self) -> Self {
        Self {
            buffers: Arc::clone(&self.buffers),
            store: Arc::clone(&self.store),
            config: Arc::clone(&self.config),
        }
    }
}

// Graceful shutdown: flush on drop
impl Drop for MicrobatchWriter {
    fn drop(&mut self) {
        tracing::info!("MicrobatchWriter dropping, flushing all buffers");

        // Best-effort flush (may not complete if runtime is shutting down)
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(async {
                if let Err(e) = self.flush_all().await {
                    tracing::error!("Failed to flush on drop: {}", e);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RawEvent;
    use serde_json::json;

    // Mock store for testing
    struct MockEventLogStore {
        commit_count: Arc<Mutex<usize>>,
        last_batch_size: Arc<Mutex<usize>>,
    }

    impl MockEventLogStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                commit_count: Arc::new(Mutex::new(0)),
                last_batch_size: Arc::new(Mutex::new(0)),
            })
        }

        async fn commit_count(&self) -> usize {
            *self.commit_count.lock().await
        }

        async fn last_batch_size(&self) -> usize {
            *self.last_batch_size.lock().await
        }
    }

    // Note: Real tests need integration with EventLogStore
    // These are structural tests only

    #[tokio::test]
    async fn test_microbatch_config_defaults() {
        let config = MicrobatchConfig::default();
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.max_delay_ms, 100);
        assert!(config.adaptive);
    }

    #[tokio::test]
    async fn test_topic_buffer_creation() {
        let buffer = TopicBuffer {
            events: Vec::new(),
            first_event_time: Instant::now(),
            waiting_senders: Vec::new(),
        };

        assert_eq!(buffer.events.len(), 0);
        assert_eq!(buffer.waiting_senders.len(), 0);
    }

    #[test]
    fn test_adjust_batch_size_increase() {
        let mut config = MicrobatchConfig::default();
        config.max_batch_size = 1000;

        MicrobatchWriter::adjust_batch_size(
            &mut config,
            "test",
            Duration::from_millis(30), // Low latency
            1000,                      // Full batch
        );

        assert_eq!(config.max_batch_size, 1200); // 1000 * 1.2
    }

    #[test]
    fn test_adjust_batch_size_decrease() {
        let mut config = MicrobatchConfig::default();
        config.max_batch_size = 1000;

        MicrobatchWriter::adjust_batch_size(
            &mut config,
            "test",
            Duration::from_millis(250), // High latency
            500,
        );

        assert_eq!(config.max_batch_size, 800); // 1000 * 0.8
    }

    #[test]
    fn test_adjust_batch_size_limits() {
        let mut config = MicrobatchConfig::default();

        // Test upper limit
        config.max_batch_size = 9500;
        MicrobatchWriter::adjust_batch_size(&mut config, "test", Duration::from_millis(30), 9500);
        assert_eq!(config.max_batch_size, 10000); // Capped at 10000

        // Test lower limit
        config.max_batch_size = 150;
        MicrobatchWriter::adjust_batch_size(&mut config, "test", Duration::from_millis(250), 150);
        assert_eq!(config.max_batch_size, 120); // 150 * 0.8

        MicrobatchWriter::adjust_batch_size(&mut config, "test", Duration::from_millis(250), 120);
        assert_eq!(config.max_batch_size, 100); // Floored at 100
    }
}
