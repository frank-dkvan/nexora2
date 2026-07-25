//! EventFirstHandler — 事件优先的摄入 handler
//!
//! 负责:
//! 1. 根据 TopicRouter 决定路由目标
//! 2. EventTable:只写 Iceberg 事件表
//! 3. Graph:只投图(兼容模式)
//! 4. Both:双写(先事件表,再图)

use crate::event_log_store::EventLogStore;
use crate::router::{Destination, TopicRouter};
use async_trait::async_trait;
use nexora_stream::{GraphIngestHandler, IngestBatch, IngestHandler};
use std::sync::Arc;

/// EventFirstHandler 实现"事件优先"摄入模式
pub struct EventFirstHandler {
    event_log_store: Arc<EventLogStore>,
    graph_handler: Arc<GraphIngestHandler>,
    router: Arc<TopicRouter>,
}

impl EventFirstHandler {
    pub fn new(
        event_log_store: Arc<EventLogStore>,
        graph_handler: Arc<GraphIngestHandler>,
        router: Arc<TopicRouter>,
    ) -> Self {
        Self {
            event_log_store,
            graph_handler,
            router,
        }
    }
}

#[async_trait]
impl IngestHandler for EventFirstHandler {
    async fn handle_batch(&self, batch: &IngestBatch) -> Result<usize, String> {
        let dest = self.router.route(&batch.topic);

        match &dest {
            Destination::EventTable => {
                // 只写事件表
                if let Some(ref raw_events) = batch.raw_events {
                    self.event_log_store
                        .append(raw_events)
                        .await
                        .map_err(|e| format!("Failed to append to event table: {:#}", e))?;

                    tracing::debug!(
                        "Appended {} events to table '{}' (EventTable mode)",
                        raw_events.len(),
                        batch.topic
                    );

                    Ok(raw_events.len())
                } else {
                    Err(format!(
                        "No raw_events in batch for EventTable destination (topic: {})",
                        batch.topic
                    ))
                }
            }

            Destination::Graph => {
                // 兼容模式:只投图
                tracing::trace!("Routing to graph only (topic: {})", batch.topic);
                self.graph_handler.handle_batch(batch).await
            }

            Destination::Both => {
                // 双写:先事件表,再图
                let event_count = if let Some(ref raw_events) = batch.raw_events {
                    self.event_log_store
                        .append(raw_events)
                        .await
                        .map_err(|e| format!("Failed to append to event table: {:#}", e))?;

                    tracing::debug!(
                        "Appended {} events to table '{}' (Both mode)",
                        raw_events.len(),
                        batch.topic
                    );

                    raw_events.len()
                } else {
                    tracing::warn!(
                        "No raw_events in batch for Both destination, skipping event table (topic: {})",
                        batch.topic
                    );
                    0
                };

                // 无论事件表写入成功与否,都尝试投图(保持图更新)
                let graph_count = self.graph_handler.handle_batch(batch).await?;

                tracing::debug!(
                    "Projected {} records to graph (topic: {})",
                    graph_count,
                    batch.topic
                );

                // 返回事件表写入的数量(作为主要计数)
                Ok(event_count)
            }
        }
    }
}


// 单元测试见 tests/view_test.rs 等集成测试
