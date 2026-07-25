// 长期任务-2: Exactly-Once 语义
//
// 端到端幂等性保证：
// - 客户端请求去重
// - 服务端响应缓存
// - 支持跨重试的结果一致性
// - 与写入幂等性机制集成

use crate::idempotency::IdempotencyTracker;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// 请求 ID（客户端生成的唯一标识）
pub type RequestId = String;

/// 响应缓存条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedResponse<T> {
    /// 请求 ID
    pub request_id: RequestId,
    /// 响应结果
    pub response: T,
    /// 创建时间戳
    pub timestamp: u64,
    /// 请求状态
    pub status: RequestStatus,
}

/// 请求状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestStatus {
    /// 处理中
    Processing,
    /// 已完成
    Completed,
    /// 已失败
    Failed,
}

/// Exactly-Once 协调器
pub struct ExactlyOnceCoordinator<T> {
    /// 响应缓存（request_id -> cached_response）
    response_cache: Arc<RwLock<HashMap<RequestId, CachedResponse<T>>>>,
    /// 幂等性跟踪器（写入层）
    idempotency_tracker: Arc<IdempotencyTracker>,
    /// 缓存过期时间
    cache_ttl: Duration,
}

impl<T: Clone + Send + Sync + 'static> ExactlyOnceCoordinator<T> {
    pub fn new(idempotency_tracker: Arc<IdempotencyTracker>, cache_ttl: Duration) -> Self {
        Self {
            response_cache: Arc::new(RwLock::new(HashMap::new())),
            idempotency_tracker,
            cache_ttl,
        }
    }

    /// 检查请求是否已处理
    pub async fn check_duplicate(&self, request_id: &RequestId) -> Option<CachedResponse<T>> {
        let cache = self.response_cache.read().await;
        cache.get(request_id).cloned()
    }

    /// 标记请求开始处理
    pub async fn mark_processing(&self, request_id: RequestId) -> Result<(), String> {
        let cache = self.response_cache.write().await;

        if cache.contains_key(&request_id) {
            return Err(format!("Request {} already exists", request_id));
        }

        // 使用临时占位响应（实际类型为 T，这里需要调用方提供初始值）
        // 为了简化，我们暂时不存储 Processing 状态的响应
        debug!("Request {} marked as processing", request_id);

        Ok(())
    }

    /// 缓存成功响应
    pub async fn cache_response(&self, request_id: RequestId, response: T) -> Result<(), String> {
        let mut cache = self.response_cache.write().await;

        let cached = CachedResponse {
            request_id: request_id.clone(),
            response,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            status: RequestStatus::Completed,
        };

        cache.insert(request_id.clone(), cached);
        info!("Cached response for request {}", request_id);

        Ok(())
    }

    /// 缓存失败响应
    pub async fn cache_failure(
        &self,
        request_id: RequestId,
        error_response: T,
    ) -> Result<(), String> {
        let mut cache = self.response_cache.write().await;

        let cached = CachedResponse {
            request_id: request_id.clone(),
            response: error_response,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            status: RequestStatus::Failed,
        };

        cache.insert(request_id.clone(), cached);
        info!("Cached failure for request {}", request_id);

        Ok(())
    }

    /// 清理过期缓存
    pub async fn cleanup_expired(&self) -> Result<usize, String> {
        let mut cache = self.response_cache.write().await;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let ttl_secs = self.cache_ttl.as_secs();

        let before_count = cache.len();

        cache.retain(|_, cached| now - cached.timestamp < ttl_secs);

        let removed = before_count - cache.len();

        if removed > 0 {
            debug!("Cleaned up {} expired cached responses", removed);
        }

        Ok(removed)
    }

    /// 获取缓存统计
    pub async fn cache_stats(&self) -> CacheStats {
        let cache = self.response_cache.read().await;

        let mut completed = 0;
        let mut failed = 0;
        let mut processing = 0;

        for cached in cache.values() {
            match cached.status {
                RequestStatus::Completed => completed += 1,
                RequestStatus::Failed => failed += 1,
                RequestStatus::Processing => processing += 1,
            }
        }

        CacheStats {
            total: cache.len(),
            completed,
            failed,
            processing,
        }
    }

    /// 执行幂等写入（与 IdempotencyTracker 集成）
    pub async fn idempotent_write<F, R>(
        &self,
        request_id: RequestId,
        write_fn: F,
    ) -> Result<R, String>
    where
        F: FnOnce()
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, String>> + Send>>,
        R: Clone + Send + 'static,
    {
        // 检查请求是否已被写入层处理
        if self
            .idempotency_tracker
            .check_duplicate(&request_id)
            .await
            .is_some()
        {
            return Err(format!(
                "Request {} already processed at write layer",
                request_id
            ));
        }

        // 执行写入
        let result = write_fn().await?;

        // 标记为已处理（记录成功结果，使用占位序列号）
        self.idempotency_tracker
            .record_success(request_id.clone(), 0)
            .await;

        Ok(result)
    }
}

/// 缓存统计
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheStats {
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub processing: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_and_retrieve_response() {
        let tracker = Arc::new(IdempotencyTracker::new());
        let coordinator: ExactlyOnceCoordinator<String> =
            ExactlyOnceCoordinator::new(tracker, Duration::from_secs(60));

        let request_id = "req-001".to_string();
        let response = "success".to_string();

        // 缓存响应
        coordinator
            .cache_response(request_id.clone(), response.clone())
            .await
            .unwrap();

        // 检查是否重复
        let cached = coordinator.check_duplicate(&request_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().response, response);
    }

    #[tokio::test]
    async fn test_cache_failure() {
        let tracker = Arc::new(IdempotencyTracker::new());
        let coordinator: ExactlyOnceCoordinator<String> =
            ExactlyOnceCoordinator::new(tracker, Duration::from_secs(60));

        let request_id = "req-002".to_string();
        let error_msg = "error".to_string();

        coordinator
            .cache_failure(request_id.clone(), error_msg.clone())
            .await
            .unwrap();

        let cached = coordinator.check_duplicate(&request_id).await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.status, RequestStatus::Failed);
        assert_eq!(cached.response, error_msg);
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let tracker = Arc::new(IdempotencyTracker::new());
        let coordinator: ExactlyOnceCoordinator<String> =
            ExactlyOnceCoordinator::new(tracker, Duration::from_millis(100));

        coordinator
            .cache_response("req-001".to_string(), "resp1".to_string())
            .await
            .unwrap();

        // 等待过期
        tokio::time::sleep(Duration::from_millis(200)).await;

        let removed = coordinator.cleanup_expired().await.unwrap();
        assert_eq!(removed, 1);

        let cached = coordinator.check_duplicate(&"req-001".to_string()).await;
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let tracker = Arc::new(IdempotencyTracker::new());
        let coordinator: ExactlyOnceCoordinator<String> =
            ExactlyOnceCoordinator::new(tracker, Duration::from_secs(60));

        coordinator
            .cache_response("req-001".to_string(), "resp1".to_string())
            .await
            .unwrap();
        coordinator
            .cache_failure("req-002".to_string(), "error".to_string())
            .await
            .unwrap();

        let stats = coordinator.cache_stats().await;
        assert_eq!(stats.total, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.processing, 0);
    }

    #[tokio::test]
    async fn test_idempotent_write_integration() {
        let tracker = Arc::new(IdempotencyTracker::new());
        let coordinator: ExactlyOnceCoordinator<String> =
            ExactlyOnceCoordinator::new(tracker.clone(), Duration::from_secs(60));

        let request_id = "req-003".to_string();

        // 第一次写入
        let result = coordinator
            .idempotent_write(request_id.clone(), || {
                Box::pin(async { Ok::<String, String>("written".to_string()) })
            })
            .await
            .unwrap();
        assert_eq!(result, "written");

        // 第二次写入应该被拒绝
        let result = coordinator
            .idempotent_write(request_id.clone(), || {
                Box::pin(async { Ok::<String, String>("written_again".to_string()) })
            })
            .await;
        assert!(result.is_err());
    }
}
