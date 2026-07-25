// 长期任务-3: Standing Query 状态管理
//
// 物化视图维护：
// - 持续查询结果缓存
// - 增量更新机制
// - 支持多个并发订阅
// - 自动清理未订阅的查询

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info};

/// 查询 ID
pub type QueryId = String;

/// 订阅 ID
pub type SubscriptionId = String;

/// 查询结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// 查询 ID
    pub query_id: QueryId,
    /// 结果数据（简化为 JSON）
    pub data: serde_json::Value,
    /// 版本号（用于增量更新）
    pub version: u64,
}

/// Standing Query 元数据
#[derive(Debug, Clone)]
struct StandingQuery {
    /// 查询 ID
    #[allow(dead_code)] // D1: Standing query stub
    query_id: QueryId,
    /// 查询表达式（简化为字符串）
    #[allow(dead_code)] // D1: Standing query stub
    query_expr: String,
    /// 当前结果
    current_result: Arc<RwLock<Option<QueryResult>>>,
    /// 订阅者集合
    subscribers: Arc<RwLock<HashSet<SubscriptionId>>>,
    /// 更新通知通道
    update_tx: broadcast::Sender<QueryResult>,
}

/// Standing Query 管理器
pub struct StandingQueryManager {
    /// 活跃的查询（query_id -> query）
    queries: Arc<RwLock<HashMap<QueryId, StandingQuery>>>,
    /// 订阅映射（subscription_id -> query_id）
    subscriptions: Arc<RwLock<HashMap<SubscriptionId, QueryId>>>,
}

impl StandingQueryManager {
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册 Standing Query
    pub async fn register_query(
        &self,
        query_id: QueryId,
        query_expr: String,
    ) -> Result<(), String> {
        let mut queries = self.queries.write().await;

        if queries.contains_key(&query_id) {
            return Err(format!("Query {} already registered", query_id));
        }

        let (update_tx, _) = broadcast::channel(100);

        let query = StandingQuery {
            query_id: query_id.clone(),
            query_expr,
            current_result: Arc::new(RwLock::new(None)),
            subscribers: Arc::new(RwLock::new(HashSet::new())),
            update_tx,
        };

        queries.insert(query_id.clone(), query);
        info!("Registered standing query: {}", query_id);

        Ok(())
    }

    /// 订阅查询
    pub async fn subscribe(
        &self,
        query_id: &QueryId,
        subscription_id: SubscriptionId,
    ) -> Result<broadcast::Receiver<QueryResult>, String> {
        let queries = self.queries.read().await;

        let query = queries
            .get(query_id)
            .ok_or_else(|| format!("Query {} not found", query_id))?;

        // 添加订阅者
        {
            let mut subscribers = query.subscribers.write().await;
            subscribers.insert(subscription_id.clone());
        }

        // 记录订阅映射
        {
            let mut subscriptions = self.subscriptions.write().await;
            subscriptions.insert(subscription_id.clone(), query_id.clone());
        }

        info!(
            "Subscription {} subscribed to query {}",
            subscription_id, query_id
        );

        // 返回更新通知接收器
        Ok(query.update_tx.subscribe())
    }

    /// 取消订阅
    pub async fn unsubscribe(&self, subscription_id: &SubscriptionId) -> Result<(), String> {
        let mut subscriptions = self.subscriptions.write().await;

        let query_id = subscriptions
            .remove(subscription_id)
            .ok_or_else(|| format!("Subscription {} not found", subscription_id))?;

        let queries = self.queries.read().await;
        if let Some(query) = queries.get(&query_id) {
            let mut subscribers = query.subscribers.write().await;
            subscribers.remove(subscription_id);

            info!(
                "Subscription {} unsubscribed from query {}",
                subscription_id, query_id
            );
        }

        Ok(())
    }

    /// 更新查询结果
    pub async fn update_result(
        &self,
        query_id: &QueryId,
        result: QueryResult,
    ) -> Result<(), String> {
        let queries = self.queries.read().await;

        let query = queries
            .get(query_id)
            .ok_or_else(|| format!("Query {} not found", query_id))?;

        // 更新当前结果
        {
            let mut current = query.current_result.write().await;
            *current = Some(result.clone());
        }

        // 通知所有订阅者
        let subscriber_count = query.subscribers.read().await.len();
        if subscriber_count > 0 {
            let _ = query.update_tx.send(result);
            debug!(
                "Notified {} subscribers of query {} update",
                subscriber_count, query_id
            );
        }

        Ok(())
    }

    /// 获取当前查询结果
    pub async fn get_current_result(
        &self,
        query_id: &QueryId,
    ) -> Result<Option<QueryResult>, String> {
        let queries = self.queries.read().await;

        let query = queries
            .get(query_id)
            .ok_or_else(|| format!("Query {} not found", query_id))?;

        let current = query.current_result.read().await;
        Ok(current.clone())
    }

    /// 清理未订阅的查询
    pub async fn cleanup_unsubscribed(&self) -> Result<usize, String> {
        let mut queries = self.queries.write().await;

        let before_count = queries.len();

        let mut to_remove = Vec::new();

        for (query_id, query) in queries.iter() {
            let subscribers = query.subscribers.read().await;
            if subscribers.is_empty() {
                to_remove.push(query_id.clone());
            }
        }

        for query_id in &to_remove {
            queries.remove(query_id);
            info!("Removing unsubscribed query: {}", query_id);
        }

        let removed = before_count - queries.len();

        if removed > 0 {
            debug!("Cleaned up {} unsubscribed queries", removed);
        }

        Ok(removed)
    }

    /// 获取统计信息
    pub async fn stats(&self) -> ManagerStats {
        let queries = self.queries.read().await;
        let subscriptions = self.subscriptions.read().await;

        let mut total_subscribers = 0;
        for query in queries.values() {
            let subscribers = query.subscribers.read().await;
            total_subscribers += subscribers.len();
        }

        ManagerStats {
            total_queries: queries.len(),
            total_subscriptions: subscriptions.len(),
            total_subscribers,
        }
    }
}

impl Default for StandingQueryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 管理器统计
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagerStats {
    pub total_queries: usize,
    pub total_subscriptions: usize,
    pub total_subscribers: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_subscribe() {
        let manager = StandingQueryManager::new();

        let query_id = "query-001".to_string();
        let query_expr = "MATCH (n) RETURN n".to_string();

        manager
            .register_query(query_id.clone(), query_expr)
            .await
            .unwrap();

        let subscription_id = "sub-001".to_string();
        let mut rx = manager
            .subscribe(&query_id, subscription_id.clone())
            .await
            .unwrap();

        // 更新结果
        let result = QueryResult {
            query_id: query_id.clone(),
            data: serde_json::json!({"nodes": [1, 2, 3]}),
            version: 1,
        };

        manager
            .update_result(&query_id, result.clone())
            .await
            .unwrap();

        // 订阅者应该收到通知
        let received = rx.recv().await.unwrap();
        assert_eq!(received.version, 1);
        assert_eq!(received.query_id, query_id);
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let manager = StandingQueryManager::new();

        let query_id = "query-002".to_string();
        manager
            .register_query(query_id.clone(), "query".to_string())
            .await
            .unwrap();

        let subscription_id = "sub-002".to_string();
        let _rx = manager
            .subscribe(&query_id, subscription_id.clone())
            .await
            .unwrap();

        manager.unsubscribe(&subscription_id).await.unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.total_subscriptions, 0);
    }

    #[tokio::test]
    async fn test_cleanup_unsubscribed() {
        let manager = StandingQueryManager::new();

        let query_id = "query-003".to_string();
        manager
            .register_query(query_id.clone(), "query".to_string())
            .await
            .unwrap();

        // 没有订阅者
        let removed = manager.cleanup_unsubscribed().await.unwrap();
        assert_eq!(removed, 1);

        let stats = manager.stats().await;
        assert_eq!(stats.total_queries, 0);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let manager = StandingQueryManager::new();

        let query_id = "query-004".to_string();
        manager
            .register_query(query_id.clone(), "query".to_string())
            .await
            .unwrap();

        let mut rx1 = manager
            .subscribe(&query_id, "sub-1".to_string())
            .await
            .unwrap();
        let mut rx2 = manager
            .subscribe(&query_id, "sub-2".to_string())
            .await
            .unwrap();

        let result = QueryResult {
            query_id: query_id.clone(),
            data: serde_json::json!({"value": 42}),
            version: 1,
        };

        manager
            .update_result(&query_id, result.clone())
            .await
            .unwrap();

        // 两个订阅者都应该收到通知
        let r1 = rx1.recv().await.unwrap();
        let r2 = rx2.recv().await.unwrap();

        assert_eq!(r1.version, 1);
        assert_eq!(r2.version, 1);
    }

    #[tokio::test]
    async fn test_get_current_result() {
        let manager = StandingQueryManager::new();

        let query_id = "query-005".to_string();
        manager
            .register_query(query_id.clone(), "query".to_string())
            .await
            .unwrap();

        let result = QueryResult {
            query_id: query_id.clone(),
            data: serde_json::json!({"value": 100}),
            version: 1,
        };

        manager
            .update_result(&query_id, result.clone())
            .await
            .unwrap();

        let current = manager.get_current_result(&query_id).await.unwrap();
        assert!(current.is_some());
        assert_eq!(current.unwrap().version, 1);
    }

    #[tokio::test]
    async fn test_stats() {
        let manager = StandingQueryManager::new();

        manager
            .register_query("q1".to_string(), "query1".to_string())
            .await
            .unwrap();
        manager
            .register_query("q2".to_string(), "query2".to_string())
            .await
            .unwrap();

        let _rx1 = manager
            .subscribe(&"q1".to_string(), "s1".to_string())
            .await
            .unwrap();
        let _rx2 = manager
            .subscribe(&"q2".to_string(), "s2".to_string())
            .await
            .unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.total_subscriptions, 2);
        assert_eq!(stats.total_subscribers, 2);
    }
}
