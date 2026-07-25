//! Retention - Snapshot 过期与保留策略 (阶段 5 Phase 2)
//!
//! 负责:
//! 1. 删除超过保留期的旧 Iceberg snapshot
//! 2. 回收存储空间
//! 3. 后台定时执行

use crate::event_log_store::EventLogStore;
use anyhow::{Context, Result};
use std::sync::Arc;

/// 保留策略配置
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// 保留天数 (超过此天数的 snapshot 会被过期)
    pub retain_days: u64,
    /// 至少保留的 snapshot 数量 (即使超过保留期也保留最近 N 个)
    pub min_snapshots: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retain_days: 90,   // 默认保留 90 天
            min_snapshots: 1,  // 至少保留 1 个 (当前快照)
        }
    }
}

impl RetentionPolicy {
    pub fn new(retain_days: u64, min_snapshots: usize) -> Self {
        Self {
            retain_days,
            min_snapshots: min_snapshots.max(1), // 至少保留 1 个
        }
    }
}

/// Snapshot 过期管理器
pub struct RetentionManager {
    event_log_store: Arc<EventLogStore>,
}

impl RetentionManager {
    pub fn new(event_log_store: Arc<EventLogStore>) -> Self {
        Self { event_log_store }
    }

    /// 识别单个表中可过期的 snapshot ID
    ///
    /// 注意: iceberg 0.9.1 的 `TableCommit` 构造是私有的,只能通过 `Transaction` 更新表,
    /// 而 `Transaction` 0.9.1 未提供 `expire_snapshots` action。因此实际删除操作待
    /// iceberg-rust 0.10+ 支持。当前实现识别出可过期的 snapshot 供上层决策/监控。
    ///
    /// 返回可过期的 snapshot ID 列表。
    pub async fn identify_expirable_snapshots(
        &self,
        table_name: &str,
        policy: &RetentionPolicy,
    ) -> Result<Vec<i64>> {
        // 1. 加载表
        let table = self
            .event_log_store
            .load_table(table_name)
            .await
            .with_context(|| format!("Failed to load table '{}'", table_name))?;

        let metadata = table.metadata();

        // 2. 收集所有 snapshot (按时间排序)
        let mut snapshots: Vec<_> = metadata.snapshots().collect();
        snapshots.sort_by_key(|s| s.timestamp_ms());

        let total = snapshots.len();
        if total <= policy.min_snapshots {
            tracing::debug!(
                "Table '{}' has {} snapshots (<= min {}), nothing to expire",
                table_name,
                total,
                policy.min_snapshots
            );
            return Ok(vec![]);
        }

        // 3. 计算过期时间点
        let now_ms = chrono::Utc::now().timestamp_millis();
        let cutoff_ms = now_ms - (policy.retain_days as i64 * 86_400 * 1000);

        // 4. 找出需要过期的 snapshot ID
        // 保留规则: 时间超过 cutoff 且不在最近 min_snapshots 个之内
        let keep_recent = policy.min_snapshots;
        let expirable_count = total.saturating_sub(keep_recent);

        let expired_ids: Vec<i64> = snapshots
            .iter()
            .take(expirable_count) // 只考虑较旧的部分
            .filter(|s| s.timestamp_ms() < cutoff_ms)
            .map(|s| s.snapshot_id())
            .collect();

        if !expired_ids.is_empty() {
            tracing::info!(
                "Table '{}': {} snapshots identified as expirable (retain_days={}, min={})",
                table_name,
                expired_ids.len(),
                policy.retain_days,
                policy.min_snapshots
            );
        }

        Ok(expired_ids)
    }

    /// 对单个表执行 snapshot 过期
    ///
    /// 注意: iceberg 0.9.1 API 限制,实际删除待 0.10+ 支持。
    /// 当前识别可过期的 snapshot 并记录日志,返回可过期数量。
    pub async fn expire_snapshots(
        &self,
        table_name: &str,
        policy: &RetentionPolicy,
    ) -> Result<usize> {
        let expirable = self
            .identify_expirable_snapshots(table_name, policy)
            .await?;

        if !expirable.is_empty() {
            tracing::warn!(
                "Table '{}': {} snapshots are expirable but iceberg 0.9.1 lacks \
                 the expire_snapshots API (Transaction/TableCommit private). \
                 Actual deletion pending iceberg-rust 0.10+.",
                table_name,
                expirable.len()
            );
        }

        Ok(expirable.len())
    }

    /// 对所有表识别可过期 snapshot
    pub async fn expire_all(&self, policy: &RetentionPolicy) -> Result<usize> {
        let tables = self.event_log_store.list_tables().await?;
        let mut total_expirable = 0;

        for table_name in tables {
            match self.expire_snapshots(&table_name, policy).await {
                Ok(count) => total_expirable += count,
                Err(e) => {
                    tracing::warn!("Failed to check snapshots for '{}': {}", table_name, e);
                }
            }
        }

        Ok(total_expirable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_policy_default() {
        let policy = RetentionPolicy::default();
        assert_eq!(policy.retain_days, 90);
        assert_eq!(policy.min_snapshots, 1);
    }

    #[test]
    fn test_retention_policy_min_snapshots_floor() {
        // min_snapshots 至少为 1
        let policy = RetentionPolicy::new(30, 0);
        assert_eq!(policy.min_snapshots, 1);
    }

    #[test]
    fn test_retention_policy_custom() {
        let policy = RetentionPolicy::new(7, 3);
        assert_eq!(policy.retain_days, 7);
        assert_eq!(policy.min_snapshots, 3);
    }
}
