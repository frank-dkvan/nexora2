// 长期任务-1: Checkpoint 机制
//
// 定期创建分片状态快照：
// - 持久化到磁盘
// - 支持增量 checkpoint
// - 崩溃后快速恢复
// - 自动清理旧快照

use crate::replication_log::{ReplicationLog, ReplicationSeq};
use crate::GraphOperation;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Checkpoint 元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Checkpoint ID
    pub checkpoint_id: u64,
    /// 创建时间戳
    pub timestamp: u64,
    /// 覆盖的分片
    pub shards: Vec<u32>,
    /// 每个分片的最高序列号
    pub shard_high_seqs: HashMap<u32, ReplicationSeq>,
    /// 快照文件路径
    pub snapshot_path: PathBuf,
}

/// 分片快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshot {
    pub shard_id: u32,
    pub operations: Vec<(ReplicationSeq, GraphOperation)>,
    pub high_seq: ReplicationSeq,
}

/// Checkpoint 管理器
pub struct CheckpointManager {
    /// Checkpoint 存储目录
    checkpoint_dir: PathBuf,
    /// ReplicationLog 引用
    replication_log: Arc<RwLock<ReplicationLog>>,
    /// 最新的 checkpoint ID
    latest_checkpoint_id: Arc<RwLock<u64>>,
    /// 保留的 checkpoint 数量
    retention_count: usize,
}

impl CheckpointManager {
    pub fn new(
        checkpoint_dir: PathBuf,
        replication_log: Arc<RwLock<ReplicationLog>>,
        retention_count: usize,
    ) -> Self {
        Self {
            checkpoint_dir,
            replication_log,
            latest_checkpoint_id: Arc::new(RwLock::new(0)),
            retention_count,
        }
    }

    /// 初始化 checkpoint 目录
    pub async fn initialize(&self) -> Result<(), String> {
        fs::create_dir_all(&self.checkpoint_dir)
            .await
            .map_err(|e| format!("Failed to create checkpoint dir: {}", e))?;

        info!(
            "Checkpoint directory initialized: {:?}",
            self.checkpoint_dir
        );
        Ok(())
    }

    /// 创建全量 checkpoint
    pub async fn create_full_checkpoint(&self, shards: Vec<u32>) -> Result<u64, String> {
        let checkpoint_id = {
            let mut id = self.latest_checkpoint_id.write().await;
            *id += 1;
            *id
        };

        let mut shard_high_seqs = HashMap::new();
        let mut all_snapshots = Vec::new();

        // 为每个分片创建快照
        let log_guard = self.replication_log.read().await;
        for shard_id in &shards {
            let catch_up = log_guard.since(*shard_id as usize, 0).await;

            let operations = match catch_up {
                crate::replication_log::CatchUp::Incremental(ops) => ops,
                crate::replication_log::CatchUp::UpToDate => Vec::new(),
                crate::replication_log::CatchUp::TooOld => {
                    warn!("Shard {} log too old for checkpoint", shard_id);
                    Vec::new()
                }
            };

            let high_seq = operations.last().map(|(seq, _)| *seq).unwrap_or(0);
            shard_high_seqs.insert(*shard_id, high_seq);

            all_snapshots.push(ShardSnapshot {
                shard_id: *shard_id,
                operations,
                high_seq,
            });
        }
        drop(log_guard);

        // 写入快照文件
        let snapshot_path = self
            .checkpoint_dir
            .join(format!("checkpoint_{}.json", checkpoint_id));

        let snapshot_data = serde_json::to_vec(&all_snapshots)
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;

        let mut file = fs::File::create(&snapshot_path)
            .await
            .map_err(|e| format!("Failed to create snapshot file: {}", e))?;

        file.write_all(&snapshot_data)
            .await
            .map_err(|e| format!("Failed to write snapshot: {}", e))?;

        file.sync_all()
            .await
            .map_err(|e| format!("Failed to sync snapshot: {}", e))?;

        // 写入元数据
        let metadata = CheckpointMetadata {
            checkpoint_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            shards: shards.clone(),
            shard_high_seqs,
            snapshot_path: snapshot_path.clone(),
        };

        let metadata_path = self
            .checkpoint_dir
            .join(format!("checkpoint_{}.meta.json", checkpoint_id));

        let metadata_data = serde_json::to_vec(&metadata)
            .map_err(|e| format!("Failed to serialize metadata: {}", e))?;

        let mut meta_file = fs::File::create(&metadata_path)
            .await
            .map_err(|e| format!("Failed to create metadata file: {}", e))?;

        meta_file
            .write_all(&metadata_data)
            .await
            .map_err(|e| format!("Failed to write metadata: {}", e))?;

        meta_file
            .sync_all()
            .await
            .map_err(|e| format!("Failed to sync metadata: {}", e))?;

        info!(
            "Created checkpoint {} with {} shards",
            checkpoint_id,
            shards.len()
        );

        // 清理旧快照
        self.cleanup_old_checkpoints().await?;

        Ok(checkpoint_id)
    }

    /// 加载最新的 checkpoint
    pub async fn load_latest_checkpoint(&self) -> Result<Option<CheckpointMetadata>, String> {
        let mut entries = fs::read_dir(&self.checkpoint_dir)
            .await
            .map_err(|e| format!("Failed to read checkpoint dir: {}", e))?;

        let mut latest_id = 0u64;
        let mut found = false;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| format!("Failed to read dir entry: {}", e))?
        {
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("checkpoint_") && filename.ends_with(".meta.json") {
                    if let Some(id_str) = filename
                        .strip_prefix("checkpoint_")
                        .and_then(|s| s.strip_suffix(".meta.json"))
                    {
                        if let Ok(id) = id_str.parse::<u64>() {
                            if id > latest_id {
                                latest_id = id;
                                found = true;
                            }
                        }
                    }
                }
            }
        }

        if !found {
            return Ok(None);
        }

        let metadata_path = self
            .checkpoint_dir
            .join(format!("checkpoint_{}.meta.json", latest_id));

        let metadata_data = fs::read(&metadata_path)
            .await
            .map_err(|e| format!("Failed to read metadata: {}", e))?;

        let metadata: CheckpointMetadata = serde_json::from_slice(&metadata_data)
            .map_err(|e| format!("Failed to deserialize metadata: {}", e))?;

        Ok(Some(metadata))
    }

    /// 从 checkpoint 恢复
    pub async fn restore_from_checkpoint(
        &self,
        checkpoint_id: u64,
    ) -> Result<Vec<ShardSnapshot>, String> {
        let snapshot_path = self
            .checkpoint_dir
            .join(format!("checkpoint_{}.json", checkpoint_id));

        let snapshot_data = fs::read(&snapshot_path)
            .await
            .map_err(|e| format!("Failed to read snapshot: {}", e))?;

        let snapshots: Vec<ShardSnapshot> = serde_json::from_slice(&snapshot_data)
            .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        info!(
            "Restored checkpoint {} with {} shards",
            checkpoint_id,
            snapshots.len()
        );

        Ok(snapshots)
    }

    /// 清理旧 checkpoint
    async fn cleanup_old_checkpoints(&self) -> Result<(), String> {
        let mut entries = fs::read_dir(&self.checkpoint_dir)
            .await
            .map_err(|e| format!("Failed to read checkpoint dir: {}", e))?;

        let mut checkpoint_ids = Vec::new();

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| format!("Failed to read dir entry: {}", e))?
        {
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("checkpoint_") && filename.ends_with(".meta.json") {
                    if let Some(id_str) = filename
                        .strip_prefix("checkpoint_")
                        .and_then(|s| s.strip_suffix(".meta.json"))
                    {
                        if let Ok(id) = id_str.parse::<u64>() {
                            checkpoint_ids.push(id);
                        }
                    }
                }
            }
        }

        checkpoint_ids.sort_unstable();

        if checkpoint_ids.len() > self.retention_count {
            let to_delete = checkpoint_ids.len() - self.retention_count;
            for id in checkpoint_ids.iter().take(to_delete) {
                let snapshot_path = self.checkpoint_dir.join(format!("checkpoint_{}.json", id));
                let metadata_path = self
                    .checkpoint_dir
                    .join(format!("checkpoint_{}.meta.json", id));

                let _ = fs::remove_file(&snapshot_path).await;
                let _ = fs::remove_file(&metadata_path).await;

                debug!("Deleted old checkpoint {}", id);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_id::NexoraId;
    use tempfile::TempDir;

    fn make_operation(value: i32) -> GraphOperation {
        GraphOperation::SetProperty {
            qid: NexoraId::from_bytes(b"test".to_vec()),
            key: "test".to_string(),
            value: serde_json::Value::Number(value.into()),
        }
    }

    #[tokio::test]
    async fn test_create_and_load_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));

        // 添加一些操作
        {
            let log_guard = log.write().await;
            log_guard.record_replica(0, 1, make_operation(1)).await;
            log_guard.record_replica(0, 2, make_operation(2)).await;
        }

        let manager = CheckpointManager::new(temp_dir.path().to_path_buf(), log.clone(), 3);
        manager.initialize().await.unwrap();

        // 创建 checkpoint
        let checkpoint_id = manager.create_full_checkpoint(vec![0]).await.unwrap();
        assert_eq!(checkpoint_id, 1);

        // 加载最新 checkpoint
        let metadata = manager.load_latest_checkpoint().await.unwrap();
        assert!(metadata.is_some());
        assert_eq!(metadata.unwrap().checkpoint_id, 1);
    }

    #[tokio::test]
    async fn test_restore_from_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));

        {
            let log_guard = log.write().await;
            log_guard.record_replica(0, 1, make_operation(1)).await;
            log_guard.record_replica(0, 2, make_operation(2)).await;
        }

        let manager = CheckpointManager::new(temp_dir.path().to_path_buf(), log.clone(), 3);
        manager.initialize().await.unwrap();

        let checkpoint_id = manager.create_full_checkpoint(vec![0]).await.unwrap();

        // 恢复
        let snapshots = manager
            .restore_from_checkpoint(checkpoint_id)
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].shard_id, 0);
        assert_eq!(snapshots[0].operations.len(), 2);
    }

    #[tokio::test]
    async fn test_cleanup_old_checkpoints() {
        let temp_dir = TempDir::new().unwrap();
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));

        let manager = CheckpointManager::new(temp_dir.path().to_path_buf(), log.clone(), 2);
        manager.initialize().await.unwrap();

        // 创建 3 个 checkpoint
        manager.create_full_checkpoint(vec![0]).await.unwrap();
        manager.create_full_checkpoint(vec![0]).await.unwrap();
        manager.create_full_checkpoint(vec![0]).await.unwrap();

        // 验证只保留最新的 2 个
        let mut entries = fs::read_dir(temp_dir.path()).await.unwrap();
        let mut count = 0;

        while let Some(entry) = entries.next_entry().await.unwrap() {
            let filename = entry.file_name();
            if filename.to_str().unwrap().starts_with("checkpoint_") {
                count += 1;
            }
        }

        // 2 个 checkpoint × 2 个文件（snapshot + metadata）= 4 个文件
        assert_eq!(count, 4);
    }
}
