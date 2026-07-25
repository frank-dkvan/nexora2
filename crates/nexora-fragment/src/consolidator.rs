//! B8: Fragment consolidation — 后台自动合并小碎片（借鉴 TileDB）。
//!
//! 小 fragment 过多会拖慢 time-travel 查询（每个都要 scan）+ 增加 metadata
//! 开销。Consolidator 按策略挑选一批小/相邻 fragment，用底层
//! `consolidate_fragments` 原语合并成一个大 fragment，然后删除旧的。

use crate::fragment_id::FragmentId;
use crate::store::{FragmentError, FragmentStore};
use crate::time_travel::consolidate_fragments;

/// 合并策略配置（借鉴 TileDB ConsolidationConfig）。
#[derive(Clone, Debug)]
pub struct ConsolidationConfig {
    /// 至少积累这么多 fragment 才触发合并（避免过早合并）。
    pub min_frags: usize,
    /// 单次最多合并这么多 fragment（避免一次合并太多导致长暂停）。
    pub max_frags: usize,
    /// 只合并小于 (最大 fragment size * size_ratio) 的 fragment（相对小碎片）。
    pub size_ratio: f64,
    /// 只合并 node_count 小于此阈值的 fragment（绝对小碎片，单位：节点数）。
    pub min_size_nodes: u64,
}

impl Default for ConsolidationConfig {
    fn default() -> Self {
        // 对齐 ROADMAP: min_frags=10, max_frags=100, size_ratio=0.3, min_size=10MB
        // 这里 min_size 用 node_count 近似（10MB / 假设每节点 ~1KB ≈ 10000 节点）
        Self {
            min_frags: 10,
            max_frags: 100,
            size_ratio: 0.3,
            min_size_nodes: 10_000,
        }
    }
}

/// 按配置策略自动挑选并合并小碎片 fragment 的合并器。
pub struct FragmentConsolidator {
    config: ConsolidationConfig,
}

impl FragmentConsolidator {
    /// 用给定配置创建合并器。
    pub fn new(config: ConsolidationConfig) -> Self {
        Self { config }
    }

    /// 用默认配置创建合并器。
    pub fn with_default() -> Self {
        Self::new(ConsolidationConfig::default())
    }

    /// 只读访问配置。
    pub fn config(&self) -> &ConsolidationConfig {
        &self.config
    }

    /// 选择待合并的 fragment：按策略筛出“小碎片”，按时间排序，取前 max_frags 个。
    /// 返回空 vec 表示当前不需要合并（未达 min_frags 阈值或无小碎片）。
    pub async fn select_fragments(&self, store: &FragmentStore) -> Vec<FragmentId> {
        let all = store.list_all().await;

        // 未达 min_frags 阈值 → 不合并（避免过早合并）。
        if all.len() < self.config.min_frags {
            return Vec::new();
        }

        // 以最大 node_count 为基准，计算相对小碎片阈值。
        let max_nodes = all.iter().map(|m| m.node_count).max().unwrap_or(0);
        let relative_threshold = (max_nodes as f64) * self.config.size_ratio;

        // 筛出“小碎片”：node_count 低于相对阈值 或 低于绝对阈值。
        // 已经 is_consolidated 的大块跳过，避免重复合并。
        let mut victims: Vec<FragmentId> = all
            .into_iter()
            .filter(|m| !m.is_consolidated)
            .filter(|m| {
                m.node_count < self.config.min_size_nodes
                    || (m.node_count as f64) < relative_threshold
            })
            .map(|m| m.id)
            .collect();

        // 按时间排序（start_us 升序，相同则 end_us 升序），保证合并集合时间相邻。
        victims.sort_by(|a, b| a.start_us.cmp(&b.start_us).then(a.end_us.cmp(&b.end_us)));

        // 单次最多合并 max_frags 个。
        victims.truncate(self.config.max_frags);
        victims
    }

    /// 执行一轮合并：选择 → 合并 → 返回合并后的 fragment id（若有）。
    /// 时间范围取被合并集合的 [min start_us, max end_us]。
    pub async fn consolidate_once(
        &self,
        store: &FragmentStore,
    ) -> Result<Option<FragmentId>, FragmentError> {
        let victims = self.select_fragments(store).await;

        // 合并单个 fragment 没有收益，至少要 2 个才值得合并。
        if victims.len() < 2 {
            return Ok(None);
        }

        // 时间范围直接取自 FragmentId 的 start_us/end_us。
        let target_start = victims.iter().map(|f| f.start_us).min().unwrap_or(0);
        let target_end = victims
            .iter()
            .map(|f| f.end_us)
            .max()
            .unwrap_or(target_start);

        let new_id = consolidate_fragments(store, &victims, target_start, target_end).await?;
        Ok(Some(new_id))
    }

    /// 启动后台合并循环，每 interval 检查并合并一次。返回 JoinHandle。
    pub fn spawn_background(
        self: std::sync::Arc<Self>,
        store: std::sync::Arc<FragmentStore>,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                match self.consolidate_once(&store).await {
                    Ok(Some(id)) => {
                        tracing::info!(?id, "fragment consolidation merged a batch")
                    }
                    Ok(None) => tracing::trace!("no fragments to consolidate"),
                    Err(e) => tracing::warn!(error = %e, "fragment consolidation failed"),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::FragmentMetadata;
    use crate::store::FragmentStore;
    use tempfile::tempdir;

    fn make_store() -> FragmentStore {
        let dir = tempdir().unwrap();
        FragmentStore::new(dir.path(), "test")
    }

    fn small_config() -> ConsolidationConfig {
        ConsolidationConfig {
            min_frags: 3,
            max_frags: 10,
            size_ratio: 0.3,
            min_size_nodes: 1000,
        }
    }

    /// 注册一个 fragment：写 nodes.jsonl 并登记 metadata（含 node_count）。
    async fn register_fragment(
        store: &FragmentStore,
        start_us: u64,
        end_us: u64,
        node_count: u64,
        lines: &[&str],
    ) -> FragmentId {
        let fid = FragmentId {
            start_us,
            end_us,
            uuid: uuid::Uuid::new_v4(),
        };
        let dir = store.create_fragment_dir(&fid).unwrap();
        std::fs::write(dir.join("nodes.jsonl"), lines.join("\n")).unwrap();

        let mut meta = FragmentMetadata::new(fid.clone(), "test".into());
        meta.node_count = node_count;
        store.register(meta).await.unwrap();
        fid
    }

    #[tokio::test]
    async fn select_returns_empty_below_min_frags() {
        let store = make_store();
        let cfg = small_config(); // min_frags = 3
        let consolidator = FragmentConsolidator::new(cfg);

        // 只注册 2 个 fragment（< min_frags）。
        register_fragment(&store, 1000, 2000, 5, &[r#"{"id":"n1"}"#]).await;
        register_fragment(&store, 2000, 3000, 5, &[r#"{"id":"n2"}"#]).await;

        let victims = consolidator.select_fragments(&store).await;
        assert!(
            victims.is_empty(),
            "低于 min_frags 阈值时不应选择任何 fragment"
        );
    }

    #[tokio::test]
    async fn select_picks_small_fragments() {
        let store = make_store();
        let consolidator = FragmentConsolidator::new(small_config());

        // 3 个小 fragment（node_count 远小于 min_size_nodes=1000）。
        register_fragment(&store, 3000, 4000, 5, &[r#"{"id":"n3"}"#]).await;
        register_fragment(&store, 1000, 2000, 5, &[r#"{"id":"n1"}"#]).await;
        register_fragment(&store, 2000, 3000, 5, &[r#"{"id":"n2"}"#]).await;

        let victims = consolidator.select_fragments(&store).await;
        assert_eq!(victims.len(), 3, "应选中全部 3 个小碎片");
        // 验证按时间升序排序。
        assert_eq!(victims[0].start_us, 1000);
        assert_eq!(victims[1].start_us, 2000);
        assert_eq!(victims[2].start_us, 3000);
    }

    #[tokio::test]
    async fn consolidate_once_merges_and_reduces_count() {
        let store = make_store();
        let consolidator = FragmentConsolidator::new(small_config());

        register_fragment(&store, 1000, 2000, 5, &[r#"{"id":"n1","timestamp":1500}"#]).await;
        register_fragment(&store, 2000, 3000, 5, &[r#"{"id":"n2","timestamp":2500}"#]).await;
        register_fragment(&store, 3000, 4000, 5, &[r#"{"id":"n3","timestamp":3500}"#]).await;

        assert_eq!(store.count().await, 3);

        let new_id = consolidator
            .consolidate_once(&store)
            .await
            .unwrap()
            .expect("应产生一个合并后的 fragment");

        // 合并后 count 减少：3 个旧的删除，1 个新的加入 → 总数 1。
        assert_eq!(store.count().await, 1);

        // 合并范围应覆盖 [min start, max end]。
        assert_eq!(new_id.start_us, 1000);
        assert_eq!(new_id.end_us, 4000);

        // 合并 fragment 应包含所有节点数据。
        let merged = std::fs::read_to_string(store.fragment_dir(&new_id).join("nodes.jsonl"))
            .expect("合并 fragment 应有 nodes.jsonl");
        assert!(merged.contains("n1"));
        assert!(merged.contains("n2"));
        assert!(merged.contains("n3"));
    }

    #[tokio::test]
    async fn consolidate_once_noop_when_nothing_to_merge() {
        let store = make_store();
        let consolidator = FragmentConsolidator::new(small_config());

        // 空 store → 返回 None。
        assert!(consolidator
            .consolidate_once(&store)
            .await
            .unwrap()
            .is_none());

        // 不足阈值（2 个 < min_frags=3）→ 返回 None。
        register_fragment(&store, 1000, 2000, 5, &[r#"{"id":"n1"}"#]).await;
        register_fragment(&store, 2000, 3000, 5, &[r#"{"id":"n2"}"#]).await;
        assert!(consolidator
            .consolidate_once(&store)
            .await
            .unwrap()
            .is_none());
    }
}
