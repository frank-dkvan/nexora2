// P1-2: 水印传播机制
//
// 为流式窗口语义提供基础设施：
// - 跟踪每个分片的事件时间水印
// - 定期广播水印到下游算子
// - 支持多分片水印对齐
// - 触发基于时间的窗口操作
//
// # 与 StandingQuery 集成（F4 后续工作）
//
// 完整的事件时间窗口 SQ 流程：
// 1. 摄入时从 event.timestamp 更新 shard watermark
//    （调用 WatermarkGenerator::advance(shard_id, event_time_ms)）
// 2. WatermarkGenerator 广播 global watermark 给下游算子
// 3. SQ 注册时携带时间窗口定义：
//    - bucket 函数：time_window_bucket(e.ts, size_ms) 对齐到窗口起始
//    - 触发条件：global_watermark() >= window_end_ms
// 4. 当 is_window_ready(window_end_ms) 返回 true 时，触发窗口 SQ 计算
// 5. 多分片场景使用 WatermarkPropagator 对齐所有分片后再触发
//
// 当前已提供原语：WatermarkGenerator（单流）、WatermarkPropagator（多分片对齐）、
// TumblingWindow（批量聚合）。SQ 触发调度器在 nexora-standing-query 中实现。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// 事件时间戳（毫秒）。
///
/// 注意：这是水印/窗口子系统的内部时间单位（**毫秒**，i64），刻意与
/// [`nexora_id::EventTime`]（**微秒**，u64）区分——窗口大小、`max_out_of_orderness`
/// 等用毫秒表达更自然。跨越两个子系统时必须用 [`from_core_event_time`] /
/// [`to_core_event_time`] 显式换算，不要直接混用数值。
pub type EventTime = i64;

/// 把内核的 [`nexora_id::EventTime`]（微秒）换算成水印子系统的毫秒时间戳。
///
/// 摄入路径拿到的是内核微秒时间（`IngestRecord.timestamp` → `EventTime`），
/// 推进水印前必须经此换算。
pub fn from_core_event_time(t: nexora_id::EventTime) -> EventTime {
    (t.as_micros() / 1_000) as EventTime
}

/// 把水印子系统的毫秒时间戳换算回内核的 [`nexora_id::EventTime`]（微秒）。
///
/// 负值（`Watermark::min()` 之类的哨兵）钳到 0，避免下溢。
pub fn to_core_event_time(ms: EventTime) -> nexora_id::EventTime {
    let micros = (ms.max(0) as u64).saturating_mul(1_000);
    nexora_id::EventTime::from_micros(micros)
}

/// 水印：表示不会再有小于此时间戳的事件到达
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Watermark {
    pub timestamp: EventTime,
}

impl Watermark {
    pub fn new(timestamp: EventTime) -> Self {
        Self { timestamp }
    }

    /// 最小水印（表示流刚开始）
    pub fn min() -> Self {
        Self {
            timestamp: EventTime::MIN,
        }
    }

    /// 最大水印（表示流结束）
    pub fn max() -> Self {
        Self {
            timestamp: EventTime::MAX,
        }
    }
}

/// 每个分片的水印状态
#[derive(Debug, Clone)]
struct ShardWatermark {
    /// 当前水印
    watermark: Watermark,
    /// 上次更新时间
    last_update: Instant,
}

impl ShardWatermark {
    fn new(watermark: Watermark) -> Self {
        Self {
            watermark,
            last_update: Instant::now(),
        }
    }

    fn update(&mut self, watermark: Watermark) {
        if watermark > self.watermark {
            self.watermark = watermark;
            self.last_update = Instant::now();
        }
    }
}

/// 水印传播器：跟踪多个分片的水印并计算全局水印
pub struct WatermarkPropagator {
    /// 分片ID -> 水印状态
    shard_watermarks: Arc<RwLock<HashMap<u32, ShardWatermark>>>,
    /// 滞后检测阈值
    straggler_threshold: Duration,
}

impl WatermarkPropagator {
    pub fn new(straggler_threshold: Duration) -> Self {
        Self {
            shard_watermarks: Arc::new(RwLock::new(HashMap::new())),
            straggler_threshold,
        }
    }

    /// 更新分片水印
    pub async fn update_shard_watermark(&self, shard_id: u32, watermark: Watermark) {
        let mut watermarks = self.shard_watermarks.write().await;
        watermarks
            .entry(shard_id)
            .and_modify(|sw| sw.update(watermark))
            .or_insert_with(|| ShardWatermark::new(watermark));
        debug!(
            shard_id,
            timestamp = watermark.timestamp,
            "Updated shard watermark"
        );
    }

    /// 计算全局水印（所有分片的最小水印）
    ///
    /// 这确保了只有当所有分片都推进到某个时间点时，
    /// 全局水印才会推进，从而保证窗口语义的正确性。
    pub async fn global_watermark(&self) -> Option<Watermark> {
        let watermarks = self.shard_watermarks.read().await;
        if watermarks.is_empty() {
            return None;
        }

        let now = Instant::now();
        let mut min_watermark = Watermark::max();
        let mut straggler_count = 0;

        for (shard_id, shard_wm) in watermarks.iter() {
            // 检测滞后分片
            if now.duration_since(shard_wm.last_update) > self.straggler_threshold {
                warn!(
                    shard_id,
                    elapsed_sec = now.duration_since(shard_wm.last_update).as_secs(),
                    "Straggler shard detected"
                );
                straggler_count += 1;
            }

            if shard_wm.watermark < min_watermark {
                min_watermark = shard_wm.watermark;
            }
        }

        if straggler_count > 0 {
            debug!(
                straggler_count,
                "Stragglers detected in watermark calculation"
            );
        }

        Some(min_watermark)
    }

    /// 注册新分片
    pub async fn register_shard(&self, shard_id: u32) {
        let mut watermarks = self.shard_watermarks.write().await;
        watermarks
            .entry(shard_id)
            .or_insert_with(|| ShardWatermark::new(Watermark::min()));
        debug!(shard_id, "Registered shard for watermark tracking");
    }

    /// 移除分片
    pub async fn unregister_shard(&self, shard_id: u32) {
        let mut watermarks = self.shard_watermarks.write().await;
        watermarks.remove(&shard_id);
        debug!(shard_id, "Unregistered shard from watermark tracking");
    }

    /// 获取分片计数
    pub async fn shard_count(&self) -> usize {
        let watermarks = self.shard_watermarks.read().await;
        watermarks.len()
    }
}

/// 水印生成器：从事件流中提取时间戳并生成水印
pub struct WatermarkGenerator {
    /// 最后观察到的事件时间
    last_event_time: EventTime,
    /// 允许的最大乱序时间（毫秒）
    max_out_of_orderness: i64,
}

impl WatermarkGenerator {
    pub fn new(max_out_of_orderness: i64) -> Self {
        Self {
            last_event_time: EventTime::MIN,
            max_out_of_orderness,
        }
    }

    /// 处理新事件并生成水印
    ///
    /// 水印 = 最大观察到的事件时间 - 允许乱序时间
    /// 这允许晚到的事件仍然被正确处理。
    pub fn on_event(&mut self, event_time: EventTime) -> Watermark {
        if event_time > self.last_event_time {
            self.last_event_time = event_time;
        }
        Watermark::new(self.last_event_time - self.max_out_of_orderness)
    }

    /// 生成当前水印（不处理新事件）
    pub fn current_watermark(&self) -> Watermark {
        Watermark::new(self.last_event_time - self.max_out_of_orderness)
    }

    /// 推进水印：处理来自任意分片的事件时间戳。
    ///
    /// `shard_id` 在单流生成器中仅用于日志追踪；时间戳单调推进全局
    /// 最大值，与分片来源无关。多分片对齐请使用 [`WatermarkPropagator`]。
    pub fn advance(&mut self, _shard_id: usize, event_time_ms: i64) {
        self.on_event(event_time_ms);
    }

    /// 返回当前全局水印时间戳（毫秒）。
    ///
    /// 等价于 `current_watermark().timestamp`。
    pub fn global_watermark(&self) -> i64 {
        self.current_watermark().timestamp
    }

    /// 判断 `window_end_ms` 对应的窗口数据是否已全部到达。
    ///
    /// 当全局水印 >= 窗口结束时间时，不会再有属于该窗口的事件迟到，
    /// 此时触发窗口聚合是安全的。
    pub fn is_window_ready(&self, window_end_ms: i64) -> bool {
        self.global_watermark() >= window_end_ms
    }
}

// ============================================================
// 滚动窗口聚合原语
// ============================================================

/// 按固定步长对事件流分桶的滚动窗口。
///
/// 不修改 Cypher 解析器；可直接从 HTTP API 调用，作为事件时间窗口聚合的
/// 轻量原语（例如 `POST /api/v2/analytics/tumbling-window`）。
#[derive(Debug, Clone)]
pub struct TumblingWindow {
    /// 窗口大小（毫秒）
    pub window_size_ms: i64,
}

/// 单个滚动窗口的聚合结果。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct WindowResult {
    /// 窗口起始时间戳（毫秒，inclusive）
    pub window_start_ms: i64,
    /// 窗口结束时间戳（毫秒，exclusive）
    pub window_end_ms: i64,
    /// 窗口内事件数量
    pub count: u64,
    /// 数值之和
    pub sum: f64,
    /// 最小值
    pub min: f64,
    /// 最大值
    pub max: f64,
}

impl TumblingWindow {
    pub fn new(window_size_ms: i64) -> Self {
        assert!(window_size_ms > 0, "window_size_ms must be positive");
        Self { window_size_ms }
    }

    /// 返回事件所属窗口的起始时间（向下对齐到窗口边界）。
    pub fn assign_bucket(&self, event_time_ms: i64) -> i64 {
        // 对负时间戳也正确处理（floor division）
        let d = self.window_size_ms;
        (event_time_ms.div_euclid(d)) * d
    }

    /// 对一批 `(timestamp_ms, value)` 事件按窗口聚合，返回有序结果列表。
    ///
    /// 结果按 `window_start_ms` 升序排列。
    pub fn aggregate(&self, events: impl Iterator<Item = (i64, f64)>) -> Vec<WindowResult> {
        use std::collections::BTreeMap;

        // (window_start) -> (count, sum, min, max)
        let mut buckets: BTreeMap<i64, (u64, f64, f64, f64)> = BTreeMap::new();

        for (ts, val) in events {
            let start = self.assign_bucket(ts);
            let entry = buckets.entry(start).or_insert((0, 0.0, f64::MAX, f64::MIN));
            entry.0 += 1;
            entry.1 += val;
            if val < entry.2 {
                entry.2 = val;
            }
            if val > entry.3 {
                entry.3 = val;
            }
        }

        buckets
            .into_iter()
            .map(|(start, (count, sum, min, max))| WindowResult {
                window_start_ms: start,
                window_end_ms: start + self.window_size_ms,
                count,
                sum,
                min: if min == f64::MAX { 0.0 } else { min },
                max: if max == f64::MIN { 0.0 } else { max },
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn event_time_unit_conversion_roundtrips_ms() {
        // 微秒 → 毫秒（截断），再换回来对齐到毫秒边界。
        let core = nexora_id::EventTime::from_micros(1_700_000_000_123_000);
        let ms = from_core_event_time(core);
        assert_eq!(ms, 1_700_000_000_123);
        let back = to_core_event_time(ms);
        assert_eq!(back.as_micros(), 1_700_000_000_123_000);
    }

    #[test]
    fn to_core_event_time_clamps_negative_sentinels() {
        // Watermark::min() 的负时间戳不能下溢成巨大的 u64。
        assert_eq!(to_core_event_time(EventTime::MIN).as_micros(), 0);
        assert_eq!(to_core_event_time(-5).as_micros(), 0);
    }

    #[tokio::test]
    async fn test_single_shard_watermark() {
        let propagator = WatermarkPropagator::new(Duration::from_secs(10));
        propagator.register_shard(0).await;

        propagator
            .update_shard_watermark(0, Watermark::new(100))
            .await;
        let global = propagator.global_watermark().await.unwrap();
        assert_eq!(global.timestamp, 100);

        propagator
            .update_shard_watermark(0, Watermark::new(200))
            .await;
        let global = propagator.global_watermark().await.unwrap();
        assert_eq!(global.timestamp, 200);
    }

    #[tokio::test]
    async fn test_multi_shard_watermark_alignment() {
        let propagator = WatermarkPropagator::new(Duration::from_secs(10));
        propagator.register_shard(0).await;
        propagator.register_shard(1).await;
        propagator.register_shard(2).await;

        // 三个分片的水印：100, 200, 300
        propagator
            .update_shard_watermark(0, Watermark::new(100))
            .await;
        propagator
            .update_shard_watermark(1, Watermark::new(200))
            .await;
        propagator
            .update_shard_watermark(2, Watermark::new(300))
            .await;

        // 全局水印应该是最小值 100
        let global = propagator.global_watermark().await.unwrap();
        assert_eq!(global.timestamp, 100);

        // 推进最慢的分片
        propagator
            .update_shard_watermark(0, Watermark::new(250))
            .await;

        // 现在全局水印应该推进到 200
        let global = propagator.global_watermark().await.unwrap();
        assert_eq!(global.timestamp, 200);
    }

    #[tokio::test]
    async fn test_watermark_monotonicity() {
        let propagator = WatermarkPropagator::new(Duration::from_secs(10));
        propagator.register_shard(0).await;

        propagator
            .update_shard_watermark(0, Watermark::new(100))
            .await;
        let wm1 = propagator.global_watermark().await.unwrap();

        // 尝试回退水印（应该被忽略）
        propagator
            .update_shard_watermark(0, Watermark::new(50))
            .await;
        let wm2 = propagator.global_watermark().await.unwrap();

        assert_eq!(wm1, wm2);
        assert_eq!(wm2.timestamp, 100);
    }

    #[tokio::test]
    async fn test_straggler_detection() {
        let propagator = WatermarkPropagator::new(Duration::from_millis(50));
        propagator.register_shard(0).await;
        propagator.register_shard(1).await;

        propagator
            .update_shard_watermark(0, Watermark::new(100))
            .await;
        propagator
            .update_shard_watermark(1, Watermark::new(100))
            .await;

        // 等待足够长时间使分片1成为滞后者
        sleep(Duration::from_millis(100)).await;

        // 只更新分片0
        propagator
            .update_shard_watermark(0, Watermark::new(200))
            .await;

        // 应该检测到滞后分片（分片1）
        let global = propagator.global_watermark().await.unwrap();
        assert_eq!(global.timestamp, 100); // 仍然被滞后分片阻塞
    }

    #[tokio::test]
    async fn test_shard_registration() {
        let propagator = WatermarkPropagator::new(Duration::from_secs(10));

        assert_eq!(propagator.shard_count().await, 0);

        propagator.register_shard(0).await;
        assert_eq!(propagator.shard_count().await, 1);

        propagator.register_shard(1).await;
        assert_eq!(propagator.shard_count().await, 2);

        propagator.unregister_shard(0).await;
        assert_eq!(propagator.shard_count().await, 1);
    }

    #[test]
    fn test_watermark_generator() {
        let mut gen = WatermarkGenerator::new(1000); // 1秒乱序

        // 处理按序事件
        let wm1 = gen.on_event(5000);
        assert_eq!(wm1.timestamp, 4000); // 5000 - 1000

        let wm2 = gen.on_event(6000);
        assert_eq!(wm2.timestamp, 5000); // 6000 - 1000

        // 处理乱序事件（晚到）
        let wm3 = gen.on_event(5500);
        assert_eq!(wm3.timestamp, 5000); // 仍然是 6000 - 1000，不回退
    }

    #[test]
    fn test_watermark_generator_out_of_order() {
        let mut gen = WatermarkGenerator::new(2000); // 2秒乱序

        gen.on_event(10000);
        gen.on_event(9000); // 乱序事件
        gen.on_event(11000);

        let wm = gen.current_watermark();
        assert_eq!(wm.timestamp, 9000); // 11000 - 2000
    }

    // ---------------------------------------------------------------
    // F4.1 新增：WatermarkGenerator::advance / global_watermark / is_window_ready
    // ---------------------------------------------------------------

    #[test]
    fn test_watermark_generator_advance_and_global() {
        let mut gen = WatermarkGenerator::new(500); // 500 ms 乱序容忍

        // 推进 shard 0 到 t=3000
        gen.advance(0, 3000);
        assert_eq!(gen.global_watermark(), 2500, "global = 3000 - 500");

        // 推进 shard 1 到 t=4000（单生成器合并所有分片）
        gen.advance(1, 4000);
        assert_eq!(gen.global_watermark(), 3500, "global = 4000 - 500");

        // 晚到事件不回退
        gen.advance(0, 2000);
        assert_eq!(gen.global_watermark(), 3500, "monotonic: 不回退");
    }

    #[test]
    fn test_watermark_generator_is_window_ready() {
        let mut gen = WatermarkGenerator::new(1000);

        gen.advance(0, 60_000);
        // global = 59_000；窗口 [0, 60_000) 尚未就绪（watermark 恰在边界）
        assert!(
            !gen.is_window_ready(60_000),
            "window not ready: wm < window_end"
        );

        gen.advance(0, 60_001);
        // global = 59_001；60_000 窗口仍未就绪
        assert!(!gen.is_window_ready(60_000));

        gen.advance(0, 61_000);
        // global = 60_000；窗口 [0, 60_000) 就绪（wm == window_end）
        assert!(
            gen.is_window_ready(60_000),
            "window ready: wm == window_end"
        );

        gen.advance(0, 65_000);
        // global = 64_000；[0, 60_000) 当然就绪
        assert!(gen.is_window_ready(60_000));
    }

    // ---------------------------------------------------------------
    // F4.2 新增：TumblingWindow
    // ---------------------------------------------------------------

    #[test]
    fn test_tumbling_window_assign_buckets() {
        let tw = TumblingWindow::new(60_000); // 1分钟窗口

        assert_eq!(tw.assign_bucket(0), 0);
        assert_eq!(tw.assign_bucket(59_999), 0);
        assert_eq!(tw.assign_bucket(60_000), 60_000);
        assert_eq!(tw.assign_bucket(119_999), 60_000);
        assert_eq!(tw.assign_bucket(120_000), 120_000);

        // 负时间戳（历史数据）
        assert_eq!(tw.assign_bucket(-1), -60_000);
    }

    #[test]
    fn test_tumbling_window_aggregate() {
        let tw = TumblingWindow::new(60_000); // 1分钟窗口

        // 三个窗口：[0,60s)、[60s,120s)、[120s,180s)
        let events = vec![
            (10_000, 1.0_f64),
            (20_000, 3.0),
            (50_000, 2.0),
            (70_000, 10.0),
            (90_000, 5.0),
            (130_000, 7.0),
        ];

        let results = tw.aggregate(events.into_iter());
        assert_eq!(results.len(), 3);

        let w0 = &results[0];
        assert_eq!(w0.window_start_ms, 0);
        assert_eq!(w0.window_end_ms, 60_000);
        assert_eq!(w0.count, 3);
        assert!((w0.sum - 6.0).abs() < 1e-9);
        assert!((w0.min - 1.0).abs() < 1e-9);
        assert!((w0.max - 3.0).abs() < 1e-9);

        let w1 = &results[1];
        assert_eq!(w1.window_start_ms, 60_000);
        assert_eq!(w1.count, 2);
        assert!((w1.sum - 15.0).abs() < 1e-9);

        let w2 = &results[2];
        assert_eq!(w2.window_start_ms, 120_000);
        assert_eq!(w2.count, 1);
        assert!((w2.sum - 7.0).abs() < 1e-9);
    }

    #[test]
    fn test_tumbling_window_aggregate_empty() {
        let tw = TumblingWindow::new(60_000);
        let results = tw.aggregate(std::iter::empty());
        assert!(results.is_empty());
    }
}
