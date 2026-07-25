//! Event-time watermarks + tumbling windows for the ingestion pipeline.
//!
//! This is the ingestion-side event-time engine, deliberately kept in
//! `nexora-stream` (not `nexora-zenoh`) so ingestion builds don't pull the
//! cluster/transport tree. It works directly in [`nexora_id::EventTime`]
//! (microseconds) — the same unit ingest records already carry — so there is no
//! unit conversion on the hot path.
//!
//! # Model
//!
//! - [`WatermarkGenerator`] tracks the maximum event time seen and emits a
//!   watermark at `max_seen - max_out_of_orderness`. The watermark is the
//!   engine's promise that no event older than it will still arrive; it only
//!   moves forward.
//! - [`TumblingWindow`] assigns each event to a fixed-size, non-overlapping
//!   bucket by its event time and accumulates a running aggregate per bucket.
//!   A bucket is *ready* (safe to emit) once the watermark passes its end.
//! - Events arriving after the watermark has already passed their bucket end
//!   are *late*. Within `allowed_lateness` past a bucket's end they still update
//!   the (possibly re-emitted) bucket; beyond that they are dropped and counted.
//!
//! Aggregation is intentionally simple (count/sum/min/max over a numeric field)
//! — the ingestion path needs a lightweight rollup, not a full query engine.

use nexora_id::EventTime;
use std::collections::BTreeMap;

/// Generates monotonic event-time watermarks from a stream of event times.
#[derive(Debug, Clone)]
pub struct WatermarkGenerator {
    /// Maximum event time observed so far (microseconds).
    max_seen: u64,
    /// Bounded out-of-orderness: how far behind `max_seen` the watermark trails,
    /// in microseconds. Larger → more tolerance for late events, higher latency.
    max_out_of_orderness_us: u64,
    /// Whether any event has been observed (before that, the watermark is MIN).
    seen_any: bool,
}

impl WatermarkGenerator {
    /// Create a generator tolerating up to `max_out_of_orderness_us` microseconds
    /// of out-of-order arrival.
    pub fn new(max_out_of_orderness_us: u64) -> Self {
        Self {
            max_seen: 0,
            max_out_of_orderness_us,
            seen_any: false,
        }
    }

    /// Observe an event time, advancing `max_seen`. Returns the current watermark.
    pub fn observe(&mut self, event_time: EventTime) -> EventTime {
        let t = event_time.as_micros();
        if !self.seen_any || t > self.max_seen {
            self.max_seen = t;
        }
        self.seen_any = true;
        self.current()
    }

    /// The current watermark: `max_seen - max_out_of_orderness` (saturating), or
    /// [`EventTime::MIN`] before any event has been observed.
    pub fn current(&self) -> EventTime {
        if !self.seen_any {
            return EventTime::MIN;
        }
        EventTime::from_micros(self.max_seen.saturating_sub(self.max_out_of_orderness_us))
    }
}

/// One tumbling window's running aggregate over a numeric value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowAggregate {
    /// Window start (inclusive), microseconds since epoch.
    pub window_start_us: u64,
    /// Window end (exclusive), microseconds since epoch.
    pub window_end_us: u64,
    /// Number of events assigned to the window.
    pub count: u64,
    /// Sum of the aggregated numeric value.
    pub sum: f64,
    /// Minimum observed value (meaningful when `count > 0`).
    pub min: f64,
    /// Maximum observed value (meaningful when `count > 0`).
    pub max: f64,
}

/// Outcome of feeding one event into a [`TumblingWindow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventOutcome {
    /// Event fell in an open (not-yet-emitted) window and was accumulated.
    Accumulated,
    /// Event was late but within `allowed_lateness`; it updated its window.
    LateAccumulated,
    /// Event was later than `allowed_lateness` past its window end; dropped.
    Dropped,
}

/// A stateful event-time tumbling-window aggregator.
///
/// Buckets are keyed by window start. Windows become emittable once the
/// watermark reaches their end; `drain_ready` returns and removes them.
#[derive(Debug, Clone)]
pub struct TumblingWindow {
    window_size_us: u64,
    allowed_lateness_us: u64,
    /// window_start → running aggregate.
    buckets: BTreeMap<u64, WindowAggregate>,
    /// The largest window end that has already been emitted, used to decide
    /// lateness. `None` until the first drain.
    last_emitted_end_us: Option<u64>,
    /// Count of events dropped for being too late (observability).
    dropped_late: u64,
}

impl TumblingWindow {
    /// Create a tumbling window of `window_size_us` microseconds, tolerating
    /// events up to `allowed_lateness_us` past a window's end before dropping.
    pub fn new(window_size_us: u64, allowed_lateness_us: u64) -> Self {
        assert!(window_size_us > 0, "window_size_us must be positive");
        Self {
            window_size_us,
            allowed_lateness_us,
            buckets: BTreeMap::new(),
            last_emitted_end_us: None,
            dropped_late: 0,
        }
    }

    /// The start of the window containing `event_time`.
    pub fn bucket_start(&self, event_time: EventTime) -> u64 {
        let t = event_time.as_micros();
        (t / self.window_size_us) * self.window_size_us
    }

    /// Feed one `(event_time, value)` into the window state.
    ///
    /// Returns whether it was accumulated, late-accumulated, or dropped. `watermark`
    /// is the current watermark, used to classify lateness: an event whose window
    /// already closed (end ≤ watermark) more than `allowed_lateness` ago is dropped.
    pub fn add(&mut self, event_time: EventTime, value: f64, watermark: EventTime) -> EventOutcome {
        let start = self.bucket_start(event_time);
        let end = start + self.window_size_us;
        let wm = watermark.as_micros();

        // Late classification: the window has closed (watermark passed its end).
        let outcome = if wm >= end {
            if wm.saturating_sub(end) > self.allowed_lateness_us {
                self.dropped_late += 1;
                return EventOutcome::Dropped;
            }
            EventOutcome::LateAccumulated
        } else {
            EventOutcome::Accumulated
        };

        let agg = self
            .buckets
            .entry(start)
            .or_insert_with(|| WindowAggregate {
                window_start_us: start,
                window_end_us: end,
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
            });
        agg.count += 1;
        agg.sum += value;
        if value < agg.min {
            agg.min = value;
        }
        if value > agg.max {
            agg.max = value;
        }
        outcome
    }

    /// Remove and return all windows whose end has been reached by `watermark`,
    /// in ascending start order. These are safe to emit — no more in-time events
    /// will arrive for them. Windows are retained for `allowed_lateness` past
    /// their end so late events can still update them before final removal.
    pub fn drain_ready(&mut self, watermark: EventTime) -> Vec<WindowAggregate> {
        let wm = watermark.as_micros();
        // A window is fully closed (evictable) once watermark passes end + lateness.
        let ready_starts: Vec<u64> = self
            .buckets
            .keys()
            .copied()
            .filter(|&start| {
                let end = start + self.window_size_us;
                wm >= end.saturating_add(self.allowed_lateness_us)
            })
            .collect();

        let mut out = Vec::with_capacity(ready_starts.len());
        for start in ready_starts {
            if let Some(agg) = self.buckets.remove(&start) {
                let end = agg.window_end_us;
                self.last_emitted_end_us =
                    Some(self.last_emitted_end_us.map_or(end, |cur| cur.max(end)));
                out.push(normalize(agg));
            }
        }
        out
    }

    /// Number of currently open (unemitted) windows.
    pub fn open_windows(&self) -> usize {
        self.buckets.len()
    }

    /// Total events dropped for exceeding `allowed_lateness`.
    pub fn dropped_late(&self) -> u64 {
        self.dropped_late
    }
}

/// Replace the sentinel min/max with 0.0 for empty aggregates so serialized
/// output never leaks `inf`.
fn normalize(mut agg: WindowAggregate) -> WindowAggregate {
    if agg.count == 0 {
        agg.min = 0.0;
        agg.max = 0.0;
    }
    agg
}

#[cfg(test)]
mod tests {
    use super::*;

    fn et(us: u64) -> EventTime {
        EventTime::from_micros(us)
    }

    #[test]
    fn watermark_advances_monotonically_and_trails_by_ooo() {
        let mut g = WatermarkGenerator::new(100);
        assert_eq!(g.current(), EventTime::MIN, "no events → MIN");

        assert_eq!(g.observe(et(1_000)).as_micros(), 900);
        // Out-of-order lower event must not move the watermark backward.
        assert_eq!(g.observe(et(500)).as_micros(), 900);
        assert_eq!(g.observe(et(2_000)).as_micros(), 1_900);
    }

    #[test]
    fn tumbling_accumulates_and_drains_when_watermark_passes() {
        // 1000µs windows, no lateness.
        let mut w = TumblingWindow::new(1_000, 0);
        // Two events in window [0,1000): values 10, 20.
        assert_eq!(w.add(et(100), 10.0, et(0)), EventOutcome::Accumulated);
        assert_eq!(w.add(et(900), 20.0, et(0)), EventOutcome::Accumulated);
        // One event in window [1000,2000): value 5.
        assert_eq!(w.add(et(1_500), 5.0, et(0)), EventOutcome::Accumulated);

        // Watermark at 999 → nothing ready.
        assert!(w.drain_ready(et(999)).is_empty());

        // Watermark at 1000 → first window closed and evictable (lateness 0).
        let ready = w.drain_ready(et(1_000));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].window_start_us, 0);
        assert_eq!(ready[0].count, 2);
        assert_eq!(ready[0].sum, 30.0);
        assert_eq!(ready[0].min, 10.0);
        assert_eq!(ready[0].max, 20.0);
        // Second window still open.
        assert_eq!(w.open_windows(), 1);
    }

    #[test]
    fn late_event_within_allowed_lateness_updates_window() {
        // 1000µs windows, 500µs lateness.
        let mut w = TumblingWindow::new(1_000, 500);
        w.add(et(100), 10.0, et(0));
        // Watermark now at 1200: window [0,1000) closed 200µs ago, within the
        // 500µs lateness → a late event still counts.
        let outcome = w.add(et(200), 5.0, et(1_200));
        assert_eq!(outcome, EventOutcome::LateAccumulated);

        // Not yet evictable: needs watermark >= end + lateness = 1500.
        assert!(w.drain_ready(et(1_400)).is_empty());
        let ready = w.drain_ready(et(1_500));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].count, 2, "late event was included");
        assert_eq!(ready[0].sum, 15.0);
        assert_eq!(w.dropped_late(), 0);
    }

    #[test]
    fn late_event_beyond_allowed_lateness_is_dropped() {
        let mut w = TumblingWindow::new(1_000, 500);
        w.add(et(100), 10.0, et(0));
        // Watermark at 2000: window [0,1000) closed 1000µs ago > 500µs lateness.
        let outcome = w.add(et(200), 99.0, et(2_000));
        assert_eq!(outcome, EventOutcome::Dropped);
        assert_eq!(w.dropped_late(), 1);

        // The dropped event must not have affected any aggregate.
        let ready = w.drain_ready(et(2_000));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].count, 1);
        assert_eq!(ready[0].sum, 10.0);
    }

    #[test]
    fn bucket_start_aligns_to_window_boundary() {
        let w = TumblingWindow::new(1_000, 0);
        assert_eq!(w.bucket_start(et(0)), 0);
        assert_eq!(w.bucket_start(et(999)), 0);
        assert_eq!(w.bucket_start(et(1_000)), 1_000);
        assert_eq!(w.bucket_start(et(2_500)), 2_000);
    }
}
