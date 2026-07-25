//! Causal ordering watermark tracker for Standing Query results.
//!
//! Tracks the progress of event processing across shards so SQ consumers
//! know when they've seen all events up to a given point in time.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// An event position: (shard_id, sequence_number).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventPosition {
    pub shard: usize,
    pub sequence: u64,
}

/// Tracks the low-water mark across all shards.
pub struct WatermarkTracker {
    /// Per-shard: highest sequence seen so far.
    shard_watermarks: HashMap<usize, u64>,
    /// Per-SQ: last watermark time.
    last_watermark: HashMap<String, Instant>,
    /// Creation time for uptime tracking.
    created_at: Instant,
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self {
            shard_watermarks: HashMap::new(),
            last_watermark: HashMap::new(),
            created_at: Instant::now(),
        }
    }

    /// Advance the watermark for a specific shard.
    pub fn advance(&mut self, shard: usize, sequence: u64) {
        let current = self.shard_watermarks.entry(shard).or_insert(0);
        if sequence > *current {
            *current = sequence;
        }
    }

    /// Get the current watermark across all known shards.
    /// Returns a map of shard_id → highest seen sequence.
    pub fn current_watermark(&self) -> HashMap<usize, u64> {
        self.shard_watermarks.clone()
    }

    /// Get the global low watermark (minimum across all shards).
    /// This is the point up to which all shards have been processed.
    /// Returns None if no shards are tracked.
    pub fn low_watermark(&self) -> Option<u64> {
        if self.shard_watermarks.is_empty() {
            return None;
        }
        self.shard_watermarks.values().min().copied()
    }

    /// Record that a SQ result was emitted.
    pub fn record_sq_emit(&mut self, sq_id: &str) {
        self.last_watermark
            .insert(sq_id.to_string(), Instant::now());
    }

    /// Check if a specific SQ is behind the global watermark.
    /// Returns the lag duration if known.
    pub fn lag(&self, sq_id: &str) -> Option<Duration> {
        self.last_watermark.get(sq_id).map(|last| last.elapsed())
    }

    /// Get the maximum lag across all SQs.
    pub fn max_lag(&self) -> Option<Duration> {
        self.last_watermark.values().map(|t| t.elapsed()).max()
    }

    /// Total number of tracked shards.
    pub fn tracked_shards(&self) -> usize {
        self.shard_watermarks.len()
    }

    /// Total uptime of the watermark tracker.
    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }
}

impl Default for WatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_shard_advance() {
        let mut tracker = WatermarkTracker::new();
        tracker.advance(0, 100);
        assert_eq!(tracker.low_watermark(), Some(100));
    }

    #[test]
    fn test_multiple_shards_low_watermark() {
        let mut tracker = WatermarkTracker::new();
        tracker.advance(0, 100);
        tracker.advance(1, 200);
        tracker.advance(2, 50);
        assert_eq!(tracker.low_watermark(), Some(50));
    }

    #[test]
    fn test_monotonic() {
        let mut tracker = WatermarkTracker::new();
        tracker.advance(0, 100);
        tracker.advance(0, 50); // should be ignored (not monotonic)
        assert_eq!(tracker.current_watermark()[&0], 100);
    }

    #[test]
    fn test_sq_lag() {
        let mut tracker = WatermarkTracker::new();
        tracker.record_sq_emit("sq-1");
        let lag = tracker.lag("sq-1");
        assert!(lag.is_some());
        assert!(lag.unwrap().as_millis() < 1000); // must be recent
    }

    #[test]
    fn test_max_lag() {
        let mut tracker = WatermarkTracker::new();
        tracker.record_sq_emit("sq-1");
        tracker.record_sq_emit("sq-2");
        let max_lag = tracker.max_lag();
        assert!(max_lag.is_some());
    }

    #[test]
    fn test_empty_tracker() {
        let tracker = WatermarkTracker::new();
        assert_eq!(tracker.low_watermark(), None);
        assert_eq!(tracker.tracked_shards(), 0);
    }

    #[test]
    fn test_tracked_shards_count() {
        let mut tracker = WatermarkTracker::new();
        tracker.advance(0, 1);
        tracker.advance(5, 10);
        tracker.advance(0, 2);
        assert_eq!(tracker.tracked_shards(), 2);
    }

    #[test]
    fn test_event_position() {
        let pos1 = EventPosition {
            shard: 0,
            sequence: 5,
        };
        let pos2 = EventPosition {
            shard: 0,
            sequence: 5,
        };
        let pos3 = EventPosition {
            shard: 1,
            sequence: 5,
        };
        assert_eq!(pos1, pos2);
        assert_ne!(pos1, pos3);
    }
}
