//! Fragment identifier with time range and UUID.
//!
//! Format: `<start_timestamp_us>_<end_timestamp_us>_<uuid>`
//! Example: `1700000000000000_1700003600000000_a1b2c3d4-e5f6-7890-abcd-ef1234567890`

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Identifies a specific time-sharded fragment.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FragmentId {
    /// Start timestamp (microseconds since epoch)
    pub start_us: u64,
    /// End timestamp (microseconds since epoch)
    pub end_us: u64,
    /// Unique identifier
    pub uuid: Uuid,
}

impl FragmentId {
    /// Create a new fragment ID for a time window.
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            start_us: start.timestamp_micros() as u64,
            end_us: end.timestamp_micros() as u64,
            uuid: Uuid::new_v4(),
        }
    }

    /// Check if this fragment overlaps with a time range.
    pub fn overlaps(&self, query_start: u64, query_end: u64) -> bool {
        self.start_us < query_end && self.end_us > query_start
    }

    /// Check if this fragment is wholly before a timestamp.
    pub fn is_before(&self, timestamp: u64) -> bool {
        self.end_us <= timestamp
    }

    /// Check if this fragment is wholly after a timestamp.
    pub fn is_after(&self, timestamp: u64) -> bool {
        self.start_us >= timestamp
    }

    /// Parse from the standard naming format.
    pub fn from_name(name: &str) -> Option<Self> {
        let parts: Vec<&str> = name.split('_').collect();
        if parts.len() != 3 {
            return None;
        }
        let start = parts[0].parse::<u64>().ok()?;
        let end = parts[1].parse::<u64>().ok()?;
        let uuid = Uuid::parse_str(parts[2]).ok()?;
        Some(Self {
            start_us: start,
            end_us: end,
            uuid,
        })
    }
}

impl fmt::Display for FragmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}_{}", self.start_us, self.end_us, self.uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fragment_id_roundtrip() {
        let id = FragmentId::new(
            DateTime::from_timestamp_micros(1700000000000000).unwrap(),
            DateTime::from_timestamp_micros(1700003600000000).unwrap(),
        );
        let s = id.to_string();
        let parsed = FragmentId::from_name(&s).unwrap();
        assert_eq!(id.start_us, parsed.start_us);
        assert_eq!(id.end_us, parsed.end_us);
        assert_eq!(id.uuid, parsed.uuid);
    }

    #[test]
    fn test_overlap_logic() {
        let id = FragmentId {
            start_us: 1000,
            end_us: 2000,
            uuid: Uuid::nil(),
        };
        assert!(id.overlaps(500, 1500)); // partial overlap
        assert!(id.overlaps(1500, 2500)); // partial overlap
        assert!(!id.overlaps(0, 500)); // before
        assert!(!id.overlaps(2500, 3000)); // after
        assert!(id.overlaps(1000, 2000)); // exact
    }
}
