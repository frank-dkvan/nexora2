use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Microsecond-precision timestamp used as the primary time key throughout Nexora.
///
/// This is a monotonically increasing unsigned 64-bit integer representing
/// microseconds since the Unix epoch. It serves as:
/// - The ordering key for event journal entries
/// - The identifier for snapshots
/// - The block ID in the persistence layer
/// - The timestamp in Standing Query results
///
/// # Wire Format
///
/// In FlatBuffers and Cassandra, EventTime is stored as a raw `u64`.
/// In Cassandra, it is mapped to a signed `BIGINT` via offset:
/// `raw_value = event_time + Long::MAX + 1` (to preserve sort order).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventTime(pub u64);

impl EventTime {
    /// The maximum possible event time (used as sentinel for singleton snapshots).
    pub const MAX: Self = Self(u64::MAX);

    /// The minimum possible event time.
    pub const MIN: Self = Self(0);

    /// Create an EventTime from a microsecond timestamp.
    pub fn from_micros(micros: u64) -> Self {
        Self(micros)
    }

    /// Create an EventTime from a `chrono::DateTime<Utc>`.
    pub fn from_datetime(dt: &chrono::DateTime<chrono::Utc>) -> Self {
        Self(dt.timestamp_micros().max(0) as u64)
    }

    /// Create an EventTime representing the current wall-clock time.
    pub fn now() -> Self {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Self(dur.as_micros() as u64)
    }

    /// The raw microsecond value.
    pub fn as_micros(&self) -> u64 {
        self.0
    }

    /// Convert to `chrono::DateTime<Utc>`.
    pub fn to_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_micros(self.0 as i64)
            .unwrap_or(chrono::DateTime::UNIX_EPOCH)
    }

    /// Convert to `SystemTime`.
    pub fn to_system_time(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_micros(self.0)
    }

    /// Return the Cassandra-compatible signed representation.
    ///
    /// Cassandra uses signed BIGINT. To preserve unsigned sort order, the mapping is:
    /// `signed = raw + i64::MIN` (i.e., `raw as i64 ^ i64::MIN`)
    pub fn to_cassandra_raw(&self) -> i64 {
        self.0 as i64 ^ i64::MIN
    }

    /// Decode from Cassandra signed representation back to EventTime.
    pub fn from_cassandra_raw(raw: i64) -> Self {
        Self((raw ^ i64::MIN) as u64)
    }
}

impl fmt::Debug for EventTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EventTime({} μs / {})", self.0, self.to_datetime())
    }
}

impl fmt::Display for EventTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_datetime())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_is_recent() {
        let now = EventTime::now();
        assert!(now.as_micros() > 0);
        let dt = now.to_datetime();
        let now_chrono = chrono::Utc::now();
        assert!((dt - now_chrono).num_seconds().unsigned_abs() < 2);
    }

    #[test]
    fn test_ordering() {
        let t1 = EventTime::from_micros(100);
        let t2 = EventTime::from_micros(200);
        assert!(t1 < t2);
    }

    #[test]
    fn test_cassandra_roundtrip() {
        let times = [0u64, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX];
        for t in times {
            let et = EventTime::from_micros(t);
            let cass = et.to_cassandra_raw();
            let restored = EventTime::from_cassandra_raw(cass);
            assert_eq!(et, restored, "roundtrip failed for {t}");
        }
    }

    #[test]
    fn test_cassandra_sort_order() {
        let t1 = EventTime::from_micros(100).to_cassandra_raw();
        let t2 = EventTime::from_micros(200).to_cassandra_raw();
        assert!(t1 < t2, "Cassandra sort order must match unsigned order");
    }

    #[test]
    fn test_from_datetime_roundtrip() {
        let now = EventTime::now();
        let dt = now.to_datetime();
        let restored = EventTime::from_datetime(&dt);
        assert_eq!(now, restored);
    }
}
