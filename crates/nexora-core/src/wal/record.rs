use crate::event::{DomainIndexEvent, NodeChangeEvent, TimedEvent};
use nexora_id::{EventTime, NexoraId};
use serde::{Deserialize, Serialize};

/// A single WAL record — the unit of durability.
///
/// Each record represents an operation that must survive a crash.
/// Records are appended sequentially to the WAL file and never modified.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalRecord {
    /// Monotonically increasing sequence number.
    pub seq_no: u64,
    /// The operation to be applied.
    pub operation: WalOperation,
    /// A4: Version number for idempotent replay and torn-write detection.
    /// Defaults to 0 for legacy records. New writes start from 1.
    /// Same semantics as seq_no, but dedicated to replay verification.
    #[serde(default)]
    pub version: u64,
}

/// The type of operation recorded in the WAL.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalOperation {
    /// A node change event (property set/remove, edge add/remove).
    NodeEvent {
        qid: NexoraId,
        event: TimedEvent<NodeChangeEvent>,
    },
    /// An atomic batch of node change events.
    NodeEvents {
        qid: NexoraId,
        events: Vec<TimedEvent<NodeChangeEvent>>,
    },
    /// A domain index event (standing query subscription).
    DomainIndexEvent {
        qid: NexoraId,
        event: TimedEvent<DomainIndexEvent>,
    },
    /// A snapshot checkpoint — marks that a snapshot has been persisted.
    /// Events before this snapshot time can be safely truncated from the WAL.
    SnapshotCheckpoint {
        qid: NexoraId,
        snapshot_time: EventTime,
    },
    /// An ingest offset commit (for exactly-once semantics).
    IngestOffsetCommit {
        ingest_id: String,
        partition: i32,
        offset: i64,
    },
    /// A raw event captured from an upstream source, durably logged before it is
    /// sealed into a columnar event-table fragment (event-first ingestion). This
    /// is the pre-seal durability record: on recovery, un-sealed raw events are
    /// replayed from here; after a seal checkpoint they can be truncated.
    RawEvent {
        event: crate::raw_event::RawEvent,
    },
}

impl WalOperation {
    /// Whether this operation is for a specific NexoraId.
    pub fn nexora_id(&self) -> Option<&NexoraId> {
        match self {
            Self::NodeEvent { qid, .. }
            | Self::NodeEvents { qid, .. }
            | Self::DomainIndexEvent { qid, .. }
            | Self::SnapshotCheckpoint { qid, .. } => Some(qid),
            Self::IngestOffsetCommit { .. } | Self::RawEvent { .. } => None,
        }
    }
}
