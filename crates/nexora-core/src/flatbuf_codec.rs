//! FlatBuffers codec — converts between Rust domain types and FlatBuffer binary format.
//!
//! This module bridges the in-memory `NodeChangeEvent` / `TimedEvent` / `WalOperation` types
//! and the FlatBuffer types generated from `.fbs` schemas in `nexora-serialization`.
//!
//! The conversion follows the Nexora persistence format:
//! - PropertyValue is encoded as MessagePack bytes (`msg_packed: [byte]` in .fbs)
//! - NexoraId is encoded as raw bytes (`[byte]` in .fbs)
//! - EdgeDirection enum values map directly
//!
//! Two WAL formats are supported:
//! - **v1 (JSON)**: Original format with magic bytes `0x5157` ("QW")
//! - **v2 (FlatBuffers)**: New format with magic bytes `0x5146` ("QF"), backward compatible

use crate::event::{NodeChangeEvent, TimedEvent};
use crate::wal::record::{WalOperation, WalRecord};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_serialization::codec::PackedCodec;
use nexora_serialization::generated::com::nexora::persistence::{
    AddEdge, AddEdgeArgs, AddProperty, AddPropertyArgs, EdgeDirection as FbEdgeDirection,
    IngestOffsetCommit, IngestOffsetCommitArgs, NodeEventUnion, NodeEventWithTime,
    NodeEventWithTimeArgs, RemoveEdge, RemoveEdgeArgs, RemoveProperty, RemovePropertyArgs,
    SnapshotCheckpoint, SnapshotCheckpointArgs, WalOperationBuffer, WalRecordBuffer,
    WalRecordBufferArgs,
};
use nexora_value::{EdgeDirection, HalfEdge, Symbol};

/// WAL magic bytes for JSON format (v1).
pub const WAL_MAGIC_JSON: [u8; 2] = [0x51, 0x57]; // "QW"
/// WAL magic bytes for FlatBuffers format (v2).
pub const WAL_MAGIC_FB: [u8; 2] = [0x51, 0x46]; // "QF"
/// WAL stop marker.
pub const WAL_STOP_MARKER: u8 = 0xFF;

/// Convert Rust EdgeDirection to FlatBuffer EdgeDirection.
fn edge_direction_to_fb(dir: &EdgeDirection) -> FbEdgeDirection {
    match dir {
        EdgeDirection::Out => FbEdgeDirection::Outgoing,
        EdgeDirection::In => FbEdgeDirection::Incoming,
    }
}

/// Convert FlatBuffer EdgeDirection to Rust EdgeDirection.
/// Undirected is mapped to In as fallback (Rust EdgeDirection has no Undirected variant).
fn fb_edge_direction_to_rust(dir: FbEdgeDirection) -> EdgeDirection {
    match dir {
        FbEdgeDirection::Outgoing => EdgeDirection::Out,
        FbEdgeDirection::Incoming => EdgeDirection::In,
        FbEdgeDirection::Undirected => EdgeDirection::In,
        _ => EdgeDirection::Out,
    }
}

/// Helper to create a byte vector in a FlatBuffer builder.
/// FlatBuffer `[byte]` maps to `Vector<i8>` in Rust, so we convert u8 → i8.
fn create_byte_vector<'a>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    data: &[u8],
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, i8>> {
    let i8_data: Vec<i8> = data.iter().map(|b| *b as i8).collect();
    builder.create_vector(&i8_data)
}

/// Helper to read a FlatBuffer byte vector as Vec<u8>.
fn read_byte_vector(fb_vector: flatbuffers::Vector<'_, i8>) -> Vec<u8> {
    fb_vector.iter().map(|b| b as u8).collect()
}

/// Encode a PropertyValue to MessagePack bytes for FlatBuffer embedding.
fn property_value_to_msgpack(value: &PropertyValue) -> Vec<u8> {
    rmp_serde::to_vec(value).unwrap_or_else(|_| {
        // Fallback to JSON if MessagePack fails
        serde_json::to_vec(value).unwrap_or_default()
    })
}

/// Decode a PropertyValue from MessagePack bytes.
fn msgpack_to_property_value(data: &[u8]) -> Option<PropertyValue> {
    rmp_serde::from_slice(data)
        .ok()
        .or_else(|| serde_json::from_slice(data).ok())
}

/// Encode a `WalRecord` as a FlatBuffer (v2 format).
/// Returns the finished FlatBuffer bytes.
pub fn encode_wal_record_fb(record: &WalRecord) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();

    match &record.operation {
        WalOperation::NodeEvent { qid, event } => {
            // Encode the node event as a NodeEventWithTime FlatBuffer
            // Encode the entire WAL record as a combined FlatBuffer
            let qid_bytes = qid.as_bytes();
            let _qid_fb = create_byte_vector(&mut builder, qid_bytes);

            // Build the inner event
            match &event.event {
                NodeChangeEvent::PropertySet { key, value } => {
                    let key_fb = builder.create_string(key.as_str());
                    let value_bytes = property_value_to_msgpack(value);
                    let value_fb = create_byte_vector(&mut builder, &value_bytes);

                    let qid_fb = create_byte_vector(&mut builder, qid_bytes);

                    let add_prop = AddProperty::create(
                        &mut builder,
                        &AddPropertyArgs {
                            key: Some(key_fb),
                            value: Some(value_fb),
                        },
                    );

                    let timed = NodeEventWithTime::create(
                        &mut builder,
                        &NodeEventWithTimeArgs {
                            qid: Some(qid_fb),
                            event_time: event.time.as_micros() as i64,
                            event_type: NodeEventUnion::AddProperty,
                            event: Some(add_prop.as_union_value()),
                        },
                    );

                    let union_value = timed.as_union_value();
                    let wal = WalRecordBuffer::create(
                        &mut builder,
                        &WalRecordBufferArgs {
                            seq_no: record.seq_no as i64,
                            operation_type: WalOperationBuffer::NodeEventWithTime,
                            operation: Some(union_value),
                        },
                    );
                    builder.finish(wal, None);
                }
                NodeChangeEvent::PropertyRemoved {
                    key,
                    previous_value,
                } => {
                    let key_fb = builder.create_string(key.as_str());
                    let value_bytes = property_value_to_msgpack(previous_value);
                    let value_fb = create_byte_vector(&mut builder, &value_bytes);

                    let remove_prop = RemoveProperty::create(
                        &mut builder,
                        &RemovePropertyArgs {
                            key: Some(key_fb),
                            value: Some(value_fb),
                        },
                    );

                    let qid_fb = create_byte_vector(&mut builder, qid_bytes);

                    let timed = NodeEventWithTime::create(
                        &mut builder,
                        &NodeEventWithTimeArgs {
                            qid: Some(qid_fb),
                            event_time: event.time.as_micros() as i64,
                            event_type: NodeEventUnion::RemoveProperty,
                            event: Some(remove_prop.as_union_value()),
                        },
                    );

                    let wal = WalRecordBuffer::create(
                        &mut builder,
                        &WalRecordBufferArgs {
                            seq_no: record.seq_no as i64,
                            operation_type: WalOperationBuffer::NodeEventWithTime,
                            operation: Some(timed.as_union_value()),
                        },
                    );
                    builder.finish(wal, None);
                }
                NodeChangeEvent::EdgeAdded { edge } => {
                    let edge_type_fb = builder.create_string(edge.edge_type.as_str());
                    let other_bytes = edge.other.as_bytes();
                    let other_id_fb = create_byte_vector(&mut builder, other_bytes);

                    let add_edge = AddEdge::create(
                        &mut builder,
                        &AddEdgeArgs {
                            edge_type: Some(edge_type_fb),
                            direction: edge_direction_to_fb(&edge.direction),
                            other_id: Some(other_id_fb),
                        },
                    );

                    let qid_fb = create_byte_vector(&mut builder, qid_bytes);

                    let timed = NodeEventWithTime::create(
                        &mut builder,
                        &NodeEventWithTimeArgs {
                            qid: Some(qid_fb),
                            event_time: event.time.as_micros() as i64,
                            event_type: NodeEventUnion::AddEdge,
                            event: Some(add_edge.as_union_value()),
                        },
                    );

                    let wal = WalRecordBuffer::create(
                        &mut builder,
                        &WalRecordBufferArgs {
                            seq_no: record.seq_no as i64,
                            operation_type: WalOperationBuffer::NodeEventWithTime,
                            operation: Some(timed.as_union_value()),
                        },
                    );
                    builder.finish(wal, None);
                }
                NodeChangeEvent::EdgeRemoved { edge } => {
                    let edge_type_fb = builder.create_string(edge.edge_type.as_str());
                    let other_bytes = edge.other.as_bytes();
                    let other_id_fb = create_byte_vector(&mut builder, other_bytes);

                    let remove_edge = RemoveEdge::create(
                        &mut builder,
                        &RemoveEdgeArgs {
                            edge_type: Some(edge_type_fb),
                            direction: edge_direction_to_fb(&edge.direction),
                            other_id: Some(other_id_fb),
                        },
                    );

                    let qid_fb = create_byte_vector(&mut builder, qid_bytes);

                    let timed = NodeEventWithTime::create(
                        &mut builder,
                        &NodeEventWithTimeArgs {
                            qid: Some(qid_fb),
                            event_time: event.time.as_micros() as i64,
                            event_type: NodeEventUnion::RemoveEdge,
                            event: Some(remove_edge.as_union_value()),
                        },
                    );

                    let wal = WalRecordBuffer::create(
                        &mut builder,
                        &WalRecordBufferArgs {
                            seq_no: record.seq_no as i64,
                            operation_type: WalOperationBuffer::NodeEventWithTime,
                            operation: Some(timed.as_union_value()),
                        },
                    );
                    builder.finish(wal, None);
                }
                // GAP-7: Label / edge-property / node-lifecycle events have no
                // dedicated FlatBuffer schema yet. Previously these returned an
                // empty Vec — a corrupt zero-length WAL record that loses the
                // event. Instead, fall back to the same JSON encoding the batch
                // (`NodeEvents`) path uses; `decode_wal_record_auto` reads JSON
                // records back, so these round-trip correctly and survive replay.
                NodeChangeEvent::LabelAdded { .. }
                | NodeChangeEvent::LabelRemoved { .. }
                | NodeChangeEvent::EdgePropertySet { .. }
                | NodeChangeEvent::EdgePropertyRemoved { .. }
                | NodeChangeEvent::NodeDeleted { .. }
                | NodeChangeEvent::NodeRestored => {
                    return serde_json::to_vec(&record)
                        .expect("JSON serialization should not fail for WalRecord");
                }
            }
        }

        WalOperation::SnapshotCheckpoint { qid, snapshot_time } => {
            let qid_bytes = qid.as_bytes();
            let qid_fb = create_byte_vector(&mut builder, qid_bytes);

            let checkpoint = SnapshotCheckpoint::create(
                &mut builder,
                &SnapshotCheckpointArgs {
                    qid: Some(qid_fb),
                    snapshot_time: snapshot_time.as_micros() as i64,
                },
            );

            let wal = WalRecordBuffer::create(
                &mut builder,
                &WalRecordBufferArgs {
                    seq_no: record.seq_no as i64,
                    operation_type: WalOperationBuffer::SnapshotCheckpoint,
                    operation: Some(checkpoint.as_union_value()),
                },
            );
            builder.finish(wal, None);
        }

        WalOperation::IngestOffsetCommit {
            ingest_id,
            partition,
            offset,
        } => {
            let ingest_id_fb = builder.create_string(ingest_id);

            let commit = IngestOffsetCommit::create(
                &mut builder,
                &IngestOffsetCommitArgs {
                    ingest_id: Some(ingest_id_fb),
                    partition: *partition,
                    offset: *offset,
                },
            );

            let wal = WalRecordBuffer::create(
                &mut builder,
                &WalRecordBufferArgs {
                    seq_no: record.seq_no as i64,
                    operation_type: WalOperationBuffer::IngestOffsetCommit,
                    operation: Some(commit.as_union_value()),
                },
            );
            builder.finish(wal, None);
        }

        // For NodeEvents (batch) and DomainIndexEvent, fall back to JSON
        // since building nested FlatBuffers with vectors of union-containing tables
        // requires careful builder management
        WalOperation::NodeEvents { .. }
        | WalOperation::DomainIndexEvent { .. }
        | WalOperation::RawEvent { .. } => {
            // Fallback: encode as JSON bytes within a FlatBuffer envelope
            // This is a transitional state until full FlatBuffer support for these types
            let json_bytes = serde_json::to_vec(&record)
                .expect("JSON serialization should not fail for WalRecord");
            let _json_fb = create_byte_vector(&mut builder, &json_bytes);

            // We still use the WalRecordBuffer envelope but with a JSON fallback
            // For now, these operations are serialized as pure JSON (v1 format)
            return json_bytes;
        }
    }

    builder.finished_data().to_vec()
}

/// Decode a FlatBuffer `WalRecordBuffer` back to a `WalRecord`.
pub fn decode_wal_record_fb(data: &[u8]) -> Option<WalRecord> {
    let fb = flatbuffers::root::<WalRecordBuffer>(data).ok()?;

    let seq_no = fb.seq_no();

    match fb.operation_type() {
        WalOperationBuffer::NodeEventWithTime => {
            let timed_fb = fb.operation_as_node_event_with_time()?;
            let event_time = EventTime::from_micros(timed_fb.event_time() as u64);

            // FIX P0-2: Extract qid from NodeEventWithTime (was missing, caused WAL replay data loss)
            let qid = NexoraId::from_bytes(read_byte_vector(timed_fb.qid()));

            let node_event = match timed_fb.event_type() {
                NodeEventUnion::AddProperty => {
                    let prop = timed_fb.event_as_add_property()?;
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new(prop.key()),
                        value: msgpack_to_property_value(&read_byte_vector(prop.value()))?,
                    }
                }
                NodeEventUnion::RemoveProperty => {
                    let prop = timed_fb.event_as_remove_property()?;
                    NodeChangeEvent::PropertyRemoved {
                        key: Symbol::new(prop.key()),
                        previous_value: msgpack_to_property_value(&read_byte_vector(prop.value()))?,
                    }
                }
                NodeEventUnion::AddEdge => {
                    let edge = timed_fb.event_as_add_edge()?;
                    NodeChangeEvent::EdgeAdded {
                        edge: HalfEdge::new(
                            Symbol::new(edge.edge_type()),
                            fb_edge_direction_to_rust(edge.direction()),
                            NexoraId::from_bytes(read_byte_vector(edge.other_id())),
                        ),
                    }
                }
                NodeEventUnion::RemoveEdge => {
                    let edge = timed_fb.event_as_remove_edge()?;
                    NodeChangeEvent::EdgeRemoved {
                        edge: HalfEdge::new(
                            Symbol::new(edge.edge_type()),
                            fb_edge_direction_to_rust(edge.direction()),
                            NexoraId::from_bytes(read_byte_vector(edge.other_id())),
                        ),
                    }
                }
                _ => return None,
            };

            Some(WalRecord {
                seq_no: seq_no as u64,
                version: seq_no as u64, // A4: version defaults to seq_no
                operation: WalOperation::NodeEvent {
                    qid,
                    event: TimedEvent::new(node_event, event_time),
                },
            })
        }

        WalOperationBuffer::SnapshotCheckpoint => {
            let checkpoint = fb.operation_as_snapshot_checkpoint()?;
            Some(WalRecord {
                seq_no: seq_no as u64,
                version: seq_no as u64, // A4: version defaults to seq_no
                operation: WalOperation::SnapshotCheckpoint {
                    qid: NexoraId::from_bytes(read_byte_vector(checkpoint.qid())),
                    snapshot_time: EventTime::from_micros(checkpoint.snapshot_time() as u64),
                },
            })
        }

        WalOperationBuffer::IngestOffsetCommit => {
            let commit = fb.operation_as_ingest_offset_commit()?;
            Some(WalRecord {
                seq_no: seq_no as u64,
                version: seq_no as u64, // A4: version defaults to seq_no
                operation: WalOperation::IngestOffsetCommit {
                    ingest_id: commit.ingest_id().to_string(),
                    partition: commit.partition(),
                    offset: commit.offset(),
                },
            })
        }

        // NodeEventsBatch and DomainIndexEventWithTime need more complex handling
        _ => None,
    }
}

/// Encode a WalRecord with PackedCodec compression (v2 format).
/// Returns (packed_bytes, original_len) for unpacking.
pub fn encode_wal_record_fb_packed(record: &WalRecord) -> (Vec<u8>, usize) {
    let fb_data = encode_wal_record_fb(record);
    let original_len = fb_data.len();
    // For NodeEvents/DomainIndexEvent that fall back to JSON,
    // the data is already compact; skip PackedCodec for JSON data
    if !fb_data.is_empty() && fb_data[0] == 0x7B {
        // JSON fallback — no packing needed
        (fb_data, original_len)
    } else {
        let packed = PackedCodec::pack(&fb_data);
        (packed, original_len)
    }
}

/// Detect WAL format from magic bytes and decode accordingly.
/// Returns the decoded WalRecord regardless of format.
pub fn decode_wal_record_auto(
    magic: &[u8; 2],
    payload: &[u8],
    original_len: usize,
) -> Result<WalRecord, String> {
    if magic == &WAL_MAGIC_JSON {
        // v1 format: JSON
        serde_json::from_slice(payload).map_err(|e| format!("JSON decode error: {e}"))
    } else if magic == &WAL_MAGIC_FB {
        // v2 format: FlatBuffer (possibly packed)
        let unpacked = if original_len > 0 && original_len != payload.len() {
            PackedCodec::unpack(payload, original_len)
                .map_err(|e| format!("PackedCodec unpack error: {e}"))?
        } else {
            payload.to_vec()
        };

        // Try FlatBuffer first
        if let Some(record) = decode_wal_record_fb(&unpacked) {
            Ok(record)
        } else {
            // Fallback to JSON (for operations not yet supported in FlatBuffers)
            serde_json::from_slice(&unpacked)
                .map_err(|e| format!("FlatBuffer/JSON decode error: {e}"))
        }
    } else {
        Err(format!("Unknown WAL magic: {magic:02x?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{NodeChangeEvent, TimedEvent};
    use nexora_id::{EventTime, NexoraId, PropertyValue};
    use nexora_value::Symbol;

    #[test]
    fn test_wal_record_node_event_flatbuffer() {
        let qid = NexoraId::new_random();
        let record = WalRecord {
            seq_no: 42,
            version: 42,
            operation: WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("status"),
                        value: PropertyValue::String("active".into()),
                    },
                    EventTime::from_micros(1000),
                ),
            },
        };

        let encoded = encode_wal_record_fb(&record);
        let decoded = decode_wal_record_fb(&encoded).unwrap();
        assert_eq!(decoded.seq_no, 42);
    }

    /// GAP-7: label / edge-property / node-lifecycle singular events used to
    /// encode to an empty Vec (a corrupt, event-losing WAL record). They must
    /// now survive an encode→decode round-trip via the JSON fallback.
    #[test]
    fn test_wal_record_landmine_events_roundtrip() {
        use crate::graph::node_task::TombstoneRecord;

        let qid = NexoraId::new_random();
        let cases = vec![
            NodeChangeEvent::LabelAdded {
                label: Symbol::new("Person"),
            },
            NodeChangeEvent::LabelRemoved {
                label: Symbol::new("Person"),
            },
            NodeChangeEvent::EdgePropertySet {
                edge_type: Symbol::new("KNOWS"),
                target: NexoraId::new_random(),
                key: Symbol::new("since"),
                value: PropertyValue::Integer(2020),
            },
            NodeChangeEvent::EdgePropertyRemoved {
                edge_type: Symbol::new("KNOWS"),
                target: NexoraId::new_random(),
                key: Symbol::new("since"),
            },
            NodeChangeEvent::NodeDeleted {
                tombstone: TombstoneRecord {
                    deleted_at: EventTime::from_micros(999),
                    deleted_by: Some("admin".into()),
                    reason: Some("test".into()),
                },
            },
            NodeChangeEvent::NodeRestored,
        ];

        for event in cases {
            let record = WalRecord {
                seq_no: 7,
                version: 7,
                operation: WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(event.clone(), EventTime::from_micros(1234)),
                },
            };

            let encoded = encode_wal_record_fb(&record);
            assert!(
                !encoded.is_empty(),
                "GAP-7: event {event:?} must not encode to an empty WAL record"
            );

            // The WAL writer tags records with WAL_MAGIC_FB; the reader's FB
            // branch falls back to JSON when flatbuf parsing fails. Exercise
            // that exact path.
            let decoded = decode_wal_record_auto(&WAL_MAGIC_FB, &encoded, 0)
                .expect("landmine event must decode via JSON fallback");
            assert_eq!(decoded.seq_no, 7);
            match &decoded.operation {
                WalOperation::NodeEvent {
                    event: decoded_event,
                    ..
                } => {
                    assert_eq!(
                        format!("{:?}", decoded_event.event),
                        format!("{event:?}"),
                        "decoded event must match the original"
                    );
                }
                other => panic!("expected NodeEvent, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_wal_record_snapshot_checkpoint_flatbuffer() {
        let qid = NexoraId::new_random();
        let record = WalRecord {
            seq_no: 10,
            version: 10,
            operation: WalOperation::SnapshotCheckpoint {
                qid: qid.clone(),
                snapshot_time: EventTime::from_micros(5000),
            },
        };

        let encoded = encode_wal_record_fb(&record);
        let decoded = decode_wal_record_fb(&encoded).unwrap();
        assert_eq!(decoded.seq_no, 10);

        match &decoded.operation {
            WalOperation::SnapshotCheckpoint {
                qid: decoded_qid,
                snapshot_time,
            } => {
                assert_eq!(decoded_qid, &qid);
                assert_eq!(snapshot_time.as_micros(), 5000);
            }
            _ => panic!("Expected SnapshotCheckpoint"),
        }
    }

    #[test]
    fn test_wal_record_ingest_offset_flatbuffer() {
        let record = WalRecord {
            seq_no: 5,
            version: 5,
            operation: WalOperation::IngestOffsetCommit {
                ingest_id: "kafka-main".to_string(),
                partition: 3,
                offset: 12345,
            },
        };

        let encoded = encode_wal_record_fb(&record);
        let decoded = decode_wal_record_fb(&encoded).unwrap();
        assert_eq!(decoded.seq_no, 5);

        match &decoded.operation {
            WalOperation::IngestOffsetCommit {
                ingest_id,
                partition,
                offset,
            } => {
                assert_eq!(ingest_id, "kafka-main");
                assert_eq!(*partition, 3);
                assert_eq!(*offset, 12345);
            }
            _ => panic!("Expected IngestOffsetCommit"),
        }
    }

    #[test]
    fn test_msgpack_property_value_roundtrip() {
        let values = [
            PropertyValue::Integer(42),
            #[allow(clippy::approx_constant)]
            PropertyValue::Float(3.14),
            PropertyValue::String("hello".into()),
            PropertyValue::Boolean(true),
            PropertyValue::Null,
        ];

        for value in &values {
            let encoded = property_value_to_msgpack(value);
            let decoded = msgpack_to_property_value(&encoded);
            assert!(decoded.is_some(), "Failed to decode {:?}", value);
        }
    }

    #[test]
    fn test_edge_direction_mapping() {
        assert_eq!(
            edge_direction_to_fb(&EdgeDirection::Out),
            FbEdgeDirection::Outgoing
        );
        assert_eq!(
            edge_direction_to_fb(&EdgeDirection::In),
            FbEdgeDirection::Incoming
        );

        assert_eq!(
            fb_edge_direction_to_rust(FbEdgeDirection::Outgoing),
            EdgeDirection::Out
        );
        assert_eq!(
            fb_edge_direction_to_rust(FbEdgeDirection::Incoming),
            EdgeDirection::In
        );
        assert_eq!(
            fb_edge_direction_to_rust(FbEdgeDirection::Undirected),
            EdgeDirection::In
        );
    }
}
