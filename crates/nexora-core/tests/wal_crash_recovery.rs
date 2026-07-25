//! A4: WAL crash recovery integration tests
//!
//! These tests validate WAL replay behavior after various failure scenarios:
//! - Clean shutdown and restart
//! - Mid-write kill (torn-write simulation)
//! - Idempotent duplicate seq handling
//! - Seq gap detection

use nexora_core::event::{NodeChangeEvent, TimedEvent};
use nexora_core::wal::{WalOperation, WalRecord, WriteAheadLog};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::Symbol;
use std::fs::OpenOptions;
use std::io::Write;

/// A4: Replay after clean shutdown — all records visible
#[test]
fn wal_replay_after_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::new_random();

    // Write 10 records with clean shutdown
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        for i in 1..=10 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("counter"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64 * 1000),
                ),
            })
            .unwrap();
        }
        wal.sync().unwrap();
        // Clean drop
    }

    // Restart and replay
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        assert_eq!(result.records.len(), 10, "all records should be visible");
        assert!(!result.had_duplicates);
        assert!(result.gaps.is_empty());

        // Verify seq continuity
        for (i, record) in result.records.iter().enumerate() {
            assert_eq!(record.seq_no, (i + 1) as u64);
        }
    }
}

/// A4: Replay after mid-write kill — torn-write truncation
#[test]
fn wal_replay_after_mid_write_kill() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::new_random();

    // Write 5 complete records
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        for i in 1..=5 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64 * 100),
                ),
            })
            .unwrap();
        }
        wal.sync().unwrap();
    }

    // Simulate torn-write: truncate the file mid-record
    {
        let wal_path = dir.path().join("current.wal");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .unwrap();

        // Get current file size
        let metadata = file.metadata().unwrap();
        let original_size = metadata.len();

        // Truncate to 80% of original size (simulating incomplete write)
        let truncated_size = (original_size * 4) / 5;
        file.set_len(truncated_size).unwrap();
        file.sync_all().unwrap();
    }

    // Replay should recover valid records and auto-truncate the torn part
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();

        // Should have fewer than 5 records due to torn-write truncation
        assert!(
            result.records.len() < 5,
            "torn-write should lose some records"
        );
        assert!(
            !result.records.is_empty(),
            "should have at least some valid records"
        );
        assert!(!result.had_duplicates);
        assert!(result.gaps.is_empty());

        // All recovered records should have continuous seq_no
        for (i, record) in result.records.iter().enumerate() {
            assert_eq!(record.seq_no, (i + 1) as u64);
        }
    }
}

/// A4: Idempotent replay — duplicate seq_no handling
#[test]
fn wal_replay_idempotent_duplicate_seq() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::new_random();

    // Write 3 normal records
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        for i in 1..=3 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("y"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64 * 100),
                ),
            })
            .unwrap();
        }
        wal.sync().unwrap();
    }

    // Manually append a duplicate with seq_no=2 (simulating retry/idempotent write)
    {
        use nexora_core::flatbuf_codec::encode_wal_record_fb;

        let wal_path = dir.path().join("current.wal");
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();

        let dup_record = WalRecord {
            seq_no: 2,
            version: 2,
            operation: WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("duplicate"),
                        value: PropertyValue::Boolean(true),
                    },
                    EventTime::from_micros(999),
                ),
            },
        };

        let fb_data = encode_wal_record_fb(&dup_record);
        let length = fb_data.len() as u32;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&fb_data);
        let crc = hasher.finalize();

        // Write magic + length + original_len + payload + crc + stop
        file.write_all(&[0x51, 0x46]).unwrap(); // WAL_MAGIC_FB
        file.write_all(&length.to_be_bytes()).unwrap();
        file.write_all(&0u32.to_be_bytes()).unwrap();
        file.write_all(&fb_data).unwrap();
        file.write_all(&crc.to_be_bytes()).unwrap();
        file.write_all(&[0xFF]).unwrap(); // WAL_STOP_MARKER
        file.flush().unwrap();
    }

    // Replay should detect and skip duplicate
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();

        assert_eq!(
            result.records.len(),
            3,
            "should have 3 unique records after dedup"
        );
        assert!(result.had_duplicates, "should detect duplicate seq_no");
        assert!(result.gaps.is_empty());

        // Verify seq continuity
        for (i, record) in result.records.iter().enumerate() {
            assert_eq!(record.seq_no, (i + 1) as u64);
        }
    }
}

/// A4: Seq gap detection — missing records
#[test]
fn wal_replay_detects_seq_gap() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::new_random();

    // Manually write records with gaps: seq 1, 2, 5, 6 (missing 3, 4)
    {
        use nexora_core::flatbuf_codec::encode_wal_record_fb;

        let wal_path = dir.path().join("current.wal");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&wal_path)
            .unwrap();

        for seq in &[1u64, 2u64, 5u64, 6u64] {
            let record = WalRecord {
                seq_no: *seq,
                version: *seq,
                operation: WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("z"),
                            value: PropertyValue::Integer(*seq as i64),
                        },
                        EventTime::from_micros(*seq * 100),
                    ),
                },
            };

            let fb_data = encode_wal_record_fb(&record);
            let length = fb_data.len() as u32;
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&fb_data);
            let crc = hasher.finalize();

            file.write_all(&[0x51, 0x46]).unwrap();
            file.write_all(&length.to_be_bytes()).unwrap();
            file.write_all(&0u32.to_be_bytes()).unwrap();
            file.write_all(&fb_data).unwrap();
            file.write_all(&crc.to_be_bytes()).unwrap();
            file.write_all(&[0xFF]).unwrap();
        }
        file.flush().unwrap();
    }

    // Replay should detect gap
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();

        assert_eq!(result.records.len(), 4);
        assert!(!result.had_duplicates);
        assert_eq!(result.gaps.len(), 1, "should detect one gap");
        assert_eq!(result.gaps[0], 3, "gap should start at seq 3");

        // Verify recovered records
        assert_eq!(result.records[0].seq_no, 1);
        assert_eq!(result.records[1].seq_no, 2);
        assert_eq!(result.records[2].seq_no, 5);
        assert_eq!(result.records[3].seq_no, 6);
    }
}
