use super::record::{WalOperation, WalRecord};
use crate::flatbuf_codec::{
    decode_wal_record_auto, encode_wal_record_fb, WAL_MAGIC_FB, WAL_MAGIC_JSON, WAL_STOP_MARKER,
};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// A4: Result of WAL replay with diagnostic information.
#[derive(Debug, Clone)]
pub struct WalReplayResult {
    /// Records successfully replayed.
    pub records: Vec<WalRecord>,
    /// True if duplicate seq_no entries were detected (idempotent writes).
    pub had_duplicates: bool,
    /// Sequence numbers where gaps were detected (missing records).
    /// Each entry is the first missing seq_no in a gap.
    pub gaps: Vec<u64>,
}

// WAL file format (v2 -- FlatBuffers):
// | magic (2 bytes = 0x5146 "QF") | length (4 bytes, big-endian) |
// | original_len (4 bytes, big-endian) | payload (N bytes, packed FlatBuffer) |
// | crc32 (4 bytes) | stop (1 byte = 0xFF) |
//
// WAL file format (v1 -- JSON, backward compatible):
// | magic (2 bytes = 0x5157 "QW") | length (4 bytes, big-endian) |
// | payload (N bytes, JSON) | crc32 (4 bytes) | stop (1 byte = 0xFF) |
//
// New writes use v2 format. Old WAL files are read as v1 (backward compatible).
//
// When encryption is enabled, the **payload** portion of each record is encrypted
// using AES-256-GCM. The magic, length, CRC, and stop marker remain in plaintext
// so that record boundaries can be found without decryption. The CRC covers the
// encrypted payload. During replay, the payload is decrypted after CRC validation.
//
// Encrypted payload format: [nonce(12B)][ciphertext(NB)][tag(16B)]
// Nonce = [file_generation(4B, BE)][seq_no(8B, BE)]   (12 bytes total)
// The file_generation is a monotonically increasing counter persisted in a
// `.wal_gen` sidecar file that survives WAL truncation. This ensures nonces
// are never reused across truncation cycles with the same encryption key.

/// Write-Ahead Log for crash recovery.
///
/// The WAL is append-only during normal operation. On crash recovery,
/// it is read sequentially and records are replayed to reconstruct state.
pub struct WriteAheadLog {
    /// Directory containing WAL segment files.
    dir: PathBuf,
    /// Current WAL file writer.
    writer: Option<BufWriter<File>>,
    /// Next sequence number to assign.
    next_seq: u64,
    /// Sync policy.
    sync_policy: WalSyncPolicy,
    /// Counter for sync policy.
    ops_since_sync: u64,
    /// Byte counter for group-commit byte-budget flushing.
    bytes_since_sync: usize,
    /// Optional AES-256 key for payload encryption.
    #[cfg(feature = "encrypt")]
    encryption_key: Option<[u8; 32]>,
    /// File generation counter — incremented on each truncate, persisted in a
    /// sidecar metadata file that survives WAL file deletion. Incorporated into
    /// the AES-GCM nonce so that nonces never repeat across truncation cycles
    /// with the same encryption key.
    #[cfg(feature = "encrypt")]
    file_generation: u32,
    /// Group-commit coordinator, present only under `WalSyncPolicy::Group`.
    /// Shared with the background flusher task via `Arc`.
    group: Option<Arc<GroupCommit>>,
}

/// When to fsync the WAL file.
#[derive(Debug, Clone, Copy)]
pub enum WalSyncPolicy {
    /// fsync after every write (safest, slowest).
    Always,
    /// fsync every N records.
    EveryN(u64),
    /// Never fsync explicitly (rely on OS, may lose recent data).
    Never,
    /// Group commit: appends only buffer; a background flusher batches the
    /// `sync_data()` across all writes that accumulated since the last flush,
    /// bounded by `max_ops` records or `max_delay` elapsed — whichever first.
    ///
    /// Durability contract is preserved for callers that wait on
    /// [`WriteAheadLog::durable_receiver`]: a write is only acknowledged once
    /// the flusher has fsynced up to its sequence number. A crash loses only
    /// writes that were never acknowledged.
    ///
    /// This is the throughput policy: one fsync amortizes N concurrent writes,
    /// turning a per-write fsync bottleneck into a per-batch one.
    Group {
        /// Force an inline flush once this many records have buffered, to bound
        /// buffer growth and tail latency under sustained load.
        max_ops: usize,
        /// Maximum time the flusher waits before syncing a non-empty buffer.
        max_delay: Duration,
        /// Byte budget: force an inline flush once this many bytes have buffered.
        /// `None` disables byte-based flushing (backward-compatible default).
        max_bytes: Option<usize>,
    },
}

/// Group-commit coordinator shared between the WAL and its background flusher.
///
/// The flusher publishes the highest fsynced sequence number through
/// `durable_seq`; committers subscribe and await their own sequence.
pub(crate) struct GroupCommit {
    /// Highest sequence number that has been fsynced to disk.
    durable_seq: watch::Sender<u64>,
    /// Signalled on every buffered append so the flusher can coalesce.
    dirty: Notify,
    /// Set on graceful shutdown to make the flusher drain and exit.
    shutdown: AtomicBool,
    max_delay: Duration,
}

/// Handle to a spawned group-commit flusher task. Dropping it detaches the
/// task (it still exits on its own once the WAL is dropped, via the `Weak`
/// reference); calling [`FlusherHandle::shutdown`] drains and joins it.
pub(crate) struct FlusherHandle {
    coord: Arc<GroupCommit>,
    handle: JoinHandle<()>,
}

impl FlusherHandle {
    /// Signal the flusher to perform a final sync and exit, then await it.
    ///
    /// FIXED P1-4: Ensures all buffered writes are fsynced before the flusher exits.
    /// This prevents data loss on graceful shutdown when using Group commit policy.
    pub(crate) async fn shutdown(self) {
        self.coord.shutdown.store(true, Ordering::SeqCst);
        // Wake the flusher immediately so it performs the final flush
        self.coord.dirty.notify_one();
        let _ = self.handle.await;
    }
}

/// Spawn the background group-commit flusher for a `Group`-policy WAL.
///
/// The flusher wakes on either a buffered append (`dirty`) or `max_delay`,
/// fsyncs everything accumulated in one `sync_data()`, and publishes the new
/// durable watermark so awaiting committers can be released. Returns `None`
/// if the WAL is not under group commit.
///
/// Ordering guarantee: the flusher takes `last_seq()` *before* releasing the
/// lock for the fsync-covered flush, so the published watermark never exceeds
/// what was actually synced.
pub(crate) fn spawn_group_flusher(
    wal: Arc<tokio::sync::Mutex<WriteAheadLog>>,
) -> Option<FlusherHandle> {
    let coord = {
        let guard = wal.try_lock().ok()?;
        guard.group_coordinator()?
    };
    let max_delay = coord.max_delay;
    let task_coord = coord.clone();
    // Hold only a Weak reference so a `GraphService` dropped without an explicit
    // shutdown does not keep the flusher (and the WAL it points at) alive
    // forever: once the last strong WAL handle is gone, `upgrade()` fails and
    // the task exits.
    let weak_wal = Arc::downgrade(&wal);
    drop(wal);
    let handle = tokio::spawn(async move {
        loop {
            // Wait for work or the delay bound, whichever comes first.
            tokio::select! {
                _ = task_coord.dirty.notified() => {}
                _ = tokio::time::sleep(max_delay) => {}
            }

            let shutting_down = task_coord.shutdown.load(Ordering::SeqCst);

            // Flush + fsync under the lock, then read the synced watermark.
            let Some(wal) = weak_wal.upgrade() else {
                // WAL dropped — nothing left to flush or ack.
                break;
            };
            {
                let mut guard = wal.lock().await;
                // FIXED P1-4: Always flush if there are pending ops, even on shutdown.
                // This ensures the final batch of writes is durable before exit.
                if guard.ops_since_sync > 0 {
                    if let Err(e) = guard.sync() {
                        warn!(error = %e, "group-commit flush failed");
                        // Don't advance the watermark on failure — committers
                        // stay parked rather than acking un-synced writes.
                        // On shutdown with failed flush, still break to avoid infinite loop,
                        // but log the data loss risk.
                        if shutting_down {
                            warn!(
                                "Shutdown with failed final flush - {} ops may be lost",
                                guard.ops_since_sync
                            );
                            break;
                        }
                        continue;
                    }
                    let synced = guard.last_seq();
                    let _ = task_coord.durable_seq.send(synced);
                }
            }

            if shutting_down {
                // Final flush completed successfully - safe to exit
                break;
            }
        }
    });
    Some(FlusherHandle { coord, handle })
}

// ---------------------------------------------------------------------------
// File-generation helpers (behind "encrypt" feature)
// ---------------------------------------------------------------------------

/// Path to the sidecar file that stores the current WAL file generation number.
#[cfg(feature = "encrypt")]
fn gen_path(dir: &Path) -> PathBuf {
    dir.join(".wal_gen")
}

/// Read the current file generation from the sidecar file.
/// Returns 0 if the file does not exist (first run).
#[cfg(feature = "encrypt")]
fn read_file_generation(dir: &Path) -> io::Result<u32> {
    let path = gen_path(dir);
    match fs::read_to_string(&path) {
        Ok(s) => s
            .trim()
            .parse::<u32>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad .wal_gen: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

/// Persist the file generation counter to the sidecar file (atomic write).
#[cfg(feature = "encrypt")]
fn write_file_generation(dir: &Path, gen: u32) -> io::Result<()> {
    let path = gen_path(dir);
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, format!("{gen}\n"))?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

impl WriteAheadLog {
    /// Open or create a WAL in the given directory.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_options(dir, WalSyncPolicy::EveryN(1000), None)
    }

    /// Open with a specific sync policy.
    pub fn open_with_policy(dir: impl AsRef<Path>, policy: WalSyncPolicy) -> io::Result<Self> {
        Self::open_with_options(dir, policy, None)
    }

    /// Open with a specific sync policy and optional encryption key.
    /// When `encryption_key` is `Some`, each record's payload is encrypted
    /// using AES-256-GCM before writing, and decrypted when reading.
    #[cfg(feature = "encrypt")]
    pub fn open_with_encryption(
        dir: impl AsRef<Path>,
        policy: WalSyncPolicy,
        encryption_key: [u8; 32],
    ) -> io::Result<Self> {
        Self::open_with_options(dir, policy, Some(encryption_key))
    }

    /// Internal open with all options.
    #[cfg(feature = "encrypt")]
    fn open_with_options(
        dir: impl AsRef<Path>,
        policy: WalSyncPolicy,
        encryption_key: Option<[u8; 32]>,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Read the persisted file generation counter for nonce derivation.
        // This value survives WAL truncation so nonces never repeat with the same key.
        let file_generation = read_file_generation(&dir)?;

        // Find existing WAL segments and determine the next sequence number
        let next_seq = Self::find_max_seq_with_key(&dir, encryption_key)? + 1;

        // Open the current WAL file for appending
        let wal_path = dir.join("current.wal");
        let file_existed = wal_path.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;

        // FIXED P1-3: If we just created a new WAL file, fsync the parent directory
        // to ensure the directory entry is durable. Without this, a crash immediately
        // after creating the file could make it invisible on recovery.
        if !file_existed {
            if let Ok(dir_file) = File::open(&dir) {
                let _ = dir_file.sync_all();
                debug!(wal_path = %wal_path.display(), "Synced parent directory after WAL creation");
            }
        }

        if encryption_key.is_some() {
            info!(
                dir = %dir.display(),
                next_seq,
                file_generation,
                encrypted = true,
                "WAL opened with encryption"
            );
        } else {
            info!(
                dir = %dir.display(),
                next_seq,
                "WAL opened"
            );
        }

        Ok(Self {
            dir,
            writer: Some(BufWriter::new(file)),
            next_seq,
            sync_policy: policy,
            ops_since_sync: 0,
            bytes_since_sync: 0,
            encryption_key,
            file_generation,
            group: Self::make_group(policy),
        })
    }

    /// Internal open with all options (no encryption feature).
    #[cfg(not(feature = "encrypt"))]
    fn open_with_options(
        dir: impl AsRef<Path>,
        policy: WalSyncPolicy,
        _encryption_key: Option<[u8; 32]>,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Find existing WAL segments and determine the next sequence number
        let next_seq = Self::find_max_seq(&dir)? + 1;

        // Open the current WAL file for appending
        let wal_path = dir.join("current.wal");
        let file_existed = wal_path.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;

        // FIXED P1-3: If we just created a new WAL file, fsync the parent directory
        // to ensure the directory entry is durable. Without this, a crash immediately
        // after creating the file could make it invisible on recovery.
        if !file_existed {
            if let Ok(dir_file) = File::open(&dir) {
                let _ = dir_file.sync_all();
                debug!(wal_path = %wal_path.display(), "Synced parent directory after WAL creation");
            }
        }

        info!(
            dir = %dir.display(),
            next_seq,
            "WAL opened"
        );

        Ok(Self {
            dir,
            writer: Some(BufWriter::new(file)),
            next_seq,
            sync_policy: policy,
            ops_since_sync: 0,
            bytes_since_sync: 0,
            group: Self::make_group(policy),
        })
    }

    /// Append a record to the WAL using FlatBuffers v2 format.
    /// Returns the assigned sequence number.
    pub fn append(&mut self, operation: WalOperation) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let record = WalRecord {
            seq_no: seq,
            operation,
            version: seq, // A4: version matches seq_no for new records
        };

        // Encode as FlatBuffer (v2 format)
        let fb_data = encode_wal_record_fb(&record);

        // Encrypt payload if encryption is enabled
        #[cfg(feature = "encrypt")]
        let (payload, original_len) = {
            if let Some(key) = &self.encryption_key {
                let original_len = fb_data.len() as u32;
                let encrypted = encrypt_payload(key, &fb_data, self.file_generation, seq)?;
                (encrypted, original_len)
            } else {
                (fb_data, 0u32)
            }
        };
        #[cfg(not(feature = "encrypt"))]
        let (payload, original_len) = (fb_data, 0u32);

        let length: u32 = payload.len() as u32;
        let crc = crc32(&payload);

        // Write: magic(2B) + length(4B) + original_len(4B) + payload(NB) + crc(4B) + stop(1B)
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "WAL writer is None"))?;
        writer.write_all(&WAL_MAGIC_FB)?;
        writer.write_all(&length.to_be_bytes())?;
        writer.write_all(&original_len.to_be_bytes())?;
        writer.write_all(&payload)?;
        writer.write_all(&crc.to_be_bytes())?;
        writer.write_all(&[WAL_STOP_MARKER])?;

        self.ops_since_sync += 1;
        // 2 (magic) + 4 (length) + 4 (original_len) + payload + 4 (crc) + 1 (stop)
        self.bytes_since_sync += 2 + 4 + 4 + payload.len() + 4 + 1;

        // Sync based on policy
        match &self.sync_policy {
            WalSyncPolicy::Always => {
                writer.flush()?;
                writer.get_ref().sync_data()?;
            }
            WalSyncPolicy::EveryN(n) if self.ops_since_sync >= *n => {
                writer.flush()?;
                writer.get_ref().sync_data()?;
                self.ops_since_sync = 0;
                self.bytes_since_sync = 0;
            }
            WalSyncPolicy::Group {
                max_ops, max_bytes, ..
            } => {
                // Buffer only. If the backlog reached the ops cap OR the byte
                // budget, flush inline so buffer growth and tail latency stay
                // bounded even if the flusher task is momentarily starved;
                // otherwise just wake the flusher so it can coalesce writes.
                let over_ops = self.ops_since_sync as usize >= *max_ops;
                let over_bytes = max_bytes
                    .map(|b| self.bytes_since_sync >= b)
                    .unwrap_or(false);
                if over_ops || over_bytes {
                    writer.flush()?;
                    writer.get_ref().sync_data()?;
                    self.ops_since_sync = 0;
                    self.bytes_since_sync = 0;
                    if let Some(group) = &self.group {
                        let _ = group.durable_seq.send(seq);
                    }
                } else if let Some(group) = &self.group {
                    group.dirty.notify_one();
                }
            }
            _ => {}
        }

        Ok(seq)
    }

    /// Flush and sync the WAL file.
    pub fn sync(&mut self) -> io::Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush()?;
            writer.get_ref().sync_data()?;
        }
        self.ops_since_sync = 0;
        self.bytes_since_sync = 0;
        Ok(())
    }

    /// The highest sequence number assigned so far (the last `append` return).
    /// Used by the group-commit flusher to publish the durable watermark after
    /// an fsync covers everything buffered up to this point.
    pub fn last_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    /// Build the group-commit coordinator for a `Group` policy (else `None`).
    fn make_group(policy: WalSyncPolicy) -> Option<Arc<GroupCommit>> {
        if let WalSyncPolicy::Group { max_delay, .. } = policy {
            let (durable_seq, _) = watch::channel(0u64);
            Some(Arc::new(GroupCommit {
                durable_seq,
                dirty: Notify::new(),
                shutdown: AtomicBool::new(false),
                max_delay,
            }))
        } else {
            None
        }
    }

    /// Subscribe to the durable-sequence watermark. A committer awaits until the
    /// returned receiver observes a value `>=` its own append sequence, at which
    /// point that write is fsynced and safe to acknowledge.
    ///
    /// Returns `None` when the policy is not `Group` (durability is synchronous
    /// under `Always`/`EveryN`, so no barrier is needed).
    pub fn durable_receiver(&self) -> Option<watch::Receiver<u64>> {
        self.group.as_ref().map(|g| g.durable_seq.subscribe())
    }

    /// Whether this WAL runs under group commit (i.e. `append` only buffers and
    /// callers must await [`Self::durable_receiver`] for durability).
    pub fn is_group_commit(&self) -> bool {
        self.group.is_some()
    }

    /// Clone the group-commit coordinator, if any. Used by
    /// [`spawn_group_flusher`] to wire up the background flush task.
    pub(crate) fn group_coordinator(&self) -> Option<Arc<GroupCommit>> {
        self.group.clone()
    }

    /// Replay all records from the WAL. Used during crash recovery.
    /// Flushes any pending writes before reading.
    pub fn replay(&mut self) -> io::Result<WalReplayResult> {
        // Flush pending writes so replay sees all data
        self.sync()?;
        #[cfg(feature = "encrypt")]
        let key = self.encryption_key;
        #[cfg(not(feature = "encrypt"))]
        let key: Option<[u8; 32]> = None;
        Self::replay_dir_with_key(&self.dir, key)
    }

    /// Replay all records from a WAL directory.
    /// If corruption is detected, truncates the file to the last valid record.
    pub fn replay_dir(dir: impl AsRef<Path>) -> io::Result<WalReplayResult> {
        Self::replay_dir_with_key(dir, None)
    }

    /// Replay all records from a WAL directory with an optional encryption key.
    #[allow(unused_variables)]
    fn replay_dir_with_key(
        dir: impl AsRef<Path>,
        encryption_key: Option<[u8; 32]>,
    ) -> io::Result<WalReplayResult> {
        let dir = dir.as_ref();
        let mut records = Vec::new();

        let wal_path = dir.join("current.wal");
        if !wal_path.exists() {
            return Ok(WalReplayResult {
                records,
                had_duplicates: false,
                gaps: Vec::new(),
            });
        }

        let file = File::open(&wal_path)?;
        let mut reader = BufReader::new(file);

        // Track position for truncation on corruption
        let mut last_valid_pos: u64 = 0;
        let mut needs_truncate = false;

        loop {
            match Self::read_record(&mut reader, encryption_key) {
                Ok(record) => {
                    last_valid_pos = reader.stream_position().unwrap_or(last_valid_pos);
                    records.push(record);
                }
                Err(WalReadError::Eof) => break,
                Err(WalReadError::CrcMismatch { expected, actual }) => {
                    warn!(
                        expected = format!("{expected:#x}"),
                        actual = format!("{actual:#x}"),
                        last_valid_pos,
                        "WAL CRC mismatch -- truncating to last valid record"
                    );
                    needs_truncate = true;
                    break;
                }
                Err(WalReadError::Truncated) => {
                    warn!(
                        last_valid_pos,
                        "WAL truncated -- incomplete record, truncating"
                    );
                    needs_truncate = true;
                    break;
                }
                Err(WalReadError::Io(e)) => {
                    warn!(error = %e, "WAL IO error, truncating");
                    needs_truncate = true;
                    break;
                }
                Err(WalReadError::InvalidData(msg)) => {
                    warn!(last_valid_pos, "WAL invalid data: {msg} -- truncating");
                    needs_truncate = true;
                    break;
                }
            }
        }

        // Drop the reader before truncating
        drop(reader);

        // Truncate the file to the last valid position
        if needs_truncate {
            let file = OpenOptions::new().write(true).open(&wal_path)?;
            file.set_len(last_valid_pos)?;
            file.sync_data()?;
            info!(
                path = %wal_path.display(),
                truncated_to = last_valid_pos,
                valid_records = records.len(),
            );
        }

        // A4: Sort records by seq_no (WAL should be sequential, but torn-write may cause gaps)
        records.sort_by_key(|r| r.seq_no);

        // A4: Detect duplicates and gaps
        let mut had_duplicates = false;
        let mut gaps = Vec::new();
        let mut deduplicated = Vec::new();
        let mut last_seq: Option<u64> = None;

        for record in records {
            if let Some(prev_seq) = last_seq {
                if record.seq_no == prev_seq {
                    // Duplicate seq_no (idempotent write)
                    warn!(
                        seq_no = record.seq_no,
                        "duplicate seq_no detected, skipping"
                    );
                    had_duplicates = true;
                    continue;
                } else if record.seq_no > prev_seq + 1 {
                    // Gap detected
                    let gap_start = prev_seq + 1;
                    warn!(
                        gap_start,
                        gap_end = record.seq_no - 1,
                        "seq_no gap detected"
                    );
                    gaps.push(gap_start);
                }
            }
            last_seq = Some(record.seq_no);
            deduplicated.push(record);
        }

        debug!(
            dir = %dir.display(),
            records = deduplicated.len(),
            had_duplicates,
            gaps = gaps.len(),
            "WAL replay complete"
        );

        Ok(WalReplayResult {
            records: deduplicated,
            had_duplicates,
            gaps,
        })
    }

    /// Read a single record from the WAL file.
    /// Supports both v1 (JSON) and v2 (FlatBuffers) formats.
    /// When `encryption_key` is provided, the payload is decrypted after CRC validation.
    #[allow(unused_variables)]
    fn read_record(
        reader: &mut impl Read,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<WalRecord, WalReadError> {
        // Read magic bytes
        let mut magic = [0u8; 2];
        match reader.read_exact(&mut magic) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(WalReadError::Eof),
            Err(e) => return Err(WalReadError::Io(e)),
        }

        // Validate magic bytes
        if magic != WAL_MAGIC_JSON && magic != WAL_MAGIC_FB {
            return Err(WalReadError::InvalidData(format!(
                "Bad magic: {:#04x?}",
                magic
            )));
        }

        // Read length
        let mut length_bytes = [0u8; 4];
        reader.read_exact(&mut length_bytes)?;
        let length = u32::from_be_bytes(length_bytes) as usize;

        // Sanity check
        if length > 100 * 1024 * 1024 {
            return Err(WalReadError::InvalidData(format!(
                "Record too large: {length} bytes"
            )));
        }

        // Read original_len (present in v2 format; v1 files have this field but it was
        // previously unused and zeroed -- we now use it to distinguish encrypted payloads)
        let mut original_len_bytes = [0u8; 4];
        reader.read_exact(&mut original_len_bytes)?;
        let original_len = u32::from_be_bytes(original_len_bytes) as usize;

        // Read payload
        let mut payload = vec![0u8; length];
        match reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalReadError::Truncated)
            }
            Err(e) => return Err(WalReadError::Io(e)),
        }

        // Read CRC
        let mut crc_bytes = [0u8; 4];
        match reader.read_exact(&mut crc_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalReadError::Truncated)
            }
            Err(e) => return Err(WalReadError::Io(e)),
        }
        let expected_crc = u32::from_be_bytes(crc_bytes);
        let actual_crc = crc32(&payload);

        if expected_crc != actual_crc {
            return Err(WalReadError::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        // Read stop marker
        let mut stop = [0u8; 1];
        reader.read_exact(&mut stop)?;
        if stop[0] != WAL_STOP_MARKER {
            return Err(WalReadError::InvalidData(format!(
                "Bad stop marker: {:#04x}",
                stop[0]
            )));
        }

        // Decrypt payload if encryption is enabled AND original_len > 0
        // (original_len > 0 is the signal that this payload was encrypted)
        #[cfg(feature = "encrypt")]
        let payload = {
            if let Some(key) = &encryption_key {
                if original_len > 0 {
                    decrypt_payload(key, &payload)
                        .map_err(|e| WalReadError::InvalidData(format!("Decryption failed: {e}")))?
                } else {
                    payload
                }
            } else {
                payload
            }
        };

        // Deserialize using format auto-detection
        decode_wal_record_auto(&magic, &payload, 0).map_err(WalReadError::InvalidData)
    }

    /// Find the maximum sequence number in existing WAL segments.
    #[allow(dead_code)]
    fn find_max_seq(dir: &Path) -> io::Result<u64> {
        let result = Self::replay_dir_with_key(dir, None)?;
        Ok(result.records.iter().map(|r| r.seq_no).max().unwrap_or(0))
    }

    /// Find the maximum sequence number in existing WAL segments (with encryption key).
    #[cfg(feature = "encrypt")]
    fn find_max_seq_with_key(dir: &Path, key: Option<[u8; 32]>) -> io::Result<u64> {
        let result = Self::replay_dir_with_key(dir, key)?;
        Ok(result.records.iter().map(|r| r.seq_no).max().unwrap_or(0))
    }

    /// Truncate the WAL (remove all records). Called after a successful snapshot.
    pub fn truncate(&mut self) -> io::Result<()> {
        // Close current writer
        self.writer = None;

        // Remove the WAL file
        let wal_path = self.dir.join("current.wal");
        if wal_path.exists() {
            fs::remove_file(&wal_path)?;
        }

        // Bump the file generation counter so nonces never repeat when using
        // the same encryption key after truncation.
        #[cfg(feature = "encrypt")]
        {
            self.file_generation = self.file_generation.wrapping_add(1);
            write_file_generation(&self.dir, self.file_generation)?;
        }

        // Reopen
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        self.writer = Some(BufWriter::new(file));
        self.next_seq = 1;

        #[cfg(feature = "encrypt")]
        {
            info!(file_generation = self.file_generation, "WAL truncated");
        }
        #[cfg(not(feature = "encrypt"))]
        {
            info!("WAL truncated");
        }
        Ok(())
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        if let Some(writer) = &mut self.writer {
            let _ = writer.flush();
            let _ = writer.get_ref().sync_data();
        }
    }
}

/// Errors during WAL reading.
enum WalReadError {
    Eof,
    CrcMismatch { expected: u32, actual: u32 },
    Truncated,
    Io(io::Error),
    InvalidData(String),
}

impl From<io::Error> for WalReadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// CRC32 checksum.
///
/// Uses `crc32fast` (SIMD/table-driven), which computes the exact same
/// CRC-32/ISO-HDLC polynomial (0xEDB88320, reflected, init 0xFFFFFFFF, final
/// XOR) as the previous bit-by-bit software loop — so checksums are byte-for-byte
/// identical and existing WAL files replay unchanged. The old per-byte loop ran
/// on the write path for every record; this replaces it with a hardware-friendly
/// implementation.
fn crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

// ---------------------------------------------------------------------------
// AES-256-GCM payload encryption helpers (behind "encrypt" feature)
// ---------------------------------------------------------------------------

/// Encrypt a WAL record payload using AES-256-GCM.
///
/// The nonce is derived from the file generation counter and sequence number:
/// `nonce = file_generation.to_be_bytes() || seq_no.to_be_bytes()` (12 bytes).
/// This guarantees each WAL record has a unique nonce: seq_no is monotonically
/// increasing within a WAL file, and file_generation is bumped on each truncate
/// so nonces never repeat across truncation cycles with the same key.
///
/// Returns: `[nonce(12B)][ciphertext(NB)][tag(16B)]`
#[cfg(feature = "encrypt")]
fn encrypt_payload(
    key: &[u8; 32],
    plaintext: &[u8],
    file_generation: u32,
    seq_no: u64,
) -> io::Result<Vec<u8>> {
    use aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    };

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid key: {e}")))?;

    // Build nonce: 4 bytes file_generation (big-endian) + 8 bytes seq_no (big-endian)
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[0..4].copy_from_slice(&file_generation.to_be_bytes());
    nonce_bytes[4..12].copy_from_slice(&seq_no.to_be_bytes());
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("encryption failed: {e}")))?;

    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);

    Ok(result)
}

/// Decrypt a WAL record payload using AES-256-GCM.
///
/// Expects: `[nonce(12B)][ciphertext(NB)][tag(16B)]`
/// Returns the plaintext.
#[cfg(feature = "encrypt")]
fn decrypt_payload(key: &[u8; 32], encrypted: &[u8]) -> io::Result<Vec<u8>> {
    use aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    };

    if encrypted.len() < 12 + 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "encrypted payload too short: {} bytes (need at least 28)",
                encrypted.len()
            ),
        ));
    }

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid key: {e}")))?;

    let nonce = Nonce::from_slice(&encrypted[..12]);
    let ciphertext_with_tag = &encrypted[12..];

    let plaintext = cipher.decrypt(nonce, ciphertext_with_tag).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("decryption failed (wrong key or corrupted data): {e}"),
        )
    })?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{NodeChangeEvent, TimedEvent};
    use nexora_id::{EventTime, NexoraId, PropertyValue};
    use nexora_value::Symbol;

    #[test]
    fn test_crc32_consistency() {
        let data = b"hello world";
        let c1 = crc32(data);
        let c2 = crc32(data);
        assert_eq!(c1, c2);
        assert_ne!(c1, 0);
    }

    /// Pin the exact CRC-32/ISO-HDLC values so the `crc32fast`-backed
    /// implementation stays byte-for-byte compatible with the previous bit-by-bit
    /// loop — this is what guarantees WAL files written before the swap still
    /// pass CRC validation on replay. These are the canonical CRC-32 outputs.
    #[test]
    fn test_crc32_known_vectors() {
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926); // canonical CRC-32 check value
        assert_eq!(crc32(b"hello world"), 0x0D4A_1185);

        // Cross-check against an independent bit-by-bit reference over random-ish
        // bytes, mirroring the exact algorithm the WAL used before the swap.
        fn reference_crc32(data: &[u8]) -> u32 {
            let mut crc: u32 = 0xFFFF_FFFF;
            for &byte in data {
                crc ^= byte as u32;
                for _ in 0..8 {
                    if crc & 1 != 0 {
                        crc = (crc >> 1) ^ 0xEDB8_8320;
                    } else {
                        crc >>= 1;
                    }
                }
            }
            !crc
        }
        let sample: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        assert_eq!(crc32(&sample), reference_crc32(&sample));
    }

    #[test]
    fn test_wal_append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        // Write records
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            let seq1 = wal
                .append(WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("speed"),
                            value: PropertyValue::Float(12.5),
                        },
                        EventTime::from_micros(1000),
                    ),
                })
                .unwrap();

            let seq2 = wal
                .append(WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("name"),
                            value: PropertyValue::String("FL-042".into()),
                        },
                        EventTime::from_micros(2000),
                    ),
                })
                .unwrap();

            assert_eq!(seq1, 1);
            assert_eq!(seq2, 2);
        }

        // Replay
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            let result = wal.replay().unwrap();
            assert_eq!(result.records.len(), 2);
            assert_eq!(result.records[0].seq_no, 1);
            assert_eq!(result.records[1].seq_no, 2);
            assert!(!result.had_duplicates);
            assert!(result.gaps.is_empty());
        }
    }

    #[test]
    fn test_wal_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(1),
                    },
                    EventTime::from_micros(100),
                ),
            })
            .unwrap();
            wal.truncate().unwrap();
        }

        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            let result = wal.replay().unwrap();
            assert!(result.records.is_empty());
        }
    }

    #[test]
    fn test_wal_persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        // Write, close, reopen, write more, verify sequence continues
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("a"),
                        value: PropertyValue::Integer(1),
                    },
                    EventTime::from_micros(100),
                ),
            })
            .unwrap();
        }

        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            let seq = wal
                .append(WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("b"),
                            value: PropertyValue::Integer(2),
                        },
                        EventTime::from_micros(200),
                    ),
                })
                .unwrap();
            assert_eq!(seq, 2); // Continues from previous seq
        }

        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            let result = wal.replay().unwrap();
            let records = &result.records;
            assert_eq!(records.len(), 2);
        }
    }

    #[test]
    fn test_wal_snapshot_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        let mut wal = WriteAheadLog::open(dir.path()).unwrap();

        // Write some events
        for i in 1..=5 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("tick"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64 * 1000),
                ),
            })
            .unwrap();
        }

        // Write a snapshot checkpoint
        wal.append(WalOperation::SnapshotCheckpoint {
            qid: qid.clone(),
            snapshot_time: EventTime::from_micros(3000),
        })
        .unwrap();

        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 6); // 5 events + 1 checkpoint

        // Find the checkpoint
        let checkpoint = records
            .iter()
            .find(|r| matches!(&r.operation, WalOperation::SnapshotCheckpoint { .. }));
        assert!(checkpoint.is_some());
    }

    #[test]
    fn test_wal_sync_policy() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Never).unwrap();

        for i in 1..=100 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("i"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        }

        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 100);
    }

    // ====== WL-006: WalSyncPolicy::Always ======
    #[test]
    fn test_wal_sync_policy_always() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();

        for i in 1..=10 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        }

        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 10);
    }

    // ====== WL-008: WalSyncPolicy::EveryN ======
    #[test]
    fn test_wal_sync_policy_every_n() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        let mut wal =
            WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::EveryN(5)).unwrap();

        for i in 1..=20 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        }

        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 20);
    }

    // ====== WL-011: Truncated record detection ======
    #[test]
    fn test_wal_truncated_record_detection() {
        let dir = tempfile::tempdir().unwrap();

        // Write valid data, then append truncated data
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            wal.append(WalOperation::NodeEvent {
                qid: NexoraId::new_random(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("ok"),
                        value: PropertyValue::Boolean(true),
                    },
                    EventTime::from_micros(1000),
                ),
            })
            .unwrap();
        }

        // Append truncated data (valid magic + length, but payload cut short)
        {
            use std::io::Write;
            let wal_path = dir.path().join("current.wal");
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            file.write_all(&WAL_MAGIC_FB).unwrap();
            file.write_all(&200u32.to_be_bytes()).unwrap(); // claims 200 bytes
            file.write_all(&0u32.to_be_bytes()).unwrap(); // original_len = 0
            file.write_all(&[0x01, 0x02]).unwrap(); // only 2 bytes
            file.flush().unwrap();
        }

        // Recovery should get 1 valid record
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 1);
    }

    // ====== WL-012: Empty WAL (nonexistent directory) ======
    #[test]
    fn test_wal_empty_replay() {
        let dir = tempfile::tempdir().unwrap();
        // Replay a directory with no WAL file
        let result = WriteAheadLog::replay_dir(dir.path()).unwrap();
        let records = &result.records;
        assert!(records.is_empty());
    }

    // ====== WL-014: Drop flushes without explicit sync ======
    #[test]
    fn test_wal_drop_flush() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        // Write records without calling sync(), then drop
        {
            let mut wal =
                WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Never).unwrap();
            for i in 1..=50 {
                wal.append(WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("x"),
                            value: PropertyValue::Integer(i),
                        },
                        EventTime::from_micros(i as u64),
                    ),
                })
                .unwrap();
            }
            // Drop happens here -- should flush
        }

        // Reopen and verify all records are visible
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 50, "Drop should flush all pending records");
    }

    // ====== WL-013: Large WAL (10000 records) ======
    #[test]
    fn test_wal_large_volume() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            for i in 0..10000 {
                wal.append(WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("tick"),
                            value: PropertyValue::Integer(i),
                        },
                        EventTime::from_micros(i as u64),
                    ),
                })
                .unwrap();
            }
        }

        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 10000);
    }

    // ====== double corruption recovery ======
    #[test]
    fn test_wal_double_corruption_recovery() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: write 3 records
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            for i in 1..=3 {
                wal.append(WalOperation::NodeEvent {
                    qid: NexoraId::new_random(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("x"),
                            value: PropertyValue::Integer(i),
                        },
                        EventTime::from_micros(i as u64),
                    ),
                })
                .unwrap();
            }
        }

        // Phase 2: append corrupted data
        {
            use std::io::Write;
            let wal_path = dir.path().join("current.wal");
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            file.write_all(&[0xFF, 0xFF]).unwrap(); // bad magic
            file.flush().unwrap();
        }

        // Phase 3: recover (should truncate corrupted part)
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 3);

        // Phase 4: append more corrupted data
        {
            use std::io::Write;
            let wal_path = dir.path().join("current.wal");
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            // Write a corrupted record: magic is correct, length claims 50, but only 1 byte is written
            file.write_all(&WAL_MAGIC_FB).unwrap();
            file.write_all(&50u32.to_be_bytes()).unwrap(); // claim payload length = 50
            file.write_all(&0u32.to_be_bytes()).unwrap(); // original_len = 0
            file.write_all(&[0xAA]).unwrap(); // only 1 byte
            file.flush().unwrap();
            // No CRC or stop marker -- intentionally corrupted
        }

        // Phase 5: recover again
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(
            records.len(),
            3,
            "Should still recover 3 records after double corruption"
        );
    }

    // ====== IngestOffsetCommit operation ======
    #[test]
    fn test_wal_ingest_offset_commit() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            wal.append(WalOperation::IngestOffsetCommit {
                ingest_id: "kafka-main".to_string(),
                partition: 3,
                offset: 12345,
            })
            .unwrap();
        }

        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        let records = &result.records;
        assert_eq!(records.len(), 1);
        match &records[0].operation {
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
    fn test_wal_raw_event_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let event = crate::raw_event::RawEvent::new(
            1_500,
            2_000,
            "kafka",
            "orders",
            Some(3),
            Some(42),
            Some("orders/eu".into()),
            serde_json::json!({"order_id": "A1", "amount": 99}),
        );

        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            wal.append(WalOperation::RawEvent {
                event: event.clone(),
            })
            .unwrap();
        }

        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();
        assert_eq!(result.records.len(), 1);
        match &result.records[0].operation {
            WalOperation::RawEvent { event: replayed } => {
                assert_eq!(*replayed, event, "raw event must survive WAL round-trip");
            }
            other => panic!("Expected RawEvent, got {other:?}"),
        }
    }

    // ====== Encryption tests (behind "encrypt" feature) ======
    #[cfg(feature = "encrypt")]
    #[test]
    fn test_wal_encrypt_append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();
        let key = [0x42u8; 32];

        // Write records with encryption
        {
            let mut wal =
                WriteAheadLog::open_with_encryption(dir.path(), WalSyncPolicy::Always, key)
                    .unwrap();
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("speed"),
                        value: PropertyValue::Float(12.5),
                    },
                    EventTime::from_micros(1000),
                ),
            })
            .unwrap();
        }

        // Replay with encryption key
        {
            let mut wal =
                WriteAheadLog::open_with_encryption(dir.path(), WalSyncPolicy::Always, key)
                    .unwrap();
            let result = wal.replay().unwrap();
            let records = &result.records;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].seq_no, 1);
        }

        // Replay WITHOUT encryption key -- should fail because raw payload is encrypted
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            // This will attempt to deserialize encrypted bytes as FlatBuffer/JSON,
            // which should fail with InvalidData.
            let result = wal.replay();
            let is_valid_record_count = match &result {
                Ok(records) => records.len() == 1,
                Err(_) => false,
            };
            assert!(
                result.is_err() || !is_valid_record_count,
                "reading encrypted WAL without key should not produce the original record"
            );
        }
    }

    #[cfg(feature = "encrypt")]
    #[test]
    fn test_wal_encrypt_wrong_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();
        let key1 = [0x42u8; 32];
        let key2 = [0x99u8; 32];

        // Write with key1
        {
            let mut wal =
                WriteAheadLog::open_with_encryption(dir.path(), WalSyncPolicy::Always, key1)
                    .unwrap();
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(1),
                    },
                    EventTime::from_micros(100),
                ),
            })
            .unwrap();
        }

        // Read with key2 -- should fail
        {
            let mut wal =
                WriteAheadLog::open_with_encryption(dir.path(), WalSyncPolicy::Always, key2)
                    .unwrap();
            let result = wal.replay().unwrap();
            let records = &result.records;
            assert!(
                records.is_empty(),
                "decryption with wrong key should result in no valid records"
            );
        }
    }

    #[cfg(feature = "encrypt")]
    #[test]
    fn test_wal_encrypt_multiple_records() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();
        let key = [0xAAu8; 32];

        {
            let mut wal =
                WriteAheadLog::open_with_encryption(dir.path(), WalSyncPolicy::Never, key).unwrap();
            for i in 1..=50 {
                wal.append(WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("count"),
                            value: PropertyValue::Integer(i),
                        },
                        EventTime::from_micros(i as u64 * 100),
                    ),
                })
                .unwrap();
            }
        }

        {
            let mut wal =
                WriteAheadLog::open_with_encryption(dir.path(), WalSyncPolicy::Never, key).unwrap();
            let result = wal.replay().unwrap();
            let records = &result.records;
            assert_eq!(records.len(), 50);
        }
    }

    #[cfg(feature = "encrypt")]
    #[test]
    fn test_encrypt_decrypt_payload_roundtrip() {
        let key = [0x77u8; 32];
        let plaintext = b"This is a test payload for WAL encryption.";

        let encrypted = encrypt_payload(&key, plaintext, 0, 42).unwrap();
        // Verify format: 12 (nonce) + len(plaintext) + 16 (tag)
        assert_eq!(encrypted.len(), 12 + plaintext.len() + 16);

        let decrypted = decrypt_payload(&key, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[cfg(feature = "encrypt")]
    #[test]
    fn test_decrypt_payload_detects_tampering() {
        let key = [0x77u8; 32];
        let plaintext = b"tamper-me";

        let mut encrypted = encrypt_payload(&key, plaintext, 0, 1).unwrap();
        // Flip a bit in the ciphertext
        encrypted[15] ^= 0x01;

        let result = decrypt_payload(&key, &encrypted);
        assert!(
            result.is_err(),
            "tampered ciphertext should fail decryption"
        );
    }

    // ====== A4: Torn-write replay tests ======

    /// A4: Detect duplicate seq_no (idempotent write)
    #[test]
    fn test_replay_detects_duplicate_seq() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        // Write 2 records normally
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(1),
                    },
                    EventTime::from_micros(100),
                ),
            })
            .unwrap();
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("y"),
                        value: PropertyValue::Integer(2),
                    },
                    EventTime::from_micros(200),
                ),
            })
            .unwrap();
            wal.sync().unwrap();
        }

        // Manually append a duplicate record with seq_no=1
        {
            use std::io::Write;
            let wal_path = dir.path().join("current.wal");
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();

            // Construct a duplicate record with seq_no=1
            let dup_record = WalRecord {
                seq_no: 1,
                version: 1,
                operation: WalOperation::NodeEvent {
                    qid: qid.clone(),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("dup"),
                            value: PropertyValue::Boolean(true),
                        },
                        EventTime::from_micros(300),
                    ),
                },
            };

            let fb_data = encode_wal_record_fb(&dup_record);
            let length = fb_data.len() as u32;
            let crc = crc32(&fb_data);

            file.write_all(&WAL_MAGIC_FB).unwrap();
            file.write_all(&length.to_be_bytes()).unwrap();
            file.write_all(&0u32.to_be_bytes()).unwrap(); // original_len = 0 (no encryption)
            file.write_all(&fb_data).unwrap();
            file.write_all(&crc.to_be_bytes()).unwrap();
            file.write_all(&[WAL_STOP_MARKER]).unwrap();
            file.flush().unwrap();
        }

        // Replay and verify duplicate detection
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            let result = wal.replay().unwrap();
            assert_eq!(
                result.records.len(),
                2,
                "should have 2 unique records after dedup"
            );
            assert!(result.had_duplicates, "should detect duplicates");
            assert!(result.gaps.is_empty(), "should have no gaps");
        }
    }

    /// A4: Detect seq_no gap (missing records)
    #[test]
    fn test_replay_detects_gap() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        // Manually write records with seq_no = 1, 2, 5 (gap at 3, 4)
        {
            use std::io::Write;
            let wal_path = dir.path().join("current.wal");
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&wal_path)
                .unwrap();

            for seq in &[1u64, 2u64, 5u64] {
                let record = WalRecord {
                    seq_no: *seq,
                    version: *seq,
                    operation: WalOperation::NodeEvent {
                        qid: qid.clone(),
                        event: TimedEvent::new(
                            NodeChangeEvent::PropertySet {
                                key: Symbol::new("x"),
                                value: PropertyValue::Integer(*seq as i64),
                            },
                            EventTime::from_micros(*seq * 100),
                        ),
                    },
                };

                let fb_data = encode_wal_record_fb(&record);
                let length = fb_data.len() as u32;
                let crc = crc32(&fb_data);

                file.write_all(&WAL_MAGIC_FB).unwrap();
                file.write_all(&length.to_be_bytes()).unwrap();
                file.write_all(&0u32.to_be_bytes()).unwrap();
                file.write_all(&fb_data).unwrap();
                file.write_all(&crc.to_be_bytes()).unwrap();
                file.write_all(&[WAL_STOP_MARKER]).unwrap();
            }
            file.flush().unwrap();
        }

        // Replay and verify gap detection
        {
            let mut wal = WriteAheadLog::open(dir.path()).unwrap();
            let result = wal.replay().unwrap();
            assert_eq!(result.records.len(), 3);
            assert!(!result.had_duplicates);
            assert_eq!(result.gaps.len(), 1, "should detect one gap");
            assert_eq!(result.gaps[0], 3, "gap should start at seq 3");
        }
    }

    /// A4: Empty WAL directory
    #[test]
    fn test_replay_empty_wal() {
        let dir = tempfile::tempdir().unwrap();
        // Don't create any WAL file
        let result = WriteAheadLog::replay_dir(dir.path()).unwrap();
        assert!(result.records.is_empty());
        assert!(!result.had_duplicates);
        assert!(result.gaps.is_empty());
    }
}
