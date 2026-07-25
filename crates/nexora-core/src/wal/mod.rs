//! Write-Ahead Log (WAL) for crash recovery.
//!
//! The WAL is the first layer of the three-layer persistence model:
//! 1. **Local WAL** (this module): Fast local recovery, <1s
//! 2. **Zenoh Storage Plugin WAL**: Cross-node persistence
//! 3. **ReductStore WAL**: Blob data persistence
//!
//! The local WAL is the **authoritative source** for recovery.
//! On crash recovery, the WAL is replayed first, then missing events
//! are synced downstream to the Storage Plugin.

mod log;
pub mod record;

pub(crate) use log::{spawn_group_flusher, FlusherHandle};
pub use log::{WalReplayResult, WalSyncPolicy, WriteAheadLog};
pub use record::{WalOperation, WalRecord};
