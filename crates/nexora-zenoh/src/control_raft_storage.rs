//! Control-plane Raft storage adapter (A2-6).
//!
//! openraft requires a single type implementing both `RaftLogStorage` and
//! `RaftStateMachine`. We have them as separate pieces (log storage in
//! `control_raft_log`, state machine in `control_raft_sm`), so this module
//! provides a wrapper that delegates to each.

use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{OptionalSend, RaftLogReader};

use crate::control_raft::ControlRaftTypeConfig;
use crate::control_raft_log::ControlLogStore;
use crate::control_raft_sm::ControlStateMachine;

/// Combined storage that delegates log ops to [`ControlLogStore`] and state
/// machine ops to [`ControlStateMachine`].
#[derive(Clone)]
pub struct ControlStorage {
    log: ControlLogStore,
    sm: ControlStateMachine,
}

impl ControlStorage {
    pub fn new(log: ControlLogStore, sm: ControlStateMachine) -> Self {
        Self { log, sm }
    }
}

impl RaftLogReader<ControlRaftTypeConfig> for ControlStorage {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<
        Vec<openraft::Entry<ControlRaftTypeConfig>>,
        openraft::StorageError<crate::control_raft::ControlNodeId>,
    > {
        self.log.try_get_log_entries(range).await
    }
}

impl RaftLogStorage<ControlRaftTypeConfig> for ControlStorage {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<
        openraft::storage::LogState<ControlRaftTypeConfig>,
        openraft::StorageError<crate::control_raft::ControlNodeId>,
    > {
        self.log.get_log_state().await
    }

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<crate::control_raft::ControlNodeId>,
    ) -> Result<(), openraft::StorageError<crate::control_raft::ControlNodeId>> {
        self.log.save_vote(vote).await
    }

    async fn read_vote(
        &mut self,
    ) -> Result<
        Option<openraft::Vote<crate::control_raft::ControlNodeId>>,
        openraft::StorageError<crate::control_raft::ControlNodeId>,
    > {
        self.log.read_vote().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<ControlRaftTypeConfig>,
    ) -> Result<(), openraft::StorageError<crate::control_raft::ControlNodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<ControlRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.log.append(entries, callback).await
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<crate::control_raft::ControlNodeId>,
    ) -> Result<(), openraft::StorageError<crate::control_raft::ControlNodeId>> {
        self.log.truncate(log_id).await
    }

    async fn purge(
        &mut self,
        log_id: openraft::LogId<crate::control_raft::ControlNodeId>,
    ) -> Result<(), openraft::StorageError<crate::control_raft::ControlNodeId>> {
        self.log.purge(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

impl RaftStateMachine<ControlRaftTypeConfig> for ControlStorage {
    type SnapshotBuilder = ControlStateMachine;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<crate::control_raft::ControlNodeId>>,
            openraft::StoredMembership<crate::control_raft::ControlNodeId, openraft::BasicNode>,
        ),
        openraft::StorageError<crate::control_raft::ControlNodeId>,
    > {
        self.sm.applied_state().await
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<
        Vec<crate::control_raft::ControlResponse>,
        openraft::StorageError<crate::control_raft::ControlNodeId>,
    >
    where
        I: IntoIterator<Item = openraft::Entry<ControlRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.sm.apply(entries).await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.sm.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        Box<std::io::Cursor<Vec<u8>>>,
        openraft::StorageError<crate::control_raft::ControlNodeId>,
    > {
        self.sm.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<crate::control_raft::ControlNodeId, openraft::BasicNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<crate::control_raft::ControlNodeId>> {
        self.sm.install_snapshot(meta, snapshot).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::storage::Snapshot<ControlRaftTypeConfig>>,
        openraft::StorageError<crate::control_raft::ControlNodeId>,
    > {
        self.sm.get_current_snapshot().await
    }
}
