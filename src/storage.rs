// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::BytesMut;
use cita_cloud_proto::common::ConsensusConfiguration;
use prost::Message;
use raft::prelude::ConfState;
use raft::prelude::Entry;
use raft::prelude::HardState;
use raft::prelude::RaftState;
use raft::prelude::Snapshot;
use raft::prelude::SnapshotMetadata;
use raft::prelude::Storage;
use raft::StorageError;
use slog::info;
use slog::warn;
use slog::Logger;
use std::cmp;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

const RAFT_SNAPSHOT_NAME: &str = "raft-snapshot";

#[derive(Debug)]
pub struct RaftStorage {
    raft_state: RaftState,
    snapshot_metadata: SnapshotMetadata,
    entries: Vec<Entry>,
    applied_index: u64,
    consensus_config: ConsensusConfiguration,
    snapshot_dir: PathBuf,
    logger: Logger,
}

impl RaftStorage {
    pub async fn new<P: AsRef<Path>>(snapshot_dir: P, logger: Logger) -> Self {
        let snapshot_dir = snapshot_dir.as_ref().to_path_buf();
        if !snapshot_dir.exists() {
            fs::create_dir_all(&snapshot_dir)
                .await
                .expect("cannot create raft wal log dir");
        }
        let mut store = Self {
            raft_state: RaftState::default(),
            snapshot_metadata: SnapshotMetadata::default(),
            entries: vec![],
            applied_index: 0,
            consensus_config: Default::default(),
            snapshot_dir,
            logger,
        };
        store.recover_from_snapshot().await;
        store
    }

    async fn recover_from_snapshot(&mut self) {
        let snapshot_path = self.snapshot_dir.join(RAFT_SNAPSHOT_NAME);
        if snapshot_path.exists() {
            let mut file = fs::OpenOptions::new()
                .read(true)
                .open(&snapshot_path)
                .await
                .unwrap();
            let mut log_data = vec![];
            file.read_to_end(&mut log_data).await.unwrap();
            if !log_data.is_empty() {
                let snapshot = Snapshot::decode_length_delimited(&mut log_data.as_slice()).unwrap();
                self.apply_snapshot(snapshot).unwrap();
                warn!(self.logger, "recover_from_snapshot");
            }
        }
    }

    // for Storage trait
    fn initial_state(&self) -> RaftState {
        self.raft_state.clone()
    }

    fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(ent) => ent.index,
            None => self.snapshot_metadata.get_index() + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(ent) => ent.index,
            None => self.snapshot_metadata.get_index(),
        }
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.snapshot_metadata.index {
            return Ok(self.snapshot_metadata.term);
        }

        let offset = self.first_index();
        if idx < offset {
            return Ok(self.raft_state.hard_state.term);
        }
        if idx > self.last_index() {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        Ok(self.entries[(idx - offset) as usize].term)
    }

    fn entries(&self, low: u64, high: u64, _: impl Into<Option<u64>>) -> raft::Result<Vec<Entry>> {
        if low < self.first_index() {
            return Err(raft::Error::Store(StorageError::Compacted));
        }

        if high > self.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                self.last_index() + 1,
                high
            );
        }

        let offset = self.entries[0].index;
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let ents = self.entries[lo..hi].to_vec();
        Ok(ents)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        if request_index > self.applied_index {
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }

        let mut snapshot = Snapshot::default();

        let meta = snapshot.mut_metadata();
        meta.index = self.applied_index;
        meta.term = self.term(self.applied_index).unwrap();
        meta.set_conf_state(self.raft_state.conf_state.clone());
        snapshot.set_data(self.consensus_config.encode_to_vec());

        Ok(snapshot)
    }

    // for updating raft state
    pub fn append_entries(&mut self, entries: &[Entry]) {
        let incoming_index = match entries.first() {
            Some(ent) => ent.index,
            None => return,
        };

        if incoming_index < self.first_index() {
            panic!(
                "overwrite compacted raft logs, compacted_index: {}, incoming_index: {}",
                self.first_index() - 1,
                incoming_index,
            );
        }
        if incoming_index > self.last_index() + 1 {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                incoming_index,
            );
        }
        if entries.iter().any(|e| e.entry_type == 0) {
            let drain = cmp::min(
                self.entries.len(),
                (self.applied_index + 1 - self.first_index()) as usize,
            );
            self.entries.drain(..drain);
        }
        self.entries.extend_from_slice(entries);
    }

    pub fn update_hard_state(&mut self, hard_state: HardState) {
        self.raft_state.hard_state = hard_state;
    }

    pub fn update_conf_state(&mut self, conf_state: ConfState) {
        self.raft_state.conf_state = conf_state;
    }

    pub async fn update_committed_index(&mut self, committed_index: u64) {
        let mut hard_state = self.raft_state.hard_state.clone();
        hard_state.commit = committed_index;
        self.update_hard_state(hard_state);
    }

    pub fn advance_applied_index(&mut self, applied_index: u64) {
        self.applied_index = applied_index;
    }

    pub fn update_block_height(&mut self, h: u64) {
        self.consensus_config.height = h;
    }

    pub fn update_consensus_config(&mut self, config: ConsensusConfiguration) {
        self.consensus_config = config;
    }

    // functions
    pub fn is_initialized(&self) -> bool {
        self.raft_state.conf_state != ConfState::default()
            || self.raft_state.hard_state != HardState::default()
    }

    pub fn get_applied_index(&self) -> u64 {
        self.applied_index
    }

    pub fn get_conf_state(&self) -> &ConfState {
        &self.raft_state.conf_state
    }

    pub fn get_block_height(&self) -> u64 {
        self.consensus_config.height
    }

    pub fn get_block_interval(&self) -> u32 {
        self.consensus_config.block_interval
    }

    pub fn get_validators(&self) -> &[Vec<u8>] {
        &self.consensus_config.validators
    }

    pub async fn store_snapshot(&mut self) {
        if !self.snapshot_dir.exists() {
            fs::create_dir_all(&self.snapshot_dir)
                .await
                .expect("cannot create raft wal log dir");
        }
        let snapshot_path = self.snapshot_dir.join(RAFT_SNAPSHOT_NAME);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&snapshot_path)
            .await
            .unwrap();
        let snapshot = self.snapshot(self.applied_index).unwrap();
        let mut buf = BytesMut::new();
        snapshot.encode_length_delimited(&mut buf).unwrap();
        file.write_all(&buf).await.unwrap();
    }

    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<(), StorageError> {
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        if self.snapshot_metadata.get_index() > index {
            warn!(
                self.logger,
                "Ignore outdated incoming snapshot with index `{}`, current snapshot index is `{}`.",
                self.snapshot_metadata.get_index(),
                index,
            );
            return Err(StorageError::SnapshotOutOfDate);
        }

        self.consensus_config = ConsensusConfiguration::decode(snapshot.data.as_slice())
            .expect("decode snapshot data failed");
        self.snapshot_metadata = meta.clone();
        self.raft_state.hard_state.term = cmp::max(self.raft_state.hard_state.term, meta.term);
        self.raft_state.hard_state.commit = cmp::max(self.raft_state.hard_state.commit, index);
        self.applied_index = index;
        self.raft_state.conf_state = meta.take_conf_state();

        self.entries.clear();

        info!(
            self.logger,
            "apply_snapshot index: {} conf_state: {:?}",
            self.applied_index,
            self.raft_state.conf_state
        );
        Ok(())
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.initial_state())
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index())
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.term(idx)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        self.entries(low, high, max_size)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        self.snapshot(request_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn new_store<P: AsRef<Path>>(log_dir: &P) -> RaftStorage {
        let logger = {
            use sloggers::file::FileLoggerBuilder;
            use sloggers::types::Severity;
            use sloggers::Build as _;

            let log_level = Severity::Debug;
            let log_path = log_dir.as_ref().join("raft-test.log");
            let mut log_builder = FileLoggerBuilder::new(&log_path);
            log_builder.level(log_level);
            log_builder.build().expect("can't build terminal logger")
        };
        RaftStorage::new(log_dir, logger).await
    }

    fn entry(term: u64, index: u64) -> Entry {
        Entry {
            term,
            index,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_advance_applied_index() {
        let log_dir = tempdir().unwrap();
        let mut store = new_store(&log_dir).await;
        store.advance_applied_index(42);
        assert_eq!(store.applied_index, 42);
    }

    #[tokio::test]
    async fn test_update_conf_state() {
        let log_dir = tempdir().unwrap();
        let mut store = new_store(&log_dir).await;
        let conf_state = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![5, 6, 7],
            ..Default::default()
        };
        store.update_conf_state(conf_state.clone());
        assert_eq!(store.raft_state.conf_state, conf_state);
    }

    #[tokio::test]
    async fn test_update_hard_state() {
        let log_dir = tempdir().unwrap();
        let mut store = new_store(&log_dir).await;
        let hard_state = HardState {
            term: 3,
            commit: 5,
            vote: 1,
        };
        store.update_hard_state(hard_state.clone());
        assert_eq!(store.raft_state.hard_state, hard_state);
    }

    #[tokio::test]
    async fn test_update_consensus_config() {
        let log_dir = tempdir().unwrap();
        let mut store = new_store(&log_dir).await;
        let consensus_config = ConsensusConfiguration {
            height: 1024,
            block_interval: 9,
            validators: vec![b"1234".to_vec(), b"5678".to_vec()],
        };
        store.update_consensus_config(consensus_config.clone());
        assert_eq!(store.consensus_config, consensus_config);
    }

    #[tokio::test]
    async fn test_empty_snapshot() {
        let log_dir = tempdir().unwrap();
        let mut store = new_store(&log_dir).await;

        let snapshot = store.snapshot(0).unwrap();
        store.apply_snapshot(snapshot).unwrap();
        store = new_store(&log_dir).await;
        assert!(store.entries.is_empty());
        assert_eq!(store.raft_state.hard_state, HardState::default());
        assert_eq!(store.raft_state.conf_state, ConfState::default());
        assert_eq!(
            store.snapshot_metadata,
            SnapshotMetadata {
                conf_state: None,
                index: 0,
                term: 0,
            }
        );
        assert_eq!(store.applied_index, 0);
    }

    #[tokio::test]
    async fn test_snapshot_committed_higher_than_applied() {
        let log_dir = tempdir().unwrap();
        let mut store = new_store(&log_dir).await;
        store.append_entries(&[
            entry(1, 1),
            entry(2, 2),
            entry(2, 3),
            entry(2, 4),
            entry(3, 5),
        ]);
        store.update_hard_state(HardState {
            term: 3,
            vote: 0,
            commit: 5,
        });
        store.advance_applied_index(3);
        let snapshot = store.snapshot(3).unwrap();
        // apply_snapshot will clear entries
        store.apply_snapshot(snapshot).unwrap();
        // This commit index should not shrink to 3.
        assert_eq!(store.raft_state.hard_state.commit, 5);
        assert_eq!(store.raft_state.hard_state.term, 3);
        assert_eq!(store.entries, &[]);
    }
}
