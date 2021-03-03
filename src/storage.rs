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

use protobuf::Message;
use raft::eraftpb::SnapshotMetadata;
use raft::prelude::ConfState;
use raft::prelude::Entry;
use raft::prelude::HardState;
use raft::prelude::Snapshot;
use raft::Error;
use raft::RaftState;
use raft::Result;
use raft::Storage;
use raft::StorageError;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;

pub struct RaftStorageCore {
    raft_state: RaftState,
    entries: Vec<Entry>,
    applied_index: u64,
    // We don't take snapshot of the controller (as the state machine)
    // which has its own sync mechanism. So, we only have metadata for
    // raft itself here.
    snapshot_metadata: SnapshotMetadata,
    engine: StorageEngine,
}

impl RaftStorageCore {
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        let mut engine = StorageEngine::new(data_dir).await;
        let raft_state = engine.get_raft_state().await;
        let applied_index = engine.get_applied_index().await;
        let snapshot_metadata = engine.get_snapshot_metadata().await;
        let entries = engine.get_entries().await;
        Self {
            raft_state,
            entries,
            applied_index,
            snapshot_metadata,
            engine,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();

        let meta = snapshot.mut_metadata();
        *meta = self.snapshot_metadata.clone();
        snapshot
    }

    pub async fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<()> {
        let mut meta = snapshot.take_metadata();
        let term = meta.term;
        let index = meta.index;

        if self.first_index() > index {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        self.snapshot_metadata = meta.clone();
        self.engine
            .set_snapshot_metadata(&self.snapshot_metadata)
            .await;

        self.mut_hard_state().term = term;
        self.mut_hard_state().commit = index;
        self.sync_hard_state().await;

        self.entries.clear();

        // Update conf states.
        self.raft_state.conf_state = meta.take_conf_state();
        self.engine
            .set_conf_state(&self.raft_state.conf_state)
            .await;
        Ok(())
    }

    pub fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index + 1,
        }
    }

    pub fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index,
        }
    }

    pub async fn append(&mut self, entries: &[Entry]) -> Result<()> {
        self.entries.extend_from_slice(entries);
        for ent in entries {
            self.engine.append(ent).await;
        }
        Ok(())
    }

    pub fn is_initialized(&self) -> bool {
        self.raft_state.conf_state != ConfState::default()
            || self.raft_state.hard_state != HardState::default()
    }

    pub fn mut_hard_state(&mut self) -> &mut HardState {
        &mut self.raft_state.hard_state
    }

    pub async fn set_hard_state(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
        self.sync_hard_state().await;
    }

    pub async fn sync_hard_state(&mut self) {
        self.engine
            .set_hard_state(&self.raft_state.hard_state)
            .await;
    }

    pub fn conf_state(&self) -> &ConfState {
        &self.raft_state.conf_state
    }

    pub async fn set_conf_state(&mut self, cs: ConfState) {
        self.raft_state.conf_state = cs;
        self.sync_conf_state().await;
    }

    pub async fn sync_conf_state(&mut self) {
        self.engine
            .set_conf_state(&self.raft_state.conf_state)
            .await;
    }

    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub async fn set_applied_index(&mut self, applied_index: u64) {
        self.applied_index = applied_index;
        self.sync_applied_index().await;
    }

    pub async fn sync_applied_index(&mut self) {
        self.engine.set_applied_index(self.applied_index).await;
    }

    pub async fn update_snapshot_metadata(&mut self) {
        // Use the latest applied_idx to construct the snapshot.
        let applied_idx = self.raft_state.hard_state.commit;
        let term = self.raft_state.hard_state.term;

        let meta = &mut self.snapshot_metadata;

        meta.index = applied_idx;
        meta.term = term;
        meta.set_conf_state(self.raft_state.conf_state.clone());

        self.sync_snapshot_metadata().await;
    }

    pub async fn sync_snapshot_metadata(&mut self) {
        self.engine
            .set_snapshot_metadata(&self.snapshot_metadata)
            .await;
    }
}

pub struct RaftStorage {
    pub core: RaftStorageCore,
}

impl RaftStorage {
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        Self {
            core: RaftStorageCore::new(data_dir).await,
        }
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> Result<RaftState> {
        Ok(self.core.raft_state.clone())
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let core = &self.core;
        if low < core.first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > core.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                core.last_index() + 1,
                high
            );
        }

        let offset = core.entries[0].index;
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let core = &self.core;
        if idx == core.snapshot_metadata.index {
            return Ok(core.snapshot_metadata.term);
        }

        if idx < core.first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }

        let offset = core.entries[0].index;
        assert!(idx >= offset);
        if idx - offset >= core.entries.len() as u64 {
            return Err(Error::Store(StorageError::Unavailable));
        }
        Ok(core.entries[(idx - offset) as usize].term)
    }

    fn first_index(&self) -> Result<u64> {
        Ok(self.core.first_index())
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.core.last_index())
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        let core = &self.core;
        let snap = core.snapshot();
        if snap.get_metadata().index < request_index {
            Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable))
        } else {
            Ok(snap)
        }
    }
}

pub struct StorageEngine {
    hard_state_file: File,
    conf_state_file: File,
    applied_index_file: File,
    snapshot_metadata_file: File,
    entry_file: File,
}

impl StorageEngine {
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        let data_dir: &Path = data_dir.as_ref();
        let hard_state_path = data_dir.join("hard_state.data");
        let conf_state_path = data_dir.join("conf_state.data");
        let applied_index_path = data_dir.join("applied_index.data");
        let snapshot_metadata_path = data_dir.join("snapshot_metadata.data");
        let entry_path = data_dir.join("entry.data");

        let mut opts = fs::OpenOptions::new();
        let opts = opts.read(true).write(true).create(true);
        let hard_state_file = opts.open(hard_state_path).await.unwrap();
        let conf_state_file = opts.open(conf_state_path).await.unwrap();
        let applied_index_file = opts.open(applied_index_path).await.unwrap();
        let snapshot_metadata_file = opts.open(snapshot_metadata_path).await.unwrap();
        let entry_file = opts.append(true).open(entry_path).await.unwrap();
        Self {
            hard_state_file,
            conf_state_file,
            applied_index_file,
            snapshot_metadata_file,
            entry_file,
        }
    }

    async fn get_raft_state(&mut self) -> RaftState {
        let hard_state = self.get_hard_state().await;
        let conf_state = self.get_conf_state().await;
        RaftState {
            hard_state,
            conf_state,
        }
    }

    async fn get_hard_state(&mut self) -> HardState {
        let f = &mut self.hard_state_file;
        let mut buf = vec![];
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_to_end(&mut buf).await.unwrap();
        if buf.is_empty() {
            HardState::default()
        } else {
            HardState::parse_from_bytes(&buf[..]).unwrap()
        }
    }

    async fn get_conf_state(&mut self) -> ConfState {
        let f = &mut self.conf_state_file;
        let mut buf = vec![];
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_to_end(&mut buf).await.unwrap();
        if buf.is_empty() {
            ConfState::default()
        } else {
            ConfState::parse_from_bytes(&buf[..]).unwrap()
        }
    }

    pub async fn get_applied_index(&mut self) -> u64 {
        let f = &mut self.applied_index_file;
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_u64().await.unwrap_or_default()
    }

    pub async fn get_entries(&mut self) -> Vec<Entry> {
        let mut buf = vec![];
        let mut entries = vec![];
        let f = &mut self.entry_file;
        f.seek(SeekFrom::Start(0)).await.unwrap();
        while let Ok(n) = f.read_u64().await {
            buf.resize_with(n as usize, Default::default);
            f.read_exact(&mut buf[..]).await.unwrap();
            let entry = Entry::parse_from_bytes(&buf[..]).unwrap();
            entries.push(entry);
        }
        entries
    }

    pub async fn get_snapshot_metadata(&mut self) -> SnapshotMetadata {
        let f = &mut self.snapshot_metadata_file;
        let mut buf = vec![];
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_to_end(&mut buf).await.unwrap();
        SnapshotMetadata::parse_from_bytes(&buf[..]).unwrap()
    }

    pub async fn append(&mut self, entry: &Entry) {
        let data = entry.write_to_bytes().unwrap();
        let data = [&(data.len() as u64).to_be_bytes(), &data[..]].concat();
        self.entry_file.write_all(&data[..]).await.unwrap();
        self.entry_file.sync_all().await.unwrap();
    }

    pub async fn set_snapshot_metadata(&mut self, meta: &SnapshotMetadata) {
        let data = meta.write_to_bytes().unwrap();
        let f = &mut self.snapshot_metadata_file;
        f.set_len(0).await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.write_all(&data[..]).await.unwrap();
        f.sync_all().await.unwrap();
    }

    pub async fn set_hard_state(&mut self, state: &HardState) {
        let data = state.write_to_bytes().unwrap();
        let f = &mut self.hard_state_file;
        f.set_len(0).await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.write_all(&data[..]).await.unwrap();
        f.sync_all().await.unwrap();
    }

    pub async fn set_conf_state(&mut self, state: &ConfState) {
        let data = state.write_to_bytes().unwrap();
        let f = &mut self.conf_state_file;
        f.set_len(0).await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.write_all(&data[..]).await.unwrap();
        f.sync_all().await.unwrap();
    }

    pub async fn set_applied_index(&mut self, applied_index: u64) {
        let f = &mut self.applied_index_file;
        f.set_len(0).await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.write_u64(applied_index).await.unwrap();
        f.sync_all().await.unwrap();
    }
}

fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: Option<u64>) {
    if entries.len() <= 1 {
        return;
    }
    let max = match max {
        None => return,
        Some(max) => max,
    };

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&e| {
            if size == 0 {
                size += u64::from(e.compute_size());
                true
            } else {
                size += u64::from(e.compute_size());
                size <= max
            }
        })
        .count();

    entries.truncate(limit);
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_store_engine() {
        let temp_dir = std::env::current_dir().unwrap();
        let test_dir = temp_dir.join(".CITA-CLOUD-RAFT-TEST");
        fs::create_dir(&test_dir).await.unwrap();
        let mut engine = StorageEngine::new(&test_dir).await;
        {
            let len = engine.get_entries().await.len();
            let ent = Entry::default();
            engine.append(&ent).await;
            assert_eq!(engine.get_entries().await.len(), len + 1);
        }
        {
            let mut cs = ConfState::default();
            cs.mut_voters().push(20209281816);
            engine.set_conf_state(&cs).await;

            let raft_state = engine.get_raft_state().await;
            assert!(raft_state
                .conf_state
                .voters
                .iter()
                .any(|&v| v == 20209281816));
        }
        {
            let hs = HardState {
                term: 1,
                vote: 2,
                commit: 3,
                ..Default::default()
            };
            engine.set_hard_state(&hs).await;

            let raft_state = engine.get_raft_state().await;
            let hard_state = raft_state.hard_state;
            assert_eq!(hard_state.term, 1);
            assert_eq!(hard_state.vote, 2);
            assert_eq!(hard_state.commit, 3);
        }
        {
            use protobuf::SingularPtrField;

            let mut meta = SnapshotMetadata {
                index: 4,
                term: 5,
                ..Default::default()
            };
            let cs = ConfState {
                learners: vec![202103031437],
                ..Default::default()
            };
            meta.conf_state = SingularPtrField::some(cs.clone());
            engine.set_snapshot_metadata(&meta).await;

            let neta = engine.get_snapshot_metadata().await;
            assert_eq!(neta.index, 4);
            assert_eq!(neta.term, 5);
            assert_eq!(neta.conf_state.unwrap(), cs);
        }
        fs::remove_dir_all(test_dir).await.unwrap();
    }
}
