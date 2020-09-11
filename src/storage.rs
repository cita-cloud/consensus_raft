use raft::Storage;
use raft::RaftState;
use raft::Error;
use raft::StorageError;
use raft::prelude::Snapshot;
use raft::eraftpb::SnapshotMetadata;
use raft::prelude::Entry;
use raft::prelude::HardState;
use raft::prelude::ConfState;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use protobuf::Message;
use std::io::SeekFrom;
use raft::Result;


pub struct RaftStorageCore {
    raft_state: RaftState,
    entries: Vec<Entry>,
    snapshot_metadata: SnapshotMetadata,
    engine: StorageEngine,
    trigger_snap_unavailable: bool,
}

impl RaftStorageCore {
    pub async fn new() -> Self {
        let mut engine = StorageEngine::new().await;
        let raft_state = engine.get_raft_state().await;
        let snapshot_metadata = engine.get_snapshot_metadata().await;
        let entries = engine.get_entries().await;
        Self {
            raft_state,
            entries,
            snapshot_metadata,
            engine,
            trigger_snap_unavailable: false,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();

        // Use the latest applied_idx to construct the snapshot.
        let applied_idx = self.raft_state.hard_state.commit;
        let term = self.raft_state.hard_state.term;
        let meta = snapshot.mut_metadata();
        meta.index = applied_idx;
        meta.term = term;

        meta.set_conf_state(self.raft_state.conf_state.clone());
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
        self.engine.set_snapshot_metadata(&self.snapshot_metadata).await;

        self.mut_hard_state().term = term;
        self.mut_hard_state().commit = index;
        self.sync_hard_state().await;

        self.entries.clear();

        // Update conf states.
        self.raft_state.conf_state = meta.take_conf_state();
        self.engine.set_conf_state(&self.raft_state.conf_state).await;
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

    pub fn mut_hard_state(&mut self) -> &mut HardState {
        &mut self.raft_state.hard_state
    }

    pub async fn set_hard_state(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
        self.sync_hard_state().await;
    }

    pub async fn sync_hard_state(&mut self){
        self.engine.set_hard_state(&self.raft_state.hard_state).await;
    }

    pub async fn set_conf_state(&mut self, cs: ConfState) {
        self.raft_state.conf_state = cs;
        self.sync_conf_state().await;
    }

    pub async fn sync_conf_state(&mut self){
        self.engine.set_conf_state(&self.raft_state.conf_state).await;
    }


}

pub struct RaftStorage {
    pub core: RaftStorageCore,
}

impl RaftStorage {
    pub async fn new() -> Self {
        Self{ core: RaftStorageCore::new().await }
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> Result<RaftState> {
        Ok(self.core.raft_state.clone())
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let ref core = self.core;
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
        let ref core = self.core;
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

    /// Implements the Storage trait.
    fn last_index(&self) -> Result<u64> {
        Ok(self.core.last_index())
    } 

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        let ref core = self.core;
        let mut snap = core.snapshot();
        if snap.get_metadata().index < request_index {
            snap.mut_metadata().index = request_index;
        }
        Ok(snap)
    }

}


pub struct StorageEngine {
    hard_state_file: File,
    conf_state_file: File,
    snapshot_metadata_file: File,
    entry_file: File,
}

impl StorageEngine {
    pub async fn new() -> Self {
        let mut opts = fs::OpenOptions::new();
        let opts = opts
            .read(true)
            .write(true)
            .create(true);
        let hard_state_file = opts.open("hard_state.data").await.unwrap();
        let conf_state_file = opts.open("conf_state.data").await.unwrap();
        let snapshot_metadata_file = opts.open("snapshot_metadata.data").await.unwrap();
        let entry_file = opts.append(true).open("entry.data").await.unwrap();
        Self {
            hard_state_file,
            conf_state_file,
            snapshot_metadata_file,
            entry_file,
        }
    }

    pub async fn get_raft_state(&mut self) -> RaftState {
        RaftState {
            hard_state: self.get_hard_state().await,
            conf_state: self.get_conf_state().await,
        }
    }

    pub async fn get_hard_state(&mut self) -> HardState {
        let ref mut f = self.hard_state_file;
        let mut buf = vec![];
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_to_end(&mut buf).await.unwrap();
        protobuf::parse_from_bytes(&buf[..]).unwrap()
    }

    pub async fn get_conf_state(&mut self) -> ConfState {
        let ref mut f = self.conf_state_file;
        let mut buf = vec![];
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_to_end(&mut buf).await.unwrap();
        protobuf::parse_from_bytes(&buf[..]).unwrap()
    }

    pub async fn get_entries(&mut self) -> Vec<Entry> {
        let mut buf = vec![];
        let mut entries = vec![];
        while let Ok(n) = self.entry_file.read_u64().await {
            buf.resize_with(n as usize, Default::default);
            self.entry_file.read_exact(&mut buf[..]).await.unwrap();
            let entry = protobuf::parse_from_bytes(&buf[..]).unwrap();
            entries.push(entry);
        }
        entries
    }

    pub async fn get_snapshot_metadata(&mut self) -> SnapshotMetadata {
        let ref mut f = self.snapshot_metadata_file;
        let mut buf = vec![];
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.read_to_end(&mut buf).await.unwrap();
        protobuf::parse_from_bytes(&buf[..]).unwrap()
    }

    pub async fn append(&mut self, entry: &Entry) {
        let data = entry.write_to_bytes().unwrap();
        let data = [&(data.len() as u64).to_be_bytes(), &data[..]].concat();
        self.entry_file.write_all(&data[..]).await.unwrap();
        self.entry_file.sync_all().await.unwrap();
    }

    pub async fn set_snapshot_metadata(&mut self, meta: &SnapshotMetadata) {
        let data = meta.write_to_bytes().unwrap();
        let ref mut f = self.snapshot_metadata_file;
        f.set_len(0).await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.write_all(&data[..]).await.unwrap();
        f.sync_all().await.unwrap();
    }

    pub async fn set_hard_state(&mut self, state: &HardState) {
        let data = state.write_to_bytes().unwrap();
        let ref mut f = self.hard_state_file;
        f.set_len(0).await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.write_all(&data[..]).await.unwrap();
        f.sync_all().await.unwrap();
    }

    pub async fn set_conf_state(&mut self, state: &ConfState) {
        let data = state.write_to_bytes().unwrap();
        let ref mut f = self.conf_state_file;
        f.set_len(0).await.unwrap();
        f.seek(SeekFrom::Start(0)).await.unwrap();
        f.write_all(&data[..]).await.unwrap();
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
