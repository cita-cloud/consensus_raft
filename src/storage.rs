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

use raft::prelude::ConfState;
use raft::prelude::Entry;
use raft::prelude::HardState;
use raft::prelude::RaftState;
use raft::prelude::Snapshot;
use raft::prelude::SnapshotMetadata;
use raft::prelude::Storage;
use raft::StorageError;
use std::cmp;
use std::convert::TryInto;
use std::path::Path;
use std::path::PathBuf;

use prost::Message;

use tokio::fs;
use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::SeekFrom;

use std::convert::TryFrom;

use bytes::Buf;

use bytes::BufMut;
use bytes::BytesMut;

use thiserror::Error as ThisError;

const NO_LIMIT: u64 = u64::MAX;
// raft-log.backup, raft-log.1.backup, ... , raft-log.${MAX_LOG_FILE_PRESERVED}.backup
const MAX_LOG_FILE_PRESERVED: u64 = 5;
const WAL_ACTIVE_LOG_NAME: &str = "raft-wal-log.active";
const WAL_BACKUP_LOG_NAME: &str = "raft-wal-log.backup";
// Use as a temp file for creating new active log
// to ensure snapshot in active log is fully written.
const WAL_TEMP_LOG_NAME: &str = "raft-wal-log.temp";

#[repr(u8)]
#[derive(Debug)]
enum WalOpType {
    UpdateHardState = 1,
    UpdateConfState = 2,
    ApplySnapshot = 3,
    AppendEntries = 4,
    AdvanceAppliedIndex = 5,
}

#[derive(ThisError, Debug)]
enum DecodeLogError {
    #[error("provide log data is insufficient")]
    InsufficientData,
    #[error("can't parse wal-op type, data may be corrupted")]
    CorruptedWalOpType,
    #[error("can't parse wal-op data, data may be corrupted, error: `{0}`")]
    CorruptedWalData(#[from] prost::DecodeError),
}

impl TryFrom<u8> for WalOpType {
    type Error = DecodeLogError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let ty = match value {
            1 => Self::UpdateHardState,
            2 => Self::UpdateConfState,
            3 => Self::ApplySnapshot,
            4 => Self::AppendEntries,
            5 => Self::AdvanceAppliedIndex,
            _ => return Err(DecodeLogError::CorruptedWalOpType),
        };
        Ok(ty)
    }
}

#[derive(Debug)]
struct LogFile {
    file: File,
    path: PathBuf,
}

impl LogFile {
    async fn create<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        preserve_path(&path, MAX_LOG_FILE_PRESERVED).await;
        let file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        Ok(Self {
            file,
            path: path.as_ref().to_path_buf(),
        })
    }

    async fn create_truncate<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        Ok(Self {
            file,
            path: path.as_ref().to_path_buf(),
        })
    }

    async fn open<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let path = path.as_ref().to_path_buf();
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        Ok(Self { file, path })
    }

    async fn rename_to(&mut self, file_name: &str) {
        let new_path = self.path.with_file_name(file_name);
        preserve_path(&new_path, MAX_LOG_FILE_PRESERVED).await;
        fs::rename(&self.path, &new_path).await.unwrap();
        self.path = new_path;
    }

    async fn flush(&mut self) {
        self.file.flush().await.unwrap()
    }

    async fn write_all(&mut self, src: &[u8]) {
        self.file.write_all(src).await.unwrap();
    }

    #[allow(unused)]
    async fn seek(&mut self, pos: SeekFrom) {
        self.file.seek(pos).await.unwrap();
    }

    #[allow(unused)]
    async fn len(&self) -> u64 {
        let metadata = self.file.metadata().await.unwrap();
        metadata.len()
    }

    async fn set_len(&mut self, len: u64) {
        self.file.set_len(len).await.unwrap();
    }

    async fn read_to_end(&mut self, buf: &mut Vec<u8>) {
        self.file.read_to_end(buf).await.unwrap();
    }

    fn file_name(&self) -> String {
        self.path.file_name().unwrap().to_string_lossy().to_string()
    }
}

#[derive(Debug)]
struct WalStorageCore {
    raft_state: RaftState,
    // We don't take snapshot of the statemachine(controller),
    // which has its own sync mechanism.
    snapshot_metadata: SnapshotMetadata,

    entries: Vec<Entry>,
    applied_index: u64,

    log_dir: PathBuf,

    active_log: Option<LogFile>,
}

impl WalStorageCore {
    async fn new<P: AsRef<Path>>(log_dir: P) -> Self {
        let log_dir = log_dir.as_ref().to_path_buf();
        if !log_dir.exists() {
            fs::create_dir_all(&log_dir).await.unwrap();
        }
        let mut store = Self {
            raft_state: RaftState::default(),
            snapshot_metadata: SnapshotMetadata::default(),
            entries: vec![],
            applied_index: 0,
            log_dir,
            active_log: None,
        };

        let active_path = store.log_dir.join(WAL_ACTIVE_LOG_NAME);
        if active_path.exists() {
            println!("recover from active_path.");
            let mut active_log = LogFile::open(active_path).await.unwrap();
            // Active log may have corrupt log tail.
            store.recover_from_log(&mut active_log, true).await;
            store.active_log.replace(active_log);
        } else {
            let backup_path = store.log_dir.join(WAL_BACKUP_LOG_NAME);
            if backup_path.exists() {
                println!("recover from backup_path.");
                let mut bakcup_log = LogFile::open(backup_path).await.unwrap();
                // Backup log should not contains corrupt data.
                store.recover_from_log(&mut bakcup_log, false).await;
            } else {
                println!("create a new wal log set.");
            }
            store.generate_new_active_log().await;
        }
        store
    }

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
            return Err(raft::Error::Store(StorageError::Compacted));
        }

        if idx > self.last_index() {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        Ok(self.entries[(idx - offset) as usize].term)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        let max_size = max_size.into();
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
        let mut ents = self.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    async fn recover_from_log(&mut self, log_file: &mut LogFile, allow_corrupt_log_tail: bool) {
        let mut log_data = vec![];
        log_file.read_to_end(&mut log_data).await;

        if log_data.is_empty() {
            return;
        }

        let mut processing_buf = log_data.as_slice();
        let mut processed = 0usize;
        while let Ok(consumed) = self.recover_one(processing_buf) {
            processed += consumed;
            processing_buf.advance(consumed);
        }

        if processed != log_data.len() {
            if allow_corrupt_log_tail {
                println!(
                    "raft log is corrupted at {}, total {} bytes",
                    processed,
                    log_data.len()
                );
                println!("truncate and backup active log");

                // backup corrupt data
                let backup_path = self.log_dir.join("corrupt-raft-log.backup");
                let mut backup_file = LogFile::create(backup_path).await.unwrap();
                backup_file.write_all(log_data.as_mut_slice()).await;
                backup_file.flush().await;

                // truncate corrupt data
                log_file.set_len(processed as u64).await;
            } else {
                panic!("log file `{}` is corrupt", log_file.file_name());
            }
        }
    }

    fn recover_one(&mut self, mut log_data: &[u8]) -> Result<usize, DecodeLogError> {
        let remaining = log_data.remaining();
        if remaining == 0 {
            return Err(DecodeLogError::InsufficientData);
        }

        let ty: WalOpType = log_data.get_u8().try_into()?;

        use WalOpType::*;
        match ty {
            UpdateHardState => {
                let hard_state = HardState::decode_length_delimited(&mut log_data)?;
                self.update_hard_state(hard_state);
            }
            UpdateConfState => {
                let conf_state = ConfState::decode_length_delimited(&mut log_data)?;
                self.update_conf_state(conf_state);
            }
            ApplySnapshot => {
                let snapshot = Snapshot::decode_length_delimited(&mut log_data)?;
                let _ = self.apply_snapshot(snapshot);
            }
            AppendEntries => {
                let length = prost::decode_length_delimiter(&mut log_data)?;
                let mut entries = Vec::with_capacity(length);
                for _ in 0..length {
                    let ent = Entry::decode_length_delimited(&mut log_data)?;
                    entries.push(ent);
                }
                self.append_entries(&entries);
            }
            AdvanceAppliedIndex => {
                let applied_index = log_data.get_u64();
                self.advance_applied_index(applied_index);
            }
        }
        let consumed = remaining - log_data.remaining();
        Ok(consumed)
    }

    fn append_entries(&mut self, entries: &[Entry]) {
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

        let offset = incoming_index - self.first_index();
        self.entries.truncate(offset as usize);
        self.entries.extend_from_slice(entries);
    }

    async fn log_append_entries(&mut self, entries: &[Entry]) {
        let mut buf = BytesMut::new();
        buf.put_u8(WalOpType::AppendEntries as u8);

        let length = entries.len();
        prost::encode_length_delimiter(length, &mut buf).unwrap();

        for ent in entries {
            ent.encode_length_delimited(&mut buf).unwrap();
        }
        self.mut_active_log().write_all(&buf).await;
    }

    fn update_hard_state(&mut self, hard_state: HardState) {
        self.raft_state.hard_state = hard_state;
    }

    async fn log_update_hard_state(&mut self, hard_state: &HardState) {
        let mut buf = BytesMut::new();
        buf.put_u8(WalOpType::UpdateHardState as u8);
        hard_state.encode_length_delimited(&mut buf).unwrap();
        self.mut_active_log().write_all(&buf).await;
    }

    fn update_conf_state(&mut self, conf_state: ConfState) {
        self.raft_state.conf_state = conf_state;
    }

    async fn log_update_conf_state(&mut self, conf_state: &ConfState) {
        let mut buf = BytesMut::new();
        buf.put_u8(WalOpType::UpdateConfState as u8);
        conf_state.encode_length_delimited(&mut buf).unwrap();
        self.mut_active_log().write_all(&buf).await;
    }

    fn advance_applied_index(&mut self, applied_index: u64) {
        self.applied_index = applied_index;
    }

    async fn log_advance_applied_index(&mut self, applied_index: u64) {
        let mut buf = BytesMut::new();
        buf.put_u8(WalOpType::AdvanceAppliedIndex as u8);
        buf.put_u64(applied_index);
        self.mut_active_log().write_all(&buf).await;
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
        // This must exist.
        meta.term = self.term(self.applied_index).unwrap();

        meta.set_conf_state(self.raft_state.conf_state.clone());

        Ok(snapshot)
    }

    fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<(), StorageError> {
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        // We allow snapshot to at least as old as the existing snapshot.
        // This may occur when creating a new active log without new applied entry
        // than the one in snapshot.
        if self.snapshot_metadata.get_index() > index {
            return Err(StorageError::SnapshotOutOfDate);
        }

        self.snapshot_metadata = meta.clone();

        self.raft_state.hard_state.term = cmp::max(self.raft_state.hard_state.term, meta.term);
        // This snapshot may be produced by this peer and is used for compact.
        // In this case, index is the applied index, which may be less than committed index.
        self.raft_state.hard_state.commit = cmp::max(self.raft_state.hard_state.commit, index);

        self.applied_index = index;

        // +1 for exclusive end
        let compacted = cmp::min(
            self.entries.len(),
            (index + 1 - self.first_index()) as usize,
        );
        self.entries.drain(..compacted);

        // Update conf states.
        self.raft_state.conf_state = meta.take_conf_state();

        println!("restore snapshot");

        Ok(())
    }

    async fn log_apply_snapshot(&mut self, snapshot: &Snapshot) {
        let mut buf = BytesMut::new();
        buf.put_u8(WalOpType::ApplySnapshot as u8);
        snapshot.encode_length_delimited(&mut buf).unwrap();
        self.mut_active_log().write_all(&buf).await;
    }

    // This is also used for log compaction.
    async fn generate_new_active_log(&mut self) {
        if let Some(mut old_log) = self.active_log.take() {
            old_log.flush().await;
            old_log.rename_to(WAL_BACKUP_LOG_NAME).await;
        }

        let temp_log_path = self.log_dir.join(WAL_TEMP_LOG_NAME);
        self.active_log
            .replace(LogFile::create_truncate(temp_log_path).await.unwrap());

        let snapshot = self.snapshot(self.applied_index).unwrap();
        self.log_apply_snapshot(&snapshot).await;
        self.apply_snapshot(snapshot).unwrap();

        // TODO: clone due to borrow checker, maybe remove it.
        // Persist unapplied entries.
        self.log_append_entries(&self.entries.clone()).await;
        // This is used for update committed index, because the snapshot only contains applied index.
        self.log_update_hard_state(&self.raft_state.hard_state.clone())
            .await;

        let acitve_log = self.mut_active_log();
        acitve_log.flush().await;
        acitve_log.rename_to(WAL_ACTIVE_LOG_NAME).await;
    }

    // panic if active log is none
    fn mut_active_log(&mut self) -> &mut LogFile {
        self.active_log.as_mut().unwrap()
    }

    async fn flush(&mut self) {
        self.mut_active_log().flush().await;
    }
}

#[derive(Debug)]
pub struct WalStorage(WalStorageCore);

// The public interface, the WAL operations.
impl WalStorage {
    pub async fn new<P: AsRef<Path>>(log_dir: P) -> Self {
        let core = WalStorageCore::new(log_dir).await;
        WalStorage(core)
    }

    pub fn is_initialized(&self) -> bool {
        self.0.raft_state.conf_state != ConfState::default()
            || self.0.raft_state.hard_state != HardState::default()
    }

    pub fn get_applied_index(&self) -> u64 {
        self.0.applied_index
    }

    pub fn get_conf_state(&self) -> &ConfState {
        &self.0.raft_state.conf_state
    }

    pub async fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), StorageError> {
        self.0.log_apply_snapshot(&snapshot).await;
        self.0.flush().await;
        self.0.apply_snapshot(snapshot)
    }

    pub async fn append_entries(&mut self, entries: &[Entry]) {
        self.0.log_append_entries(entries).await;
        self.0.flush().await;
        self.0.append_entries(entries);
    }

    pub async fn update_conf_state(&mut self, conf_state: ConfState) {
        self.0.log_update_conf_state(&conf_state).await;
        self.0.flush().await;
        self.0.update_conf_state(conf_state);
    }

    pub async fn update_hard_state(&mut self, hard_state: HardState) {
        self.0.log_update_hard_state(&hard_state).await;
        self.0.flush().await;
        self.0.update_hard_state(hard_state);
    }

    pub async fn update_committed_index(&mut self, committed_index: u64) {
        let mut hard_state = self.0.raft_state.hard_state.clone();
        hard_state.commit = committed_index;
        self.update_hard_state(hard_state).await;
    }

    pub async fn advance_applied_index(&mut self, applied_index: u64) {
        self.0.log_advance_applied_index(applied_index).await;
        self.0.flush().await;
        self.0.advance_applied_index(applied_index);
    }
}

impl Storage for WalStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.0.initial_state())
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.0.first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.0.last_index())
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.0.term(idx)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        self.0.entries(low, high, max_size)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        self.0.snapshot(request_index)
    }
}

fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: Option<u64>) {
    if entries.len() <= 1 {
        return;
    }
    let max = match max {
        None | Some(NO_LIMIT) => return,
        Some(max) => max,
    };

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&e| {
            if size == 0 {
                size += e.encoded_len() as u64;
                true
            } else {
                size += e.encoded_len() as u64;
                size <= max
            }
        })
        .count();

    entries.truncate(limit);
}

// Get the next log path.
fn next_log_path<P: AsRef<Path>>(log_path: P) -> PathBuf {
    let buf = log_path.as_ref().to_path_buf();
    let log_name = buf.file_name().unwrap().to_string_lossy();
    let log_name_components = log_name.split_terminator('.').collect::<Vec<_>>();
    let next_log_name = match log_name_components.as_slice() {
        [base_name, count, ext] => {
            let count = count.parse::<u64>().unwrap();
            format!("{}.{}.{}", base_name, count + 1, ext)
        }
        [base_name, ext] => {
            format!("{}.{}.{}", base_name, 1, ext)
        }
        unexpected => {
            panic!("unexpected log name: {}", unexpected.join("."))
        }
    };
    buf.with_file_name(next_log_name)
}

// Rename the path if exists.
// Examples:
// raft-log.backup -> raft-log.1.backup -> raft-log.2.bakcup -> ... -> raft-log.${max_preserve}.backup
async fn preserve_path<P: AsRef<Path>>(path: P, max_preserve: u64) {
    // Do recursion manually due to async-fn's limitation.
    let mut current = path.as_ref().to_path_buf();
    let mut stack: Vec<PathBuf> = vec![];

    while current.exists() && (stack.len() as u64) < max_preserve {
        stack.push(current.clone());
        let next = next_log_path(current);
        current = next;
    }
    while let Some(prev) = stack.pop() {
        fs::rename(&prev, current).await.unwrap();
        current = prev;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::ffi::OsString;
    use tempfile::tempdir;

    fn entry(term: u64, index: u64) -> Entry {
        Entry {
            term,
            index,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_preserve_path() {
        test_preserve_path_with_cnt(0).await;
        test_preserve_path_with_cnt(1).await;
        test_preserve_path_with_cnt(2).await;
        test_preserve_path_with_cnt(11).await;
    }

    async fn test_preserve_path_with_cnt(dup_cnt: usize) {
        let dir = tempdir().unwrap();

        for _ in 0..dup_cnt {
            let log_path = {
                let mut buf = dir.as_ref().to_path_buf();
                buf.push("test-raft-log.backup");
                buf
            };
            // Path preservation is triggered here.
            let _ = LogFile::create(log_path).await.unwrap();
        }

        let expected = {
            let mut log_set = HashSet::<OsString>::new();
            for i in 0..cmp::min(dup_cnt as u64, MAX_LOG_FILE_PRESERVED + 1) {
                if i == 0 {
                    log_set.insert("test-raft-log.backup".into());
                } else {
                    log_set.insert(format!("test-raft-log.{}.backup", i).into());
                }
            }
            log_set
        };

        let got = {
            let mut log_set = HashSet::<OsString>::new();
            let mut it = fs::read_dir(&dir).await.unwrap();
            while let Ok(Some(ent)) = it.next_entry().await {
                log_set.insert(ent.file_name());
            }
            log_set
        };

        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn test_rewrite_entries() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        store
            .append_entries(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await;
        store
            .append_entries(&[entry(2, 2), entry(2, 3), entry(2, 4)])
            .await;

        let expect_entries = [entry(1, 1), entry(2, 2), entry(2, 3), entry(2, 4)];

        // before and after recovery
        assert_eq!(store.0.entries, &expect_entries);
        store = WalStorage::new(&log_dir).await;
        assert_eq!(store.0.entries, &expect_entries);
    }

    #[tokio::test]
    async fn test_advance_applied_index() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        store.advance_applied_index(42).await;

        assert_eq!(store.0.applied_index, 42);
        store = WalStorage::new(&log_dir).await;
        assert_eq!(store.0.applied_index, 42);
    }

    #[tokio::test]
    async fn test_update_conf_state() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        let conf_state = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![5, 6, 7],
            ..Default::default()
        };
        store.update_conf_state(conf_state.clone()).await;

        assert_eq!(store.0.raft_state.conf_state, conf_state);
        store = WalStorage::new(&log_dir).await;
        assert_eq!(store.0.raft_state.conf_state, conf_state);
    }

    #[tokio::test]
    async fn test_update_hard_state() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        let hard_state = HardState {
            term: 3,
            commit: 5,
            vote: 1,
        };
        store.update_hard_state(hard_state.clone()).await;

        assert_eq!(store.0.raft_state.hard_state, hard_state);
        store = WalStorage::new(&log_dir).await;
        assert_eq!(store.0.raft_state.hard_state, hard_state);
    }

    #[tokio::test]
    async fn test_empty_snapshot() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;
        let snapshot = store.snapshot(0).unwrap();
        store.apply_snapshot(snapshot).await.unwrap();
        store = WalStorage::new(&log_dir).await;
        assert!(store.0.entries.is_empty());
        assert_eq!(store.0.raft_state.hard_state, HardState::default());
        assert_eq!(store.0.raft_state.conf_state, ConfState::default());
        assert_eq!(
            store.0.snapshot_metadata,
            SnapshotMetadata {
                conf_state: Some(ConfState::default()),
                index: 0,
                term: 0,
            }
        );
        assert_eq!(store.0.applied_index, 0);
    }

    #[tokio::test]
    async fn test_outdated_snapshot() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        store.append_entries(&[entry(1, 1), entry(1, 2)]).await;
        store.advance_applied_index(2).await;

        let mut snapshot = store.snapshot(0).unwrap();
        store.apply_snapshot(snapshot.clone()).await.unwrap();

        snapshot.mut_metadata().index = 1;
        assert_eq!(
            store.apply_snapshot(snapshot).await,
            Err(StorageError::SnapshotOutOfDate)
        );
    }

    #[tokio::test]
    async fn test_snapshot_preserve_uncommitted_entries() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        let hard_state = HardState {
            term: 1,
            commit: 1,
            vote: 1,
        };
        store.update_hard_state(hard_state.clone()).await;

        let conf_state = ConfState {
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        store.update_conf_state(conf_state.clone()).await;

        store
            .append_entries(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await;

        let applied_index = 3;
        store.advance_applied_index(applied_index).await;

        store.append_entries(&[entry(1, 4), entry(1, 5)]).await;

        assert_eq!(
            store.0.entries,
            &[
                entry(1, 1),
                entry(1, 2),
                entry(1, 3),
                entry(1, 4),
                entry(1, 5),
            ]
        );

        let snapshot = store.snapshot(3).unwrap();
        store.apply_snapshot(snapshot).await.unwrap();

        assert_eq!(store.0.entries, &[entry(1, 4), entry(1, 5),]);

        store = WalStorage::new(&log_dir).await;

        assert_eq!(store.0.entries, &[entry(1, 4), entry(1, 5),]);
    }

    #[tokio::test]
    async fn test_snapshot_committed_higher_than_applied() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        store
            .append_entries(&[
                entry(1, 1),
                entry(2, 2),
                entry(2, 3),
                entry(2, 4),
                entry(3, 5),
            ])
            .await;

        store
            .update_hard_state(HardState {
                term: 3,
                vote: 0,
                commit: 5,
            })
            .await;

        store.advance_applied_index(3).await;

        let snapshot = store.snapshot(3).unwrap();
        store.apply_snapshot(snapshot).await.unwrap();

        // This commit index should not shrink to 3.
        assert_eq!(store.0.raft_state.hard_state.commit, 5);
        assert_eq!(store.0.raft_state.hard_state.term, 3);
        assert_eq!(store.0.entries, &[entry(2, 4), entry(3, 5)]);
    }

    #[tokio::test]
    async fn test_recover_from_active_log() {
        test_recover_from_log(false).await;
    }

    #[tokio::test]
    async fn test_recover_from_backup_log() {
        test_recover_from_log(true).await;
    }

    async fn test_recover_from_log(from_backup: bool) {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;

        store
            .append_entries(&[entry(1, 1), entry(2, 2), entry(2, 3)])
            .await;
        store.append_entries(&[entry(2, 4), entry(3, 5)]).await;

        store
            .update_hard_state(HardState {
                term: 3,
                vote: 0,
                commit: 5,
            })
            .await;

        store.advance_applied_index(3).await;

        store.0.generate_new_active_log().await;

        assert_eq!(&store.0.entries, &[entry(2, 4), entry(3, 5),]);
        assert_eq!(store.0.applied_index, 3);
        assert_eq!(store.0.raft_state.hard_state.commit, 5);
        assert_eq!(store.0.raft_state.hard_state.term, 3);

        if from_backup {
            // Remove current active log file to force recover from backup log.
            fs::remove_file(log_dir.as_ref().join(WAL_ACTIVE_LOG_NAME))
                .await
                .unwrap();
        }

        store = WalStorage::new(&log_dir).await;

        assert_eq!(&store.0.entries, &[entry(2, 4), entry(3, 5),]);
        assert_eq!(store.0.applied_index, 3);
        assert_eq!(store.0.raft_state.hard_state.commit, 5);
        assert_eq!(store.0.raft_state.hard_state.term, 3);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_panic_backup_log_corrupt() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;
        store
            .append_entries(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await;

        store.advance_applied_index(2).await;

        store.0.generate_new_active_log().await;

        assert_eq!(&store.0.entries, &[entry(1, 3),]);
        assert_eq!(store.0.applied_index, 2);

        // Remove current active log file to force recover from backup log.
        fs::remove_file(log_dir.as_ref().join(WAL_ACTIVE_LOG_NAME))
            .await
            .unwrap();

        let mut backup_log = LogFile::open(log_dir.as_ref().join(WAL_BACKUP_LOG_NAME))
            .await
            .unwrap();
        backup_log.seek(SeekFrom::End(0)).await;
        backup_log.write_all(&[255]).await;

        // Should panic here
        let _ = WalStorage::new(&log_dir).await;
    }

    #[tokio::test]
    async fn test_allow_active_log_corrupt_tail() {
        let log_dir = tempdir().unwrap();
        let mut store = WalStorage::new(&log_dir).await;
        store
            .append_entries(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await;

        store.advance_applied_index(1).await;

        let corrupt_pos = store.0.active_log.as_ref().unwrap().len().await;

        store.advance_applied_index(2).await;

        let active_log = store.0.mut_active_log();
        active_log.seek(SeekFrom::Start(corrupt_pos)).await;
        active_log.write_all(&[255]).await;

        let store = WalStorage::new(&log_dir).await;

        assert_eq!(&store.0.entries, &[entry(1, 1), entry(1, 2), entry(1, 3),]);
        assert_eq!(store.0.applied_index, 1);
    }
}
