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

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use serde::Deserialize;

// Default literals for serde is not supported yet.
// https://github.com/serde-rs/serde/issues/368
mod default {
    pub fn log_level() -> String {
        "info".into()
    }

    pub fn log_to_stdout() -> bool {
        false
    }

    pub fn log_dir() -> String {
        "logs".into()
    }

    pub fn log_file_name() -> String {
        "consensus-service.log".into()
    }

    pub fn log_rotate_size() -> u64 {
        // 128 MB
        128 * 1024 * 1024
    }

    pub fn log_rotate_keep() -> usize {
        5
    }

    pub fn log_rotate_compress() -> bool {
        false
    }

    pub fn tick_interval_in_millis() -> u64 {
        200
    }

    pub fn heartbeat_tick() -> u64 {
        15
    }

    pub fn election_tick() -> u64 {
        50
    }

    pub fn check_quorum() -> bool {
        false
    }

    pub fn transfer_leader_timeout_in_secs() -> u64 {
        12
    }

    pub fn wal_path() -> String {
        "raft-data-dir".into()
    }

    pub fn max_wal_log_file_preserved() -> u64 {
        5
    }

    pub fn wal_log_file_compact_limit() -> u64 {
        // 128 MB
        128 * 1024 * 1024
    }

    // active wal log may contain incomplete tail data
    pub fn allow_corrupt_wal_log_tail() -> bool {
        true
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsensusServiceConfig {
    pub node_addr: String,

    pub grpc_listen_port: u16,

    pub network_port: u16,
    pub controller_port: u16,

    // log
    #[serde(default = "default::log_level")]
    pub log_level: String,
    // if it's true, log to stdout instead of file
    #[serde(default = "default::log_to_stdout")]
    pub log_to_stdout: bool,
    // if log to file
    #[serde(default = "default::log_dir")]
    pub log_dir: String,
    #[serde(default = "default::log_file_name")]
    pub log_file_name: String,
    #[serde(default = "default::log_rotate_size")]
    pub log_rotate_size: u64,
    #[serde(default = "default::log_rotate_keep")]
    pub log_rotate_keep: usize,
    #[serde(default = "default::log_rotate_compress")]
    pub log_rotate_compress: bool,

    // raft
    #[serde(default = "default::tick_interval_in_millis")]
    pub tick_interval_in_millis: u64,
    #[serde(default = "default::heartbeat_tick")]
    pub heartbeat_tick: u64,
    #[serde(default = "default::election_tick")]
    pub election_tick: u64,

    #[serde(default = "default::check_quorum")]
    pub check_quorum: bool,

    // transfer leader if no receiving valid proposal from controller
    #[serde(default = "default::transfer_leader_timeout_in_secs")]
    pub transfer_leader_timeout_in_secs: u64,

    // raft wal log
    #[serde(default = "default::wal_path")]
    pub wal_path: String,
    #[serde(default = "default::max_wal_log_file_preserved")]
    pub max_wal_log_file_preserved: u64,
    #[serde(default = "default::wal_log_file_compact_limit")]
    pub wal_log_file_compact_limit: u64,
    #[serde(default = "default::allow_corrupt_wal_log_tail")]
    pub allow_corrupt_wal_log_tail: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(rename = "consensus_raft")]
    consensus: ConsensusServiceConfig,
}

pub fn load_config(path: impl AsRef<Path>) -> ConsensusServiceConfig {
    let s = {
        let mut f = File::open(path).unwrap();
        let mut buf = String::new();
        f.read_to_string(&mut buf).unwrap();
        buf
    };

    let config: Config = toml::from_str(&s).unwrap();
    config.consensus
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let s = "[consensus_raft]\ngrpc_listen_port = 50001\nnetwork_port = 50000\ncontroller_port = 50004\nnode_addr = \"0x1234\"";
        let _: Config = toml::from_str(s).unwrap();
    }
}
