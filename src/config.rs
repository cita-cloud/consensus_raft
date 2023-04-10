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

use cloud_util::common::read_toml;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ConsensusServiceConfig {
    pub node_addr: String,
    pub grpc_listen_port: u16,
    pub network_port: u16,
    pub controller_port: u16,
    // log
    pub log_level: String,
    // if it's true, log to stdout instead of
    pub log_to_stdout: bool,
    // if log to file
    pub log_dir: String,
    pub log_file_name: String,
    pub log_rotate_size: u64,
    pub log_rotate_keep: usize,
    pub log_rotate_compress: bool,
    // raft
    pub tick_interval_in_millis: u64,
    pub heartbeat_tick: u64,
    pub election_tick: u64,
    pub check_quorum: bool,
    // raft data path
    pub raft_data_path: String,
    //metrics
    pub enable_metrics: bool,
    pub metrics_port: u16,
    pub metrics_buckets: Vec<f64>,
}

impl Default for ConsensusServiceConfig {
    fn default() -> Self {
        Self {
            node_addr: "".to_string(),
            grpc_listen_port: 50001,
            network_port: 50000,
            controller_port: 50004,
            // log
            log_level: "info".into(),
            // if it's true, log to stdout instead of
            log_to_stdout: false,
            // if log to file
            log_dir: "logs".into(),
            log_file_name: "consensus-service.log".into(),
            log_rotate_size: 128 * 1024 * 1024, //128MB
            log_rotate_keep: 5,
            log_rotate_compress: false,
            // raft
            tick_interval_in_millis: 200,
            heartbeat_tick: 15,
            election_tick: 50,
            check_quorum: false,
            // raft data dir
            raft_data_path: "raft-data-dir".into(),
            //metrics
            enable_metrics: true,
            metrics_port: 60001,
            metrics_buckets: vec![
                0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0,
            ],
        }
    }
}

impl ConsensusServiceConfig {
    pub fn new(config_str: &str) -> Self {
        let mut config: ConsensusServiceConfig = read_toml(config_str, "consensus_raft");
        let node_address_path = config.node_addr.clone();
        config.node_addr = fs::read_to_string(node_address_path).unwrap();
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        let config = ConsensusServiceConfig::new("example/config.toml");

        assert_eq!(config.controller_port, 50004);
        assert_eq!(config.network_port, 50000);
        assert_eq!(config.grpc_listen_port, 50001);
        assert_eq!(
            config.node_addr,
            "f805cf7df8dffc4fd9c98791d72d00c9587dc1d9".to_string()
        );
    }
}
