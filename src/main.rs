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

mod client;
mod config;
mod peer;
mod storage;
mod utils;

use clap::App;
use clap::Arg;

use git_version::git_version;

use slog::info;
use sloggers::file::FileLoggerBuilder;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::types::Severity;
use sloggers::Build as _;

use tokio::fs;

use peer::Peer;

use utils::set_panic_handler;

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/cita-cloud/consensus_raft";

const DEFAULT_GRPC_LISTEN_PORT: u16 = 50001;
const DEFAULT_LOG_TYPE: &str = "file";

fn main() {
    let default_grpc_listen_port = DEFAULT_GRPC_LISTEN_PORT.to_string();
    let run_cmd = App::new("run")
        .about("run the service")
        .arg(
            Arg::new("port")
                .about("grpc listen port")
                .long("port")
                .short('p')
                .takes_value(true)
                .validator(|s| s.parse::<u16>())
                .default_value(&default_grpc_listen_port),
        )
        .arg(
            Arg::new("log")
                .about("log type")
                .possible_values(&["file", "terminal"])
                .default_value(DEFAULT_LOG_TYPE),
        );
    let git_cmd = App::new("git").about("show git info");

    let app = App::new("consensus_raft")
        .author("Rivtower Technology")
        .about("Consensus service for CITA-Cloud")
        .subcommands([run_cmd, git_cmd]);

    let matches = app.get_matches();

    let rt = tokio::runtime::Runtime::new().unwrap();
    match matches.subcommand() {
        Some(("run", m)) => {
            let local_port = m.value_of("port").unwrap().parse::<u16>().unwrap();
            let log_type = m.value_of("log").unwrap();
            rt.block_on(run(local_port, log_type)).unwrap();
        }
        Some(("git", _)) => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        None => {
            rt.block_on(run(DEFAULT_GRPC_LISTEN_PORT, DEFAULT_LOG_TYPE))
                .unwrap();
        }
        _ => unreachable!(),
    }
}

async fn run(local_port: u16, log_type: &str) -> anyhow::Result<()> {
    let log_level = Severity::Debug;
    let logger = match log_type {
        "file" => {
            // File log
            let log_path = {
                let log_dir = std::env::current_dir().unwrap().join("logs");
                let _ = std::fs::create_dir(&log_dir);
                log_dir.join("consensus-service.log")
            };
            let mut log_builder = FileLoggerBuilder::new(log_path);
            log_builder.level(log_level);
            // 50 MB
            log_builder.rotate_size(50 * 1024 * 1024);
            log_builder.rotate_keep(5);
            log_builder.rotate_compress(true);
            log_builder.build().expect("can't build file logger")
        }
        "terminal" => {
            let mut log_builder = TerminalLoggerBuilder::new();
            log_builder.level(log_level);
            log_builder.build().expect("can't build terminal logger")
        }
        unexpected => {
            panic!(
                "unexpected log type `{}`, only `file` and `terminal` are allowed.",
                unexpected
            );
        }
    };

    set_panic_handler(logger.clone());

    // read consensus-config.toml
    let config = {
        let buf = fs::read_to_string("consensus-config.toml").await?;
        config::RaftConfig::new(&buf)
    };

    let node_addr = {
        let s = fs::read_to_string("node_address").await?;
        hex::decode(s.strip_prefix("0x").unwrap_or(&s))?
    };

    let network_port = config.network_port;
    let controller_port = config.controller_port;

    let data_dir = std::env::current_dir()?.join("raft-data-dir");
    let mut peer = Peer::setup(
        node_addr,
        local_port,
        controller_port,
        network_port,
        data_dir,
        logger.clone(),
    )
    .await;

    peer.run().await;

    info!(logger, "raft service exit");

    Ok(())
}
