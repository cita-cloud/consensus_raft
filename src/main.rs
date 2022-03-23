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

use clap::{crate_authors, crate_version, Arg, Command};
use std::path::Path;
use std::path::PathBuf;

use git_version::git_version;

use slog::info;
use sloggers::file::FileLoggerBuilder;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::Build as _;

use peer::Peer;

use config::ConsensusServiceConfig;
use utils::set_panic_handler;

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/cita-cloud/consensus_raft";

fn main() {
    let run_cmd = Command::new("run")
        .about("run the service")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .help("the consensus config")
                .takes_value(true)
                .validator(|s| s.parse::<PathBuf>())
                .default_value("config.toml"),
        )
        .arg(
            Arg::new("stdout")
                .help("if specified, log to stdout. Overrides the config")
                .long("stdout")
                .conflicts_with_all(&["log-dir", "log-file-name"]),
        )
        .arg(
            Arg::new("log-dir")
                .help("the log dir. Overrides the config")
                .short('d')
                .long("log-dir")
                .takes_value(true)
                .validator(|s| s.parse::<PathBuf>()),
        )
        .arg(
            Arg::new("log-file-name")
                .help("the log file name. Overrride the config")
                .short('f')
                .long("log-file-name")
                .takes_value(true)
                .validator(|s| s.parse::<PathBuf>()),
        );
    let git_cmd = Command::new("git").about("show git info");

    let app = Command::new("consensus_raft")
        .author(crate_authors!())
        .version(crate_version!())
        .about("Consensus service for CITA-Cloud")
        .subcommands([run_cmd, git_cmd]);

    let matches = app.get_matches();

    match matches.subcommand() {
        Some(("run", m)) => {
            let config = {
                let path = m.value_of("config").unwrap();
                ConsensusServiceConfig::new(path)
            };

            let log_level = config.log_level.parse().expect("unrecognized log level");
            let logger = if m.is_present("stdout") || config.log_to_stdout {
                let mut log_builder = TerminalLoggerBuilder::new();
                log_builder.level(config.log_level.parse().expect("unrecognized log level"));
                log_builder.build().expect("can't build terminal logger")
            } else {
                // File log
                let log_path = {
                    let log_dir = Path::new(m.value_of("log-dir").unwrap_or(&config.log_dir));
                    let log_file_name =
                        m.value_of("log-file-name").unwrap_or(&config.log_file_name);

                    if !log_dir.exists() {
                        std::fs::create_dir_all(&log_dir).expect("cannot create log dir");
                    }
                    log_dir.join(log_file_name)
                };
                let mut log_builder = FileLoggerBuilder::new(log_path);
                log_builder.level(log_level);
                // 50 MB
                log_builder.rotate_size(config.log_rotate_size);
                log_builder.rotate_keep(config.log_rotate_keep);
                log_builder.rotate_compress(config.log_rotate_compress);
                log_builder.build().expect("can't build file logger")
            };

            set_panic_handler(logger.clone());

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let mut peer = Peer::setup(config, logger.clone()).await;
                peer.run().await;
                info!(logger, "raft service exit");
            });
        }
        Some(("git", _)) => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        None => {
            println!("no subcommand provided");
        }
        _ => unreachable!(),
    }
}
