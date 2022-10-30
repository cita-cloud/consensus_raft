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
mod health_check;
mod peer;
mod storage;
mod utils;
use clap::{crate_authors, crate_version, value_parser, Arg, ArgAction, Command};
use config::ConsensusServiceConfig;
use peer::Peer;
use slog::info;
use sloggers::file::FileLoggerBuilder;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::Build as _;
use std::path::Path;
use std::path::PathBuf;
use utils::set_panic_handler;

fn main() {
    let run_cmd = Command::new("run")
        .about("run the service")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .help("specify consensus config file path")
                .value_parser(value_parser!(PathBuf))
                .default_value("config.toml"),
        )
        .arg(
            Arg::new("stdout")
                .help("if specified, log to stdout. Overrides the config")
                .long("stdout")
                .action(ArgAction::SetTrue)
                .conflicts_with_all(&["log-dir", "log-file-name"]),
        )
        .arg(
            Arg::new("log-dir")
                .help("the log dir. Overrides the config")
                .short('d')
                .long("log-dir")
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("log-file-name")
                .help("the log file name. Overrride the config")
                .short('f')
                .long("log-file-name")
                .value_parser(value_parser!(PathBuf)),
        );

    let app = Command::new("consensus_raft")
        .author(crate_authors!())
        .version(crate_version!())
        .about("Consensus service for CITA-Cloud")
        .subcommands([run_cmd]);

    let matches = app.get_matches();

    match matches.subcommand() {
        Some(("run", m)) => {
            let config = {
                let path = m.get_one::<PathBuf>("config").unwrap();
                ConsensusServiceConfig::new(path.as_path().to_str().unwrap())
            };

            let log_level = config.log_level.parse().expect("unrecognized log level");
            let logger = if m.contains_id("stdout") || config.log_to_stdout {
                let mut log_builder = TerminalLoggerBuilder::new();
                log_builder.level(config.log_level.parse().expect("unrecognized log level"));
                log_builder.build().expect("can't build terminal logger")
            } else {
                // File log
                let log_path = {
                    let log_dir = m
                        .get_one::<PathBuf>("log-dir")
                        .map(|p| p.as_path())
                        .unwrap_or_else(|| Path::new(&config.log_dir));
                    let log_file_name = m
                        .get_one::<PathBuf>("log-file-name")
                        .map(|p| p.as_path())
                        .unwrap_or_else(|| Path::new(&config.log_file_name));

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

            rt.spawn(cloud_util::signal::handle_signals());
            rt.block_on(async move {
                let mut peer = Peer::setup(config, logger.clone()).await;
                peer.run().await;
                info!(logger, "raft service exit");
            });
        }
        None => {
            println!("no subcommand provided");
        }
        _ => unreachable!(),
    }
}
