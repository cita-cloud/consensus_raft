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
mod grpc_server;
mod health_check;
mod peer;
mod storage;
mod utils;

use crate::client::{Controller, Network};
use crate::grpc_server::start_grpc_server;
use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::common::{ConsensusConfiguration, Proposal};
use clap::{crate_authors, crate_version, value_parser, Arg, ArgAction, Command};
use config::ConsensusServiceConfig;
use peer::Peer;
use slog::{debug, info, o, warn};
use sloggers::file::FileLoggerBuilder;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::Build as _;
use std::path::Path;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use utils::{addr_to_peer_id, clap_about};

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
                .conflicts_with_all(["log-dir", "log-file-name"]),
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
        .about(clap_about())
        .subcommands([run_cmd]);

    let matches = app.get_matches();

    match matches.subcommand() {
        Some(("run", m)) => {
            let config = {
                let path = m.get_one::<PathBuf>("config").unwrap();
                ConsensusServiceConfig::new(path.as_path().to_str().unwrap())
            };

            let log_level = config.log_level.parse().expect("unrecognized log level");
            let logger = if m.get_one::<bool>("stdout").copied().unwrap() || config.log_to_stdout {
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
                        std::fs::create_dir_all(log_dir).expect("cannot create log dir");
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

            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async move {
                let node_addr = {
                    let s = &config.node_addr;
                    hex::decode(s.strip_prefix("0x").unwrap_or(s)).expect("decode node_addr failed")
                };

                let local_id = addr_to_peer_id(&node_addr);

                //start grpc server
                let rx_signal = cloud_util::graceful_shutdown::graceful_shutdown();
                // raft Communicate with controller
                let (controller_tx, mut controller_rx) =
                    mpsc::channel::<ConsensusConfiguration>(1000);

                // raft Communicate with network
                let (peer_tx, peer_rx) = flume::bounded(64);

                // Controller grpc client
                let controller = {
                    let logger = logger.new(o!("tag" => "controller"));
                    Controller::new(config.controller_port, logger)
                };

                // Network grpc client
                let network = {
                    let logger = logger.new(o!("tag" => "network"));
                    Network::setup(
                        local_id,
                        config.grpc_listen_port,
                        config.network_port,
                        peer_tx,
                        logger,
                    )
                    .await
                };

                // stop raft when I'm not in the validators list
                let (stop_tx, stop_rx) = flume::bounded(1);

                // start consensu grpc server
                start_grpc_server(
                    config.clone(),
                    logger.clone(),
                    rx_signal.clone(),
                    controller_tx,
                    network.clone(),
                )
                .await;

                // wait for exit signal
                tokio::spawn(async move {
                    let _ = rx_signal.recv_async().await;
                    // stop task before exit
                    drop(stop_tx);
                });

                // raft task handle
                let mut opt_raft_task: Option<JoinHandle<()>> = None;

                // used to delay abort raft task
                // None means I'm validator no need to abort
                // Some means I'm not validator, and the height is when I should abort
                // but we need to delay abort, because the raft task may not complete
                // to fix restart node during delay abort, we should presist abort height
                let abort_height_path = Path::new(&config.raft_data_path).join("abort_height");
                let mut opt_abort_height = if abort_height_path.exists() {
                    let height = std::fs::read_to_string(&abort_height_path)
                        .expect("read abort height failed")
                        .parse::<u64>()
                        .expect("parse abort height failed");
                    Some(height)
                } else {
                    None
                };

                loop {
                    tokio::time::sleep(Duration::from_secs(3)).await;

                    // check exit signal
                    let ret = stop_rx.try_recv();
                    if ret == Err(flume::TryRecvError::Disconnected) {
                        break;
                    }

                    // check reconfigure message from controller
                    let mut opt_trigger_config = None;
                    // read all reconfigure messages in channel
                    // when start a new node, sync blocks quickly, there will be many reconfigure messages in channel
                    // we only need proc latest one
                    while let Ok(config) = controller_rx.try_recv() {
                        opt_trigger_config = Some(config);
                    }
                    if opt_trigger_config.is_none() {
                        let pwp = ProposalWithProof {
                            proposal: Some(Proposal {
                                height: u64::MAX,
                                data: vec![],
                            }),
                            proof: vec![],
                        };
                        debug!(logger, "ping_controller..");
                        match controller.commit_block(pwp).await {
                            Ok(config) => {
                                opt_trigger_config = Some(config);
                            }
                            Err(e) => {
                                warn!(logger, "commit block failed: {}", e);
                            }
                        }
                    }
                    if let Some(trigger_config) = opt_trigger_config {
                        info!(
                            logger,
                            "get reconfigure from controller, height is {}", trigger_config.height
                        );
                        // if node restart during delay abort, we should start raft task at first
                        if trigger_config.validators.contains(&node_addr)
                            || (opt_abort_height.is_some()
                                && trigger_config.height <= opt_abort_height.unwrap())
                        {
                            opt_abort_height = None;
                            if opt_raft_task.is_none()
                                || opt_raft_task.as_ref().unwrap().is_finished()
                            {
                                let controller_cloned = controller.clone();
                                let network_cloned = network.clone();
                                let logger_cloned = logger.clone();
                                let config_cloned = config.clone();
                                let stop_rx_cloned = stop_rx.clone();
                                let peer_rx_cloned = peer_rx.clone();
                                let handle = tokio::spawn(async move {
                                    info!(logger_cloned, "raft start");
                                    let mut peer = Peer::setup(
                                        config_cloned,
                                        logger_cloned.clone(),
                                        trigger_config,
                                        network_cloned,
                                        controller_cloned,
                                        peer_rx_cloned,
                                    )
                                    .await;
                                    peer.run(stop_rx_cloned).await;
                                    info!(logger_cloned, "raft exit");
                                });
                                opt_raft_task = Some(handle);
                            }
                        } else {
                            info!(logger, "I'm not in the validators list");
                            if opt_abort_height.is_none() {
                                std::fs::write(
                                    &abort_height_path,
                                    trigger_config.height.to_string(),
                                )
                                .expect("write abort height failed");
                                opt_abort_height = Some(trigger_config.height);
                            }
                            if let Some(ref handle) = opt_raft_task {
                                if !handle.is_finished()
                                    && trigger_config.height > opt_abort_height.unwrap()
                                {
                                    info!(logger, "abort raft");
                                    handle.abort();
                                }
                            }
                        }
                    }
                }
            });
        }
        None => {
            println!("no subcommand provided");
        }
        _ => unreachable!(),
    }
}
