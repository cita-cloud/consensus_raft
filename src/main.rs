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

mod config;
mod mailbox;
mod panic_hook;
mod peer;
mod storage;

use clap::Clap;
use git_version::git_version;

use slog::error;
use slog::info;
use slog::trace;
use slog::Logger;
use sloggers::file::FileLoggerBuilder;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::types::Severity;
use sloggers::Build as _;

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/cita-cloud/consensus_raft";

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(version = "0.2.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of this service.
    #[clap(short = 'p', long = "port", default_value = "50001")]
    grpc_port: String,
    #[clap(long = "log", default_value = "file")]
    log: String,
}

fn main() {
    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            let log_level = Severity::Debug;
            let logger = match opts.log.as_str() {
                "file" => {
                    // File log
                    let log_path = {
                        let log_dir = std::env::current_dir().unwrap().join("logs");
                        let _ = std::fs::create_dir(&log_dir);
                        log_dir.join("consensus_service.log")
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
                _unexpected => {
                    panic!(
                        "unexpected log type `{}`, only `file` and `terminal` are allowed.",
                        _unexpected
                    );
                }
            };

            panic_hook::set_panic_handler(logger.clone());

            info!(logger, "server start, grpc port: {}", opts.grpc_port);

            if let Err(e) = run(opts, logger.clone()) {
                error!(logger, "server shutdown with error: {}", e);
            }

            info!(logger, "server end");
        }
    }
}

async fn register_network_msg_handler(
    network_port: u16,
    port: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let network_addr = format!("http://127.0.0.1:{}", network_port);
    let mut client = NetworkServiceClient::connect(network_addr).await?;

    let request = Request::new(RegisterInfo {
        module_name: "consensus".to_owned(),
        hostname: "127.0.0.1".to_owned(),
        port,
    });

    let response = client.register_network_msg_handler(request).await?;

    Ok(response.into_inner().is_success)
}

use cita_cloud_proto::consensus::consensus_service_server::ConsensusServiceServer;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer;
use cita_cloud_proto::network::network_service_client::NetworkServiceClient;
use cita_cloud_proto::network::RegisterInfo;

use crate::mailbox::Mailbox;
use std::time::Duration;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time;
use tonic::{transport::Server, Request};

#[tokio::main]
async fn run(opts: RunOpts, logger: Logger) -> Result<(), Box<dyn std::error::Error>> {
    // read consensus-config.toml
    let buffer = std::fs::read_to_string("consensus-config.toml")
        .unwrap_or_else(|err| panic!("Error while loading config: [{}]", err));
    let config = config::RaftConfig::new(&buffer);

    let node_addr = hex::decode(&fs::read("node_address").await?[2..])?;
    let node_id = address_to_peer_id(&node_addr);
    let network_port = config.network_port;
    let controller_port = config.controller_port;

    // register `network_msg_handler` to the network.
    let grpc_port_clone = opts.grpc_port.clone();
    let logger_cloned = logger.clone();
    tokio::spawn(async move {
        let logger = logger_cloned;
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            // register endpoint
            {
                let ret = register_network_msg_handler(network_port, grpc_port_clone.clone()).await;
                if ret.is_ok() && ret.unwrap() {
                    info!(logger, "register network msg handler success!");
                    break;
                }
            }
            trace!(logger, "register network msg handler failed! Retrying");
            interval.tick().await;
        }
    });

    // mailbox
    let addr_str = format!("127.0.0.1:{}", opts.grpc_port);
    let addr = addr_str.parse()?;

    let (msg_tx, msg_rx) = mpsc::unbounded_channel();
    let mut mailbox = Mailbox::new(
        node_id,
        controller_port,
        network_port,
        msg_tx.clone(),
        logger.clone(),
    )
    .await;
    let mailbox_control = mailbox.control();

    // peer
    let data_dir = std::env::current_dir()?.join("raft-data-dir");
    let mut peer = peer::Peer::new(
        node_addr,
        msg_tx,
        msg_rx,
        mailbox_control.clone(),
        data_dir,
        logger.clone(),
    )
    .await;

    // run
    let raft_service = peer.service();

    info!(logger, "start raft");
    tokio::spawn(async move {
        mailbox.run().await;
    });

    tokio::spawn(async move {
        peer.run().await;
    });

    info!(logger, "start grpc server!");
    Server::builder()
        .add_service(ConsensusServiceServer::new(raft_service.clone()))
        .add_service(NetworkMsgHandlerServiceServer::new(raft_service.clone()))
        .serve(addr)
        .await?;
    Ok(())
}

// This is a very hacky way to map address to peer_id.
// Because raft needs an id of integer type, but only
// addresses are provided.
// This also make peer_id unreadable.
//
// According to DefaultHasher's doc, it's fine when all peers are using the same build.
// "This hasher is not guaranteed to be the same as all other DefaultHasher instances,
// but is the same as all other DefaultHasher instances created through new or default."
//
// I don't like this, maybe fix it in the future, but leaves it here for now.
fn address_to_peer_id(addr: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    hasher.write(addr);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::address_to_peer_id;

    #[test]
    fn test_address_to_peer_id() {
        let addrs: Vec<&[u8]> = vec![b"", b"1", b"1234"];
        for addr in addrs {
            let first = address_to_peer_id(&addr);
            let second = address_to_peer_id(&addr);
            assert_eq!(first, second);
        }
    }
}
