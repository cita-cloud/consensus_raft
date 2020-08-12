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
mod error;
mod network;
mod peer;

use clap::Clap;
use git_version::git_version;
use log::{info, warn};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/whfuyn";

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
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
    #[clap(short = 'p', long = "port", default_value = "50003")]
    grpc_port: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            // init log4rs
            log4rs::init_file("consensus-log4rs.yaml", Default::default()).unwrap();
            info!("grpc port of this service: {}", opts.grpc_port);
            let _ = run(opts);
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

use std::time::Duration;
use tokio::time;
use tonic::{transport::Server, Request};

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
    // read consensus-config.toml
    let buffer = std::fs::read_to_string("consensus-config.toml")
        .unwrap_or_else(|err| panic!("Error while loading config: [{}]", err));
    let config = config::RaftConfig::new(&buffer);

    let network_port = config.network_port;
    let controller_port = config.controller_port;

    let grpc_port_clone = opts.grpc_port.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            // register endpoint
            {
                let ret = register_network_msg_handler(network_port, grpc_port_clone.clone()).await;
                if ret.is_ok() && ret.unwrap() {
                    info!("register network msg handler success!");
                    break;
                }
            }
            warn!("register network msg handler failed! Retrying");
            interval.tick().await;
        }
    });

    let addr_str = format!("127.0.0.1:{}", opts.grpc_port);
    let addr = addr_str.parse()?;

    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let port = opts.grpc_port.parse::<u64>().unwrap();
    let id = (port - 50001) / 1000 + 1;
    let raft_server = peer::RaftServer::new(id, tx.clone(), controller_port, network_port);

    info!("start raft server");
    tokio::spawn(raft_server.clone().start(tx.clone(), rx));
    if id == 1 {
        info!("init leader #{}", id);
        tokio::spawn(peer::RaftServer::add_follower(5, tx.clone()));
    }
    else {
        info!("init follower #{}", id);
    }
    info!("start grpc server!");
    Server::builder()
        .add_service(ConsensusServiceServer::new(raft_server.clone()))
        .add_service(NetworkMsgHandlerServiceServer::new(raft_server.clone()))
        .serve(addr)
        .await?;
    Ok(())
}
