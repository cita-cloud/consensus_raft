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

use crate::client::Network;
use crate::config::ConsensusServiceConfig;
use crate::health_check::HealthCheckServer;
use cita_cloud_proto::common::ConsensusConfiguration;
use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::common::StatusCode;
use cita_cloud_proto::consensus::consensus_service_server::ConsensusService;
use cita_cloud_proto::consensus::consensus_service_server::ConsensusServiceServer;
use cita_cloud_proto::health_check::health_server::HealthServer;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer;
use cloud_util::metrics::{run_metrics_exporter, MiddlewareLayer};
use slog::info;
use slog::Logger;
use tokio::sync::mpsc;
use tonic::transport::Server;

#[derive(Debug)]
pub struct RaftConsensusService(mpsc::Sender<ConsensusConfiguration>);

#[tonic::async_trait]
impl ConsensusService for RaftConsensusService {
    async fn reconfigure(
        &self,
        request: tonic::Request<ConsensusConfiguration>,
    ) -> std::result::Result<tonic::Response<StatusCode>, tonic::Status> {
        let config = request.into_inner();
        let tx = self.0.clone();
        // FIXME: it's not safe; but if we wait for it, it may cause a deadlock
        tokio::spawn(async move {
            let _ = tx.send(config).await;
        });
        // horrible
        Ok(tonic::Response::new(StatusCode { code: 0 }))
    }

    async fn check_block(
        &self,
        _request: tonic::Request<ProposalWithProof>,
    ) -> Result<tonic::Response<StatusCode>, tonic::Status> {
        // Reply ok since we assume no byzantine faults.
        // horrible
        Ok(tonic::Response::new(StatusCode { code: 0 }))
    }
}

pub async fn start_grpc_server(
    config: ConsensusServiceConfig,
    logger: Logger,
    rx_signal: flume::Receiver<()>,
    controller_tx: mpsc::Sender<ConsensusConfiguration>,
    network: Network,
) {
    let raft_svc = RaftConsensusService(controller_tx);

    let logger_cloned = logger.clone();
    let grpc_listen_port = config.grpc_listen_port;
    info!(
        logger_cloned,
        "grpc port of consensus_raft: {}", grpc_listen_port
    );

    let layer = if config.enable_metrics {
        tokio::spawn(async move {
            run_metrics_exporter(config.metrics_port).await.unwrap();
        });

        Some(
            tower::ServiceBuilder::new()
                .layer(MiddlewareLayer::new(config.metrics_buckets))
                .into_inner(),
        )
    } else {
        None
    };

    info!(logger_cloned, "start consensus_raft grpc server");
    if let Some(layer) = layer {
        tokio::spawn(async move {
            info!(logger_cloned, "metrics on");
            let addr = format!("[::]:{grpc_listen_port}").parse().unwrap();
            let res = Server::builder()
                .layer(layer)
                .add_service(ConsensusServiceServer::new(raft_svc))
                .add_service(NetworkMsgHandlerServiceServer::new(network))
                .add_service(HealthServer::new(HealthCheckServer {}))
                .serve_with_shutdown(
                    addr,
                    cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_signal),
                )
                .await;

            if let Err(e) = res {
                info!(logger_cloned, "grpc service exit with error: `{:?}`", e);
            } else {
                info!(logger_cloned, "grpc service exit");
            }
        });
    } else {
        tokio::spawn(async move {
            info!(logger_cloned, "metrics off");
            let addr = format!("[::]:{grpc_listen_port}").parse().unwrap();
            let res = Server::builder()
                .add_service(ConsensusServiceServer::new(raft_svc))
                .add_service(NetworkMsgHandlerServiceServer::new(network))
                .add_service(HealthServer::new(HealthCheckServer {}))
                .serve_with_shutdown(
                    addr,
                    cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_signal),
                )
                .await;

            if let Err(e) = res {
                info!(logger_cloned, "grpc service exit with error: `{:?}`", e);
            } else {
                info!(logger_cloned, "grpc service exit");
            }
        });
    }
}
