use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time;

use tonic::transport::channel::{Channel, Endpoint};

use prost::Message as _;
use raft::eraftpb::Message as RaftMsg;

use slog::debug;
use slog::info;
use slog::Logger;

use cita_cloud_proto::{
    common::{
        ConsensusConfiguration, ConsensusConfigurationResponse, Empty, Proposal, ProposalResponse,
        ProposalWithProof, StatusCode,
    },
    controller::consensus2_controller_service_client::Consensus2ControllerServiceClient as ControllerClient,
    network::{
        network_msg_handler_service_server::NetworkMsgHandlerService,
        network_service_client::NetworkServiceClient as NetworkClient, NetworkMsg, RegisterInfo,
    },
};

#[derive(Debug, Clone)]
pub struct Controller {
    // grpc client
    client: ControllerClient<Channel>,
    // slog logger, currently unused
    #[allow(unused)]
    logger: Logger,
}

impl Controller {
    pub fn new(port: u16, logger: Logger) -> Self {
        let client = {
            // TODO: maybe return a result
            let uri = format!("http://127.0.0.1:{}", port);

            info!(logger, "controller grpc addr: {}", uri);

            let channel = Endpoint::from_shared(uri).unwrap().connect_lazy();
            ControllerClient::new(channel)
        };
        Self { client, logger }
    }

    pub async fn get_proposal(&self) -> Result<Proposal, tonic::Status> {
        let resp = self
            .client
            .clone()
            .get_proposal(Empty {})
            .await
            .map(|resp| resp.into_inner())?;

        // horrible
        if let ProposalResponse {
            // These mysterious numbers come from https://github.com/cita-cloud/status_code
            status: Some(StatusCode { code: 0 | 109 }),
            proposal: Some(proposal),
        } = resp
        {
            Ok(proposal)
        } else {
            Err(tonic::Status::internal(format!("bad resp: {:?}", resp)))
        }
    }

    pub async fn check_proposal(&self, proposal: Proposal) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .clone()
            .check_proposal(proposal)
            .await
            .map(|resp| resp.into_inner())?;

        // horrible
        Ok(resp.code == 0)
    }

    pub async fn commit_block(
        &self,
        pwp: ProposalWithProof,
    ) -> Result<ConsensusConfiguration, tonic::Status> {
        let resp = self
            .client
            .clone()
            .commit_block(pwp)
            .await
            .map(|resp| resp.into_inner())?;

        // horrible
        if let ConsensusConfigurationResponse {
            status: Some(StatusCode { code: 0 }),
            config: Some(config),
        } = resp
        {
            Ok(config)
        } else {
            Err(tonic::Status::internal(format!("bad resp: {:?}", resp)))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Network(Arc<Inner>);

impl Deref for Network {
    type Target = Inner;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl Network {
    pub async fn setup(
        local_id: u64,
        local_port: u16,
        network_port: u16,
        msg_tx: mpsc::Sender<RaftMsg>,
        logger: Logger,
    ) -> Self {
        let mut inner = Inner::new(local_id, network_port, msg_tx, logger);
        inner.register_to_network(local_port).await;
        inner.probe().await;
        Self(Arc::new(inner))
    }
}

#[derive(Debug)]
pub struct Inner {
    // local peer id
    local_id: u64,
    // Send msg to our local peer
    msg_tx: mpsc::Sender<RaftMsg>,

    // raft_id -> origin
    // Cached origin to avoid broadcasting. These records maybe outdated,
    // but it's network(the microservice)'s responsibility to fallback
    // to broadcast. We only do update when the incoming msg's from and
    // origin mismatched.
    mailbook: RwLock<HashMap<u64, u64>>,

    // grpc client
    client: NetworkClient<Channel>,

    // slog logger
    logger: Logger,
}

impl Inner {
    fn new(
        local_id: u64,
        network_port: u16,
        msg_tx: mpsc::Sender<RaftMsg>,
        logger: Logger,
    ) -> Self {
        let client = {
            // TODO: maybe return a result
            let uri = format!("http://127.0.0.1:{}", network_port);

            info!(logger, "network grpc addr: {}", uri);

            let channel = Endpoint::from_shared(uri).unwrap().connect_lazy();
            NetworkClient::new(channel)
        };

        Self {
            local_id,
            msg_tx,
            mailbook: RwLock::new(HashMap::new()),
            client,
            logger,
        }
    }

    async fn register_to_network(&mut self, local_port: u16) {
        let mut retry = time::interval(time::Duration::from_secs(1));

        let request = RegisterInfo {
            module_name: "consensus".to_owned(),
            hostname: "127.0.0.1".to_owned(),
            port: local_port.to_string(),
        };

        info!(self.logger, "registering network msg handler...");
        loop {
            retry.tick().await;

            if let Ok(resp) = self
                .client
                .register_network_msg_handler(request.clone())
                .await
            {
                // horrible
                if resp.into_inner().code == 0 {
                    break;
                }
            }
        }
        info!(self.logger, "network msg handler registered.");
    }

    pub async fn send_msg(&self, msg: RaftMsg) {
        let to = msg.to;
        // This `origin` is something like session id.
        let mut msg = NetworkMsg {
            module: "consensus".to_string(),
            r#type: "raft".to_owned(),
            origin: 0,
            msg: msg.encode_to_vec(),
        };

        let origin = self.mailbook.read().await.get(&to).copied();
        if let Some(origin) = origin {
            msg.origin = origin;
            let _ = self.client.clone().send_msg(msg).await;
        } else {
            // We don't know about the targeting peer. Drop the msg and probe.
            // It's ok because the raft peer will automatically retry.
            debug!(self.logger, "drop msg and probe"; "missing_peer" => to);
            self.probe().await;
        }
    }

    async fn update_mailbook(&self, peer_id: u64, origin: u64) {
        let record = self.mailbook.read().await.get(&peer_id).copied();
        match record {
            Some(recorded) if origin != recorded => {
                let old = self.mailbook.write().await.insert(peer_id, origin);
                debug!(
                    self.logger,
                    "mailbook updated";
                    "peer_id" => peer_id,
                    "new_origin" => origin,
                    "old" => old,
                );
            }
            None => {
                self.mailbook.write().await.insert(peer_id, origin);
                debug!(
                    self.logger,
                    "mailbook add record";
                    "peer_id" => peer_id,
                    "origin" => origin,
                );
            }
            _ => (),
        }
    }

    // Broadcast msg to probe peer's origin.
    async fn probe(&self) {
        let msg = NetworkMsg {
            module: "consensus".to_string(),
            r#type: "probe".to_string(),
            origin: 0,
            msg: self.local_id.to_be_bytes().to_vec(),
        };
        let _ = self.client.clone().broadcast(msg).await;
    }
}

#[tonic::async_trait]
impl NetworkMsgHandlerService for Network {
    async fn process_network_msg(
        &self,
        request: tonic::Request<NetworkMsg>,
    ) -> Result<tonic::Response<StatusCode>, tonic::Status> {
        let msg = request.into_inner();
        if msg.module != "consensus" {
            return Err(tonic::Status::invalid_argument("wrong module"));
        }

        let origin = msg.origin;
        match msg.r#type.as_str() {
            // Raft's messages
            "raft" => {
                let raft_msg = RaftMsg::decode(msg.msg.as_slice())
                    .map_err(|_| tonic::Status::invalid_argument("can't decode raft msg"))?;

                self.update_mailbook(raft_msg.from, origin).await;

                if raft_msg.to != self.local_id {
                    let rest_msg = NetworkMsg {
                        module: "consensus".to_string(),
                        r#type: "refresh".to_owned(),
                        origin,
                        msg: raft_msg.to.to_be_bytes().to_vec(),
                    };
                    let _ = self.client.clone().send_msg(rest_msg).await;
                } else {
                    self.msg_tx
                        .send(raft_msg)
                        .await
                        .map_err(|_| tonic::Status::internal("consensus service is closed"))?;
                }
            }
            // We've sent msg to a wrong peer. probe
            "refresh" => {
                let peer_id = parse_peer_id(msg.msg.as_slice())?;
                self.probe().await;
                debug!(self.logger, "received refresh resp"; "origin" => origin, "missing_peer" => peer_id);
            }
            "probe" => {
                let peer_id = parse_peer_id(msg.msg.as_slice())?;
                self.update_mailbook(peer_id, origin).await;

                let probe_resp = NetworkMsg {
                    module: "consensus".to_string(),
                    r#type: "probe-resp".to_string(),
                    origin,
                    msg: self.local_id.to_be_bytes().to_vec(),
                };
                let _ = self.client.clone().send_msg(probe_resp).await;
                debug!(self.logger, "peer "; "missing_peer" => peer_id);
            }
            "probe-resp" => {
                let peer_id = parse_peer_id(msg.msg.as_slice())?;
                self.update_mailbook(peer_id, origin).await;
            }
            _ => {
                return Err(tonic::Status::invalid_argument("unknown msg type"));
            }
        }

        // horrible
        Ok(tonic::Response::new(StatusCode { code: 0 }))
    }
}

fn parse_peer_id(data: &[u8]) -> Result<u64, tonic::Status> {
    let buf = data
        .try_into()
        .map_err(|_| tonic::Status::invalid_argument("invalid peer id"))?;
    Ok(u64::from_be_bytes(buf))
}
