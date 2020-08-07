use cita_ng_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_ng_proto::network::{network_service_client::NetworkServiceClient, NetworkMsg};
use std::time::Duration;
use tokio::time;

use cita_ng_proto::common::{Empty, Hash};
#[allow(unused)]
use log::{info, warn};
use raft::eraftpb::Message;
use tonic::transport::channel::Channel;
use crate::error::Result;

pub struct NetworkManager {
    controller_port: u16,
    network_port: u16,
    controller: Option<Consensus2ControllerServiceClient<Channel>>,
    network: Option<NetworkServiceClient<Channel>>,
}

impl NetworkManager {
    pub fn new(controller_port: u16, network_port: u16) -> Self {
        Self {
            controller_port,
            network_port,
            controller: None,
            network: None,
        }
    }

    async fn controller_client(&mut self) -> &mut Consensus2ControllerServiceClient<Channel> {
        if let Some(ref mut client) = self.controller {
            return client;
        } else {
            let d = Duration::from_millis(100);
            let mut interval = time::interval(d);
            let controller_addr = format!("http://127.0.0.1:{}", self.controller_port);
            info!("connecting to controller...");
            loop {
                match Consensus2ControllerServiceClient::connect(controller_addr.clone()).await {
                    Ok(client) => {
                        self.controller = Some(client);
                        return self.controller.as_mut().unwrap();
                    }
                    Err(e) => {
                        info!("connect to controller failed: `{}`", e);
                    }
                }
                interval.tick().await;
                info!("Retrying to connect controller");
            }
        }
    }

    async fn network_client(&mut self) -> &mut NetworkServiceClient<Channel> {
        if let Some(ref mut client) = self.network {
            return client;
        } else {
            let d = Duration::from_millis(100);
            let mut interval = time::interval(d);
            let network_addr = format!("http://127.0.0.1:{}", self.network_port);
            info!("connecting to network...");
            loop {
                match NetworkServiceClient::connect(network_addr.clone()).await {
                    Ok(client) => {
                        self.network = Some(client);
                        return self.network.as_mut().unwrap();
                    }
                    Err(e) => {
                        info!("connect to network failed: `{}`", e);
                    }
                }
                interval.tick().await;
                info!("Retrying to connect network");
            }
        }
    }

    pub async fn check_proposal(
        &mut self,
        proposal: Vec<u8>,
    ) -> Result<bool> {
        info!("check proposal...");
        let controller = self.controller_client().await;
        let request = tonic::Request::new(Hash { hash: proposal });
        let response = controller.check_proposal(request).await?;
        Ok(response.into_inner().is_success)
    }

    pub async fn commit_block(
        &mut self,
        proposal: Vec<u8>,
    ) -> Result<()> {
        info!("commit block...");
        let controller = self.controller_client().await;
        let request = tonic::Request::new(Hash { hash: proposal });
        let _response = controller.commit_block(request).await?;
        Ok(())
    }

    pub async fn get_proposal(&mut self) -> Result<Vec<u8>> {
        info!("get proposal...");
        let controller = self.controller_client().await;
        let request = tonic::Request::new(Empty {});
        let response = controller.get_proposal(request).await?;
        Ok(response.into_inner().hash)
    }

    pub async fn broadcast(&mut self, msg: Message) -> Result<()> {
        use protobuf::Message as _;

        info!("broadcast...");

        let network = self.network_client().await;
        let payload = msg.write_to_bytes().unwrap();
        let request = tonic::Request::new(NetworkMsg {
            module: "consensus".to_owned(),
            r#type: "raft".to_owned(),
            origin: 0,
            msg: payload,
        });
        let _response = network.broadcast(request).await?;
        Ok(())
    }

    pub async fn send_msg(&mut self, msg: Message) -> Result<()> {
        use protobuf::Message as _;

        info!("broadcast...");

        let network = self.network_client().await;
        let payload = msg.write_to_bytes().unwrap();
        let request = tonic::Request::new(NetworkMsg {
            module: "consensus".to_owned(),
            r#type: "raft".to_owned(),
            origin: 0,
            msg: payload,
        });
        let _response = network.send_msg(request).await?;
        Ok(())
    }
}
