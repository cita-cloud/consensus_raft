use crate::error::Result;
use cita_ng_proto::common::{Empty, Hash};
use cita_ng_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_ng_proto::network::{network_service_client::NetworkServiceClient, NetworkMsg};
use log::info;
use raft::eraftpb::Message;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time;
use tonic::transport::channel::Channel;

pub struct NetworkManager {
    controller_port: u16,
    network_port: u16,
    controller: Option<Consensus2ControllerServiceClient<Channel>>,
    network: Option<NetworkServiceClient<Channel>>,
    dispatch_table: HashMap<u64, u64>,
}

impl NetworkManager {
    pub fn new(controller_port: u16, network_port: u16) -> Self {
        Self {
            controller_port,
            network_port,
            controller: None,
            network: None,
            dispatch_table: HashMap::new(),
        }
    }

    async fn controller_client(&mut self) -> &mut Consensus2ControllerServiceClient<Channel> {
        if let Some(ref mut client) = self.controller {
            client
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
            client
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

    pub async fn check_proposal(&mut self, proposal: Vec<u8>) -> Result<bool> {
        info!("check proposal...");
        let controller = self.controller_client().await;
        let request = tonic::Request::new(Hash { hash: proposal });
        let response = controller.check_proposal(request).await?;
        Ok(response.into_inner().is_success)
    }

    pub async fn commit_block(&mut self, proposal: Vec<u8>) -> Result<()> {
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

        info!("send_msg...");

        let to = msg.to;
        dbg!(&self.dispatch_table);
        if let Some(&origin) = self.dispatch_table.get(&to) {
            info!("send single msg to {}: {}.", to, origin);
            let network = self.network_client().await;
            let payload = msg.write_to_bytes().unwrap();
            let request = tonic::Request::new(NetworkMsg {
                module: "consensus".to_owned(),
                r#type: "raft".to_owned(),
                origin,
                msg: payload,
            });
            let _response = network.send_msg(request).await?;
            Ok(())
        } else {
            info!("fall back.");
            self.broadcast(msg).await
        }
    }

    pub fn update_table(&mut self, from: u64, origin: u64) {
        info!("update table: from-{} origin-{}", from, origin);
        let _prev = self.dispatch_table.insert(from, origin);
    }
}
