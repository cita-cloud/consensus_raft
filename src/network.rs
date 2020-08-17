use crate::error::Result;
use cita_cloud_proto::common::{Empty, Hash};
use cita_cloud_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_cloud_proto::network::{network_service_client::NetworkServiceClient, NetworkMsg};
use log::info;
use raft::eraftpb::Message;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time;
use tonic::transport::channel::Channel;

type ControllerClient = Consensus2ControllerServiceClient<Channel>;
type NetworkClient = NetworkServiceClient<Channel>;

#[derive(Clone)]
pub struct NetworkManager {
    controller_port: u16,
    network_port: u16,
    controller_pool: Arc<Mutex<Vec<ControllerClient>>>,
    network_pool: Arc<Mutex<Vec<NetworkClient>>>,
    dispatch_table: Arc<RwLock<HashMap<u64, u64>>>,
}

struct PoolGuard<T: Send + 'static> {
    pool: Arc<Mutex<Vec<T>>>,
    droplet: Option<T>,
}

impl<T: Send + 'static> Deref for PoolGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.droplet.as_ref().unwrap()
    }
}

impl<T: Send + 'static> DerefMut for PoolGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.droplet.as_mut().unwrap()
    }
}

impl<T: Send + 'static> Drop for PoolGuard<T> {
    fn drop(&mut self) {
        let droplet = self.droplet.take().unwrap();
        let pool = self.pool.clone();
        tokio::spawn(async move {
            pool.lock().await.push(droplet);
        });
    }
}

impl NetworkManager {
    pub fn with_capacity(controller_port: u16, network_port: u16, capacity: u64) -> Self {
        let controller_pool = Arc::new(Mutex::new(vec![]));
        let network_pool = Arc::new(Mutex::new(vec![]));
        let dispatch_table = Arc::new(RwLock::new(HashMap::new()));
        tokio::spawn(Self::fill_controller_pool(
            controller_port,
            controller_pool.clone(),
            capacity,
        ));
        tokio::spawn(Self::fill_network_pool(
            network_port,
            network_pool.clone(),
            capacity,
        ));
        Self {
            controller_port,
            network_port,
            controller_pool,
            network_pool,
            dispatch_table,
        }
    }

    async fn fill_controller_pool(
        controller_port: u16,
        pool: Arc<Mutex<Vec<ControllerClient>>>,
        capacity: u64,
    ) {
        for _ in 0..capacity {
            let pool_cloned = pool.clone();
            tokio::spawn(async move {
                let client = Self::get_controller_client(controller_port).await;
                pool_cloned.lock().await.push(client);
            });
        }
    }

    async fn fill_network_pool(
        network_port: u16,
        pool: Arc<Mutex<Vec<NetworkClient>>>,
        capacity: u64,
    ) {
        for _ in 0..capacity {
            let pool_cloned = pool.clone();
            tokio::spawn(async move {
                let client = Self::get_network_client(network_port).await;
                pool_cloned.lock().await.push(client);
            });
        }
    }

    async fn get_controller_client(controller_port: u16) -> ControllerClient {
        let d = Duration::from_millis(100);
        let mut interval = time::interval(d);
        let controller_addr = format!("http://127.0.0.1:{}", controller_port);
        info!("connecting to controller...");
        loop {
            match Consensus2ControllerServiceClient::connect(controller_addr.clone()).await {
                Ok(client) => return client,
                Err(e) => {
                    info!("connect to controller failed: `{}`", e);
                }
            }
            interval.tick().await;
            info!("Retrying to connect controller");
        }
    }

    async fn get_network_client(network_port: u16) -> NetworkClient {
        let d = Duration::from_millis(100);
        let mut interval = time::interval(d);
        let network_addr = format!("http://127.0.0.1:{}", network_port);
        info!("connecting to network...");
        loop {
            match NetworkServiceClient::connect(network_addr.clone()).await {
                Ok(client) => return client,
                Err(e) => {
                    info!("connect to network failed: `{}`", e);
                }
            }
            interval.tick().await;
            info!("Retrying to connect network");
        }
    }

    async fn controller_client(&mut self) -> PoolGuard<ControllerClient> {
        loop {
            {
                let mut pool = self.controller_pool.lock().await;
                if !pool.is_empty() {
                    return PoolGuard::<ControllerClient> {
                        pool: self.controller_pool.clone(),
                        droplet: pool.pop(),
                    };
                }
            }
            let d = Duration::from_millis(100);
            let mut interval = time::interval(d);
            interval.tick().await;
        }
    }

    async fn network_client(&mut self) -> PoolGuard<NetworkClient> {
        loop {
            {
                let mut pool = self.network_pool.lock().await;
                if !pool.is_empty() {
                    return PoolGuard::<NetworkClient> {
                        pool: self.network_pool.clone(),
                        droplet: pool.pop(),
                    };
                }
            }
            let d = Duration::from_millis(100);
            let mut interval = time::interval(d);
            interval.tick().await;
        }
    }

    pub async fn check_proposal(&mut self, proposal: Vec<u8>) -> Result<bool> {
        info!("check proposal...");
        let request = tonic::Request::new(Hash { hash: proposal });
        let mut controller = self.controller_client().await;
        let response = controller.check_proposal(request).await?;
        Ok(response.into_inner().is_success)
    }

    pub async fn commit_block(&mut self, proposal: Vec<u8>) -> Result<()> {
        info!("commit block...");
        let request = tonic::Request::new(Hash { hash: proposal });
        let mut controller = self.controller_client().await;
        let _response = controller.commit_block(request).await?;
        Ok(())
    }

    pub async fn get_proposal(&mut self) -> Result<Vec<u8>> {
        info!("get proposal...");
        let request = tonic::Request::new(Empty {});
        let mut controller = self.controller_client().await;
        let response = controller.get_proposal(request).await?;
        Ok(response.into_inner().hash)
    }

    pub async fn broadcast(&mut self, msg: Message) -> Result<()> {
        use protobuf::Message as _;

        info!("broadcast...");

        let payload = msg.write_to_bytes().unwrap();
        let request = tonic::Request::new(NetworkMsg {
            module: "consensus".to_owned(),
            r#type: "raft".to_owned(),
            origin: 0,
            msg: payload,
        });
        let mut network = self.network_client().await;
        let _response = network.broadcast(request).await?;
        Ok(())
    }

    pub async fn send_msg(&mut self, msg: Message) -> Result<()> {
        use protobuf::Message as _;

        info!("send_msg...");

        let to = msg.to;
        let entry = {
            let r = self.dispatch_table.read().await;
            r.get(&to).cloned()
        };
        if let Some(origin) = entry {
            info!("send single msg to {}: {}.", to, origin);
            let payload = msg.write_to_bytes().unwrap();
            let request = tonic::Request::new(NetworkMsg {
                module: "consensus".to_owned(),
                r#type: "raft".to_owned(),
                origin,
                msg: payload,
            });
            let mut network = self.network_client().await;
            let _response = network.send_msg(request).await?;
            Ok(())
        } else {
            info!("fall back.");
            self.broadcast(msg).await
        }
    }

    pub async fn update_table(&self, from: u64, origin: u64) {
        info!("update table: from-{} origin-{}", from, origin);
        let _prev = self.dispatch_table.write().await.insert(from, origin);
    }
}
