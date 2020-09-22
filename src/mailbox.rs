use slog::info;
use slog::warn;
use slog::Logger;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time;
use tonic::transport::channel::Channel;

use cita_cloud_proto::common::{Empty, Hash, ProposalWithProof};
use cita_cloud_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;

use cita_cloud_proto::network::{network_service_client::NetworkServiceClient, NetworkMsg};

use crossbeam::queue::ArrayQueue;

use std::fmt::Debug;

use anyhow::Result;

type ControllerClient = Consensus2ControllerServiceClient<Channel>;
type NetworkClient = NetworkServiceClient<Channel>;

pub trait Letter: Clone + Debug + Send + Sized + Sync + 'static {
    type Address: std::hash::Hash + std::cmp::Eq;
    type ReadError: Debug;
    fn to(&self) -> Option<Self::Address>;
    fn from(&self) -> Self::Address;
    fn write_down(&self) -> Vec<u8>;
    fn read_from(paper: &[u8]) -> Result<Self, Self::ReadError>;
}

#[derive(Debug)]
pub enum Mail<T: Letter> {
    ToMe { origin: u64, mail: MyMail<T> },
    ToController(ControllerMail),
    ToNetwork(NetworkMail<T>),
}

#[derive(Debug)]
pub enum MyMail<T: Letter> {
    Normal {
        msg: T,
        reply_tx: oneshot::Sender<Result<()>>,
    },
}

#[derive(Debug)]
pub enum ControllerMail {
    GetProposal {
        reply_tx: oneshot::Sender<Result<Vec<u8>>>,
    },
    #[allow(unused)]
    CheckProposal {
        proposal: Vec<u8>,
        reply_tx: oneshot::Sender<Result<bool>>,
    },
    CommitBlock {
        pwp: ProposalWithProof,
        reply_tx: oneshot::Sender<Result<()>>,
    },
}

#[derive(Debug)]
pub enum NetworkMail<T: Letter> {
    GetNetworkStatus {
        reply_tx: oneshot::Sender<Result<u64>>,
    },
    SendMessage {
        session_id: Option<u64>,
        msg: T,
        reply_tx: oneshot::Sender<Result<()>>,
    },
    BroadcastMessage {
        msg: T,
        reply_tx: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone, Debug)]
pub struct MailboxControl<T: Letter> {
    mail_put: mpsc::UnboundedSender<Mail<T>>,
}

impl<T: Letter> MailboxControl<T> {
    // local

    pub async fn put_mail(&self, origin: u64, msg: T) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mail = MyMail::Normal { msg, reply_tx };
        self.mail_put.clone().send(Mail::ToMe { origin, mail })?;
        reply_rx.await?
    }

    // controller

    pub async fn get_proposal(&self) -> Result<Vec<u8>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mail = ControllerMail::GetProposal { reply_tx };
        self.mail_put.clone().send(Mail::ToController(mail))?;
        reply_rx.await?
    }

    #[allow(unused)]
    pub async fn check_proposal(&self, proposal: Vec<u8>) -> Result<bool> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mail = ControllerMail::CheckProposal { proposal, reply_tx };
        self.mail_put.clone().send(Mail::ToController(mail))?;
        reply_rx.await?
    }

    pub async fn commit_block(&self, pwp: ProposalWithProof) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mail = ControllerMail::CommitBlock { pwp, reply_tx };
        self.mail_put.clone().send(Mail::ToController(mail))?;
        reply_rx.await?
    }

    // network

    pub async fn get_network_status(&self) -> Result<u64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mail = NetworkMail::GetNetworkStatus { reply_tx };
        self.mail_put.clone().send(Mail::ToNetwork(mail))?;
        reply_rx.await?
    }

    pub async fn send_message(&self, msg: T) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mail = NetworkMail::SendMessage {
            session_id: None,
            msg,
            reply_tx,
        };
        self.mail_put.clone().send(Mail::ToNetwork(mail))?;
        reply_rx.await?
    }

    #[allow(unused)]
    pub async fn broadcast_message(&self, msg: T) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mail = NetworkMail::BroadcastMessage { msg, reply_tx };
        self.mail_put.clone().send(Mail::ToNetwork(mail))?;
        reply_rx.await?
    }
}

pub struct Mailbox<T: Letter> {
    local_addr: T::Address,

    mail_put: mpsc::UnboundedSender<Mail<T>>,
    mail_get: mpsc::UnboundedReceiver<Mail<T>>,

    mailbook: HashMap<T::Address, u64>,

    controller_sender: mpsc::UnboundedSender<ControllerMail>,
    network_sender: mpsc::UnboundedSender<NetworkMail<T>>,

    send_to: mpsc::UnboundedSender<T>,

    logger: Logger,
}

impl<T: Letter> Mailbox<T> {
    pub async fn new(
        local_addr: T::Address,
        controller_port: u16,
        network_port: u16,
        send_to: mpsc::UnboundedSender<T>,
        logger: Logger,
    ) -> Self {
        let (controller_sender, controller_receiver) = mpsc::unbounded_channel();
        let (network_sender, network_receiver) = mpsc::unbounded_channel();
        tokio::spawn(Self::serve_controller(
            controller_port,
            10,
            controller_receiver,
            logger.clone(),
        ));
        tokio::spawn(Self::serve_network(
            network_port,
            10,
            network_receiver,
            logger.clone(),
        ));
        let (mail_put, mail_get) = mpsc::unbounded_channel();
        Self {
            local_addr,
            mailbook: HashMap::new(),
            mail_put,
            mail_get,
            controller_sender,
            network_sender,
            send_to,
            logger,
        }
    }

    pub async fn run(&mut self) {
        while let Some(m) = self.mail_get.recv().await {
            if let Err(e) = self.handle_mail(m) {
                warn!(self.logger, "handle mail failed: `{}`", e);
            }
        }
    }

    fn handle_mail(&mut self, mail: Mail<T>) -> Result<()> {
        use Mail::*;
        match mail {
            ToMe { origin, mail } => match mail {
                MyMail::Normal { msg, reply_tx } => {
                    let from = msg.from();
                    let to = msg.to();
                    if to.is_none() || to.as_ref() == Some(&self.local_addr) {
                        self.mailbook.insert(from, origin);
                        self.send_to.send(msg)?;
                    }
                    reply_tx.send(Ok(())).unwrap();
                }
            },
            ToController(m) => {
                self.controller_sender.send(m)?;
            }
            ToNetwork(mut m) => {
                if let NetworkMail::SendMessage {
                    session_id, msg, ..
                } = &mut m
                {
                    let to = msg.to();
                    assert!(to.is_some(), "Mail dest must exist. To broadcast, use NetworkMail::BroadcastMessage instead.");
                    let to = to.unwrap();
                    if let Some(&origin) = self.mailbook.get(&to) {
                        *session_id = Some(origin);
                    }
                }
                self.network_sender.send(m)?;
            }
        }
        Ok(())
    }

    pub fn control(&self) -> MailboxControl<T> {
        MailboxControl {
            mail_put: self.mail_put.clone(),
        }
    }

    async fn serve_controller(
        controller_port: u16,
        worker_num: usize,
        mut controller_receiver: mpsc::UnboundedReceiver<ControllerMail>,
        logger: Logger,
    ) {
        let mail_queue = Arc::new(ArrayQueue::<ControllerMail>::new(worker_num * 32));
        for _ in 0..worker_num {
            let mail_queue = mail_queue.clone();
            let logger = logger.clone();
            tokio::spawn(async move {
                let controller = Self::connect_controller(controller_port, logger).await;
                Self::handle_controller_mail(controller, mail_queue).await;
            });
        }
        while let Some(mail) = controller_receiver.recv().await {
            if mail_queue.push(mail).is_err() {
                warn!(logger, "controller task queue is full, discard mail");
            }
        }
    }

    async fn handle_controller_mail(
        mut controller: ControllerClient,
        mail_queue: Arc<ArrayQueue<ControllerMail>>,
    ) {
        let mut interval = time::interval(Duration::from_millis(3));
        loop {
            interval.tick().await;
            while let Ok(mail) = mail_queue.pop() {
                use ControllerMail::*;
                match mail {
                    GetProposal { reply_tx } => {
                        let request = tonic::Request::new(Empty {});
                        let response = controller
                            .get_proposal(request)
                            .await
                            .map(|resp| resp.into_inner().hash)
                            .map_err(|e| e.into());
                        let _ = reply_tx.send(response);
                    }
                    CheckProposal { proposal, reply_tx } => {
                        let request = tonic::Request::new(Hash { hash: proposal });
                        let response = controller
                            .check_proposal(request)
                            .await
                            .map(|resp| resp.into_inner().is_success)
                            .map_err(|e| e.into());
                        let _ = reply_tx.send(response);
                    }
                    CommitBlock { pwp, reply_tx } => {
                        let request = tonic::Request::new(pwp);
                        let response = controller
                            .commit_block(request)
                            .await
                            .map(|_resp| ())
                            .map_err(|e| e.into());
                        let _ = reply_tx.send(response);
                    }
                }
            }
        }
    }

    async fn serve_network(
        network_port: u16,
        worker_num: usize,
        mut network_receiver: mpsc::UnboundedReceiver<NetworkMail<T>>,
        logger: Logger,
    ) {
        let mail_queue = Arc::new(ArrayQueue::<NetworkMail<T>>::new(worker_num * 32));
        for _ in 0..worker_num {
            let mail_queue = mail_queue.clone();
            let logger = logger.clone();
            tokio::spawn(async move {
                let network = Self::connect_network(network_port, logger.clone()).await;
                Self::handle_network_mail(network, mail_queue, logger).await;
            });
        }
        while let Some(mail) = network_receiver.recv().await {
            if mail_queue.push(mail).is_err() {
                warn!(logger, "network mail queue is full, discard mail");
            }
        }
    }

    async fn handle_network_mail(
        mut network: NetworkClient,
        network_mail_queue: Arc<ArrayQueue<NetworkMail<T>>>,
        logger: Logger,
    ) {
        let mut interval = time::interval(Duration::from_millis(3));
        loop {
            interval.tick().await;
            while let Ok(mail) = network_mail_queue.pop() {
                use NetworkMail::*;
                match mail {
                    GetNetworkStatus { reply_tx } => {
                        let request = tonic::Request::new(Empty {});
                        let resp = network
                            .get_network_status(request)
                            .await
                            .map(|r| r.into_inner().peer_count)
                            .map_err(|e| e.into());
                        if let Err(e) = reply_tx.send(resp) {
                            warn!(logger, "reply GetNetworkStatus failed: `{:?}`", e);
                        }
                    }
                    SendMessage {
                        session_id: Some(origin),
                        msg,
                        reply_tx,
                    } => {
                        let request = tonic::Request::new(NetworkMsg {
                            module: "consensus".to_owned(),
                            r#type: "raft".to_owned(),
                            origin,
                            msg: msg.write_down(),
                        });
                        let resp = network
                            .send_msg(request)
                            .await
                            .map(|_resp| ())
                            .map_err(|e| e.into());
                        if let Err(e) = reply_tx.send(resp) {
                            warn!(logger, "reply SendMessage failed: `{:?}`", e);
                        }
                    }
                    SendMessage {
                        session_id: None,
                        msg,
                        reply_tx,
                    }
                    | BroadcastMessage { msg, reply_tx } => {
                        let request = tonic::Request::new(NetworkMsg {
                            module: "consensus".to_owned(),
                            r#type: "raft".to_owned(),
                            origin: 0,
                            msg: msg.write_down(),
                        });
                        let resp = network
                            .broadcast(request)
                            .await
                            .map(|_resp| ())
                            .map_err(|e| e.into());
                        if let Err(e) = reply_tx.send(resp) {
                            warn!(
                                logger,
                                "reply non-dest SendMessage or BroadcastMessage failed: `{:?}`", e
                            );
                        }
                    }
                }
            }
        }
    }

    async fn connect_controller(controller_port: u16, logger: Logger) -> ControllerClient {
        let d = Duration::from_secs(1);
        let mut interval = time::interval(d);
        let controller_addr = format!("http://127.0.0.1:{}", controller_port);
        info!(logger, "connecting to controller...");
        loop {
            interval.tick().await;
            match Consensus2ControllerServiceClient::connect(controller_addr.clone()).await {
                Ok(client) => return client,
                Err(e) => {
                    info!(logger, "connect to controller failed: `{}`", e);
                }
            }
            info!(logger, "Retrying to connect controller");
        }
    }

    async fn connect_network(network_port: u16, logger: Logger) -> NetworkClient {
        let d = Duration::from_secs(1);
        let mut interval = time::interval(d);
        let network_addr = format!("http://127.0.0.1:{}", network_port);
        info!(logger, "connecting to network...");
        loop {
            interval.tick().await;
            match NetworkServiceClient::connect(network_addr.clone()).await {
                Ok(client) => return client,
                Err(e) => {
                    info!(logger, "connect to network failed: `{}`", e);
                }
            }
            info!(logger, "Retrying to connect network");
        }
    }
}
