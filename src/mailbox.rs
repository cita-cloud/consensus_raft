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

use slog::debug;
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

/// This part is for handling communication with controller, network,
/// and msgs coming from other peers.

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
    #[allow(unused)]
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

    #[allow(unused)]
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
                        // Record the mapping from logical addr to the network addr
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
                    // Get the network addr from the record.
                    // If None, it will fall back to broadcast.
                    if let Some(&origin) = self.mailbook.get(&to) {
                        *session_id = Some(origin);
                    }
                }
                self.network_sender.send(m)?;
            }
        }
        Ok(())
    }

    /// Get `MailboxControl`.
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
        let controller = Self::connect_controller(controller_port, logger.clone()).await;
        for _ in 0..worker_num {
            let mail_queue = mail_queue.clone();
            let controller = controller.clone();
            let logger = logger.clone();
            tokio::spawn(async move {
                Self::handle_controller_mail(controller, mail_queue, logger).await;
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
        logger: Logger,
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
                        if let Err(e) = reply_tx.send(response) {
                            warn!(logger, "send `GetProposal` reply failed: `{:?}`", e);
                        }
                    }
                    CheckProposal { proposal, reply_tx } => {
                        let request = tonic::Request::new(Hash { hash: proposal });
                        let response = controller
                            .check_proposal(request)
                            .await
                            .map(|resp| resp.into_inner().is_success)
                            .map_err(|e| e.into());
                        if let Err(e) = reply_tx.send(response) {
                            warn!(logger, "send `CheckProposal` reply failed: `{:?}`", e);
                        }
                    }
                    CommitBlock { pwp, reply_tx } => {
                        let request = tonic::Request::new(pwp);
                        let response = controller
                            .commit_block(request)
                            .await
                            .map(|_resp| ())
                            .map_err(|e| e.into());
                        if let Err(e) = reply_tx.send(response) {
                            warn!(logger, "send `CommitBlock` reply failed: `{:?}`", e);
                        }
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
        let network = Self::connect_network(network_port, logger.clone()).await;
        for _ in 0..worker_num {
            let mail_queue = mail_queue.clone();
            let logger = logger.clone();
            let network = network.clone();
            tokio::spawn(async move {
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
                            warn!(logger, "send `GetNetworkStatus` reply failed: `{:?}`", e);
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
                            warn!(logger, "send `SendMessage` reply failed: `{:?}`", e);
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
                                "send `SendMessageWithoutSessionId` or `BroadcastMessage` reply failed: `{:?}`", e
                            );
                        }
                    }
                }
            }
        }
    }

    // Connect to the controller. Retry on failure.
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
                    debug!(logger, "connect to controller failed: `{}`", e);
                }
            }
            debug!(logger, "Retrying to connect controller");
        }
    }

    // Connect to the network. Retry on failure.
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
                    debug!(logger, "connect to network failed: `{}`", e);
                }
            }
            debug!(logger, "Retrying to connect network");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct Letty {
        to: u32,
        from: u32,
    }

    impl Letter for Letty {
        type Address = u32;
        type ReadError = ();

        fn to(&self) -> Option<Self::Address> {
            if self.to > 0 {
                Some(self.to)
            } else {
                None
            }
        }

        fn from(&self) -> Self::Address {
            self.from
        }

        fn write_down(&self) -> Vec<u8> {
            [self.to.to_be_bytes(), self.from.to_be_bytes()].concat()
        }

        fn read_from(paper: &[u8]) -> Result<Self, Self::ReadError> {
            use std::convert::TryInto;
            if paper.len() != 8 {
                Err(())
            } else {
                let to = u32::from_be_bytes(paper[..4].try_into().unwrap());
                let from = u32::from_be_bytes(paper[4..8].try_into().unwrap());
                Ok(Self { to, from })
            }
        }
    }

    #[tokio::test]
    async fn test_mail_control() {
        use tokio::sync::mpsc;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let control = MailboxControl { mail_put: tx };

        let handle = tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                match m {
                    Mail::ToMe {
                        origin,
                        mail: MyMail::Normal { msg, reply_tx },
                    } => {
                        assert_eq!(origin, 0);
                        assert_eq!(msg, Letty { to: 1024, from: 42 });
                        reply_tx.send(Ok(())).unwrap();
                    }
                    Mail::ToController(ControllerMail::GetProposal { reply_tx }) => {
                        reply_tx
                            .send(Ok(b"The quick brown fox jumps over the lazy dog"[..].into()))
                            .unwrap();
                    }
                    Mail::ToController(ControllerMail::CheckProposal { proposal, reply_tx }) => {
                        assert_eq!(proposal, b"hello world");
                        reply_tx.send(Ok(true)).unwrap();
                    }
                    Mail::ToController(ControllerMail::CommitBlock { pwp, reply_tx }) => {
                        assert!(!pwp.proposal.is_empty());
                        assert!(!pwp.proof.is_empty());
                        reply_tx.send(Ok(())).unwrap();
                    }
                    Mail::ToNetwork(NetworkMail::GetNetworkStatus { reply_tx }) => {
                        reply_tx.send(Ok(42)).unwrap();
                    }
                    Mail::ToNetwork(NetworkMail::SendMessage {
                        session_id,
                        msg,
                        reply_tx,
                    }) => {
                        assert_eq!(session_id, None);
                        assert_eq!(
                            msg,
                            Letty {
                                to: 12345,
                                from: 54321
                            }
                        );
                        reply_tx.send(Ok(())).unwrap();
                    }
                    Mail::ToNetwork(NetworkMail::BroadcastMessage { msg, reply_tx }) => {
                        assert_eq!(
                            msg,
                            Letty {
                                to: 12345,
                                from: 54321
                            }
                        );
                        reply_tx.send(Ok(())).unwrap();
                    }
                }
            }
        });

        control
            .put_mail(0, Letty { to: 1024, from: 42 })
            .await
            .unwrap();

        assert_eq!(
            control.get_proposal().await.unwrap(),
            &b"The quick brown fox jumps over the lazy dog"[..],
        );
        assert_eq!(control.get_network_status().await.unwrap(), 42);
        assert_eq!(
            control
                .check_proposal(b"hello world"[..].into())
                .await
                .unwrap(),
            true,
        );
        control.commit_block(
            ProposalWithProof {
                proposal:
                    b"No three positive integers a, b, and c satisfy the equation a^n + b^n = c^n (for n > 2)"[..].into(),
                proof:
                    b"I have discovered a truly marvelous proof of this, which this margin is too narrow to contain."[..].into(),
            }
        ).await.unwrap();

        control
            .send_message(Letty {
                to: 12345,
                from: 54321,
            })
            .await
            .unwrap();
        control
            .broadcast_message(Letty {
                to: 12345,
                from: 54321,
            })
            .await
            .unwrap();
        drop(control);
        handle.await.unwrap();
    }
}
