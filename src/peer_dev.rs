use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Message as RaftMsg;
use raft::eraftpb::MessageType;
use raft::prelude::ConfChange;
use raft::prelude::ConfState;
use raft::prelude::Config;
use raft::prelude::Entry;
use raft::prelude::EntryType;
use raft::prelude::HardState;
use raft::prelude::RawNode;
use raft::prelude::Ready;
use raft::prelude::Snapshot;
// use raft::storage::MemStorage;
use raft::StateRole;

use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time;

use slog::o;
use slog::Drain;

use anyhow::Result;

use protobuf::Message as _;

use crate::mailbox::Letter;
use crate::mailbox::MailboxControl;
use crate::storage::RaftStorage;
use log::{info, warn};

#[derive(Debug, Clone)]
pub enum PeerMsg {
    // Local message to control this peer.
    Control(ControlMsg),
    // Raft message to and from peers.
    Verified(RaftMsg), // Verified MsgAppend
    Normal(RaftMsg),
}

impl Letter for PeerMsg {
    type Address = u64;
    type ReadError = anyhow::Error;

    fn to(&self) -> Option<Self::Address> {
        match self {
            PeerMsg::Normal(raft_msg) => Some(raft_msg.to),
            PeerMsg::Control(_) | PeerMsg::Verified(_) => panic!("attempt to send local msg"),
        }
    }

    fn from(&self) -> Self::Address {
        match self {
            PeerMsg::Normal(raft_msg) => raft_msg.from,
            PeerMsg::Control(_) | PeerMsg::Verified(_) => panic!("attempt to send local msg"),
        }
    }

    fn write_down(&self) -> Vec<u8> {
        match self {
            PeerMsg::Normal(raft_msg) => raft_msg.write_to_bytes().unwrap(),
            PeerMsg::Control(_) | PeerMsg::Verified(_) => panic!("attempt to send local msg"),
        }
    }

    fn read_from(paper: &[u8]) -> std::result::Result<Self, Self::ReadError> {
        Ok(PeerMsg::Normal(protobuf::parse_from_bytes(paper)?))
    }
}

#[derive(Debug, Clone)]
pub enum ControlMsg {
    Propose { hash: Vec<u8> },
    AddNode { node_id: u64 },
    RemoveNode { node_id: u64 },
    ApplySnapshot { snapshot: Snapshot },
    Campaign,
    Tick,
}

pub struct Peer {
    raft: RawNode<RaftStorage>,
    pub msg_tx: mpsc::UnboundedSender<PeerMsg>,
    msg_rx: mpsc::UnboundedReceiver<PeerMsg>,
    mailbox_control: MailboxControl<PeerMsg>,
    peers: HashSet<u64>,
}

impl Peer {
    pub async fn new(
        id: u64,
        msg_tx: mpsc::UnboundedSender<PeerMsg>,
        msg_rx: mpsc::UnboundedReceiver<PeerMsg>,
        mailbox_control: MailboxControl<PeerMsg>,
    ) -> Self {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain)
            .chan_size(4096)
            .overflow_strategy(slog_async::OverflowStrategy::Block)
            .build()
            .fuse();
        let logger = slog::Logger::root(drain, o!());
        let logger = logger.new(o!("tag" => format!("peer_{}", id)));

        let cfg = Config {
            id,
            election_tick: 30,
            heartbeat_tick: 3,
            check_quorum: true,
            ..Default::default()
        };

        let mut storage = RaftStorage::new().await;
        if id == 1 {
            let mut s = Snapshot::default();
            s.mut_metadata().index = 1;
            s.mut_metadata().term = 1;
            s.mut_metadata().mut_conf_state().voters = vec![1];

            storage.core.apply_snapshot(s).await.unwrap();
        }

        let raw_node = RawNode::new(&cfg, storage, &logger).unwrap();

        let mut peers = HashSet::new();
        peers.insert(id);

        Self {
            raft: raw_node,
            msg_tx,
            msg_rx,
            mailbox_control,
            peers,
        }
    }

    pub async fn run(&mut self) {
        let mut started = false;

        while let Some(msg) = self.msg_rx.recv().await {
            if !started {
                tokio::spawn(Self::wait_proposal(
                    self.id(),
                    self.mailbox_control.clone(),
                    self.msg_tx.clone(),
                ));
                tokio::spawn(Self::pacemaker(self.msg_tx.clone()));
                started = true;
            }
            self.handle_msg(msg).await;
        }
    }

    fn id(&self) -> u64 {
        self.raft.raft.r.id
    }

    async fn handle_msg(&mut self, msg: PeerMsg) {
        match msg {
            PeerMsg::Control(ControlMsg::Propose { hash }) => {
                if let Err(e) = self.raft.propose(vec![], hash) {
                    warn!("propose failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::AddNode { node_id }) => {
                let mut cc = ConfChange::default();
                cc.node_id = node_id;
                cc.set_change_type(ConfChangeType::AddNode);
                if let Err(e) = self.raft.propose_conf_change(vec![], cc) {
                    warn!("add node failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::RemoveNode { node_id }) => {
                let mut cc = ConfChange::default();
                cc.node_id = node_id;
                cc.set_change_type(ConfChangeType::RemoveNode);
                if let Err(e) = self.raft.propose_conf_change(vec![], cc) {
                    warn!("remove node failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::ApplySnapshot { snapshot }) => {
                if let Err(e) = self.raft.mut_store().core.apply_snapshot(snapshot).await {
                    warn!("apply snapshot failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::Campaign) => {
                if let Err(e) = self.raft.campaign() {
                    warn!("campaign failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::Tick) => {
                self.raft.tick();
            }
            PeerMsg::Verified(raft_msg) => {
                if let Err(e) = self.raft.step(raft_msg) {
                    warn!("raft step failed: `{}`", e);
                }
            }
            PeerMsg::Normal(raft_msg) => {
                if let MessageType::MsgAppend = raft_msg.msg_type {
                    let msg_tx = self.msg_tx.clone();
                    let mailbox_control = self.mailbox_control.clone();
                    tokio::spawn(async move {
                        let mut is_ok = true;
                        for ent in raft_msg.entries.iter() {
                            if let EntryType::EntryNormal = ent.entry_type {
                                if let Ok(false) | Err(_) =
                                    mailbox_control.check_proposal(ent.data.clone()).await
                                {
                                    info!("check_proposal failed: `{:?}`", &ent.data);
                                    is_ok = false;
                                }
                            }
                        }
                        if is_ok {
                            let msg = PeerMsg::Verified(raft_msg);
                            msg_tx.send(msg).unwrap();
                        } else {
                            info!("check proposal failed");
                        }
                    });
                } else if let Err(e) = self.raft.step(raft_msg) {
                    warn!("raft step failed: `{}`", e);
                }
            }
        }
        if let Err(e) = self.handle_ready().await {
            warn!("handle ready failed: `{}`", e);
        }
    }

    fn is_leader(&self) -> bool {
        self.raft.raft.state == StateRole::Leader
    }

    pub fn control(&self) -> PeerControl {
        PeerControl {
            id: self.id(),
            msg_tx: self.msg_tx.clone(),
        }
    }

    pub fn get_service(&self) -> RaftService<PeerMsg> {
        RaftService {
            mailbox_control: self.mailbox_control.clone(),
            peer_control: self.control(),
        }
    }

    async fn wait_proposal(
        id: u64,
        mailbox_control: MailboxControl<PeerMsg>,
        tx: mpsc::UnboundedSender<PeerMsg>,
    ) {
        let block_interval = Duration::from_secs(6);
        let mut ticker = time::interval(block_interval);
        // wait for peers to start
        ticker.tick().await;
        ticker.tick().await;
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if id == 1 {
                match mailbox_control.get_proposal().await {
                    Ok(hash) => tx
                        .send(PeerMsg::Control(ControlMsg::Propose { hash }))
                        .unwrap(),
                    Err(e) => warn!("get proposal failed: `{}`", e),
                }
            }
        }
    }

    async fn pacemaker(msg_tx: mpsc::UnboundedSender<PeerMsg>) {
        let pace = Duration::from_millis(100);
        let mut ticker = time::interval(pace);
        loop {
            ticker.tick().await;
            msg_tx.send(PeerMsg::Control(ControlMsg::Tick)).unwrap();
        }
    }

    async fn handle_ready(&mut self) -> Result<()> {
        if !self.raft.has_ready() {
            return Ok(());
        }
        let mut ready = self.raft.ready();
        let store = self.raft.mut_store();

        if !ready.snapshot().is_empty() {
            let s = ready.snapshot().clone();
            if let Err(e) = store.core.apply_snapshot(s).await {
                info!("apply snapshot failed: {:?}", e);
            }
        }

        if !ready.entries().is_empty() {
            if let Err(e) = store.core.append(ready.entries()).await {
                info!("append entries failed: {:?}", e);
            }
        }

        if let Some(hs) = ready.hs() {
            store.core.set_hard_state(hs.clone()).await;
        }

        let messages = ready.messages.drain(..).collect::<Vec<_>>();
        let mailbox_control = self.mailbox_control.clone();
        tokio::spawn(async move {
            for msg in messages {
                let pm = PeerMsg::Normal(msg);
                if let Err(e) = mailbox_control.send_message(pm).await {
                    info!("send msg failed: {:?}", e);
                }
            }
        });

        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() {
                    continue;
                }
                match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        info!("commiting proposal..");
                        let proposal = entry.data.clone();
                        let mailbox_control = self.mailbox_control.clone();
                        tokio::spawn(async move {
                            let pwp = ProposalWithProof {
                                proposal,
                                proof: vec![],
                            };
                            while let Err(e) = mailbox_control.commit_block(pwp.clone()).await {
                                warn!("commit block failed: {:?}", e);
                            }
                        });
                    }
                    EntryType::EntryConfChange => {
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data)?;
                        let cs = self.raft.apply_conf_change(&cc)?;
                        self.raft.mut_store().core.set_conf_state(cs).await;
                        match cc.change_type {
                            ConfChangeType::AddNode => {
                                info!("{} add node #{}", cc.id, cc.node_id);
                                self.peers.insert(cc.node_id);
                            }
                            ConfChangeType::RemoveNode => {
                                info!("{} remove node #{}", cc.id, cc.node_id);
                                self.peers.remove(&cc.node_id);
                            }
                            ConfChangeType::AddLearnerNode => unimplemented!(),
                        }
                    }
                    EntryType::EntryConfChangeV2 => warn!("ConfChangeV2 unimplemented."),
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                let store = self.raft.mut_store();
                store.core.mut_hard_state().commit = last_committed.index;
                store.core.mut_hard_state().term = last_committed.term;
                store.core.sync_hard_state().await;
            }
        }
        self.raft.advance(ready);
        Ok(())
    }
}

#[derive(Clone)]
pub struct PeerControl {
    id: u64,
    msg_tx: mpsc::UnboundedSender<PeerMsg>,
}

impl PeerControl {
    pub fn add_node(&self, node_id: u64) {
        let msg = PeerMsg::Control(ControlMsg::AddNode { node_id });
        self.msg_tx.send(msg).unwrap();
    }

    pub fn remove_node(&self, node_id: u64) {
        let msg = PeerMsg::Control(ControlMsg::RemoveNode { node_id });
        self.msg_tx.send(msg).unwrap();
    }

    pub fn campaign(&self) {
        let msg = PeerMsg::Control(ControlMsg::Campaign);
        self.msg_tx.send(msg).unwrap();
    }

    pub fn init_leader(&self) {
        // let msg = PeerMsg::Control(ControlMsg::ApplySnapshot{ snapshot: s });
        // self.msg_tx.send(msg).unwrap();
        self.campaign();
    }

    // return true since we assume no byzantine fault.
    pub fn check_block(&self, _pwp: ProposalWithProof) -> bool {
        // let msg = PeerMsg::Control(ControlMsg::CheckBlock { pwp });
        // self.msg_tx.send(msg).unwrap()
        // TODO
        true
    }
}

use cita_cloud_proto::common::SimpleResponse;
use cita_cloud_proto::common::{Empty, Hash, ProposalWithProof};
use cita_cloud_proto::consensus::{
    consensus_service_server::ConsensusService, ConsensusConfiguration,
};
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerService;
use cita_cloud_proto::network::NetworkMsg;

#[derive(Clone)]
pub struct RaftService<T: Letter> {
    pub mailbox_control: MailboxControl<T>,
    pub peer_control: PeerControl,
}

#[tonic::async_trait]
impl<T: Letter> NetworkMsgHandlerService for RaftService<T> {
    async fn process_network_msg(
        &self,
        request: tonic::Request<NetworkMsg>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        // info!("process_network_msg request: {:?}", request);

        let msg = request.into_inner();
        if msg.module != "consensus" {
            Err(tonic::Status::invalid_argument("wrong module"))
        } else {
            let letter = Letter::read_from(msg.msg.as_slice()).map_err(|e| {
                tonic::Status::invalid_argument(format!("msg fail to decode: `{:?}`", e))
            })?;
            let origin = msg.origin;
            self.mailbox_control.put_mail(origin, letter).await.unwrap();
            let reply = SimpleResponse { is_success: true };
            Ok(tonic::Response::new(reply))
        }
    }
}

#[tonic::async_trait]
impl<T: Letter> ConsensusService for RaftService<T> {
    async fn reconfigure(
        &self,
        _request: tonic::Request<ConsensusConfiguration>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        // info!("reconfigure request: {:?}", request);
        // TODO
        let reply = SimpleResponse { is_success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn check_block(
        &self,
        request: tonic::Request<ProposalWithProof>,
    ) -> Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let pwp = request.into_inner();
        let is_success = self.peer_control.check_block(pwp);
        let reply = SimpleResponse { is_success };
        Ok(tonic::Response::new(reply))
    }
}
