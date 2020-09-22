use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Message as RaftMsg;
use raft::eraftpb::MessageType;
use raft::prelude::ConfChange;
use raft::prelude::Config;
use raft::prelude::EntryType;
use raft::prelude::RawNode;
use raft::prelude::Snapshot;
use raft::StateRole;

use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time;

use slog::o;
use slog::Logger;

use slog::info;
use slog::trace;
use slog::warn;

use anyhow::Result;

use protobuf::Message as _;

use crate::mailbox::Letter;
use crate::mailbox::MailboxControl;
use crate::storage::RaftStorage;

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

// Use mpsc channel instead of oneshot since it requires Clone.
#[derive(Debug, Clone)]
pub enum ControlMsg {
    Propose {
        hash: Vec<u8>,
    },
    AddNode {
        node_id: u64,
    },
    RemoveNode {
        node_id: u64,
    },
    GetNodeList {
        reply_tx: mpsc::UnboundedSender<Vec<u64>>,
    },
    ApplySnapshot {
        snapshot: Snapshot,
    },
    GetBlockInterval {
        reply_tx: mpsc::UnboundedSender<u32>,
    },
    SetConsensusConfig {
        config: ConsensusConfiguration,
        reply_tx: mpsc::UnboundedSender<()>,
    },
    CheckBlock {
        pwp: ProposalWithProof,
        reply_tx: mpsc::UnboundedSender<bool>,
    },
    IsLeader {
        reply_tx: mpsc::UnboundedSender<bool>,
    },
    Campaign,
    Tick,
}

pub struct Peer {
    raft: RawNode<RaftStorage>,
    consensus_config: ConsensusConfiguration,

    msg_tx: mpsc::UnboundedSender<PeerMsg>,
    msg_rx: mpsc::UnboundedReceiver<PeerMsg>,
    mailbox_control: MailboxControl<PeerMsg>,

    logger: Logger,
}

impl Peer {
    pub async fn new(
        id: u64,
        consensus_config: ConsensusConfiguration,
        msg_tx: mpsc::UnboundedSender<PeerMsg>,
        msg_rx: mpsc::UnboundedReceiver<PeerMsg>,
        mailbox_control: MailboxControl<PeerMsg>,
        init_leader_id: u64,
        logger: Logger,
    ) -> Self {
        let logger = logger.new(o!("tag" => format!("peer_{}", id)));

        let mut storage = RaftStorage::new().await;
        // This step is according to the example in raft-rs to initialize the leader.
        if id == init_leader_id && !storage.core.is_initialized() {
            let mut s = Snapshot::default();
            s.mut_metadata().index = 1;
            s.mut_metadata().term = 1;
            s.mut_metadata().mut_conf_state().voters = vec![id];

            storage.core.apply_snapshot(s).await.unwrap();
        }

        let applied = storage.core.applied_index();
        let cfg = Config {
            id,
            election_tick: 30,
            heartbeat_tick: 3,
            check_quorum: true,
            applied,
            ..Default::default()
        };

        let raw_node = RawNode::new(&cfg, storage, &logger).unwrap();

        Self {
            raft: raw_node,
            msg_tx,
            msg_rx,
            mailbox_control,
            consensus_config,
            logger,
        }
    }

    pub async fn run(&mut self) {
        let mut started = false;

        while let Some(msg) = self.msg_rx.recv().await {
            if !started {
                // Leader should be started by campaign msg.
                // Followers should be started by leader's msg.
                // Don't start them when receives SetConsensusConfig msg.
                if let PeerMsg::Control(ControlMsg::SetConsensusConfig { .. }) = &msg {
                    // Do nothing.
                } else {
                    // Try to get a proposal every block interval secs.
                    let logger = self
                        .logger
                        .new(o!("init_block_interval" => self.consensus_config.block_interval));
                    tokio::spawn(Self::wait_proposal(self.service(), logger));
                    // Send tick msg to raft periodically.
                    tokio::spawn(Self::pacemaker(self.msg_tx.clone()));
                    started = true;
                }
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
                    warn!(self.logger, "propose failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::AddNode { node_id }) => {
                let mut cc = ConfChange::default();
                cc.node_id = node_id;
                cc.set_change_type(ConfChangeType::AddNode);
                if let Err(e) = self.raft.propose_conf_change(vec![], cc) {
                    warn!(self.logger, "add node failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::RemoveNode { node_id }) => {
                let mut cc = ConfChange::default();
                cc.node_id = node_id;
                cc.set_change_type(ConfChangeType::RemoveNode);
                if let Err(e) = self.raft.propose_conf_change(vec![], cc) {
                    warn!(self.logger, "remove node failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::GetNodeList { reply_tx }) => {
                if let Err(e) = reply_tx.send(self.raft.store().core.conf_state().voters.clone()) {
                    warn!(self.logger, "reply GetNodeList request failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::ApplySnapshot { snapshot }) => {
                if let Err(e) = self.raft.mut_store().core.apply_snapshot(snapshot).await {
                    warn!(self.logger, "apply snapshot failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::GetBlockInterval { reply_tx }) => {
                if let Err(e) = reply_tx.send(self.consensus_config.block_interval) {
                    warn!(
                        self.logger,
                        "reply GetBlockInterval request failed: `{}`", e
                    );
                }
            }
            // Reply true since we assume no byzantine faults.
            PeerMsg::Control(ControlMsg::CheckBlock {
                pwp: _pwp,
                reply_tx,
            }) => {
                if let Err(e) = reply_tx.send(true) {
                    warn!(self.logger, "reply CheckBlock request failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::SetConsensusConfig { config, reply_tx }) => {
                self.consensus_config = config;
                if let Err(e) = reply_tx.send(()) {
                    warn!(
                        self.logger,
                        "reply SetConsensusConfig request failed: `{}`", e
                    );
                }
            }
            PeerMsg::Control(ControlMsg::IsLeader { reply_tx }) => {
                if let Err(e) = reply_tx.send(self.is_leader()) {
                    warn!(self.logger, "reply IsLeader request failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::Campaign) => {
                if let Err(e) = self.raft.campaign() {
                    warn!(self.logger, "campaign failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::Tick) => {
                self.raft.tick();
            }
            // Checked MsgAppend msg, steps it directly.
            PeerMsg::Verified(raft_msg) => {
                trace!(self.logger, "stepping verified MsgAppend"; "entries" => ?raft_msg.entries);
                if let Err(e) = self.raft.step(raft_msg) {
                    warn!(self.logger, "raft step verified msg failed: `{}`", e);
                }
            }
            PeerMsg::Normal(raft_msg) => {
                // If the msg is to append entries, check it first.
                if let MessageType::MsgAppend = raft_msg.msg_type {
                    let msg_tx = self.msg_tx.clone();
                    let mailbox_control = self.mailbox_control.clone();
                    let logger = self.logger.clone();
                    tokio::spawn(async move {
                        let mut is_ok = true;
                        for ent in raft_msg.entries.iter() {
                            // Ignore empty entries.
                            if !ent.data.is_empty() {
                                if let EntryType::EntryNormal = ent.entry_type {
                                    if let Ok(false) | Err(_) =
                                        mailbox_control.check_proposal(ent.data.clone()).await
                                    {
                                        warn!(logger, "check_proposal failed: `{:?}`", &ent.data);
                                        is_ok = false;
                                        break;
                                    }
                                }
                            }
                        }
                        // Send the msg back to raft if all of its entries passes the check.
                        if is_ok {
                            let msg = PeerMsg::Verified(raft_msg);
                            msg_tx.send(msg).unwrap();
                        }
                    });
                } else if let Err(e) = self.raft.step(raft_msg) {
                    warn!(self.logger, "raft step failed: `{}`", e);
                }
            }
        }
        if let Err(e) = self.handle_ready().await {
            warn!(self.logger, "handle ready failed: `{}`", e);
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

    pub fn service(&self) -> RaftService<PeerMsg> {
        RaftService {
            mailbox_control: self.mailbox_control.clone(),
            peer_control: self.control(),
        }
    }

    async fn wait_proposal(service: RaftService<PeerMsg>, logger: Logger) {
        let d = Duration::from_secs(1);
        let mut ticker = time::interval(d);
        let mut last_propose_time = Instant::now();
        for _ in 0..16 {
            ticker.tick().await;
        }
        loop {
            ticker.tick().await;
            let block_interval = service.peer_control.get_block_interval().await;
            if last_propose_time.elapsed().as_secs() >= block_interval as u64
                && service.peer_control.is_leader().await
            {
                info!(logger, "start propose");
                last_propose_time = Instant::now();
                match service.mailbox_control.get_proposal().await {
                    Ok(hash) => service.peer_control.propose(hash),
                    Err(e) => warn!(logger, "get proposal failed: `{}`", e),
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
                warn!(self.logger, "apply snapshot failed: {}", e);
            }
        }

        if !ready.entries().is_empty() {
            if let Err(e) = store.core.append(ready.entries()).await {
                warn!(self.logger, "append entries failed: {}", e);
            }
        }

        if let Some(hs) = ready.hs() {
            store.core.set_hard_state(hs.clone()).await;
        }

        let messages = ready.messages.drain(..).collect::<Vec<_>>();
        let mailbox_control = self.mailbox_control.clone();
        let logger_cloned = self.logger.clone();
        tokio::spawn(async move {
            for msg in messages {
                let pm = PeerMsg::Normal(msg);
                if let Err(e) = mailbox_control.send_message(pm).await {
                    warn!(logger_cloned, "send msg failed: {}", e);
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
                        info!(self.logger, "commiting proposal..");
                        let proposal = entry.data.clone();
                        // Ignore any empty entries that may be produced by raft itself.
                        if !proposal.is_empty() {
                            let mailbox_control = self.mailbox_control.clone();
                            let logger = self.logger.clone();
                            tokio::spawn(async move {
                                let pwp = ProposalWithProof {
                                    proposal,
                                    proof: vec![],
                                };
                                let mut retry_interval = time::interval(Duration::from_secs(3));
                                while let Err(e) = mailbox_control.commit_block(pwp.clone()).await {
                                    retry_interval.tick().await;
                                    warn!(logger, "commit block failed: `{:?}`", e);
                                }
                            });
                        }
                    }
                    EntryType::EntryConfChange => {
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data)?;
                        let cs = self.raft.apply_conf_change(&cc)?;
                        self.raft.mut_store().core.set_conf_state(cs).await;
                        match cc.change_type {
                            ConfChangeType::AddNode => {
                                info!(self.logger, "add node #{}", cc.node_id);
                            }
                            ConfChangeType::RemoveNode => {
                                info!(self.logger, "remove node #{}", cc.node_id);
                            }
                            ConfChangeType::AddLearnerNode => {
                                warn!(self.logger, "AddLearnerNode unimplemented.")
                            }
                        }
                    }
                    EntryType::EntryConfChangeV2 => {
                        warn!(self.logger, "ConfChangeV2 unimplemented.")
                    }
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                let store = self.raft.mut_store();
                store.core.mut_hard_state().commit = last_committed.index;
                store.core.mut_hard_state().term = last_committed.term;
                store.core.sync_hard_state().await;
                store.core.set_applied_index(last_committed.index).await;
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

    #[allow(unused)]
    pub fn remove_node(&self, node_id: u64) {
        let msg = PeerMsg::Control(ControlMsg::RemoveNode { node_id });
        self.msg_tx.send(msg).unwrap();
    }

    pub async fn get_node_list(&self) -> Vec<u64> {
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let msg = PeerMsg::Control(ControlMsg::GetNodeList { reply_tx });
        self.msg_tx.send(msg).unwrap();
        reply_rx.recv().await.unwrap()
    }

    #[allow(unused)]
    pub fn apply_snapshot(&self, snapshot: Snapshot) {
        let msg = PeerMsg::Control(ControlMsg::ApplySnapshot { snapshot });
        self.msg_tx.send(msg).unwrap();
    }

    pub fn propose(&self, hash: Vec<u8>) {
        let msg = PeerMsg::Control(ControlMsg::Propose { hash });
        self.msg_tx.send(msg).unwrap();
    }

    pub fn campaign(&self) {
        let msg = PeerMsg::Control(ControlMsg::Campaign);
        self.msg_tx.send(msg).unwrap();
    }

    pub fn init_leader(&self) {
        self.campaign();
    }

    pub async fn is_leader(&self) -> bool {
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let msg = PeerMsg::Control(ControlMsg::IsLeader { reply_tx });
        self.msg_tx.send(msg).unwrap();
        reply_rx.recv().await.unwrap()
    }

    pub async fn get_block_interval(&self) -> u32 {
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let msg = PeerMsg::Control(ControlMsg::GetBlockInterval { reply_tx });
        self.msg_tx.send(msg).unwrap();
        reply_rx.recv().await.unwrap()
    }

    pub async fn check_block(&self, pwp: ProposalWithProof) -> bool {
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let msg = PeerMsg::Control(ControlMsg::CheckBlock { pwp, reply_tx });
        self.msg_tx.send(msg).unwrap();
        reply_rx.recv().await.unwrap()
    }

    pub async fn set_consensus_config(&self, config: ConsensusConfiguration) {
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let msg = PeerMsg::Control(ControlMsg::SetConsensusConfig { config, reply_tx });
        self.msg_tx.send(msg).unwrap();
        reply_rx.recv().await.unwrap()
    }
}

use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::common::SimpleResponse;
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
        request: tonic::Request<ConsensusConfiguration>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let config = request.into_inner();
        self.peer_control.set_consensus_config(config).await;
        let reply = SimpleResponse { is_success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn check_block(
        &self,
        request: tonic::Request<ProposalWithProof>,
    ) -> Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let pwp = request.into_inner();
        let is_success = self.peer_control.check_block(pwp).await;
        let reply = SimpleResponse { is_success };
        Ok(tonic::Response::new(reply))
    }
}
