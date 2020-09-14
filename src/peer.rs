use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Message;
use raft::prelude::ConfChange;
use raft::prelude::ConfState;
use raft::prelude::Config;
use raft::prelude::Entry;
use raft::prelude::EntryType;
use raft::prelude::HardState;
use raft::prelude::RawNode;
use raft::prelude::Ready;
use raft::prelude::Snapshot;
use raft::storage::MemStorage;
use raft::StateRole;

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time;

use log::{info, warn};
use protobuf::Message as _;
use slog::o;
use slog::Drain;

use crate::error::{Error, Result};
use crate::network::NetworkManager;

#[derive(Debug)]
pub enum RaftServerMessage {
    Proposal { proposal: Proposal },
    Raft { message: Message },
}

#[derive(Debug)]
pub enum Proposal {
    Normal { hash: Vec<u8> },
    ConfChange { cc: ConfChange },
    // TransferLeader,
}

#[derive(Clone)]
struct RaftGroup {
    node: Arc<Mutex<Option<RawNode<MemStorage>>>>,
}

impl RaftGroup {
    fn new(raw_node: Option<RawNode<MemStorage>>) -> Self {
        let node = Arc::new(Mutex::new(raw_node));
        Self { node }
    }

    async fn lock(&self) -> RaftGroupGuard {
        RaftGroupGuard {
            raft_group: self.node.clone().lock_owned().await,
        }
    }
}

use tokio::sync::OwnedMutexGuard;

struct RaftGroupGuard {
    raft_group: OwnedMutexGuard<Option<RawNode<MemStorage>>>,
}

impl RaftGroupGuard {
    fn is_initialized(&self) -> bool {
        self.raft_group.is_some()
    }

    fn propose(&mut self, hash: Vec<u8>) -> Result<()> {
        if let Some(ref mut r) = *self.raft_group {
            r.propose(vec![], hash)?;
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn propose_conf_change(&mut self, cc: ConfChange) -> Result<()> {
        if let Some(ref mut r) = *self.raft_group {
            r.propose_conf_change(vec![], cc)?;
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn step(&mut self, msg: Message) -> Result<()> {
        if !self.is_initialized() {
            self.initialize_raft_from_message(&msg)?;
        }
        self.raft_group.as_mut().unwrap().step(msg)?;
        Ok(())
    }

    fn initialize_raft_from_message(&mut self, msg: &Message) -> Result<()> {
        fn is_initial_msg(msg: &Message) -> bool {
            use raft::eraftpb::MessageType;
            let msg_type = msg.get_msg_type();
            msg_type == MessageType::MsgRequestVote
                || msg_type == MessageType::MsgRequestPreVote
                || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)
        }
        if is_initial_msg(msg) {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let drain = slog_async::Async::new(drain)
                .chan_size(4096)
                .overflow_strategy(slog_async::OverflowStrategy::Block)
                .build()
                .fuse();
            let logger = slog::Logger::root(drain, o!());

            let mut cfg = Config {
                election_tick: 30,
                heartbeat_tick: 3,
                check_quorum: true,
                ..Default::default()
            };
            cfg.id = msg.to;
            let logger = logger.new(o!("tag" => format!("peer_{}", msg.to)));
            let storage = MemStorage::new();
            let raw_node = RawNode::new(&cfg, storage, &logger)?;
            self.raft_group.replace(raw_node);
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn has_ready(&self) -> Result<bool> {
        if let Some(ref r) = *self.raft_group {
            Ok(r.has_ready())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn ready(&mut self) -> Result<Ready> {
        if let Some(ref mut r) = *self.raft_group {
            Ok(r.ready())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn log_store(&self) -> Result<MemStorage> {
        if let Some(ref r) = *self.raft_group {
            Ok(r.raft.raft_log.store.clone())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        if let Some(ref mut r) = *self.raft_group {
            r.mut_store().wl().apply_snapshot(snapshot)?;
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn append_entries(&mut self, ents: &[Entry]) -> Result<()> {
        if let Some(ref mut r) = *self.raft_group {
            r.mut_store().wl().append(ents)?;
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn set_hardstate(&mut self, hs: HardState) -> Result<()> {
        if let Some(ref mut r) = *self.raft_group {
            r.mut_store().wl().set_hardstate(hs);
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn apply_conf_change(&mut self, cs: &ConfChange) -> Result<ConfState> {
        if let Some(ref mut r) = *self.raft_group {
            Ok(r.apply_conf_change(cs)?)
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn advance(&mut self, ready: Ready) -> Result<()> {
        if let Some(ref mut r) = *self.raft_group {
            r.advance(ready);
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn tick(&mut self) -> Result<bool> {
        if let Some(ref mut r) = *self.raft_group {
            Ok(r.tick())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    fn is_leader(&self) -> Result<bool> {
        if let Some(ref r) = *self.raft_group {
            Ok(r.raft.state == StateRole::Leader)
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }
}

#[derive(Clone)]
pub struct Peer {
    raft_group: RaftGroup,
    network_manager: NetworkManager,
    config: Arc<RwLock<Option<ConsensusConfiguration>>>,
}

impl Peer {
    fn create_leader(id: u64, controller_port: u16, network_port: u16) -> Self {
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

        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];
        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();

        let mut raw_node = RawNode::new(&cfg, storage, &logger).unwrap();
        raw_node.campaign().unwrap();
        let raft_group = RaftGroup::new(Some(raw_node));
        let network_manager = NetworkManager::with_capacity(controller_port, network_port, 16);
        Self {
            raft_group,
            network_manager,
            config: Arc::new(RwLock::new(None)),
        }
    }

    fn create_follower(controller_port: u16, network_port: u16) -> Self {
        let raft_group = RaftGroup::new(None);
        let network_manager = NetworkManager::with_capacity(controller_port, network_port, 16);
        Self {
            raft_group,
            network_manager,
            config: Arc::new(RwLock::new(None)),
        }
    }

    async fn process_proposal(&mut self, proposal: Proposal) -> Result<()> {
        info!("process proposal");
        match proposal {
            Proposal::Normal { hash } => {
                if self.check_proposal_hash(hash.clone()).await? {
                    self.raft_group.lock().await.propose(hash)?;
                } else {
                    info!("check porposal failed.");
                }
            }
            Proposal::ConfChange { cc } => {
                self.raft_group.lock().await.propose_conf_change(cc)?;
            }
        }
        Ok(())
    }

    async fn check_proposal_hash(&mut self, hash: Vec<u8>) -> Result<bool> {
        self.network_manager.check_proposal(hash.clone()).await
    }

    async fn on_ready(&mut self) -> Result<()> {
        let mut raft_group = self.raft_group.lock().await;
        if !raft_group.has_ready()? {
            return Ok(());
        }
        let store = raft_group.log_store()?;
        let mut ready = raft_group.ready()?;

        if !ready.snapshot().is_empty() {
            let s = ready.snapshot().clone();
            if let Err(e) = raft_group.apply_snapshot(s) {
                info!("apply snapshot failed: {:?}", e);
            }
        }

        if !ready.entries().is_empty() {
            if let Err(e) = raft_group.append_entries(ready.entries()) {
                info!("append entries failed: {:?}", e);
            }
        }

        if let Some(hs) = ready.hs() {
            if let Err(e) = raft_group.set_hardstate(hs.clone()) {
                info!("set hardstate failed: {:?}", e);
            }
        }

        info!("sending msg..");
        let messages = ready.messages.drain(..).collect::<Vec<_>>();
        let mut conn = self.network_manager.clone();
        tokio::spawn(async move {
            for msg in messages {
                if let Err(e) = conn.send_msg(msg).await {
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
                        let mut network_manager = self.network_manager.clone();
                        tokio::spawn(async move {
                            if let Err(e) = network_manager.commit_block(proposal.clone()).await {
                                warn!("commit block failed: {:?}", e);
                            }
                        });
                    }
                    EntryType::EntryConfChange => {
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data)?;
                        let store = store.clone();
                        let cs = raft_group.apply_conf_change(&cc)?;
                        store.wl().set_conf_state(cs);
                    }
                    EntryType::EntryConfChangeV2 => warn!("ConfChangeV2 unimplemented."),
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                let mut s = store.wl();
                s.mut_hard_state().commit = last_committed.index;
                s.mut_hard_state().term = last_committed.term;
            }
        }
        raft_group.advance(ready)
    }

    async fn tick(&mut self) -> Result<bool> {
        self.raft_group.lock().await.tick()
    }

    async fn step(&mut self, msg: Message) -> Result<()> {
        self.raft_group.lock().await.step(msg)
    }

    async fn is_leader(&self) -> Result<bool> {
        self.raft_group.lock().await.is_leader()
    }
}

#[derive(Clone)]
pub struct RaftServer {
    id: u64,
    peer: Peer,
    tx: mpsc::Sender<RaftServerMessage>,
}

impl RaftServer {
    pub fn new(
        id: u64,
        tx: mpsc::Sender<RaftServerMessage>,
        controller_port: u16,
        network_port: u16,
    ) -> Self {
        let peer = if id == 1 {
            Peer::create_leader(id, controller_port, network_port)
        } else {
            Peer::create_follower(controller_port, network_port)
        };

        Self { id, peer, tx }
    }

    pub async fn start(
        self,
        tx: mpsc::Sender<RaftServerMessage>,
        mut rx: mpsc::Receiver<RaftServerMessage>,
    ) -> Result<()> {
        tokio::spawn(self.clone().wait_proposal(tx));
        let mut tick_clock = Instant::now();
        let tick_interval = Duration::from_millis(100);

        let d = Duration::from_millis(10);
        let mut interval = time::interval(d);
        loop {
            match rx.try_recv() {
                Ok(RaftServerMessage::Proposal { proposal }) => {
                    tokio::spawn(self.clone().raft_process_proposal(proposal));
                }
                Ok(RaftServerMessage::Raft { message }) => {
                    tokio::spawn(self.clone().raft_step(message));
                }
                Err(mpsc::error::TryRecvError::Empty) => (),
                Err(mpsc::error::TryRecvError::Closed) => {
                    info!("Recv closed.");
                    return Ok(());
                }
            }
            if tick_clock.elapsed() >= tick_interval {
                tokio::spawn(self.clone().raft_tick());
                tick_clock = Instant::now();
            }
            tokio::spawn(self.clone().raft_on_ready());
            interval.tick().await;
        }
    }

    async fn raft_process_proposal(mut self, proposal: Proposal) {
        if let Err(e) = self.peer.process_proposal(proposal).await {
            warn!("process proposal failed: {:?}", e);
        }
    }

    async fn raft_step(mut self, message: Message) {
        if message.to == self.id {
            use raft::eraftpb::MessageType::*;
            let mut is_ok = true;
            if let MsgAppend = message.msg_type {
                for ent in message.entries.iter() {
                    if let EntryType::EntryNormal = ent.entry_type {
                        if let Ok(false) | Err(_) =
                            self.peer.check_proposal_hash(ent.data.clone()).await
                        {
                            is_ok = false;
                        }
                    }
                }
            }
            if is_ok {
                if let Err(e) = self.peer.step(message).await {
                    warn!("raft group step message failed: {:?}", e);
                }
            } else {
                warn!("check block hash failed");
            }
        } else {
            warn!("#{} server ignore message to #{}", self.id, message.to);
        }
    }

    async fn raft_tick(mut self) {
        if let Err(e) = self.peer.tick().await {
            warn!("raft group tick failed: {:?}", e);
        }
    }

    async fn raft_on_ready(mut self) {
        if let Err(e) = self.peer.on_ready().await {
            warn!("raft group on_ready failed: {:?}", e);
        }
    }

    async fn wait_proposal(mut self, mut tx: mpsc::Sender<RaftServerMessage>) {
        let mut block_clock = Instant::now();
        let mut block_interval = Duration::from_millis(500);
        loop {
            if block_clock.elapsed() >= block_interval {
                if let Some(ref config) = *self.peer.config.read().await {
                    block_interval = Duration::from_secs(config.block_interval as u64);
                    if let Ok(true) = self.peer.is_leader().await {
                        if let Ok(hash) = self.peer.network_manager.get_proposal().await {
                            let proposal = Proposal::Normal { hash };
                            tx.send(RaftServerMessage::Proposal { proposal })
                                .await
                                .unwrap();
                        }
                    }
                }
                block_clock = Instant::now();
            }
            let mut interval = time::interval(Duration::from_millis(500));
            interval.tick().await;
        }
    }

    pub async fn add_follower(n: u64, mut tx: mpsc::Sender<RaftServerMessage>) {
        info!("add follower");
        let d = Duration::from_secs(5);
        let mut delay = time::interval(d);
        for id in 2..=n {
            delay.tick().await;
            let mut cc = ConfChange::default();
            cc.node_id = id;
            cc.set_change_type(ConfChangeType::AddNode);
            let proposal = Proposal::ConfChange { cc };
            tx.send(RaftServerMessage::Proposal { proposal })
                .await
                .unwrap();
        }
    }
}

use cita_cloud_proto::common::SimpleResponse;
use cita_cloud_proto::consensus::{
    consensus_service_server::ConsensusService, ConsensusConfiguration,
};
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerService;
use cita_cloud_proto::network::NetworkMsg;

#[tonic::async_trait]
impl NetworkMsgHandlerService for RaftServer {
    async fn process_network_msg(
        &self,
        request: tonic::Request<NetworkMsg>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        info!("process_network_msg request: {:?}", request);

        let msg = request.into_inner();
        if msg.module != "consensus" {
            Err(tonic::Status::invalid_argument("wrong module"))
        } else {
            let raft_msg: Message = protobuf::parse_from_bytes(msg.msg.as_slice()).unwrap();
            let origin = msg.origin;
            let from = raft_msg.from;
          
            let to = raft_msg.to;
            if to == self.id {
                self.tx
                    .clone()
                    .send(RaftServerMessage::Raft { message: raft_msg })
                    .await
                    .unwrap();
            }
            let network_manager = self.peer.network_manager.clone();
            network_manager.update_table(from, origin).await;
            let reply = SimpleResponse { is_success: true };
            Ok(tonic::Response::new(reply))
        }
    }
}

#[tonic::async_trait]
impl ConsensusService for RaftServer {
    async fn reconfigure(
        &self,
        request: tonic::Request<ConsensusConfiguration>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        info!("reconfigure request: {:?}", request);
        let new_config = request.into_inner();
        self.peer.config.write().await.replace(new_config);
        let reply = SimpleResponse { is_success: true };
        Ok(tonic::Response::new(reply))
    }
}
