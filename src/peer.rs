use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Message;
use raft::prelude::ConfChange;
use raft::prelude::ConfState;
use raft::prelude::Config;
use raft::prelude::EntryType;
use raft::prelude::RawNode;
use raft::prelude::Ready;
use raft::prelude::Snapshot;
use raft::storage::MemStorage;
use raft::StateRole;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time;

use crate::network::NetworkManager;
use crate::error::{Error, Result};

#[allow(unused)]
use log::{info, warn};
use protobuf::Message as _;
use slog::o;
use slog::Drain;


#[derive(Debug)]
pub enum RaftServerMessage {
    Proposal { proposal: Proposal },
    Raft { message: Message },
}

#[derive(Debug)]
pub enum Proposal {
    Normal { hash: Vec<u8> },
    ConfChange { cc: ConfChange },
    TransferLeader,
}

struct RaftGroup {
    node: Option<Arc<RwLock<RawNode<MemStorage>>>>,
}

impl RaftGroup {
    fn new(raw_node: Option<RawNode<MemStorage>>) -> Self {
        let node = raw_node.map(|r| Arc::new(RwLock::new(r)));
        Self { node }
    }

    fn is_initialized(&self) -> bool {
        self.node.is_some()
    }

    async fn is_leader(&self) -> Result<bool> {
        if let Some(ref r) = self.node {
            let r = r.read().await;
            Ok(r.raft.state == StateRole::Leader)
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn propose(&self, hash: Vec<u8>) -> Result<()> {
        if let Some(ref r) = self.node {
            let mut r = r.write().await;
            r.propose(vec![], hash).unwrap();
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn propose_conf_change(&self, cc: ConfChange) -> Result<()> {
        if let Some(ref r) = self.node {
            let mut r = r.write().await;
            r.propose_conf_change(vec![], cc)?;
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn step(&mut self, msg: Message) -> Result<()> {
        if self.node.is_none() {
            self.initialize_raft_from_message(&msg)?;
        }
        let mut r = self.node.as_mut().unwrap().write().await;
        r.step(msg)?;
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
                election_tick: 10,
                heartbeat_tick: 3,
                ..Default::default()
            };
            cfg.id = msg.to;
            let logger = logger.new(o!("tag" => format!("peer_{}", msg.to)));
            let storage = MemStorage::new();
            let r = RawNode::new(&cfg, storage, &logger)?;
            self.node = Some(Arc::new(RwLock::new(r)));
            Ok(())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn has_ready(&self) -> Result<bool> {
        if let Some(ref r) = self.node {
            let r = r.read().await;
            Ok(r.has_ready())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn ready(&self) -> Result<Ready> {
        if let Some(ref r) = self.node {
            let mut r = r.write().await;
            Ok(r.ready())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn log_store(&self) -> Result<MemStorage> {
        if let Some(ref r) = self.node {
            let r = r.read().await;
            Ok(r.raft.raft_log.store.clone())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn apply_conf_change(&self, cs: &ConfChange) -> Result<ConfState> {
        if let Some(ref r) = self.node {
            let mut r = r.write().await;
            Ok(r.apply_conf_change(cs)?)
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn advance(&self, ready: Ready) -> Result<()> {
        if let Some(ref r) = self.node {
            let mut r = r.write().await;
            Ok(r.advance(ready))
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }

    async fn tick(&self) -> Result<bool> {
        if let Some(ref r) = self.node {
            let mut r = r.write().await;
            Ok(r.tick())
        } else {
            Err(Error::RaftGroupUninitialized)
        }
    }
}

pub struct Peer {
    raft_group: RaftGroup,
    network_manager: NetworkManager,
    config: Option<ConsensusConfiguration>,
    committed: HashSet<Vec<u8>>,
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
            election_tick: 10,
            heartbeat_tick: 3,
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
        let network_manager = NetworkManager::new(controller_port, network_port);
        Self {
            raft_group,
            network_manager,
            config: None,
            committed: HashSet::new(),
        }
    }

    fn create_follower(controller_port: u16, network_port: u16) -> Self {
        let raft_group = RaftGroup::new(None);
        let network_manager = NetworkManager::new(controller_port, network_port);
        Self {
            raft_group,
            network_manager,
            config: None,
            committed: HashSet::new(),
        }
    }

    async fn process_proposal(&mut self, proposal: Proposal) -> Result<()>{
        info!("process proposal");
        match proposal {
            Proposal::Normal { hash } => {
                if !self.committed.contains(&hash)
                    && self
                        .network_manager
                        .check_proposal(hash.clone())
                        .await
                        .unwrap()
                {
                    self.raft_group.propose(hash).await?;
                }
            }
            Proposal::ConfChange { cc } => {
                self.raft_group.propose_conf_change(cc).await?;
            }
            Proposal::TransferLeader => info!("transfer leader unimplemented"),
        }
        Ok(())
    }

    async fn on_ready(&mut self) -> Result<()> {
        if !self.raft_group.has_ready().await? {
            return Ok(());
        }
        let store = self.raft_group.log_store().await?;
        let mut ready = self.raft_group.ready().await?;
        if let Err(e) = store.wl().append(ready.entries()) {
            warn!("store append entries failed: `{}`", e);
            return Ok(());
        }
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = store.wl().apply_snapshot(s) {
                info!("apply snapshot failed: {:?}", e);
            }
        }

        for msg in ready.messages.drain(..) {
            info!("broadcast msg..");
            self.network_manager
                .broadcast(msg)
                .await?;
        }
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }
                if let EntryType::EntryConfChange = entry.get_entry_type() {
                    // For conf change messages, make them effective.
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.raft_group.apply_conf_change(&cc).await?;
                    store.wl().set_conf_state(cs);
                } else {
                    info!("commiting proposal..");
                    let proposal = entry.data.clone();
                    if self.raft_group.is_leader().await? {
                        info!("leader commiting proposal..");
                        self.network_manager
                            .commit_block(proposal.clone())
                            .await
                            .expect("commit failed");
                    } else {
                        info!("follower ignore commiting");
                    }
                    self.committed.insert(proposal);
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                let mut s = store.wl();
                s.mut_hard_state().commit = last_committed.index;
                s.mut_hard_state().term = last_committed.term;
            }
        }
        // Call `RawNode::advance` interface to update position flags in the raft.
        self.raft_group.advance(ready).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct RaftServer {
    peer: Arc<RwLock<Peer>>,
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

        let peer = Arc::new(RwLock::new(peer));
        Self { peer, tx }
    }

    pub async fn start(self, mut rx: mpsc::Receiver<RaftServerMessage>) -> Result<()> {
        let mut tick_clock = Instant::now();
        let tick_interval = Duration::from_millis(100);

        let mut block_clock = Instant::now();
        let mut block_interval = Duration::from_millis(100);

        let d = Duration::from_millis(10);
        let mut interval = time::interval(d);
        loop {
            {
                let mut peer = self.peer.write().await;
                match rx.try_recv() {
                    Ok(RaftServerMessage::Proposal { proposal }) => {
                        match peer.process_proposal(proposal).await {
                            // ignore uninitialized here
                            Err(Error::RaftGroupUninitialized) => (),
                            other => other?,
                        }
                    }
                    Ok(RaftServerMessage::Raft { message }) => {
                        peer.raft_group.step(message).await?;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => (),
                    Err(mpsc::error::TryRecvError::Closed) => {
                        info!("Recv closed.");
                        return Ok(());
                    }
                }
                if peer.raft_group.is_initialized() {
                    if block_clock.elapsed() >= block_interval {
                        if let (Some(config), true) =
                            (&peer.config, peer.raft_group.is_leader().await.unwrap())
                        {
                            // block_interval = Duration::from_millis(1000);
                            block_interval = Duration::from_secs((config.block_interval) as u64);
                            let hash = peer.network_manager.get_proposal().await?;
                            if peer.network_manager.check_proposal(hash.clone()).await? {
                                let proposal = Proposal::Normal { hash };
                                peer.process_proposal(proposal).await?;
                            }
                        }
                        block_clock = Instant::now();
                    }
                    if tick_clock.elapsed() >= tick_interval {
                        peer.raft_group.tick().await?;
                        tick_clock = Instant::now();
                    }
                    peer.on_ready().await?;
                }
            }
            interval.tick().await;
        }
    }

    pub async fn add_follower(mut tx: mpsc::Sender<RaftServerMessage>) {
        let d = Duration::from_secs(20);
        let mut interval = time::interval(d);
        interval.tick().await;
        let mut cc = ConfChange::default();
        cc.node_id = 2;
        cc.set_change_type(ConfChangeType::AddNode);
        let proposal = Proposal::ConfChange { cc };
        tx.send(RaftServerMessage::Proposal { proposal })
            .await
            .unwrap();
    }
}

use cita_ng_proto::common::SimpleResponse;
use cita_ng_proto::consensus::{
    consensus_service_server::ConsensusService, ConsensusConfiguration,
};
use cita_ng_proto::network::network_msg_handler_service_server::NetworkMsgHandlerService;
use cita_ng_proto::network::NetworkMsg;

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
            let raft_msg = protobuf::parse_from_bytes(msg.msg.as_slice()).unwrap();
            self.tx
                .clone()
                .send(RaftServerMessage::Raft { message: raft_msg })
                .await
                .unwrap();
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
        {
            let mut r = self.peer.write().await;
            r.config.replace(new_config);
        }
        let reply = SimpleResponse { is_success: true };
        Ok(tonic::Response::new(reply))
    }
}
