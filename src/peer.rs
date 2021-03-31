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

use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Message as RaftMsg;
use raft::eraftpb::MessageType;
use raft::prelude::ConfChange;
use raft::prelude::Config;
use raft::prelude::Entry;
use raft::prelude::EntryType;
use raft::prelude::Message;
use raft::prelude::RawNode;
use raft::prelude::Snapshot;
use raft::StateRole;

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time;

use slog::o;
use slog::Logger;

use slog::info;
use slog::trace;
use slog::warn;

use anyhow::anyhow;
use anyhow::Result;

use protobuf::Message as _;

use crate::mailbox::Letter;
use crate::mailbox::MailboxControl;
use crate::storage::RaftStorage;

/// An unified msg type for both local control messages
/// and raft internal messages. It's used by the mailbox.
#[derive(Debug, Clone)]
pub enum PeerMsg {
    // Local message to control this peer.
    Control(ControlMsg),
    // Raft message to and from peers.
    Normal(RaftMsg),
}

impl Letter for PeerMsg {
    type Address = u64;
    type ReadError = anyhow::Error;

    fn to(&self) -> Option<Self::Address> {
        match self {
            PeerMsg::Normal(raft_msg) => Some(raft_msg.to),
            PeerMsg::Control(_) => panic!("attempt to send local msg"),
        }
    }

    fn from(&self) -> Self::Address {
        match self {
            PeerMsg::Normal(raft_msg) => raft_msg.from,
            PeerMsg::Control(_) => panic!("attempt to send local msg"),
        }
    }

    fn write_down(&self) -> Vec<u8> {
        match self {
            PeerMsg::Normal(raft_msg) => raft_msg.write_to_bytes().unwrap(),
            PeerMsg::Control(_) => panic!("attempt to send local msg"),
        }
    }

    fn read_from(paper: &[u8]) -> std::result::Result<Self, Self::ReadError> {
        Ok(PeerMsg::Normal(RaftMsg::parse_from_bytes(paper)?))
    }
}

/// Control msg to interact with the peer.
#[derive(Debug, Clone)]
pub enum ControlMsg {
    // Use mpsc channel for `reply_tx` instead of oneshot since we requires the msg to be Clone.
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
    /// Arguments:
    /// - id: current peer's id, can't be 0
    /// - consensus_config: block interval and validators
    /// - msg_tx: a sender that send msg to msg_rx
    /// - msg_rx: a recevier that recv msg from msg_tx
    /// - mailbox_control: handle the communication with controller, network, and other peers
    /// - init_leader_id: the initial leader's id
    /// - data_dir: a path to where the data is stored
    /// - logger: slog's logger
    // TODO: reduce arguments.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        id: u64,
        consensus_config: ConsensusConfiguration,
        msg_tx: mpsc::UnboundedSender<PeerMsg>,
        msg_rx: mpsc::UnboundedReceiver<PeerMsg>,
        mailbox_control: MailboxControl<PeerMsg>,
        init_leader_id: u64,
        data_dir: impl AsRef<Path>,
        logger: Logger,
    ) -> Self {
        let logger = logger.new(o!("tag" => format!("peer_{}", id)));

        let mut storage = RaftStorage::new(data_dir).await;
        // This step is according to the example in raft-rs to initialize the leader.
        if id == init_leader_id && !storage.core.is_initialized() {
            let mut s = Snapshot::default();
            s.mut_metadata().index = 1;
            s.mut_metadata().term = 1;
            s.mut_metadata().mut_conf_state().voters = vec![id];

            storage.core.apply_snapshot(s).await.unwrap();
        }

        // Load applied index from stable storage.
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
                if let PeerMsg::Control(ControlMsg::SetConsensusConfig { reply_tx, .. }) = &msg {
                    // Ignore it since we haven't started yet.
                    reply_tx.send(()).unwrap();
                    continue;
                } else {
                    // Try to get a proposal every block interval secs.
                    tokio::spawn(Self::wait_proposal(self.service(), self.logger.clone()));
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
                info!(self.logger, "propose"; "hash" => hex::encode(&hash));
                if let Err(e) = self.raft.propose(vec![], hash) {
                    warn!(self.logger, "propose failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::AddNode { node_id }) => {
                let cc = ConfChange {
                    change_type: ConfChangeType::AddNode,
                    node_id,
                    ..Default::default()
                };
                if let Err(e) = self.raft.propose_conf_change(vec![], cc) {
                    warn!(self.logger, "add node failed: `{}`", e);
                }
            }
            PeerMsg::Control(ControlMsg::RemoveNode { node_id }) => {
                let cc = ConfChange {
                    change_type: ConfChangeType::RemoveNode,
                    node_id,
                    ..Default::default()
                };
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
            // Changing config is to change:
            //   1. block interval
            //   2. membership of peers (For now, only adding node is supported)
            PeerMsg::Control(ControlMsg::SetConsensusConfig { config, reply_tx }) => {
                trace!(self.logger, "change consensus config"; "new config" => ?config);
                self.consensus_config = config.clone();
                let voters = &self.raft.store().core.conf_state().voters;

                let current_peer_count = voters.len() as u64;
                let new_peer_count = config.validators.len() as u64;

                let peer_control = self.control();
                if current_peer_count < new_peer_count {
                    info!(
                        self.logger, "sync peer";
                        "current_peer_cnt" => current_peer_count,
                        "new_peer_cnt" => new_peer_count,
                    );
                    // TODO: do this in a better way.
                    tokio::spawn(async move {
                        let mut ticker = time::interval(Duration::from_secs(2));
                        let nodes: Vec<u64> = (1..=new_peer_count).collect();
                        loop {
                            ticker.tick().await;
                            let current_nodes: HashSet<u64> =
                                peer_control.get_node_list().await.into_iter().collect();

                            if let Some(&node_id) =
                                nodes.iter().find(|&id| !current_nodes.contains(id))
                            {
                                peer_control.add_node(node_id);
                            } else {
                                break;
                            }
                        }
                    });
                }
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
            PeerMsg::Normal(raft_msg) => {
                // If the msg is to append entries, check it first.
                let mut is_ok = true;
                if let MessageType::MsgAppend = raft_msg.msg_type {
                    // Spawn tasks to check proposal and wait for their result.
                    let mut check_results = Vec::with_capacity(raft_msg.entries.len());
                    for ent in raft_msg.entries.iter().filter(|ent| !ent.data.is_empty()) {
                        if let EntryType::EntryNormal = ent.entry_type {
                            let mailbox_control = self.mailbox_control.clone();
                            let proposal = ent.data.clone();
                            let logger = self.logger.clone();
                            let handle = tokio::spawn(async move {
                                match mailbox_control.check_proposal(proposal.clone()).await {
                                    Ok(true) => true,
                                    Ok(false) => {
                                        warn!(
                                            logger,
                                            "check proposal `{}` failed",
                                            hex::encode(&proposal)
                                        );
                                        false
                                    }
                                    Err(e) => {
                                        warn!(logger, "can't check proposal: {}", e);
                                        false
                                    }
                                }
                            });
                            check_results.push(handle);
                        }
                    }

                    for result in check_results {
                        if !result.await.unwrap() {
                            is_ok = false;
                            break;
                        }
                    }
                }

                // Step this msg if:
                //   1. It's a append msg and pass controller's check.
                //   2. It's other msg.
                if is_ok {
                    if let Err(e) = self.raft.step(raft_msg) {
                        warn!(self.logger, "raft step failed: `{}`", e);
                    }
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

    // The current leader will get proposal
    // from controller every block interval secs.
    async fn wait_proposal(service: RaftService<PeerMsg>, logger: Logger) {
        let d = Duration::from_secs(1);
        let mut ticker = time::interval(d);
        let mut last_propose_time = Instant::now();
        loop {
            ticker.tick().await;
            let block_interval = service.peer_control.get_block_interval().await;
            if last_propose_time.elapsed().as_secs() >= block_interval as u64
                && service.peer_control.is_leader().await
            {
                trace!(logger, "get propose");
                last_propose_time = Instant::now();
                match service.mailbox_control.get_proposal().await {
                    Ok(hash) => service.peer_control.propose(hash),
                    Err(e) => warn!(logger, "get proposal failed: `{}`", e),
                }
            }
        }
    }

    // Tick raft's logical clock.
    async fn pacemaker(msg_tx: mpsc::UnboundedSender<PeerMsg>) {
        let pace = Duration::from_millis(200);
        let mut ticker = time::interval(pace);
        loop {
            ticker.tick().await;
            msg_tx.send(PeerMsg::Control(ControlMsg::Tick)).unwrap();
        }
    }

    // Handle the ready state produced by raft.
    async fn handle_ready(&mut self) -> Result<()> {
        if !self.raft.has_ready() {
            return Ok(());
        }

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raft.ready();

        // Send out the message come from the node.
        Self::send_messages(
            ready.take_messages(),
            self.mailbox_control.clone(),
            self.logger.clone(),
        );

        // Apply the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let store = self.raft.mut_store();
            let s = ready.snapshot().clone();
            if let Err(e) = store.core.apply_snapshot(s).await {
                return Err(anyhow!("apply snapshot failed: {}", e));
            }
        }

        // Apply committed_entries, this includes:
        //   1. Commit block to controller
        //   2. Apply raft config change
        self.handle_committed_entries(ready.take_committed_entries())
            .await?;

        // Persistent raft logs.
        if !ready.entries().is_empty() {
            let store = self.raft.mut_store();
            if let Err(e) = store.core.append(ready.entries()).await {
                return Err(anyhow!("append entries failed: {}", e));
            }
        }

        // Raft HardState changed, and we need to persist it.
        if let Some(hs) = ready.hs() {
            let store = self.raft.mut_store();
            store.core.set_hard_state(hs.clone()).await;
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        let mut light_rd = self.raft.advance(ready);

        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            let store = self.raft.mut_store();
            store.core.mut_hard_state().set_commit(commit);
            store.core.sync_hard_state().await;
        }

        // Send out the messages.
        Self::send_messages(
            light_rd.take_messages(),
            self.mailbox_control.clone(),
            self.logger.clone(),
        );

        // Apply all committed entries.
        self.handle_committed_entries(light_rd.take_committed_entries())
            .await?;

        // Advance the apply index.
        self.raft.advance_apply();

        Ok(())
    }

    fn send_messages(
        msgs: Vec<Vec<Message>>,
        mailbox_control: MailboxControl<PeerMsg>,
        logger: Logger,
    ) {
        tokio::spawn(async move {
            for msg in msgs.into_iter().flatten() {
                let pm = PeerMsg::Normal(msg);
                if let Err(e) = mailbox_control.send_message(pm).await {
                    warn!(logger, "send msg failed: {}", e);
                }
            }
        });
    }

    async fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) -> Result<()> {
        // Fitler out empty entries produced by new elected leaders.
        for entry in committed_entries
            .into_iter()
            .filter(|ent| !ent.data.is_empty())
        {
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    info!(self.logger, "commiting proposal"; "entry" => hex::encode(entry.data.clone()));
                    let proposal = entry.data.clone();

                    let pwp = ProposalWithProof {
                        proposal,
                        // Empty proof for non-BFT consensus.
                        proof: vec![],
                    };
                    let retry_secs = 3;
                    let mut retry_interval = time::interval(Duration::from_secs(retry_secs));
                    while let Err(e) = self.mailbox_control.commit_block(pwp.clone()).await {
                        retry_interval.tick().await;
                        warn!(
                            self.logger,
                            "commit block failed, retry in {} secs. Reason: `{}`", retry_secs, e
                        );
                    }
                }
                EntryType::EntryConfChange => {
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data)?;
                    let cs = self.raft.apply_conf_change(&cc)?;

                    let store = self.raft.mut_store();
                    store.core.set_conf_state(cs).await;

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

            // Persist applied index.
            let store = self.raft.mut_store();
            store.core.set_applied_index(entry.index).await;
        }
        Ok(())
    }
}

// Helper to interact with peers.
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
                tonic::Status::invalid_argument(format!("msg fail to decode: `{}`", e))
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
        _request: tonic::Request<ProposalWithProof>,
    ) -> Result<tonic::Response<SimpleResponse>, tonic::Status> {
        // Reply true since we assume no byzantine faults.
        let reply = SimpleResponse { is_success: true };
        Ok(tonic::Response::new(reply))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn test_peer_control() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let control = PeerControl { id: 1, msg_tx: tx };

        let handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    PeerMsg::Control(ControlMsg::AddNode { node_id: 666 })
                    | PeerMsg::Control(ControlMsg::RemoveNode { node_id: 666 }) => continue,
                    PeerMsg::Control(ControlMsg::GetNodeList { reply_tx }) => {
                        reply_tx.send(vec![20, 20, 9, 29]).unwrap();
                    }
                    PeerMsg::Control(ControlMsg::ApplySnapshot { snapshot }) => {
                        assert_eq!(snapshot, Snapshot::default());
                    }
                    PeerMsg::Control(ControlMsg::Propose { hash }) => {
                        assert_eq!(&hash[..], b"Akari!");
                    }
                    PeerMsg::Control(ControlMsg::Campaign) => continue,
                    PeerMsg::Control(ControlMsg::IsLeader { reply_tx }) => {
                        reply_tx.send(true).unwrap();
                    }
                    PeerMsg::Control(ControlMsg::GetBlockInterval { reply_tx }) => {
                        reply_tx.send(6).unwrap();
                    }
                    PeerMsg::Control(ControlMsg::SetConsensusConfig { config, reply_tx }) => {
                        let ConsensusConfiguration {
                            block_interval,
                            validators,
                        } = config;
                        assert_eq!(block_interval, 6);
                        assert_eq!(validators, vec![vec![1, 2, 3]]);
                        reply_tx.send(()).unwrap();
                    }
                    msg => panic!("unexpected msg: `{:?}`", msg),
                }
            }
        });

        control.add_node(666);
        control.remove_node(666);
        assert_eq!(control.get_node_list().await, vec![20, 20, 9, 29]);
        control.apply_snapshot(Snapshot::default());
        control.propose(b"Akari!"[..].into());
        control.campaign();
        assert!(control.is_leader().await);
        assert_eq!(control.get_block_interval().await, 6);
        let config = ConsensusConfiguration {
            block_interval: 6,
            validators: vec![vec![1, 2, 3]],
        };
        control.set_consensus_config(config).await;
        drop(control);
        handle.await.unwrap();
    }
}
