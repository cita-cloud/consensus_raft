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

use std::path::Path;

use tokio::sync::mpsc;
use tokio::time;

use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;

use anyhow::anyhow;
use anyhow::Result;

use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Message as RaftMsg;
use raft::eraftpb::MessageType;
use raft::prelude::ConfChange;
use raft::prelude::ConfChangeSingle;
use raft::prelude::ConfChangeV2;
use raft::prelude::Config;
use raft::prelude::Entry;
use raft::prelude::EntryType;
use raft::prelude::Message;
use raft::prelude::RawNode;
use raft::prelude::Snapshot;
use raft::StateRole;

use cita_cloud_proto::common::Proposal;

use prost::Message as _;

use crate::mailbox::Letter;
use crate::mailbox::MailboxControl;
use crate::storage::WalStorage;

/// An unified msg type for both local control messages
/// and raft internal messages. It's used by the mailbox.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
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
            PeerMsg::Normal(raft_msg) => {
                let mut buf = Vec::with_capacity(raft_msg.encoded_len());
                raft_msg.encode(&mut buf).unwrap();
                buf
            }
            PeerMsg::Control(_) => panic!("attempt to send local msg"),
        }
    }

    fn read_from(paper: &[u8]) -> std::result::Result<Self, Self::ReadError> {
        Ok(PeerMsg::Normal(RaftMsg::decode(paper)?))
    }
}

/// Control msg to interact with the peer.
#[derive(Debug, Clone)]
pub enum ControlMsg {
    // Use mpsc channel for `reply_tx` instead of oneshot since we requires the msg to be Clone.
    Propose {
        data: Vec<u8>,
    },
    GetBlockInterval {
        reply_tx: mpsc::UnboundedSender<u32>,
    },
    SetConsensusConfig {
        config: ConsensusConfiguration,
        reply_tx: mpsc::UnboundedSender<bool>,
    },
    IsLeader {
        reply_tx: mpsc::UnboundedSender<bool>,
    },
    Campaign,
    Tick,
}

pub struct Peer {
    raft: RawNode<WalStorage>,
    consensus_config: ConsensusConfiguration,

    msg_tx: mpsc::UnboundedSender<PeerMsg>,
    msg_rx: mpsc::UnboundedReceiver<PeerMsg>,
    mailbox_control: MailboxControl<PeerMsg>,

    started: bool,

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

        let mut storage = WalStorage::new(data_dir).await;
        // This step is according to the example in raft-rs to initialize the leader.
        let mut started = storage.is_initialized();
        if id == init_leader_id && !started {
            let mut s = Snapshot::default();
            s.mut_metadata().index = 5;
            s.mut_metadata().term = 5;
            s.mut_metadata().mut_conf_state().voters = vec![id];

            storage.apply_snapshot(s).await.unwrap();
            started = true;
        }

        // Load applied index from stable storage.
        let applied = storage.get_applied_index();
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
            started,
            logger,
        }
    }

    pub async fn run(&mut self) {
        if self.started {
            self.start_raft();
        }

        while let Some(msg) = self.msg_rx.recv().await {
            if !self.started {
                // Leader should be started by campaign msg.
                // Followers should be started by leader's msg.
                // Don't start them when receives SetConsensusConfig msg.
                if let PeerMsg::Control(ControlMsg::SetConsensusConfig { reply_tx, .. }) = &msg {
                    // Ignore it since we haven't started yet.
                    reply_tx.send(false).unwrap();
                    continue;
                } else {
                    self.start_raft();
                }
            }
            self.handle_msg(msg).await;
        }
    }

    fn start_raft(&mut self) {
        // Try to get a proposal every block interval secs.
        tokio::spawn(Self::wait_proposal(self.service(), self.logger.clone()));
        // Send tick msg to raft periodically.
        tokio::spawn(Self::pacemaker(self.msg_tx.clone()));
        self.started = true;
    }

    fn id(&self) -> u64 {
        self.raft.raft.r.id
    }

    async fn handle_msg(&mut self, msg: PeerMsg) {
        match msg {
            PeerMsg::Control(ControlMsg::Propose { data }) => {
                info!(self.logger, "propose"; "hash" => hex::encode(&data));
                if let Err(e) = self.raft.propose(vec![], data) {
                    warn!(self.logger, "propose failed: `{}`", e);
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
                info!(self.logger, "change consensus config"; "new config" => ?config);
                self.consensus_config = config.clone();

                let current_peer_count = self.raft.store().get_conf_state().voters.len() as u64;
                let new_peer_count = config.validators.len() as u64;

                let cc = {
                    let mut changes = vec![];
                    for id in (current_peer_count + 1)..=new_peer_count {
                        let ccs = ConfChangeSingle {
                            change_type: ConfChangeType::AddNode as i32,
                            node_id: id,
                        };
                        changes.push(ccs);
                    }
                    ConfChangeV2 {
                        changes,
                        ..Default::default()
                    }
                };

                let mut is_success = true;
                if let Err(e) = self.raft.propose_conf_change(vec![], cc) {
                    warn!(self.logger, "add node failed: `{}`", e);
                    is_success = false;
                }

                if let Err(e) = reply_tx.send(is_success) {
                    error!(
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
                if let Some(MessageType::MsgAppend) = MessageType::from_i32(raft_msg.msg_type) {
                    // Spawn tasks to check proposal and wait for their result.
                    let mut check_results = Vec::with_capacity(raft_msg.entries.len());
                    for ent in raft_msg.entries.iter().filter(|ent| !ent.data.is_empty()) {
                        if let Some(EntryType::EntryNormal) = EntryType::from_i32(ent.entry_type) {
                            let mailbox_control = self.mailbox_control.clone();
                            let proposal = match Proposal::decode(ent.data.as_slice()) {
                                Ok(proposal) => proposal,
                                Err(e) => {
                                    error!(
                                        self.logger,
                                        "can't decode proposal data: `{}`, error: `{}`",
                                        hex::encode(&ent.data),
                                        e
                                    );
                                    return;
                                }
                            };

                            let logger = self.logger.clone();
                            let handle = tokio::spawn(async move {
                                match mailbox_control.check_proposal(proposal.clone()).await {
                                    Ok(true) => true,
                                    Ok(false) => {
                                        warn!(
                                            logger,
                                            "check proposal failed, height: `{}` data: `{}`",
                                            proposal.height,
                                            hex::encode(&proposal.data)
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
        let mut propose_time = time::Instant::now();
        loop {
            time::sleep_until(propose_time).await;
            if service.peer_control.is_leader().await {
                debug!(logger, "get proposal..");
                match service.mailbox_control.get_proposal().await {
                    Ok(proposal) => {
                        let data = {
                            let mut buf = Vec::with_capacity(proposal.encoded_len());
                            proposal.encode(&mut buf).unwrap();
                            buf
                        };
                        service.peer_control.propose(data);
                    }
                    Err(e) => warn!(logger, "get proposal failed: `{}`", e),
                }
            }
            let block_interval = service.peer_control.get_block_interval().await;
            propose_time += time::Duration::from_secs(block_interval as u64);
        }
    }

    // Tick raft's logical clock.
    async fn pacemaker(msg_tx: mpsc::UnboundedSender<PeerMsg>) {
        let pace = time::Duration::from_millis(200);
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
            if let Err(e) = store.apply_snapshot(s).await {
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
            store.append_entries(ready.entries()).await;
        }

        // Raft HardState changed, and we need to persist it.
        if let Some(hs) = ready.hs() {
            let store = self.raft.mut_store();
            store.update_hard_state(hs.clone()).await;
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        let mut light_rd = self.raft.advance(ready);

        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            let store = self.raft.mut_store();
            store.update_committed_index(commit).await;
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
                    let proposal = Proposal::decode(entry.data.as_slice()).unwrap();

                    let pwp = ProposalWithProof {
                        proposal: Some(proposal),
                        // Empty proof for non-BFT consensus.
                        proof: vec![],
                    };
                    let retry_secs = 3;
                    let mut retry_interval = time::interval(time::Duration::from_secs(retry_secs));
                    while let Err(e) = self.mailbox_control.commit_block(pwp.clone()).await {
                        retry_interval.tick().await;
                        warn!(
                            self.logger,
                            "commit block failed, retry in {} secs. Reason: `{}`", retry_secs, e
                        );
                    }
                }
                EntryType::EntryConfChange => {
                    let cc = ConfChange::decode(entry.data.as_slice())?;
                    let cs = self.raft.apply_conf_change(&cc)?;

                    self.raft.mut_store().update_conf_state(cs).await;

                    match ConfChangeType::from_i32(cc.change_type) {
                        Some(ConfChangeType::AddNode) => {
                            info!(self.logger, "add node #{}", cc.node_id);
                        }
                        Some(ConfChangeType::RemoveNode) => {
                            info!(self.logger, "remove node #{}", cc.node_id);
                        }
                        Some(ConfChangeType::AddLearnerNode) => {
                            warn!(self.logger, "AddLearnerNode unimplemented.")
                        }
                        unexpected => {
                            error!(self.logger, "unexpected conf change type: {:?}", unexpected);
                        }
                    }
                }
                EntryType::EntryConfChangeV2 => {
                    let cc = ConfChangeV2::decode(entry.data.as_slice())?;
                    let cs = self.raft.apply_conf_change(&cc)?;
                    info!(self.logger, "conf change: {:?}", cc);
                    info!(self.logger, "now config state is: {:?}", cs);
                    self.raft.mut_store().update_conf_state(cs).await;
                }
            }

            // Persist applied index.
            self.raft
                .mut_store()
                .advance_applied_index(entry.index)
                .await;
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
    pub fn propose(&self, data: Vec<u8>) {
        let msg = PeerMsg::Control(ControlMsg::Propose { data });
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

    pub async fn set_consensus_config(&self, config: ConsensusConfiguration) -> bool {
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
        let is_success = self.peer_control.set_consensus_config(config).await;
        let reply = SimpleResponse { is_success };
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
                    PeerMsg::Control(ControlMsg::Propose { data }) => {
                        assert_eq!(&data[..], b"Akari!");
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
                            height,
                            block_interval,
                            validators,
                        } = config;
                        assert_eq!(height, 42);
                        assert_eq!(block_interval, 6);
                        assert_eq!(validators, vec![vec![1, 2, 3]]);
                        reply_tx.send(true).unwrap();
                    }
                    msg => panic!("unexpected msg: `{:?}`", msg),
                }
            }
        });

        control.propose(b"Akari!"[..].into());
        control.campaign();
        assert!(control.is_leader().await);
        assert_eq!(control.get_block_interval().await, 6);
        let config = ConsensusConfiguration {
            height: 42,
            block_interval: 6,
            validators: vec![vec![1, 2, 3]],
        };
        control.set_consensus_config(config).await;
        drop(control);
        handle.await.unwrap();
    }
}
