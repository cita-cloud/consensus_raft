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

use crate::client::{Controller, Network};
use crate::config::ConsensusServiceConfig;
use crate::storage::RaftStorage;
use crate::utils::{addr_to_peer_id, short_hex};
use cita_cloud_proto::common::ConsensusConfiguration;
use cita_cloud_proto::common::Proposal;
use cita_cloud_proto::common::ProposalWithProof;
use prost::Message as _;
use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Message as RaftMsg;
use raft::eraftpb::Snapshot;
use raft::prelude::ConfChangeSingle;
use raft::prelude::ConfChangeV2;
use raft::prelude::Config as RaftConfig;
use raft::prelude::Entry;
use raft::prelude::EntryType;
use raft::prelude::Message;
use raft::ProgressState;
use raft::RawNode;
use slog::o;
use slog::Logger;
use slog::{debug, error, info, warn};
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio::time;

pub struct Peer {
    // raft core
    core: RawNode<RaftStorage>,

    // peers' raft msg receiver.
    peer_rx: flume::Receiver<RaftMsg>,

    // grpc client to talk with other micro-services.
    controller: Controller,
    network: Network,

    // pending_conf_change
    pending_conf_change: Vec<ConfChangeV2>,
    pending_conf_change_proposed: bool,

    // pending proposal
    pending_proposal: Option<Proposal>,
    pending_proposal_proposed: bool,

    send_time_out_to_transferee: bool,

    // slog logger
    logger: Logger,
}

impl Peer {
    // return wants_campaign and is_validator
    fn update_config(
        trigger_config: &ConsensusConfiguration,
        node_addr: &[u8],
        storage: &mut RaftStorage,
    ) -> (bool, bool) {
        if let Some(index) = trigger_config
            .validators
            .iter()
            .position(|addr| addr == node_addr)
        {
            let mut wants_campaign = false;
            if !storage.is_initialized() {
                let snapshot = {
                    let voters = trigger_config
                        .validators
                        .iter()
                        .map(|addr| addr_to_peer_id(addr))
                        .collect();

                    // We start with index 5 and term 5
                    let mut s = Snapshot::default();
                    s.mut_metadata().index = 5;
                    s.mut_metadata().term = 5;
                    s.mut_metadata().mut_conf_state().voters = voters;
                    s
                };
                storage.apply_snapshot(snapshot).unwrap();
                wants_campaign = index == 0;
            }
            (wants_campaign, true)
        } else {
            (false, false)
        }
    }

    pub async fn setup(
        config: ConsensusServiceConfig,
        logger: Logger,
        trigger_config: ConsensusConfiguration,
        network: Network,
        controller: Controller,
        peer_rx: flume::Receiver<RaftMsg>,
    ) -> Self {
        let mut trigger_config = trigger_config;

        let node_addr = {
            let s = &config.node_addr;
            hex::decode(s.strip_prefix("0x").unwrap_or(s)).expect("decode node_addr failed")
        };

        let local_id = addr_to_peer_id(&node_addr);

        // Recover data from log
        let mut storage = {
            let logger = logger.new(o!("tag" => "storage"));
            RaftStorage::new(&config.raft_data_path, logger.clone()).await
        };

        // if recorded_height == trigger_height + 1, try to recommit entry
        let recorded_height = storage.get_block_height();
        let trigger_height = trigger_config.height;
        if recorded_height == trigger_height + 1 {
            info!(
                logger,
                "raft height: {}, controller height: {}, recommit entry",
                recorded_height,
                trigger_height
            );
            if let Some(entry) = storage.read_persist_entry().await {
                if !entry.data.is_empty() {
                    let proposal = Proposal::decode(entry.data.as_slice()).unwrap();
                    if proposal.height == recorded_height {
                        match controller.check_proposal(proposal.clone()).await {
                            Ok(true) => {
                                let pwp = ProposalWithProof {
                                    proposal: Some(proposal),
                                    proof: vec![],
                                };
                                match controller.commit_block(pwp).await {
                                    Ok(config) => {
                                        trigger_config = config;
                                        info!(logger, "recommit entry succeed");
                                    }
                                    Err(e) => {
                                        warn!(logger, "recommit entry failed: {}. retry", e);
                                    }
                                }
                            }
                            Ok(false) => {
                                warn!(
                                    logger,
                                    "recommit entry failed: check proposal failed, try to recv trigger_config"
                                );
                            }
                            Err(e) => {
                                warn!(logger, "recommit entry failed: {}. retry", e);
                            }
                        }
                    }
                } else {
                    warn!(logger, "recommit entry failed: empty entry");
                }
            } else {
                warn!(logger, "recommit entry failed: entry not found");
            }
        }

        let (wants_campaign, is_validator) =
            Self::update_config(&trigger_config, &node_addr, &mut storage);

        if !is_validator {
            info!(
                logger,
                "incoming config doesn't contain this node, will exit for next one"
            );
        }

        let recorded_height = storage.get_block_height();
        let trigger_height = trigger_config.height;
        #[allow(clippy::comparison_chain)]
        if trigger_height > recorded_height || recorded_height == 0 {
            storage.update_consensus_config(trigger_config);
        } else if trigger_config.height < recorded_height {
            warn!(
                logger, "block height in initial reconfigure is lower than recorded; skip it";
                "reconfigure" => trigger_config.height,
                "recorded" => recorded_height
            );
        }

        info!(logger, "reconfigure ack");

        let core = {
            // Load applied index from stable storage.
            let applied = storage.get_applied_index();
            // TODO: customize config
            let cfg = RaftConfig {
                id: local_id,
                election_tick: config.election_tick as usize,
                heartbeat_tick: config.heartbeat_tick as usize,
                check_quorum: config.check_quorum,
                applied,
                ..Default::default()
            };
            RawNode::new(&cfg, storage, &logger).expect("cannot create raft raw node")
        };

        let mut this = Self {
            core,
            peer_rx,

            controller,
            network,

            pending_proposal: None,
            pending_proposal_proposed: false,

            pending_conf_change: vec![],
            pending_conf_change_proposed: false,

            send_time_out_to_transferee: false,

            logger: logger.clone(),
        };

        this.maybe_pending_conf_change();

        if wants_campaign {
            info!(logger, "start campaign ...");
            this.core.campaign().unwrap();
            info!(logger, "campaign success");
        }

        this
    }

    fn is_leader(&self) -> bool {
        self.core.raft.state == raft::StateRole::Leader
    }

    fn block_height(&self) -> u64 {
        self.core.store().get_block_height()
    }

    fn block_interval(&self) -> u32 {
        self.core.store().get_block_interval()
    }

    async fn send_msgs(&self, msgs: Vec<Message>) {
        for msg in msgs {
            // TODO: should we send it in parallel?
            self.network.send_msg(msg).await;
        }
    }

    pub async fn run(&mut self, stop_rx: flume::Receiver<()>) {
        let mut fetching_proposal: Option<JoinHandle<Result<Proposal, tonic::Status>>> = None;

        let tick_interval = Duration::from_millis(200);
        let tick_timeout = time::sleep(tick_interval);
        tokio::pin!(tick_timeout);

        // fetch it now
        let fetching_timeout = time::sleep(Duration::from_secs(0));
        tokio::pin!(fetching_timeout);

        // used for transferring leader when we can't get a valid proposal from controller
        let mut last_time_start_fetching: Option<Instant> = None;

        loop {
            tokio::select! {
                // timing
                _ = &mut tick_timeout => {
                    self.core.tick();
                    tick_timeout
                        .as_mut()
                        .reset(time::Instant::now() + tick_interval);
                }
                _ = &mut fetching_timeout, if self.is_leader() && fetching_proposal.is_none() && self.pending_proposal.is_none() => {
                    // fetching new proposal
                    info!(self.logger, "fetching proposal..");
                    let controller = self.controller.clone();
                    fetching_proposal.replace(tokio::spawn(async move {
                        controller.get_proposal().await
                    }));

                    if last_time_start_fetching.is_none() {
                        last_time_start_fetching.replace(Instant::now());
                    }
                }

                // future for fetching proposal from controller
                // the async {..} wrapper is for lazy evaluation
                Ok(fetch_result) = async { fetching_proposal.as_mut().unwrap().await }, if fetching_proposal.is_some() => {
                    fetching_proposal.take();
                    fetching_timeout.as_mut().reset(time::Instant::now() + Duration::from_secs(self.block_interval().into()));

                    match fetch_result {
                        Ok(proposal) => {
                            info!(self.logger, "new proposal"; "height" => proposal.height, "data" => short_hex(&proposal.data));
                            self.pending_proposal.replace(proposal);
                            self.pending_proposal_proposed = false;
                        }
                        Err(e) => {
                            warn!(self.logger, "fetching proposal failed: `{}`", e);
                        }
                    }
                }

                // raft msg from remote peers
                Ok(raft_msg) = self.peer_rx.recv_async() => {
                    if let Err(e) = self.core.step(raft_msg) {
                        error!(self.logger, "step raft msg failed: `{}`", e);
                    }
                }

                // stop raft
                _ = stop_rx.recv_async() => {
                    break;
                },
            }

            //if incoming change needs to transfer leader, then transfer leader first
            let need_to_transfer_leader = self.is_leader()
                && !self.pending_conf_change.is_empty()
                && self
                    .pending_conf_change
                    .last()
                    .unwrap()
                    .changes
                    .iter()
                    .any(|c| {
                        c.change_type == ConfChangeType::RemoveNode as i32
                            && c.node_id == self.core.raft.id
                    });

            // try to transfer leader
            // if send_time_out_to_transferee is true means the leader has sent MsgTimeoutNow to a transferee
            if need_to_transfer_leader && !self.send_time_out_to_transferee {
                let validators: HashSet<u64> = self
                    .core
                    .store()
                    .get_validators()
                    .iter()
                    .map(|v| addr_to_peer_id(v))
                    .collect();
                use rand::prelude::IteratorRandom;
                use rand::thread_rng;
                // random pick a up-to-date transferee
                let transferee = self
                    .core
                    .raft
                    .prs()
                    .iter()
                    .filter_map(|(&id, pr)| {
                        if pr.recent_active
                            && pr.state == ProgressState::Replicate
                            && pr.matched == self.core.raft.raft_log.last_index()
                            && validators.contains(&id)
                        {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .choose(&mut thread_rng());
                if let Some(transferee) = transferee {
                    self.core.transfer_leader(transferee);
                    self.send_time_out_to_transferee = true;
                } else {
                    warn!(self.logger, "finding a qualified transferee");
                }
            }

            if self.is_leader() {
                // propose pending conf change
                if !self.pending_conf_change_proposed
                    && !self.pending_conf_change.is_empty()
                    && !need_to_transfer_leader
                    && !self.core.raft.has_pending_conf()
                {
                    match self
                        .core
                        .propose_conf_change(vec![], self.pending_conf_change.pop().unwrap())
                    {
                        Ok(()) => {
                            info!(self.logger, "pending conf change proposed");
                            self.pending_conf_change_proposed = true;
                        }
                        Err(e) => warn!(self.logger, "propose conf change failed: `{}`", e),
                    }
                }

                // propose pending proposal
                if !self.pending_proposal_proposed
                    && self.pending_proposal.is_some()
                    && self.pending_conf_change.is_empty()
                    && !self.pending_conf_change_proposed
                    && !self.core.raft.has_pending_conf()
                {
                    let proposal = self.pending_proposal.as_ref().unwrap();
                    let expected_height = self.block_height() + 1;
                    if proposal.height == expected_height {
                        // received a valid proposal.
                        last_time_start_fetching.take();

                        let proposal_bytes = proposal.encode_to_vec();

                        if let Err(e) = self.core.propose(vec![], proposal_bytes) {
                            warn!(self.logger, "can't propose proposal: `{}`", e);
                        } else {
                            info!(self.logger, "pending proposal proposed"; "height" => proposal.height, "data" => short_hex(&proposal.data));
                            self.pending_proposal_proposed = true;
                        }
                    } else {
                        warn!(
                            self.logger,
                            "receive a proposal with invalid height, drop it";
                            "proposal height" => proposal.height,
                            "expect height" => expected_height,
                        );
                        self.pending_proposal.take();
                    }
                }

                if let Some(t) = last_time_start_fetching.as_ref() {
                    // transfer leader if no receiving valid proposal from controller
                    if t.elapsed().as_secs() > self.core.store().get_block_interval() as u64 * 4 {
                        // transfer leader only if not in conf change
                        if self
                            .core
                            .store()
                            .get_conf_state()
                            .voters_outgoing
                            .is_empty()
                        {
                            use rand::prelude::IteratorRandom;
                            use rand::thread_rng;
                            // random pick a up-to-date transferee
                            let transferee = self
                                .core
                                .raft
                                .prs()
                                .iter()
                                .filter_map(|(&id, pr)| {
                                    if pr.recent_active
                                        && pr.state == ProgressState::Replicate
                                        && pr.matched == self.core.raft.raft_log.last_index()
                                    {
                                        Some(id)
                                    } else {
                                        None
                                    }
                                })
                                .choose(&mut thread_rng());
                            if let Some(transferee) = transferee {
                                last_time_start_fetching.take();
                                self.core.transfer_leader(transferee);
                            }
                        }
                    }
                }
            } else {
                if let Some(h) = fetching_proposal.take() {
                    h.abort();
                }
                last_time_start_fetching.take();
                self.pending_proposal.take();
                self.pending_proposal_proposed = false;
                // If we step down and become leader again, we need to re-propose pending conf change.
                self.pending_conf_change_proposed = false;
                self.send_time_out_to_transferee = false;
            }

            self.handle_ready().await;
        }
    }

    async fn handle_ready(&mut self) {
        if !self.core.has_ready() {
            return;
        }

        let mut ready = self.core.ready();
        self.send_msgs(ready.take_messages()).await;

        // Apply the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = self.core.mut_store().apply_snapshot(s) {
                error!(self.logger, "cannot apply snapshot: `{}`", e);
            }
            self.core.mut_store().persist_snapshot().await;
            self.maybe_pending_conf_change()
        }

        self.handle_committed_entries(ready.take_committed_entries())
            .await;

        // Persistent raft entry
        let entries = ready.entries();
        if !entries.is_empty() {
            self.core.mut_store().append_entries(entries);
            self.core.mut_store().persist_entry(entries).await;
        }

        // Raft HardState changed, and we need to persist it.
        if let Some(hs) = ready.hs() {
            self.core.mut_store().update_hard_state(hs.clone());
        }

        if !ready.persisted_messages().is_empty() {
            self.send_msgs(ready.take_persisted_messages()).await;
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        let mut light_rd = self.core.advance(ready);

        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            self.core.mut_store().update_committed_index(commit);
        }

        // Send out the messages.
        self.send_msgs(light_rd.take_messages()).await;

        // Apply all committed entries.
        self.handle_committed_entries(light_rd.take_committed_entries())
            .await;

        // Advance the apply index.
        self.core.advance_apply();
    }

    async fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) {
        for entry in committed_entries.into_iter() {
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    if !entry.data.is_empty() {
                        let proposal = Proposal::decode(entry.data.as_slice()).unwrap();
                        let proposal_height = proposal.height;
                        let proposal_data_hex = &short_hex(&proposal.data);
                        let current_height = self.block_height();
                        if proposal_height <= current_height {
                            info!(self.logger, "proposal height less than or equal current block height, don't check proposal"; "proposal_height" => proposal_height, "current_height" => current_height);
                        } else {
                            info!(self.logger, "checking proposal.."; "height" => proposal_height, "data" => proposal_data_hex);

                            match self.controller.check_proposal(proposal.clone()).await {
                                Ok(true) => {
                                    let pwp = ProposalWithProof {
                                        proposal: Some(proposal),
                                        proof: vec![],
                                    };
                                    info!(self.logger, "committing proposal..");
                                    match self.controller.commit_block(pwp).await {
                                        Ok(config) => {
                                            info!(self.logger, "block committed"; "height" => proposal_height, "data" => proposal_data_hex);
                                            self.core.mut_store().update_consensus_config(config);
                                            self.maybe_pending_conf_change();
                                        }
                                        Err(e) => {
                                            warn!(self.logger, "commit block failed: {}", e);
                                        }
                                    }
                                }
                                Ok(false) => warn!(
                                    self.logger,
                                    "check proposal failed, controller replies a false"
                                ),
                                Err(e) => warn!(self.logger, "check proposal failed: {}", e),
                            }
                        }

                        if let Some(pending) = self.pending_proposal.as_ref() {
                            if pending.height <= proposal_height {
                                debug!(self.logger, "pending proposal removed");
                                self.pending_proposal.take();
                            } else {
                                debug!(
                                    self.logger, "pending proposal is higher than committed";
                                    "pending_height" => pending.height, "committed_height" => proposal_height
                                );
                            }
                        }

                        if proposal_height > self.block_height() {
                            self.core.mut_store().update_block_height(proposal_height);
                        }
                    }

                    self.core.mut_store().advance_applied_index(entry.index);
                    self.core.mut_store().persist_snapshot().await;
                }
                // All conf changes are v2.
                EntryType::EntryConfChange => panic!("unexpected EntryConfChange(V1)"),
                EntryType::EntryConfChangeV2 => {
                    let cc = ConfChangeV2::decode(entry.data.as_slice())
                        .expect("cannot decode ConfChangeV2");
                    let cs = self
                        .core
                        .apply_conf_change(&cc)
                        .expect("apply conf change failed");
                    info!(
                        self.logger,
                        "apply config change `{:?}`; now config state is: {:?}", cc, cs
                    );
                    self.core.mut_store().update_conf_state(cs);
                    self.pending_conf_change_proposed = false;
                    self.core.mut_store().advance_applied_index(entry.index);
                    self.core.mut_store().persist_snapshot().await;
                }
            }
        }
    }

    fn maybe_pending_conf_change(&mut self) {
        let current_peers: HashSet<u64> = self
            .core
            .store()
            .get_conf_state()
            .voters
            .iter()
            .copied()
            .collect();

        let target_peers: HashSet<u64> = self
            .core
            .store()
            .get_validators()
            .iter()
            .map(|v| addr_to_peer_id(v))
            .collect();

        let cc = {
            let added: HashSet<u64> = target_peers.difference(&current_peers).copied().collect();
            let removed: HashSet<u64> = current_peers.difference(&target_peers).copied().collect();

            let mut added_ccs = vec![];
            let mut removed_ccs = vec![];
            for id in added.clone() {
                let ccs = ConfChangeSingle {
                    change_type: ConfChangeType::AddNode as i32,
                    node_id: id,
                };
                added_ccs.push(ccs);
            }
            for id in removed.clone() {
                let ccs = ConfChangeSingle {
                    change_type: ConfChangeType::RemoveNode as i32,
                    node_id: id,
                };
                removed_ccs.push(ccs);
            }

            // if replace all validators at one time, split it to two steps of config change, first to add new, and then remove old
            if added == target_peers && removed == current_peers {
                info!(self.logger, "replacing all validators");
                let conf_change_add = ConfChangeV2 {
                    changes: added_ccs,
                    ..Default::default()
                };
                let conf_change_remove = ConfChangeV2 {
                    changes: removed_ccs,
                    ..Default::default()
                };
                self.pending_conf_change = vec![conf_change_remove, conf_change_add];
                self.pending_conf_change_proposed = false;
                return;
            }

            added_ccs.append(&mut removed_ccs);
            ConfChangeV2 {
                changes: added_ccs,
                ..Default::default()
            }
        };

        if !cc.changes.is_empty() {
            info!(self.logger, "new pending conf change: `{:?}`", cc);
            self.pending_conf_change = vec![cc];
        } else {
            self.pending_conf_change = vec![];
        }
        self.pending_conf_change_proposed = false;
    }
}
