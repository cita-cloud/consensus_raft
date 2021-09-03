use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use raft::eraftpb::ConfChangeType;
use raft::eraftpb::Snapshot;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;

use raft::eraftpb::Message as RaftMsg;
use raft::prelude::ConfChangeSingle;
use raft::prelude::ConfChangeV2;
use raft::prelude::Config as RaftConfig;
use raft::prelude::Entry;
use raft::prelude::EntryType;
use raft::prelude::Message;
use raft::RawNode;

use slog::o;
use slog::Logger;
use slog::{debug, error, info, warn};

use prost::Message as _;

use cita_cloud_proto::common::ConsensusConfiguration;
use cita_cloud_proto::common::Proposal;
use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::common::SimpleResponse;
use cita_cloud_proto::consensus::consensus_service_server::ConsensusService;

use crate::client::{Controller, Network};
use crate::storage::WalStorage;
use crate::utils::{address_to_peer_id, short_hex};

// Compact wal log if file size > 64MB.
const WAL_COMPACT_LIMIT: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct RaftConsensusService(mpsc::Sender<ConsensusConfiguration>);

#[tonic::async_trait]
impl ConsensusService for RaftConsensusService {
    async fn reconfigure(
        &self,
        request: tonic::Request<ConsensusConfiguration>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let config = request.into_inner();
        let _ = self.0.send(config).await;
        // TODO: it's not safe
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

pub struct Peer {
    // raft core
    core: RawNode<WalStorage>,

    // peers' raft msg receiver.
    peer_rx: mpsc::Receiver<RaftMsg>,

    // controller msg sender. Used for ConsensusService.
    controller_tx: mpsc::Sender<ConsensusConfiguration>,
    // controller msg receiver. Currently only used for `reconfigure`.
    controller_rx: mpsc::Receiver<ConsensusConfiguration>,

    // grpc client to talk with other micro-services.
    controller: Controller,
    network: Network,

    // pending_conf_change
    pending_conf_change: Option<ConfChangeV2>,
    pending_conf_change_proposed: bool,

    // pending proposal
    pending_proposal: Option<Proposal>,
    pending_proposal_proposed: bool,

    // slog logger
    logger: Logger,
}

impl Peer {
    pub async fn setup(
        local_id: u64,
        local_port: u16,
        controller_port: u16,
        network_port: u16,
        data_dir: impl AsRef<Path>,
        logger: Logger,
    ) -> Self {
        let logger = logger.new(o!("tag" => format!("peer_{}", local_id)));

        let controller = {
            let logger = logger.new(o!("tag" => "controller"));
            Controller::new(controller_port, logger)
        };

        let (controller_tx, controller_rx) = mpsc::channel(0);

        // Network
        let (peer_tx, peer_rx) = mpsc::channel(64);
        let network = {
            let logger = logger.new(o!("tag" => "network"));
            Network::setup(local_id, local_port, network_port, peer_tx, logger).await
        };

        // Recover data from log
        let storage = {
            let logger = logger.new(o!("tag" => "storage"));
            WalStorage::new(&data_dir, WAL_COMPACT_LIMIT, logger.clone()).await
        };

        let core = {
            // Load applied index from stable storage.
            let applied = storage.get_applied_index();
            // TODO: customize config
            let cfg = RaftConfig {
                id: local_id,
                election_tick: 15,
                heartbeat_tick: 5,
                check_quorum: true,
                applied,
                ..Default::default()
            };
            RawNode::new(&cfg, storage, &logger).expect("cannot create raft raw node")
        };

        Self {
            core,
            peer_rx,

            controller_tx,
            controller_rx,

            controller,
            network,

            pending_proposal: None,
            pending_proposal_proposed: false,

            pending_conf_change: None,
            pending_conf_change_proposed: false,

            logger,
        }
    }

    pub fn service(&self) -> (RaftConsensusService, Network) {
        (
            RaftConsensusService(self.controller_tx.clone()),
            self.network.clone(),
        )
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
            let network = self.network.clone();
            tokio::spawn(async move {
                network.send_msg(msg).await;
            });
        }
    }

    pub async fn run(&mut self) {
        // Wait for controller's reconfigure.
        let trigger_config = self.controller_rx.recv().await.unwrap();
        self.core
            .mut_store()
            .update_consensus_config(trigger_config)
            .await;
        self.maybe_pending_conf_change();

        let mut fetching_proposal: Option<JoinHandle<Proposal>> = None;

        let tick_interval = Duration::from_millis(200);
        let tick_timeout = time::sleep(tick_interval);
        tokio::pin!(tick_timeout);

        let fetching_timeout = time::sleep(Duration::from_secs(0));
        tokio::pin!(fetching_timeout);

        loop {
            tokio::select! {
                // timing
                _ = &mut tick_timeout => (),
                _ = &mut fetching_timeout, if self.is_leader() => (),

                // handle reconfigure
                config = self.controller_rx.recv() => {
                    let config = config.unwrap();
                    self.core.mut_store().update_consensus_config(config).await;
                    self.maybe_pending_conf_change();
                }
                // raft msg from remote peers
                raft_msg = self.peer_rx.recv() => {
                    let raft_msg = raft_msg.unwrap();
                    if let Err(e) = self.core.step(raft_msg) {
                        error!(self.logger, "step raft msg failed: `{}`", e);
                    }
                    if !self.core.has_ready() {
                        continue;
                    }
                }
                // future for fetching proposal from controller
                fetch_result = fetching_proposal.as_mut().unwrap(), if fetching_proposal.is_some() => {
                    fetching_proposal.take();

                    match fetch_result {
                        Ok(proposal) => {
                            fetching_timeout.as_mut().reset(time::Instant::now() + Duration::from_secs(self.block_interval().into()));
                            self.pending_proposal.replace(proposal);
                            self.pending_proposal_proposed = false;
                        }
                        Err(e) => {
                            warn!(self.logger, "fetching proposal failed: `{}`", e);
                        }
                    }
                }
            }

            if tick_timeout.is_elapsed() {
                self.core.tick();
                tick_timeout
                    .as_mut()
                    .reset(time::Instant::now() + tick_interval);
            }

            if self.is_leader() {
                // propose pending conf change
                if !self.pending_conf_change_proposed && self.pending_conf_change.is_some() {
                    match self
                        .core
                        .propose_conf_change(vec![], self.pending_conf_change.clone().unwrap())
                    {
                        Ok(()) => self.pending_conf_change_proposed = true,
                        Err(e) => warn!(self.logger, "propose conf change failed: `{}`", e),
                    }
                }

                // propose pending proposal
                if !self.pending_proposal_proposed
                    && self.pending_proposal.is_some()
                    && self.pending_conf_change.is_none()
                {
                    let proposal = self.pending_proposal.as_ref().unwrap();
                    let epxected_height = self.block_height() + 1;
                    if proposal.height == epxected_height {
                        let proposal_bytes = proposal.encode_to_vec();

                        if let Err(e) = self.core.propose(vec![], proposal_bytes) {
                            warn!(self.logger, "can't propose proposal: `{}`", e);
                        } else {
                            self.pending_proposal_proposed = true;
                        }
                    } else {
                        warn!(
                            self.logger,
                            "receive a proposal with invalid height, drop it";
                            "proposal height" => proposal.height,
                            "expect height" => epxected_height,
                        );
                        self.pending_proposal.take();
                    }
                }

                // fetching new proposal
                if self.pending_proposal.is_none()
                    && fetching_proposal.is_none()
                    && fetching_timeout.is_elapsed()
                {
                    let controller = self.controller.clone();
                    fetching_proposal.replace(tokio::spawn(async move {
                        // TODO: hanlde error
                        controller.get_proposal().await.unwrap()
                    }));
                }
            } else {
                if let Some(h) = fetching_proposal.take() {
                    h.abort();
                }
                self.pending_proposal.take();
                self.pending_proposal_proposed = false;
                // If we step down and become leader again, we need to re-propose pending conf change.
                self.pending_conf_change_proposed = false;
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
            if let Err(e) = self.core.mut_store().apply_snapshot(s).await {
                panic!("cannot apply snapshot: `{}`", e);
            }
        }

        self.handle_committed_entries(ready.take_committed_entries())
            .await;

        // Persistent raft logs.
        if !ready.entries().is_empty() {
            self.core.mut_store().append_entries(ready.entries()).await;
        }

        // Raft HardState changed, and we need to persist it.
        if let Some(hs) = ready.hs() {
            self.core.mut_store().update_hard_state(hs.clone()).await;
        }

        if !ready.persisted_messages().is_empty() {
            self.send_msgs(ready.take_persisted_messages()).await;
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        let mut light_rd = self.core.advance(ready);

        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            let store = self.core.mut_store();
            store.update_committed_index(commit).await;
        }

        // Send out the messages.
        self.send_msgs(light_rd.take_messages()).await;

        // Apply all committed entries.
        self.handle_committed_entries(light_rd.take_committed_entries())
            .await;

        // Advance the apply index.
        self.core.advance_apply();

        // Maybe compact log file.
        self.core.mut_store().maybe_compact().await;
    }

    async fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) {
        // Fitler out empty entries produced by new elected leaders.
        for entry in committed_entries
            .into_iter()
            .filter(|ent| !ent.data.is_empty())
        {
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let proposal = Proposal::decode(entry.data.as_slice()).unwrap();
                    let expected_height = self.core.store().get_block_height() + 1;
                    // We must maintain the consistence in ourself
                    assert_eq!(proposal.height, expected_height);

                    // TODO: handle grpc error
                    if self
                        .controller
                        .check_proposal(proposal.clone())
                        .await
                        .unwrap_or(false)
                    {
                        let pwp = ProposalWithProof {
                            proposal: Some(proposal),
                            proof: vec![],
                        };
                        if let Ok(config) = self.controller.commit_block(pwp).await {
                            self.core.mut_store().update_consensus_config(config).await;
                            self.maybe_pending_conf_change();
                        }
                    }

                    self.core
                        .mut_store()
                        .set_block_height(expected_height)
                        .await;
                    if let Some(p) = self.pending_proposal.as_ref() {
                        if p.height <= expected_height {
                            self.pending_proposal.take();
                        }
                    }
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
                    self.core.mut_store().update_conf_state(cs).await;
                    // maybe clean the pending conf change.
                    self.maybe_pending_conf_change();
                }
            }

            self.core
                .mut_store()
                .advance_applied_index(entry.index)
                .await;
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
            .map(|v| address_to_peer_id(v))
            .collect();

        let cc = {
            let added: Vec<u64> = target_peers.difference(&current_peers).copied().collect();
            let removed: Vec<u64> = current_peers.difference(&target_peers).copied().collect();

            let mut changes = vec![];
            for id in added {
                let ccs = ConfChangeSingle {
                    change_type: ConfChangeType::AddNode as i32,
                    node_id: id,
                };
                changes.push(ccs);
            }
            for id in removed {
                let ccs = ConfChangeSingle {
                    change_type: ConfChangeType::RemoveNode as i32,
                    node_id: id,
                };
                changes.push(ccs);
            }
            ConfChangeV2 {
                changes,
                ..Default::default()
            }
        };

        self.pending_conf_change.replace(cc);
        self.pending_conf_change_proposed = false;
    }
}
