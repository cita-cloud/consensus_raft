use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

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
use raft::ProgressState;
use raft::RawNode;

use slog::o;
use slog::Logger;
use slog::{debug, error, info, warn};

use prost::Message as _;

use tonic::transport::Server;

use cita_cloud_proto::common::ConsensusConfiguration;
use cita_cloud_proto::common::Proposal;
use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::common::StatusCode;
use cita_cloud_proto::consensus::consensus_service_server::ConsensusService;
use cita_cloud_proto::consensus::consensus_service_server::ConsensusServiceServer;
use cita_cloud_proto::health_check::health_server::HealthServer;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer;
use cloud_util::metrics::{run_metrics_exporter, MiddlewareLayer};

use crate::client::{Controller, Network};
use crate::config::ConsensusServiceConfig;
use crate::health_check::HealthCheckServer;
use crate::storage::WalStorage;
use crate::utils::{addr_to_peer_id, short_hex};

#[derive(Debug)]
pub struct RaftConsensusService(mpsc::Sender<ConsensusConfiguration>);

#[tonic::async_trait]
impl ConsensusService for RaftConsensusService {
    async fn reconfigure(
        &self,
        request: tonic::Request<ConsensusConfiguration>,
    ) -> std::result::Result<tonic::Response<StatusCode>, tonic::Status> {
        let config = request.into_inner();
        let tx = self.0.clone();
        // FIXME: it's not safe; but if we wait for it, it may cause a deadlock
        tokio::spawn(async move {
            let _ = tx.send(config).await;
        });
        // horrible
        Ok(tonic::Response::new(StatusCode { code: 0 }))
    }

    async fn check_block(
        &self,
        _request: tonic::Request<ProposalWithProof>,
    ) -> Result<tonic::Response<StatusCode>, tonic::Status> {
        // Reply ok since we assume no byzantine faults.
        // horrible
        Ok(tonic::Response::new(StatusCode { code: 0 }))
    }
}

pub struct Peer {
    // raft core
    core: RawNode<WalStorage>,

    // peers' raft msg receiver.
    peer_rx: mpsc::Receiver<RaftMsg>,

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

    // transfer leader if no receiving valid proposal from controller.
    transfer_leader_timeout: u64,

    // slog logger
    logger: Logger,
}

impl Peer {
    pub async fn setup(config: ConsensusServiceConfig, logger: Logger) -> Self {
        let node_addr = {
            let s = &config.node_addr;
            hex::decode(s.strip_prefix("0x").unwrap_or(s)).expect("decode node_addr failed")
        };

        let local_id = addr_to_peer_id(&node_addr);

        // Controller grpc client
        let controller = {
            let logger = logger.new(o!("tag" => "controller"));
            Controller::new(config.controller_port, logger)
        };

        // Communicate with controller
        let (controller_tx, mut controller_rx) = mpsc::channel::<ConsensusConfiguration>(1);
        let raft_svc = RaftConsensusService(controller_tx);

        // Network grpc client
        let (peer_tx, peer_rx) = mpsc::channel(64);
        let network = {
            let logger = logger.new(o!("tag" => "network"));
            Network::setup(
                local_id,
                config.grpc_listen_port,
                config.network_port,
                peer_tx,
                logger,
            )
            .await
        };
        let network_svc = network.clone();

        let logger_cloned = logger.clone();
        let grpc_listen_port = config.grpc_listen_port;
        info!(
            logger_cloned,
            "grpc port of consensus_raft: {}", grpc_listen_port
        );

        let layer = if config.enable_metrics {
            tokio::spawn(async move {
                run_metrics_exporter(config.metrics_port).await.unwrap();
            });

            Some(
                tower::ServiceBuilder::new()
                    .layer(MiddlewareLayer::new(config.metrics_buckets))
                    .into_inner(),
            )
        } else {
            None
        };

        info!(logger_cloned, "start consensus_raft grpc server");
        if layer.is_some() {
            tokio::spawn(async move {
                info!(logger_cloned, "metrics on");
                let addr = format!("127.0.0.1:{}", grpc_listen_port).parse().unwrap();
                let res = Server::builder()
                    .layer(layer.unwrap())
                    .add_service(ConsensusServiceServer::new(raft_svc))
                    .add_service(NetworkMsgHandlerServiceServer::new(network_svc))
                    .add_service(HealthServer::new(HealthCheckServer {}))
                    .serve(addr)
                    .await;

                if let Err(e) = res {
                    info!(logger_cloned, "grpc service exit with error: `{}`", e);
                } else {
                    info!(logger_cloned, "grpc service exit");
                }
            });
        } else {
            tokio::spawn(async move {
                info!(logger_cloned, "metrics off");
                let addr = format!("127.0.0.1:{}", grpc_listen_port).parse().unwrap();
                let res = Server::builder()
                    .add_service(ConsensusServiceServer::new(raft_svc))
                    .add_service(NetworkMsgHandlerServiceServer::new(network_svc))
                    .add_service(HealthServer::new(HealthCheckServer {}))
                    .serve(addr)
                    .await;

                if let Err(e) = res {
                    info!(logger_cloned, "grpc service exit with error: `{}`", e);
                } else {
                    info!(logger_cloned, "grpc service exit");
                }
            });
        }

        // Recover data from log
        let mut storage = {
            let logger = logger.new(o!("tag" => "storage"));
            WalStorage::new(
                &config.wal_path,
                config.wal_log_file_compact_limit,
                config.max_wal_log_file_preserved,
                config.allow_corrupt_wal_log_tail,
                logger.clone(),
            )
            .await
        };

        // Wait for controller's reconfigure.
        info!(logger, "waiting for `reconfigure` from controller..");
        let (wants_campaign, trigger_config) = loop {
            let trigger_config = controller_rx.recv().await.unwrap();
            if let Some(index) = trigger_config
                .validators
                .iter()
                .position(|addr| addr == &node_addr)
            {
                let mut wants_compaign = false;
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

                    storage.apply_snapshot(snapshot).await.unwrap();
                    wants_compaign = index == 0;
                }

                break (wants_compaign, trigger_config);
            } else {
                info!(
                    logger,
                    "incoming config doesn't contain this node, wait for next one"
                );
            }
        };

        let recorded_height = storage.get_block_height();
        let trigger_height = trigger_config.height;
        #[allow(clippy::comparison_chain)]
        if trigger_height > recorded_height || recorded_height == 0 {
            storage.update_consensus_config(trigger_config).await;
        } else if trigger_config.height < recorded_height {
            warn!(
                logger, "block height in intial reconfigure is lower than recorded; skip it";
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

            controller_rx,

            controller,
            network,

            pending_proposal: None,
            pending_proposal_proposed: false,

            pending_conf_change: None,
            pending_conf_change_proposed: false,

            transfer_leader_timeout: config.transfer_leader_timeout_in_secs,

            logger,
        };

        this.maybe_pending_conf_change();

        if wants_campaign {
            this.core.campaign().unwrap();
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

    pub async fn run(&mut self) {
        let mut fetching_proposal: Option<JoinHandle<Result<Proposal, tonic::Status>>> = None;

        let tick_interval = Duration::from_millis(200);
        let tick_timeout = time::sleep(tick_interval);
        tokio::pin!(tick_timeout);

        // fetch it now
        let fetching_timeout = time::sleep(Duration::from_secs(0));
        tokio::pin!(fetching_timeout);

        // ping controller
        let init_timeout = time::sleep(Duration::from_secs(1));
        tokio::pin!(init_timeout);

        // used for transfering leader when we can't get a valid proposal from controller
        let mut last_time_start_fetching: Option<Instant> = None;

        loop {
            tokio::select! {
                // ping controller
                _ = &mut init_timeout => {
                    if self.core.mut_store().get_validators().is_empty() {
                        self.ping_controller().await;
                        init_timeout
                            .as_mut()
                            .reset(time::Instant::now() + Duration::from_secs(1));
                    } else {
                        init_timeout
                            .as_mut()
                            .reset(time::Instant::now() + Duration::from_secs(0xdeadbeef));
                    }
                }
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

                // reconfigure
                Some(config) = self.controller_rx.recv() => {
                    info!(self.logger, "incoming reconfigure request: `{:?}`", config);

                    let current_block_height = self.block_height();
                    let config_height = config.height;
                    if config_height > current_block_height {
                        self.core.mut_store().update_consensus_config(config).await;
                        self.maybe_pending_conf_change();
                    } else {
                        warn!(
                            self.logger,
                            "ignoring reconfigure request with lower height";
                            "current_block_height" => current_block_height,
                            "reconfigure_height" => config.height,
                        );
                    }
                }
                // raft msg from remote peers
                Some(raft_msg) = self.peer_rx.recv() => {
                    if let Err(e) = self.core.step(raft_msg) {
                        error!(self.logger, "step raft msg failed: `{}`", e);
                    }
                }
            }

            if self.is_leader() {
                // propose pending conf change
                if !self.pending_conf_change_proposed && self.pending_conf_change.is_some() {
                    match self
                        .core
                        .propose_conf_change(vec![], self.pending_conf_change.clone().unwrap())
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
                    && self.pending_conf_change.is_none()
                {
                    let proposal = self.pending_proposal.as_ref().unwrap();
                    let epxected_height = self.block_height() + 1;
                    if proposal.height == epxected_height {
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
                            "expect height" => epxected_height,
                        );
                        self.pending_proposal.take();
                    }
                }

                if let Some(t) = last_time_start_fetching.as_ref() {
                    if t.elapsed().as_secs() > self.transfer_leader_timeout {
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
                error!(self.logger, "cannot apply snapshot: `{}`", e);
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
                    let proposal_height = proposal.height;
                    let proposal_data_hex = &short_hex(&proposal.data);
                    let current_height = self.block_height();
                    if proposal_height < current_height {
                        info!(self.logger, "proposal height lower than current block height, don't check proposal"; "proposal_height" => proposal_height, "current_height" => current_height);
                    } else {
                        info!(self.logger, "checking proposal.."; "height" => proposal_height, "data" => proposal_data_hex);

                        match self.controller.check_proposal(proposal.clone()).await {
                            Ok(true) => {
                                let pwp = ProposalWithProof {
                                    proposal: Some(proposal),
                                    proof: vec![],
                                };
                                info!(self.logger, "commiting proposal..");
                                match self.controller.commit_block(pwp).await {
                                    Ok(config) => {
                                        info!(self.logger, "block committed"; "height" => proposal_height, "data" => proposal_data_hex);
                                        self.core.mut_store().update_consensus_config(config).await;
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
                        self.core
                            .mut_store()
                            .update_block_height(proposal_height)
                            .await;
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
            .map(|v| addr_to_peer_id(v))
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

        if !cc.changes.is_empty() {
            info!(self.logger, "new pending conf change: `{:?}`", cc);
            self.pending_conf_change.replace(cc);
        } else {
            self.pending_conf_change.take();
        }
        self.pending_conf_change_proposed = false;
    }

    async fn ping_controller(&mut self) {
        let pwp = ProposalWithProof {
            proposal: Some(Proposal {
                height: u64::MAX,
                data: vec![],
            }),
            proof: vec![],
        };
        info!(self.logger, "ping_controller..");
        match self.controller.commit_block(pwp).await {
            Ok(config) => {
                self.core.mut_store().update_consensus_config(config).await;
                self.maybe_pending_conf_change();
            }
            Err(e) => {
                warn!(self.logger, "commit block failed: {}", e);
            }
        }
    }
}
