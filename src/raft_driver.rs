//! The async driver that turns the pure [`crate::raft::Raft`] engine into a live,
//! networked control plane.
//!
//! It owns the engine + its WAL + a peer transport, runs the tick loop, ships
//! Raft messages between brain members over HTTP, and applies committed commands
//! to the state machine. It implements [`ControlPlane`], so the API and reconciler
//! drive it exactly like the single-member [`crate::cluster::LocalControlPlane`].
//!
//! **Safety contract.** The engine hands back a [`crate::raft::Ready`] whose
//! `persist` must reach disk **before** its messages are sent or its commands
//! applied (a member must never acknowledge a vote/entry it hasn't durably
//! recorded). Every path here — tick, propose, inbound RPC, response delivery —
//! routes through [`RaftControlPlane::drain`], which persists first.
//!
//! Locking: the engine is a plain `Mutex` held only for the synchronous
//! engine call (`tick`/`step`/`propose` + `ready`); it is released before any
//! disk or network I/O, so no lock is ever held across an `.await`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use axum::{extract::State, routing::post, Json, Router};
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

use crate::cluster::{apply_command, Command, ControlPlane};
use crate::membership::Membership;
use crate::model::{ScalePlan, ShardAssignment};
use crate::placement::Placement;
use crate::raft::{
    Addressed, AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, NodeId,
    Persisted, Raft, RaftConfig, RaftMessage, Ready, RequestVoteReq, RequestVoteResp,
};
use crate::raft_store::RaftStore;

/// Driver tick cadence. With the default [`RaftConfig`] this gives a ~150ms
/// heartbeat and a ~500–900ms election timeout.
const TICK_MS: u64 = 50;

/// Compact the Raft log once it grows past this many live entries (folding the
/// prefix into a state-machine snapshot). The brain's write rate is low, so this
/// is rarely reached; it just bounds the log + WAL + restart-replay over a long life.
const COMPACT_LOG_THRESHOLD: usize = 256;

/// Peer transport for Raft RPCs. `None` from `send` means "couldn't reach the
/// peer this time" — Raft tolerates dropped messages and retries on the next tick.
pub enum Transport {
    /// Production: JSON-over-HTTP to a peer's `/raft/{vote,append,snapshot}`
    /// endpoints, bearer-authenticated with the shared secret when one is set.
    Http {
        client: reqwest::Client,
        secret: Option<String>,
    },
    /// Tests / degenerate single-member: never sends (there are no peers).
    #[cfg(test)]
    Disabled,
}

impl Transport {
    pub fn http() -> Self {
        Transport::Http {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap_or_default(),
            secret: raft_secret_from_env(),
        }
    }

    async fn send(&self, to: &NodeId, msg: RaftMessage) -> Option<RaftMessage> {
        match self {
            Transport::Http { client, secret } => http_send(client, secret, to, msg).await,
            #[cfg(test)]
            Transport::Disabled => None,
        }
    }
}

/// The shared secret peer brains authenticate Raft RPCs with, from
/// `FIDUCIA_BRAIN_RAFT_SECRET`. `None` (unset/empty) ⇒ auth disabled (dev / single box).
fn raft_secret_from_env() -> Option<String> {
    std::env::var("FIDUCIA_BRAIN_RAFT_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Send one Raft *request* to a peer and return its *response* message. Responses
/// (`*Resp`) are never sent proactively — they ride back as the HTTP reply to the
/// originating request — so they map to `None` here.
async fn http_send(
    client: &reqwest::Client,
    secret: &Option<String>,
    to: &NodeId,
    msg: RaftMessage,
) -> Option<RaftMessage> {
    let base = to.trim_end_matches('/');
    let auth = |req: reqwest::RequestBuilder| match secret {
        Some(s) => req.bearer_auth(s),
        None => req,
    };
    match msg {
        RaftMessage::RequestVote(req) => {
            let resp: RequestVoteResp = auth(client.post(format!("{base}/raft/vote")).json(&req))
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            Some(RaftMessage::RequestVoteResp(resp))
        }
        RaftMessage::AppendEntries(req) => {
            let resp: AppendEntriesResp =
                auth(client.post(format!("{base}/raft/append")).json(&req))
                    .send()
                    .await
                    .ok()?
                    .json()
                    .await
                    .ok()?;
            Some(RaftMessage::AppendEntriesResp(resp))
        }
        RaftMessage::InstallSnapshot(req) => {
            let resp: InstallSnapshotResp =
                auth(client.post(format!("{base}/raft/snapshot")).json(&req))
                    .send()
                    .await
                    .ok()?
                    .json()
                    .await
                    .ok()?;
            Some(RaftMessage::InstallSnapshotResp(resp))
        }
        RaftMessage::RequestVoteResp(_)
        | RaftMessage::AppendEntriesResp(_)
        | RaftMessage::InstallSnapshotResp(_) => None,
    }
}

/// The live, replicated control plane: a [`Raft`] engine plus the I/O around it.
pub struct RaftControlPlane {
    engine: Mutex<Raft>,
    /// Durable WAL. `None` disables persistence (kept for tests / in-memory runs).
    store: Option<RaftStore>,
    transport: Transport,
    /// Shared secret a peer must present (bearer) on `/raft/*`; `None` ⇒ auth off.
    raft_secret: Option<String>,
    /// Outbound Raft messages, drained by the spawned outbox task.
    outbox: mpsc::UnboundedSender<Vec<Addressed>>,
    outbox_rx: Mutex<Option<mpsc::UnboundedReceiver<Vec<Addressed>>>>,
    /// Serializes WAL writes so concurrent delivers can't race on the temp file
    /// or persist an older snapshot after a newer one.
    io_lock: Mutex<()>,
    // State-machine handles the committed log is applied to.
    membership: Arc<Membership>,
    placement: Arc<Placement>,
    plan: Arc<Mutex<ScalePlan>>,
}

impl RaftControlPlane {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        cfg: RaftConfig,
        store: Option<RaftStore>,
        restored: Persisted,
        transport: Transport,
        membership: Arc<Membership>,
        placement: Arc<Placement>,
        plan: Arc<Mutex<ScalePlan>>,
    ) -> Arc<Self> {
        let engine = Raft::new(id.clone(), peers, cfg, seed_from(&id), restored);
        let (outbox, rx) = mpsc::unbounded_channel();
        Arc::new(RaftControlPlane {
            engine: Mutex::new(engine),
            store,
            transport,
            raft_secret: raft_secret_from_env(),
            outbox,
            outbox_rx: Mutex::new(Some(rx)),
            io_lock: Mutex::new(()),
            membership,
            placement,
            plan,
        })
    }

    /// Spawn the background tick loop and outbox sender. Call once.
    pub fn spawn(self: &Arc<Self>) {
        // Tick loop: advance logical time; leaders heartbeat, others count down.
        let ticker = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(TICK_MS));
            loop {
                interval.tick().await;
                let ready = {
                    let mut engine = ticker.engine.lock().unwrap();
                    engine.tick();
                    engine.ready()
                };
                ticker.drain(ready, None);
            }
        });

        // Outbox: for each outgoing request, send it and feed the response back.
        let sender = self.clone();
        let mut rx = self
            .outbox_rx
            .lock()
            .unwrap()
            .take()
            .expect("spawn called once");
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                for addressed in batch {
                    let me = sender.clone();
                    tokio::spawn(async move {
                        if let Some(resp) = me.transport.send(&addressed.to, addressed.msg).await {
                            me.deliver(addressed.to, resp);
                        }
                    });
                }
            }
        });
    }

    /// Feed a peer's response back into the engine and process the fallout.
    fn deliver(&self, from: NodeId, resp: RaftMessage) {
        let ready = {
            let mut engine = self.engine.lock().unwrap();
            engine.step(from, resp);
            engine.ready()
        };
        self.drain(ready, None);
    }

    /// Persist (first!), apply committed commands, then route messages. When
    /// `reply_to` is set (an inbound RPC), the single response addressed back to
    /// that peer is returned for the HTTP reply rather than dispatched.
    fn drain(&self, ready: Ready, reply_to: Option<&NodeId>) -> Option<RaftMessage> {
        if ready.persist.is_some() {
            if let Some(store) = &self.store {
                // Serialize persistence and write the engine's LATEST durable
                // state. Concurrent delivers can each yield a dirty `ready`;
                // without this, two saves would race on the shared temp file AND
                // could write an older snapshot after a newer one (a restart could
                // then lose a recorded vote or entry — a safety violation). The IO
                // lock orders the writes, and re-reading current state under it
                // means a later save never regresses what an earlier one persisted.
                let _io = self.io_lock.lock().unwrap();
                let snapshot = self.engine.lock().unwrap().persisted_snapshot();
                if let Err(err) = store.save(&snapshot) {
                    // Durability is the whole point; surface loudly. (A hardening
                    // step could step the member down on repeated failures.)
                    tracing::error!("raft: failed to persist state: {err}");
                }
            }
        }

        // Reset the state machine to an installed snapshot before applying anything
        // newer on top of it (an InstallSnapshot jumps us past compacted entries).
        if let Some(data) = &ready.restore {
            restore_state_machine(data, &self.placement, &self.plan);
        }

        for command in ready.committed {
            apply_command(&self.membership, &self.placement, &self.plan, command);
        }

        // Bound the log: once it grows past the threshold, fold its committed prefix
        // into a state-machine snapshot and drop those entries.
        self.maybe_compact(ready.applied_upto);

        let mut reply = None;
        let mut others = Vec::new();
        for addressed in ready.messages {
            let is_response = matches!(
                addressed.msg,
                RaftMessage::RequestVoteResp(_) | RaftMessage::AppendEntriesResp(_)
            );
            if reply.is_none() && is_response && reply_to == Some(&addressed.to) {
                reply = Some(addressed.msg);
            } else {
                others.push(addressed);
            }
        }
        if !others.is_empty() {
            let _ = self.outbox.send(others);
        }
        reply
    }

    fn step_inbound(&self, from: NodeId, msg: RaftMessage) -> Option<RaftMessage> {
        let ready = {
            let mut engine = self.engine.lock().unwrap();
            engine.step(from.clone(), msg);
            engine.ready()
        };
        self.drain(ready, Some(&from))
    }

    /// Handle an inbound `RequestVote` and produce the reply (for `/raft/vote`).
    pub fn handle_request_vote(&self, req: RequestVoteReq) -> RequestVoteResp {
        let from = req.candidate_id.clone();
        let pre_vote = req.pre_vote;
        match self.step_inbound(from, RaftMessage::RequestVote(req)) {
            Some(RaftMessage::RequestVoteResp(resp)) => resp,
            _ => RequestVoteResp {
                term: self.engine.lock().unwrap().term(),
                granted: false,
                pre_vote,
            },
        }
    }

    /// Handle an inbound `AppendEntries` and produce the reply (for `/raft/append`).
    pub fn handle_append_entries(&self, req: AppendEntriesReq) -> AppendEntriesResp {
        let from = req.leader_id.clone();
        match self.step_inbound(from, RaftMessage::AppendEntries(req)) {
            Some(RaftMessage::AppendEntriesResp(resp)) => resp,
            _ => AppendEntriesResp {
                term: self.engine.lock().unwrap().term(),
                success: false,
                match_index: 0,
            },
        }
    }

    /// Handle an inbound `InstallSnapshot` and produce the reply (for `/raft/snapshot`).
    pub fn handle_install_snapshot(&self, req: InstallSnapshotReq) -> InstallSnapshotResp {
        let from = req.leader_id.clone();
        match self.step_inbound(from, RaftMessage::InstallSnapshot(req)) {
            Some(RaftMessage::InstallSnapshotResp(resp)) => resp,
            _ => InstallSnapshotResp {
                term: self.engine.lock().unwrap().term(),
                success: false,
                last_included_index: 0,
            },
        }
    }

    /// Once the live log passes [`COMPACT_LOG_THRESHOLD`], snapshot the state
    /// machine as of `applied_upto` and drop the folded-in log prefix, then persist
    /// the compacted state. All commands are idempotent, so a snapshot that reflects
    /// a slightly newer index than `applied_upto` is still safe to compact at it.
    fn maybe_compact(&self, applied_upto: u64) {
        let should = {
            let engine = self.engine.lock().unwrap();
            engine.log_len() >= COMPACT_LOG_THRESHOLD && applied_upto > engine.base_index()
        };
        if !should {
            return;
        }
        let data = snapshot_state_machine(&self.placement, &self.plan);
        {
            let mut engine = self.engine.lock().unwrap();
            engine.compact(applied_upto, data);
        }
        // Persist the now-compacted log + snapshot immediately (ordered under io_lock,
        // re-reading current state — same discipline as `drain`).
        if let Some(store) = &self.store {
            let _io = self.io_lock.lock().unwrap();
            let snapshot = self.engine.lock().unwrap().persisted_snapshot();
            if let Err(err) = store.save(&snapshot) {
                tracing::error!("raft: failed to persist after compaction: {err}");
            }
        }
    }
}

impl ControlPlane for RaftControlPlane {
    fn is_leader(&self) -> bool {
        self.engine.lock().unwrap().is_leader()
    }

    fn leader_addr(&self) -> Option<String> {
        self.engine.lock().unwrap().leader().cloned()
    }

    fn propose(&self, command: Command) -> bool {
        let ready = {
            let mut engine = self.engine.lock().unwrap();
            match engine.propose(command) {
                Ok(_) => engine.ready(),
                Err(_) => return false, // not leader — caller forwards to leader_addr()
            }
        };
        self.drain(ready, None);
        true
    }
}

/// The peer-facing Raft routes, to merge into the brain's HTTP server.
pub fn raft_router(cp: Arc<RaftControlPlane>) -> Router {
    Router::new()
        .route("/raft/vote", post(vote))
        .route("/raft/append", post(append))
        .route("/raft/snapshot", post(snapshot))
        .with_state(cp)
}

async fn vote(
    State(cp): State<Arc<RaftControlPlane>>,
    headers: HeaderMap,
    Json(req): Json<RequestVoteReq>,
) -> Result<Json<RequestVoteResp>, StatusCode> {
    authorize(&cp, &headers)?;
    Ok(Json(cp.handle_request_vote(req)))
}

async fn append(
    State(cp): State<Arc<RaftControlPlane>>,
    headers: HeaderMap,
    Json(req): Json<AppendEntriesReq>,
) -> Result<Json<AppendEntriesResp>, StatusCode> {
    authorize(&cp, &headers)?;
    Ok(Json(cp.handle_append_entries(req)))
}

async fn snapshot(
    State(cp): State<Arc<RaftControlPlane>>,
    headers: HeaderMap,
    Json(req): Json<InstallSnapshotReq>,
) -> Result<Json<InstallSnapshotResp>, StatusCode> {
    authorize(&cp, &headers)?;
    Ok(Json(cp.handle_install_snapshot(req)))
}

/// Reject a peer Raft RPC that doesn't present the shared secret (when one is
/// configured). `Ok(())` when auth is disabled or the bearer token matches.
fn authorize(cp: &RaftControlPlane, headers: &HeaderMap) -> Result<(), StatusCode> {
    let Some(secret) = &cp.raft_secret else {
        return Ok(()); // auth disabled (FIDUCIA_BRAIN_RAFT_SECRET unset)
    };
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if presented == Some(secret.as_str()) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// The brain's replicated state machine, serialized for a Raft snapshot: the
/// placement map plus the scale plan. (Membership is leader-local soft state,
/// re-derived from heartbeats, so it is deliberately not part of the snapshot.)
#[derive(Serialize, Deserialize)]
struct StateSnapshot {
    shards: Vec<ShardAssignment>,
    plan: ScalePlan,
}

fn snapshot_state_machine(placement: &Placement, plan: &Mutex<ScalePlan>) -> Vec<u8> {
    let snapshot = StateSnapshot {
        shards: placement.snapshot(),
        plan: plan.lock().unwrap().clone(),
    };
    serde_json::to_vec(&snapshot).unwrap_or_default()
}

fn restore_state_machine(data: &[u8], placement: &Placement, plan: &Mutex<ScalePlan>) {
    match serde_json::from_slice::<StateSnapshot>(data) {
        Ok(snapshot) => {
            placement.restore_from(snapshot.shards);
            *plan.lock().unwrap() = snapshot.plan;
        }
        Err(err) => tracing::error!("raft: corrupt state snapshot, not restoring: {err}"),
    }
}

/// Deterministic per-member seed (FNV-1a of the id) so members don't all use the
/// same randomized election timeout.
fn seed_from(id: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in id.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::MembershipConfig;

    fn cfg() -> RaftConfig {
        RaftConfig {
            heartbeat_ticks: 1,
            election_min_ticks: 3,
            election_max_ticks: 6,
        }
    }

    fn handles() -> (Arc<Membership>, Arc<Placement>, Arc<Mutex<ScalePlan>>) {
        (
            Arc::new(Membership::new(MembershipConfig::default())),
            Arc::new(Placement::new(4)),
            Arc::new(Mutex::new(ScalePlan {
                target_nodes: 3,
                replication_factor: 3,
            })),
        )
    }

    /// One synchronous tick + drain (stands in for the spawned tick loop so the
    /// test stays deterministic — no runtime, no timers).
    fn tick(cp: &Arc<RaftControlPlane>) {
        let ready = {
            let mut engine = cp.engine.lock().unwrap();
            engine.tick();
            engine.ready()
        };
        cp.drain(ready, None);
    }

    fn elect_self(cp: &Arc<RaftControlPlane>) {
        for _ in 0..10 {
            tick(cp);
            if cp.is_leader() {
                return;
            }
        }
        panic!("single member did not elect itself");
    }

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "fiducia-driver-{tag}-{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn single_member_driver_commits_persists_and_applies() {
        let dir = unique_dir("commit");
        let (store, restored) = RaftStore::open(&dir).unwrap();
        let (membership, placement, plan) = handles();
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![], // single member
            cfg(),
            Some(store),
            restored,
            Transport::Disabled,
            membership,
            placement,
            plan.clone(),
        );

        elect_self(&cp);
        assert!(cp.is_leader());

        // Propose through the ControlPlane trait → commit (quorum of 1) → apply.
        assert!(cp.propose(Command::SetScalePlan(ScalePlan {
            target_nodes: 9,
            replication_factor: 3,
        })));
        assert_eq!(plan.lock().unwrap().target_nodes, 9, "applied to the state machine");

        // And it was persisted: a fresh store sees the committed entry.
        let (_s, reread) = RaftStore::open(&dir).unwrap();
        assert!(reread.commit_index >= 1, "commit persisted");
        assert!(reread.log.iter().any(|e| matches!(
            &e.command,
            Some(Command::SetScalePlan(p)) if p.target_nodes == 9
        )));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn driver_replays_persisted_log_into_a_fresh_state_machine_on_restart() {
        let dir = unique_dir("restart");

        // First boot: elect, propose, persist.
        {
            let (store, restored) = RaftStore::open(&dir).unwrap();
            let (membership, placement, plan) = handles();
            let cp = RaftControlPlane::new(
                "http://brain-0:8095".to_string(),
                vec![],
                cfg(),
                Some(store),
                restored,
                Transport::Disabled,
                membership,
                placement,
                plan,
            );
            elect_self(&cp);
            assert!(cp.propose(Command::SetScalePlan(ScalePlan {
                target_nodes: 11,
                replication_factor: 3,
            })));
        }

        // Restart: a fresh driver + fresh (empty) state machine recovers from disk.
        let (store, restored) = RaftStore::open(&dir).unwrap();
        let (membership, placement, plan) = handles();
        assert_eq!(plan.lock().unwrap().target_nodes, 3, "state machine starts empty");
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![],
            cfg(),
            Some(store),
            restored,
            Transport::Disabled,
            membership,
            placement,
            plan.clone(),
        );
        // The first drain replays the committed log into the state machine.
        tick(&cp);
        assert_eq!(
            plan.lock().unwrap().target_nodes,
            11,
            "committed scale plan rebuilt from the persisted log after restart"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    async fn await_leader(cps: &[Arc<RaftControlPlane>], timeout: Duration) -> Option<usize> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(i) = cps.iter().position(|cp| cp.is_leader()) {
                return Some(i);
            }
            if std::time::Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The real thing: three drivers on localhost HTTP ports elect a leader and
    /// replicate a command over `Transport::Http` + `raft_router` (vote/append) —
    /// exercising the serialize → POST → handler → reply → `deliver` path that the
    /// in-process engine harness in `raft.rs` does not.
    #[tokio::test]
    async fn three_brains_elect_and_replicate_over_http() {
        // Bind three ephemeral ports first so every member knows all peers' URLs.
        let mut listeners = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            ids.push(format!("http://{}", l.local_addr().unwrap()));
            listeners.push(l);
        }

        let mut cps = Vec::new();
        let mut plans = Vec::new();
        for (i, listener) in listeners.into_iter().enumerate() {
            let peers: Vec<String> = ids
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, id)| id.clone())
                .collect();
            let (membership, placement, plan) = handles();
            let cp = RaftControlPlane::new(
                ids[i].clone(),
                peers,
                cfg(),
                None, // in-memory: this test exercises the HTTP path, not durability
                Persisted::default(),
                Transport::http(),
                membership,
                placement,
                plan.clone(),
            );
            cp.spawn();
            let app = raft_router(cp.clone());
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            cps.push(cp);
            plans.push(plan);
        }

        // A leader must emerge from real elections over HTTP.
        let leader = await_leader(&cps, Duration::from_secs(5))
            .await
            .expect("a leader is elected over HTTP");

        // Propose through the leader; it must replicate + apply on all three.
        assert!(cps[leader].propose(Command::SetScalePlan(ScalePlan {
            target_nodes: 7,
            replication_factor: 3,
        })));

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if plans.iter().all(|p| p.lock().unwrap().target_nodes == 7) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "scale plan did not replicate to all members over HTTP"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
