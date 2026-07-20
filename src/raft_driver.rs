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
//! Locking: the engine is a plain `Mutex`; no lock is ever held across an
//! `.await`. Normal persistence re-reads the engine under the serialized I/O
//! lock. Compaction holds the engine lock across its synchronous fsync so the
//! persisted candidate can be installed atomically in memory only on success.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use axum::{extract::State, routing::post, Json, Router};
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::cluster::{apply_command, Command, ControlPlane};
use crate::membership::Membership;
use crate::model::{ScalePlan, ShardAssignment};
use crate::placement::Placement;
use crate::raft::{
    Addressed, AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp,
    NodeId, Persisted, Raft, RaftConfig, RaftMessage, Ready, RequestVoteReq, RequestVoteResp,
};
use crate::raft_store::RaftStore;

/// Driver tick cadence. With the default [`RaftConfig`] this gives a ~150ms
/// heartbeat and a ~500–900ms election timeout.
const TICK_MS: u64 = 50;

/// Compact the Raft log once it grows past this many live entries (folding the
/// prefix into a state-machine snapshot). The brain's write rate is low, so this
/// is rarely reached; it just bounds the log + WAL + restart-replay over a long life.
const COMPACT_LOG_THRESHOLD: usize = 256;

/// How long [`ControlPlane::propose`] waits for its entry to *commit* before
/// reporting failure. Long enough to ride out a heartbeat round-trip and a peer
/// restart, short enough that a caller (an operator's `POST /v1/scale`) gets an
/// answer rather than hanging on a partitioned leader. Shortened under `cfg(test)`
/// so the isolated-leader test doesn't sit through the production wait.
#[cfg(not(test))]
const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const COMMIT_TIMEOUT: Duration = Duration::from_secs(1);

/// Peer transport for Raft RPCs. `None` from `send` means "couldn't reach the
/// peer this time" — Raft tolerates dropped messages and retries on the next tick.
pub enum Transport {
    /// Production: JSON-over-HTTP to a peer's `/raft/{vote,append,snapshot}`
    /// endpoints, bearer-authenticated with the required shared secret.
    Http {
        client: reqwest::Client,
        secret: RaftSecret,
    },
    /// Tests / degenerate single-member: never sends (there are no peers).
    #[cfg(test)]
    Disabled,
}

impl Transport {
    pub fn http(secret: RaftSecret) -> Self {
        Transport::Http {
            // Fail fast at startup: falling back to `Client::default()` would
            // silently drop the timeout, letting a hung peer stall RPC tasks
            // indefinitely.
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("failed to build the Raft peer HTTP client"),
            secret,
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

/// Validated bearer secret for the cross-cluster Raft peer plane.
#[derive(Clone)]
pub struct RaftSecret(String);

impl RaftSecret {
    pub fn from_env() -> io::Result<Self> {
        Self::parse(std::env::var("FIDUCIA_BRAIN_RAFT_SECRET").ok())
    }

    fn parse(value: Option<String>) -> io::Result<Self> {
        let value = value.unwrap_or_default();
        let value = value.trim();
        if value.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FIDUCIA_BRAIN_RAFT_SECRET is required when FIDUCIA_BRAIN_PEERS enables the peer Raft plane",
            ));
        }
        Ok(Self(value.to_owned()))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

/// POST one Raft RPC to `{base}{path}` and decode the JSON reply.
///
/// Raft tolerates lost messages (the engine retries on later ticks), so failures
/// map to `None` — but they are logged at `warn` first. A silently unreachable
/// peer plane (wrong `FIDUCIA_BRAIN_RAFT_SECRET`, DNS/TLS breakage, a peer that
/// answers with non-JSON) previously produced *no* log line at all, which made a
/// total consensus outage — no elected leader, ever — invisible to operators.
async fn post_rpc<Req, Resp>(
    client: &reqwest::Client,
    secret: &RaftSecret,
    base: &str,
    path: &str,
    rpc: &'static str,
    req: &Req,
) -> Option<Resp>
where
    Req: Serialize,
    Resp: serde::de::DeserializeOwned,
{
    let url = format!("{base}{path}");
    let resp = match client
        .post(&url)
        .bearer_auth(secret.as_str())
        .json(req)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!(peer = %url, rpc, error = %err, "raft: peer RPC failed to send");
            return None;
        }
    };
    let status = resp.status();
    if !status.is_success() {
        tracing::warn!(
            peer = %url, rpc, %status,
            "raft: peer rejected RPC (on 401, check FIDUCIA_BRAIN_RAFT_SECRET on both members)"
        );
        return None;
    }
    match resp.json::<Resp>().await {
        Ok(decoded) => Some(decoded),
        Err(err) => {
            tracing::warn!(peer = %url, rpc, error = %err, "raft: peer RPC reply failed to decode");
            None
        }
    }
}

/// Send one Raft *request* to a peer and return its *response* message. Responses
/// (`*Resp`) are never sent proactively — they ride back as the HTTP reply to the
/// originating request — so they map to `None` here.
async fn http_send(
    client: &reqwest::Client,
    secret: &RaftSecret,
    to: &NodeId,
    msg: RaftMessage,
) -> Option<RaftMessage> {
    let base = to.trim_end_matches('/');
    match msg {
        RaftMessage::RequestVote(req) => post_rpc(client, secret, base, "/raft/vote", "vote", &req)
            .await
            .map(RaftMessage::RequestVoteResp),
        RaftMessage::AppendEntries(req) => {
            post_rpc(client, secret, base, "/raft/append", "append", &req)
                .await
                .map(RaftMessage::AppendEntriesResp)
        }
        RaftMessage::InstallSnapshot(req) => {
            post_rpc(client, secret, base, "/raft/snapshot", "snapshot", &req)
                .await
                .map(RaftMessage::InstallSnapshotResp)
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
    /// Shared secret every peer must present (bearer) on `/raft/*`.
    raft_secret: RaftSecret,
    /// Sticky fail-closed state. Once durability or outbox handoff fails, this
    /// member remains unavailable until restart and cannot acknowledge work.
    available: AtomicBool,
    /// Outbound Raft messages, drained by the spawned outbox task.
    outbox: mpsc::UnboundedSender<Vec<Addressed>>,
    outbox_rx: Mutex<Option<mpsc::UnboundedReceiver<Vec<Addressed>>>>,
    /// Serializes WAL writes so concurrent delivers can't race on the temp file
    /// or persist an older snapshot after a newer one.
    io_lock: Mutex<()>,
    /// Highest log index the state machine currently reflects. Committed batches
    /// are applied in strict index order: concurrent drains each carry a disjoint
    /// batch handed out in commit order under the engine lock, but they can reach
    /// the apply point in any order, and applying a stale batch after a newer one
    /// would clobber newer placement/scale state (the leader's reconcile loop
    /// re-derives it, but a follower's copy would stay wrong until the next
    /// restore). Waiters block on `apply_cv` until their predecessor has applied.
    apply_watermark: Mutex<u64>,
    apply_cv: Condvar,
    /// Signalled (on the engine mutex) whenever a drain may have moved the commit
    /// point, so `propose` wakes as soon as its entry commits instead of polling.
    commit_cv: Condvar,
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
        raft_secret: RaftSecret,
        membership: Arc<Membership>,
        placement: Arc<Placement>,
        plan: Arc<Mutex<ScalePlan>>,
    ) -> io::Result<Arc<Self>> {
        // Recovery: entries at or below `base_index` exist only inside the WAL
        // snapshot — the log no longer has them. The engine resumes replay from
        // `base_index`, so the snapshot must be folded into the live state machine
        // here, or a compacted-and-restarted member would serve an empty placement
        // map (and regress the scale plan) until the next leader InstallSnapshot.
        if let Some(snapshot) = &restored.snapshot {
            restore_state_machine(snapshot, &placement, &plan).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to restore the state machine from the WAL snapshot: {err}"),
                )
            })?;
        }
        let apply_watermark = restored.base_index;
        let engine = Raft::new(id.clone(), peers, cfg, seed_from(&id), restored);
        let (outbox, rx) = mpsc::unbounded_channel();
        Ok(Arc::new(RaftControlPlane {
            engine: Mutex::new(engine),
            store,
            transport,
            raft_secret,
            available: AtomicBool::new(true),
            outbox,
            outbox_rx: Mutex::new(Some(rx)),
            io_lock: Mutex::new(()),
            apply_watermark: Mutex::new(apply_watermark),
            apply_cv: Condvar::new(),
            commit_cv: Condvar::new(),
            membership,
            placement,
            plan,
        }))
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::SeqCst)
    }

    fn fail_closed(&self, reason: impl std::fmt::Display) {
        if self.available.swap(false, Ordering::SeqCst) {
            tracing::error!(%reason, "raft: control plane is unavailable until restart");
        }
        // Wake any drain waiting for an earlier batch to apply — that batch may
        // never arrive now; waiters re-check availability and bail.
        self.apply_cv.notify_all();
    }

    /// Spawn the background tick loop and outbox sender. Call once.
    pub fn spawn(self: &Arc<Self>) {
        // Tick loop: advance logical time; leaders heartbeat, others count down.
        let ticker = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(TICK_MS));
            loop {
                interval.tick().await;
                if !ticker.is_available() {
                    break;
                }
                let ready = {
                    let mut engine = ticker.engine.lock().unwrap();
                    engine.tick();
                    engine.ready()
                };
                if ticker.drain(ready, None).is_err() {
                    // drain() already failed closed (logged, member unavailable
                    // until restart) — stop ticking instead of spinning on it.
                    break;
                }
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
        if !self.is_available() {
            return;
        }
        let ready = {
            let mut engine = self.engine.lock().unwrap();
            engine.step(from, resp);
            engine.ready()
        };
        // A drain failure has already failed closed (logged + unavailable until
        // restart); there is no response to route for a background delivery.
        let _ = self.drain(ready, None);
    }

    /// Persist (first!), apply committed commands, then route messages. When
    /// `reply_to` is set (an inbound RPC), the single response addressed back to
    /// that peer is returned for the HTTP reply rather than dispatched.
    fn drain(&self, ready: Ready, reply_to: Option<&NodeId>) -> Result<Option<RaftMessage>, ()> {
        if !self.is_available() {
            return Err(());
        }
        if ready.persist.is_some() {
            if let Some(store) = &self.store {
                // Serialize persistence and write the engine's LATEST durable
                // state. Concurrent delivers can each yield a dirty `ready`;
                // without this, two saves would race on the shared temp file AND
                // could write an older snapshot after a newer one (a restart could
                // then lose a recorded vote or entry — a safety violation). The IO
                // lock orders the writes, and re-reading current state under it
                // means a later save never regresses what an earlier one persisted.
                //
                // The fsync is synchronous and must finish before this RPC is acked
                // (a follower must never acknowledge an entry it has not durably
                // stored), so the latency is fundamental, not a bug. It runs on a
                // Tokio worker; at the brain's low write rate (3-member group,
                // ~150ms heartbeats) that is fine, and compaction keeps each write
                // small by bounding the log. If write rate ever grows, move the
                // fsync to a dedicated persister thread the async handler awaits via
                // a oneshot — keeping persist-before-ack without parking a worker.
                let _io = self.io_lock.lock().unwrap();
                if !self.is_available() {
                    return Err(());
                }
                let snapshot = self.engine.lock().unwrap().persisted_snapshot();
                if let Err(err) = store.save(&snapshot) {
                    self.fail_closed(format_args!("failed to persist Raft state: {err}"));
                    return Err(());
                }
            }
        }

        // Serialize state-machine mutation IN LOG ORDER (see `apply_watermark`).
        // A restore (clear + rebuild to an installed snapshot) never waits: the
        // engine only accepts a snapshot strictly beyond both its base and commit
        // indices, so a restore always jumps the watermark *forward*, past any
        // batch that could still be in flight; newer committed entries then apply
        // on top. A plain batch waits until the watermark reaches its
        // `applied_from`, so batches apply oldest-first even when concurrent
        // drains race to this point. Note `applied_upto > applied_from` with an
        // empty `committed` is real (a new leader's no-op term marker) and must
        // still advance the watermark, or every later batch would wait forever.
        if ready.restore.is_some() || ready.applied_upto > ready.applied_from {
            let mut watermark = self.apply_watermark.lock().unwrap();
            if !self.is_available() {
                return Err(());
            }
            if let Some(data) = &ready.restore {
                if ready.applied_upto >= *watermark {
                    if let Err(err) = restore_state_machine(data, &self.placement, &self.plan) {
                        self.fail_closed(format_args!("failed to restore Raft snapshot: {err}"));
                        return Err(());
                    }
                    for (_, command) in ready.committed {
                        apply_command(&self.membership, &self.placement, &self.plan, command);
                    }
                    *watermark = ready.applied_upto;
                } else {
                    // Unreachable per the engine's stale-snapshot guard; never
                    // regress the state machine if it somehow happens.
                    tracing::warn!(
                        applied_upto = ready.applied_upto,
                        watermark = *watermark,
                        "raft: ignored a stale snapshot restore"
                    );
                }
                self.apply_cv.notify_all();
            } else {
                // Wait for every earlier batch (watermark reaches applied_from).
                // The timeout is a liveness backstop: if the predecessor's drain
                // failed closed, waiters wake, observe unavailability, and bail.
                while *watermark < ready.applied_from {
                    if !self.is_available() {
                        return Err(());
                    }
                    (watermark, _) = self
                        .apply_cv
                        .wait_timeout(watermark, Duration::from_millis(50))
                        .unwrap();
                }
                if !self.is_available() {
                    return Err(());
                }
                // Apply only entries the state machine hasn't seen: a restore can
                // have jumped the watermark past part (or all) of this batch.
                for (index, command) in ready.committed {
                    if index > *watermark {
                        apply_command(&self.membership, &self.placement, &self.plan, command);
                    }
                }
                if ready.applied_upto > *watermark {
                    *watermark = ready.applied_upto;
                }
                self.apply_cv.notify_all();
            }
        }

        // Bound the log: once it grows past the threshold, fold its committed prefix
        // into a state-machine snapshot and drop those entries.
        self.maybe_compact(ready.applied_upto)?;

        let mut reply = None;
        let mut others = Vec::new();
        for addressed in ready.messages {
            let is_response = matches!(
                addressed.msg,
                RaftMessage::RequestVoteResp(_)
                    | RaftMessage::AppendEntriesResp(_)
                    | RaftMessage::InstallSnapshotResp(_)
            );
            if reply.is_none() && is_response && reply_to == Some(&addressed.to) {
                reply = Some(addressed.msg);
            } else {
                others.push(addressed);
            }
        }
        if !others.is_empty() && self.outbox.send(others).is_err() {
            self.fail_closed("Raft outbox closed before message handoff");
            return Err(());
        }
        // The commit point may have moved (this drain, or the one that produced
        // the response it answers) — wake any `propose` waiting on it.
        self.commit_cv.notify_all();
        Ok(reply)
    }

    /// Block until log `index` is committed in `term`, i.e. a quorum has durably
    /// accepted it and no later leader can truncate it away. Returns false if the
    /// wait times out, this member is deposed (its uncommitted entry may then be
    /// overwritten), or it fails closed.
    fn await_commit(&self, index: u64, term: u64) -> bool {
        let deadline = std::time::Instant::now() + COMMIT_TIMEOUT;
        let mut engine = self.engine.lock().unwrap();
        loop {
            if engine.commit_index() >= index && engine.term() == term {
                return true;
            }
            if engine.term() != term || !engine.is_leader() {
                return false; // deposed before committing — the write is not durable
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() || !self.is_available() {
                return false;
            }
            // Cap each wait so availability/term changes that don't notify are
            // still noticed promptly.
            let (guard, _) = self
                .commit_cv
                .wait_timeout(engine, remaining.min(Duration::from_millis(20)))
                .unwrap();
            engine = guard;
        }
    }

    fn step_inbound(&self, from: NodeId, msg: RaftMessage) -> Option<RaftMessage> {
        if !self.is_available() {
            return None;
        }
        let ready = {
            let mut engine = self.engine.lock().unwrap();
            engine.step(from.clone(), msg);
            engine.ready()
        };
        self.drain(ready, Some(&from)).ok().flatten()
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
    fn maybe_compact(&self, applied_upto: u64) -> Result<(), ()> {
        let should = {
            let engine = self.engine.lock().unwrap();
            engine.log_len() >= COMPACT_LOG_THRESHOLD && applied_upto > engine.base_index()
        };
        if !should {
            return Ok(());
        }
        let data = match snapshot_state_machine(&self.placement, &self.plan) {
            Ok(data) => data,
            Err(err) => {
                self.fail_closed(format_args!("failed to serialize Raft snapshot: {err}"));
                return Err(());
            }
        };
        // Persist a candidate compacted engine before installing it in memory.
        // A failed snapshot/WAL write therefore leaves even the live engine's
        // pre-compaction log intact before the member becomes unavailable.
        if let Some(store) = &self.store {
            let _io = self.io_lock.lock().unwrap();
            if !self.is_available() {
                return Err(());
            }
            let mut engine = self.engine.lock().unwrap();
            let mut compacted = engine.clone();
            compacted.compact(applied_upto, data);
            let snapshot = compacted.persisted_snapshot();
            if let Err(err) = store.save(&snapshot) {
                self.fail_closed(format_args!("failed to persist Raft compaction: {err}"));
                return Err(());
            }
            *engine = compacted;
        } else {
            self.engine.lock().unwrap().compact(applied_upto, data);
        }
        Ok(())
    }
}

impl ControlPlane for RaftControlPlane {
    fn is_available(&self) -> bool {
        RaftControlPlane::is_available(self)
    }

    fn is_leader(&self) -> bool {
        self.is_available() && self.engine.lock().unwrap().is_leader()
    }

    fn leader_addr(&self) -> Option<String> {
        self.is_available()
            .then(|| self.engine.lock().unwrap().leader().cloned())
            .flatten()
    }

    /// Returns only once the command is **committed** — a local append plus a
    /// local fsync is not durability: a partitioned leader can append (and
    /// persist) entries the next leader truncates away, so acking on local
    /// persistence alone would 200-OK writes the cluster never accepted.
    fn propose(&self, command: Command) -> bool {
        if !self.is_available() {
            return false;
        }
        let (index, term, ready) = {
            let mut engine = self.engine.lock().unwrap();
            match engine.propose(command) {
                Ok(index) => (index, engine.term(), engine.ready()),
                Err(_) => return false, // not leader — caller forwards to leader_addr()
            }
        };
        if self.drain(ready, None).is_err() {
            return false;
        }
        self.await_commit(index, term)
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

/// Reject a peer Raft RPC that doesn't present the required shared secret.
fn authorize(cp: &RaftControlPlane, headers: &HeaderMap) -> Result<(), StatusCode> {
    let secret = &cp.raft_secret;
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    // Constant-time compare so the bearer secret can't be recovered a byte at a
    // time via response timing. `ct_eq` on byte slices does not short-circuit on
    // the first differing byte.
    match presented {
        Some(token) if bool::from(token.as_bytes().ct_eq(secret.as_str().as_bytes())) => Ok(()),
        _ => Err(StatusCode::UNAUTHORIZED),
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

fn snapshot_state_machine(
    placement: &Placement,
    plan: &Mutex<ScalePlan>,
) -> Result<Vec<u8>, serde_json::Error> {
    let snapshot = StateSnapshot {
        shards: placement.snapshot(),
        plan: plan.lock().unwrap().clone(),
    };
    serde_json::to_vec(&snapshot)
}

fn restore_state_machine(
    data: &[u8],
    placement: &Placement,
    plan: &Mutex<ScalePlan>,
) -> Result<(), serde_json::Error> {
    let snapshot = serde_json::from_slice::<StateSnapshot>(data)?;
    placement.restore_from(snapshot.shards);
    *plan.lock().unwrap() = snapshot.plan;
    Ok(())
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
    use crate::raft::Role;

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

    fn test_secret() -> RaftSecret {
        RaftSecret::parse(Some("test-raft-secret".to_string())).unwrap()
    }

    /// One synchronous tick + drain (stands in for the spawned tick loop so the
    /// test stays deterministic — no runtime, no timers).
    fn tick(cp: &Arc<RaftControlPlane>) {
        let ready = {
            let mut engine = cp.engine.lock().unwrap();
            engine.tick();
            engine.ready()
        };
        let _ = cp.drain(ready, None);
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
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");

        elect_self(&cp);
        assert!(cp.is_leader());

        // Propose through the ControlPlane trait → commit (quorum of 1) → apply.
        assert!(cp.propose(Command::SetScalePlan(ScalePlan {
            target_nodes: 9,
            replication_factor: 3,
        })));
        assert_eq!(
            plan.lock().unwrap().target_nodes,
            9,
            "applied to the state machine"
        );

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
                test_secret(),
                membership,
                placement,
                plan,
            )
            .expect("driver");
            elect_self(&cp);
            assert!(cp.propose(Command::SetScalePlan(ScalePlan {
                target_nodes: 11,
                replication_factor: 3,
            })));
        }

        // Restart: a fresh driver + fresh (empty) state machine recovers from disk.
        let (store, restored) = RaftStore::open(&dir).unwrap();
        let (membership, placement, plan) = handles();
        assert_eq!(
            plan.lock().unwrap().target_nodes,
            3,
            "state machine starts empty"
        );
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![],
            cfg(),
            Some(store),
            restored,
            Transport::Disabled,
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");
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
    // Multi-threaded on purpose (as in production, where `#[tokio::main]` is):
    // `propose` blocks its thread until the entry commits, and the commit arrives
    // on the peer-response tasks — which need a worker of their own to run on.
    #[tokio::test(flavor = "multi_thread")]
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
                Transport::http(test_secret()),
                test_secret(),
                membership,
                placement,
                plan.clone(),
            )
            .expect("driver");
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

    #[test]
    fn peer_secret_is_required_and_whitespace_is_rejected() {
        assert!(RaftSecret::parse(None).is_err());
        assert!(RaftSecret::parse(Some("   ".to_string())).is_err());
        assert_eq!(
            RaftSecret::parse(Some("  shared-secret  ".to_string()))
                .unwrap()
                .as_str(),
            "shared-secret"
        );
    }

    #[test]
    fn persistence_failure_makes_member_unavailable_before_acknowledging() {
        let dir = unique_dir("fail-closed");
        let (store, restored) = RaftStore::open(&dir).unwrap();
        let (membership, placement, plan) = handles();
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![],
            cfg(),
            Some(store),
            restored,
            Transport::Disabled,
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");
        elect_self(&cp);
        cp.store.as_ref().unwrap().fail_saves_for_test();

        assert!(!cp.propose(Command::SetScalePlan(ScalePlan {
            target_nodes: 99,
            replication_factor: 3,
        })));
        assert!(!cp.is_available());
        assert!(!cp.is_leader());
        assert_eq!(
            plan.lock().unwrap().target_nodes,
            3,
            "unpersisted command must not be applied"
        );
    }

    /// A WAL failure during **compaction** must fail the member closed while
    /// leaving the pre-compaction engine — and everything already durable —
    /// intact: compaction persists a *candidate* compacted engine before
    /// installing it in memory, so an aborted compaction can never lose a
    /// committed entry. A fresh store reopen recovers the full log.
    #[test]
    fn wal_failure_during_compaction_fails_closed_without_losing_committed_entries() {
        let dir = unique_dir("compaction-fail");
        let (store, restored) = RaftStore::open(&dir).unwrap();
        let (membership, placement, plan) = handles();
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![],
            cfg(),
            Some(store),
            restored,
            Transport::Disabled,
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");
        elect_self(&cp);

        // Drive committed entries past COMPACT_LOG_THRESHOLD on the engine and
        // persist them with saves still healthy. (The drain path compacts in
        // the same drain that crosses the threshold, so going through
        // `propose` could never leave a durable, uncompacted, over-threshold
        // log behind — the shape needed to make the *compaction* save the
        // first one to fail.)
        let commit_index = {
            let mut engine = cp.engine.lock().unwrap();
            for i in 0..COMPACT_LOG_THRESHOLD as u32 {
                engine
                    .propose(Command::SetScalePlan(ScalePlan {
                        target_nodes: 100 + i,
                        replication_factor: 3,
                    }))
                    .expect("leader propose");
            }
            let snapshot = engine.persisted_snapshot();
            cp.store
                .as_ref()
                .unwrap()
                .save(&snapshot)
                .expect("persist the over-threshold log while saves are healthy");
            assert!(engine.log_len() >= COMPACT_LOG_THRESHOLD);
            assert_eq!(engine.base_index(), 0, "nothing compacted yet");
            snapshot.commit_index
        };
        let live_before = cp.engine.lock().unwrap().log_len();

        // Now every save fails — the next one attempted is compaction's.
        cp.store.as_ref().unwrap().fail_saves_for_test();
        assert!(
            cp.maybe_compact(commit_index).is_err(),
            "failed compaction persist must surface as an error"
        );

        // Fail closed, with the in-memory pre-compaction engine untouched.
        assert!(!cp.is_available());
        assert!(!cp.is_leader());
        {
            let engine = cp.engine.lock().unwrap();
            assert_eq!(engine.log_len(), live_before, "log prefix not dropped");
            assert_eq!(engine.base_index(), 0, "candidate compaction not installed");
        }
        assert!(!cp.propose(Command::SetScalePlan(ScalePlan {
            target_nodes: 999,
            replication_factor: 3,
        })));

        // A fresh reopen of the same directory recovers every committed entry:
        // the aborted compaction wrote nothing, so nothing was lost.
        let (_s, reread) = RaftStore::open(&dir).unwrap();
        assert_eq!(reread.commit_index, commit_index);
        assert_eq!(reread.base_index, 0);
        assert_eq!(reread.log.len(), commit_index as usize);
        assert!(reread.log.iter().any(|e| matches!(
            &e.command,
            Some(Command::SetScalePlan(p))
                if p.target_nodes == 100 + COMPACT_LOG_THRESHOLD as u32 - 1
        )));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Regression: committed batches must apply to the state machine in log
    /// order even when concurrent drains race to the apply point. A later batch
    /// arriving first must WAIT for its predecessor — applying it early would let
    /// the older batch clobber newer placement/scale state on a follower.
    #[test]
    fn committed_batches_apply_in_log_order_even_when_drains_race() {
        let (membership, placement, plan) = handles();
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![],
            cfg(),
            None,
            Persisted::default(),
            Transport::Disabled,
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");

        let batch = |from: u64, upto: u64, nodes: u32| Ready {
            persist: None,
            restore: None,
            applied_from: from,
            applied_upto: upto,
            messages: Vec::new(),
            committed: vec![(
                upto,
                Command::SetScalePlan(ScalePlan {
                    target_nodes: nodes,
                    replication_factor: 3,
                }),
            )],
        };

        // Deliver the NEWER batch (entry 3) from another thread first; it must
        // block until the older work has been applied.
        let racer = {
            let cp = cp.clone();
            std::thread::spawn(move || cp.drain(batch(2, 3, 20), None))
        };
        // Give the racer time to reach the apply point and (correctly) wait.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            plan.lock().unwrap().target_nodes,
            3,
            "the out-of-order batch must not have applied yet"
        );

        // Entry 1 carries a command; entry 2 is a leader no-op (empty committed,
        // but applied_upto still advances — it must move the watermark, or the
        // racer above would wait forever).
        cp.drain(batch(0, 1, 10), None).expect("drain batch 1");
        cp.drain(
            Ready {
                persist: None,
                restore: None,
                applied_from: 1,
                applied_upto: 2,
                messages: Vec::new(),
                committed: Vec::new(),
            },
            None,
        )
        .expect("drain the no-op batch");
        racer.join().unwrap().expect("drain batch 3");

        assert_eq!(
            plan.lock().unwrap().target_nodes,
            20,
            "the newest committed batch must win once all have applied in order"
        );
    }

    /// Drive a member to leadership with no live transport: tick until it
    /// campaigns, then hand it its peers' grants directly.
    fn elect_with_peer_grants(cp: &Arc<RaftControlPlane>, peers: &[String]) {
        for _ in 0..40 {
            tick(cp);
            if cp.is_leader() {
                return;
            }
            let (term, role) = {
                let engine = cp.engine.lock().unwrap();
                (engine.term(), engine.role())
            };
            let pre_vote = match role {
                Role::PreCandidate => true,
                Role::Candidate => false,
                _ => continue,
            };
            for peer in peers {
                let ready = {
                    let mut engine = cp.engine.lock().unwrap();
                    engine.step(
                        peer.clone(),
                        RaftMessage::RequestVoteResp(RequestVoteResp {
                            term,
                            granted: true,
                            pre_vote,
                        }),
                    );
                    engine.ready()
                };
                let _ = cp.drain(ready, None);
            }
            if cp.is_leader() {
                return;
            }
        }
        panic!("member did not reach leadership");
    }

    /// Regression (F3): `propose` must return only once the entry has COMMITTED.
    /// A leader whose peers are unreachable can append and fsync locally forever;
    /// acking that would 200-OK a `/v1/scale` write the next leader truncates
    /// away — and the state machine never sees it either.
    #[test]
    fn a_leader_without_a_quorum_does_not_acknowledge_an_uncommitted_write() {
        let peers = vec![
            "http://brain-1:8095".to_string(),
            "http://brain-2:8095".to_string(),
        ];
        let (membership, placement, plan) = handles();
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            peers.clone(),
            cfg(),
            None,
            Persisted::default(),
            Transport::Disabled, // the peers granted votes, then went silent
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");
        elect_with_peer_grants(&cp, &peers);
        assert!(cp.is_leader());

        assert!(
            !cp.propose(Command::SetScalePlan(ScalePlan {
                target_nodes: 42,
                replication_factor: 3,
            })),
            "an entry no quorum accepted must not be acknowledged"
        );
        assert_eq!(
            plan.lock().unwrap().target_nodes,
            3,
            "and it must not have been applied to the state machine"
        );
    }

    /// The healthy counterpart: once a peer acks the entry, the same `propose`
    /// completes — the commit wait is not simply "always fail on a real group".
    #[test]
    fn propose_returns_true_as_soon_as_a_quorum_acks_the_entry() {
        let peers = vec![
            "http://brain-1:8095".to_string(),
            "http://brain-2:8095".to_string(),
        ];
        let (membership, placement, plan) = handles();
        let cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            peers.clone(),
            cfg(),
            None,
            Persisted::default(),
            Transport::Disabled,
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");
        elect_with_peer_grants(&cp, &peers);

        // A peer acks everything the leader has, from another thread, while
        // `propose` is blocked waiting for the commit point to move.
        let acker = {
            let cp = cp.clone();
            let peer = peers[0].clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let ready = {
                        let mut engine = cp.engine.lock().unwrap();
                        let term = engine.term();
                        let match_index = engine.base_index() + engine.log_len() as u64;
                        engine.step(
                            peer.clone(),
                            RaftMessage::AppendEntriesResp(AppendEntriesResp {
                                term,
                                success: true,
                                match_index,
                            }),
                        );
                        engine.ready()
                    };
                    let _ = cp.drain(ready, None);
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        };

        assert!(
            cp.propose(Command::SetScalePlan(ScalePlan {
                target_nodes: 8,
                replication_factor: 3,
            })),
            "a quorum-acked entry is acknowledged"
        );
        assert_eq!(plan.lock().unwrap().target_nodes, 8, "and applied");
        acker.join().unwrap();
    }

    /// Regression: after compaction, entries at or below `base_index` exist only
    /// in the WAL snapshot. A restarted member must fold that snapshot back into
    /// its (fresh, empty) state machine at construction — otherwise it serves an
    /// empty placement map / default scale plan until the next InstallSnapshot.
    #[test]
    fn boot_restores_the_state_machine_from_the_wal_snapshot() {
        let snapshot = serde_json::to_vec(&StateSnapshot {
            shards: Vec::new(),
            plan: ScalePlan {
                target_nodes: 17,
                replication_factor: 3,
            },
        })
        .unwrap();
        let restored = Persisted {
            current_term: 3,
            voted_for: None,
            commit_index: 5,
            log: Vec::new(),
            base_index: 5,
            base_term: 3,
            snapshot: Some(snapshot),
        };

        let (membership, placement, plan) = handles();
        assert_eq!(plan.lock().unwrap().target_nodes, 3, "starts at default");
        let _cp = RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![],
            cfg(),
            None,
            restored,
            Transport::Disabled,
            test_secret(),
            membership,
            placement,
            plan.clone(),
        )
        .expect("driver");
        assert_eq!(
            plan.lock().unwrap().target_nodes,
            17,
            "scale plan folded back from the WAL snapshot at construction"
        );
    }

    /// A corrupt WAL snapshot must refuse construction (fail closed) rather than
    /// come up with a silently-empty state machine.
    #[test]
    fn boot_with_a_corrupt_wal_snapshot_fails_closed() {
        let restored = Persisted {
            current_term: 3,
            voted_for: None,
            commit_index: 5,
            log: Vec::new(),
            base_index: 5,
            base_term: 3,
            snapshot: Some(b"not json".to_vec()),
        };
        let (membership, placement, plan) = handles();
        assert!(RaftControlPlane::new(
            "http://brain-0:8095".to_string(),
            vec![],
            cfg(),
            None,
            restored,
            Transport::Disabled,
            test_secret(),
            membership,
            placement,
            plan,
        )
        .is_err());
    }
}
