//! The brain's **own** Raft — a small, fixed-membership, single-group consensus
//! engine that replicates the control-plane [`Command`] log across the brain
//! members (one per cloud), so the placement map + scale plan survive losing a
//! brain node and stay consistent across all three clusters.
//!
//! This is a **separate implementation** from `fiducia-node`'s Raft (deliberately
//! — the two codebases evolve independently). The brain's job is simpler than the
//! node's, which lets this engine stay small:
//!
//!   * **fixed membership** — the brain group is a static set (3 members from
//!     env), so there is no joint-consensus / config-change machinery;
//!   * **single group** — one log, one state machine (not a group per shard);
//!   * **low write rate** — placement decisions, not customer ops.
//!
//! It is written as a **pure state machine** so the consensus logic is
//! deterministically unit-testable with no async and no sockets:
//!
//!   * [`Raft::tick`] advances logical time (one driver tick),
//!   * [`Raft::step`] consumes one inbound message,
//!   * [`Raft::propose`] (leader only) appends a command,
//!   * [`Raft::ready`] drains the side effects — messages to send, state to
//!     persist, and committed commands to apply.
//!
//! All I/O lives in the driver (`raft_driver`), which must honor one ordering
//! rule for safety: **persist what `ready()` hands back before sending its
//! messages or applying its commands.**

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::cluster::Command;

/// A brain member's id (its addressable `host:port`, like the node's `FIDUCIA_NODE_ID`).
pub type NodeId = String;

/// One replicated log entry. `index` is 1-based and contiguous. `command` is
/// `None` for the no-op a fresh leader appends to commit its term.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Option<Command>,
}

/// Raft state that must be durable (recovered from / saved to [`crate::raft_store`]).
#[derive(Debug, Default, Clone)]
pub struct Persisted {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub commit_index: u64,
    pub log: Vec<LogEntry>,
    /// Snapshot base: everything at or before `base_index` (term `base_term`) is
    /// compacted out of `log` and folded into `snapshot` (the serialized state
    /// machine at that point). `0` / `None` ⇒ nothing compacted yet (the log
    /// still starts at index 1, exactly as before compaction existed).
    pub base_index: u64,
    pub base_term: u64,
    pub snapshot: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    /// Running a non-binding PreVote straw poll before incrementing the term
    /// (Raft thesis §9.6) — stops a partitioned member from inflating its term
    /// and disrupting a healthy leader when it rejoins.
    PreCandidate,
    Candidate,
    Leader,
}

// ── RPC wire types (cross both the in-process harness and HTTP identically) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReq {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    /// PreVote round: the candidate's *would-be* term, not yet adopted.
    #[serde(default)]
    pub pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResp {
    pub term: u64,
    pub granted: bool,
    /// Echo of the request's `pre_vote`, so the candidate tallies it in the right round.
    #[serde(default)]
    pub pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReq {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResp {
    pub term: u64,
    pub success: bool,
    /// Follower's last log index afterward (sets `match_index`, or fast-rewinds
    /// `next_index` on failure).
    pub match_index: u64,
}

/// Sent by a leader to a follower that needs an entry the leader has already
/// compacted away: it carries the serialized state machine at `last_included_*`
/// so the follower can jump straight to that point instead of replaying the log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReq {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    /// The serialized state machine at `last_included_index` (opaque to Raft).
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResp {
    pub term: u64,
    pub success: bool,
    /// The index the follower now has durably (its new `match_index`).
    pub last_included_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVoteReq),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntriesReq),
    AppendEntriesResp(AppendEntriesResp),
    InstallSnapshot(InstallSnapshotReq),
    InstallSnapshotResp(InstallSnapshotResp),
}

/// An outbound message and its recipient (the sender is always `self.id`).
#[derive(Debug, Clone)]
pub struct Addressed {
    pub to: NodeId,
    pub msg: RaftMessage,
}

/// Timing, in driver ticks. The election timeout is randomized in
/// `[election_min_ticks, election_max_ticks]` so members don't campaign in lockstep.
#[derive(Debug, Clone, Copy)]
pub struct RaftConfig {
    pub heartbeat_ticks: u64,
    pub election_min_ticks: u64,
    pub election_max_ticks: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        // With a ~50ms driver tick: heartbeat 150ms, election 500–900ms — well
        // above 3× the heartbeat so a healthy leader isn't spuriously displaced.
        RaftConfig {
            heartbeat_ticks: 3,
            election_min_ticks: 10,
            election_max_ticks: 18,
        }
    }
}

/// The side effects of a `tick`/`step`/`propose`, drained by the driver.
#[derive(Debug, Default)]
pub struct Ready {
    /// If set, persist this **before** sending `messages` or applying `committed`.
    pub persist: Option<Persisted>,
    /// If set (after receiving an InstallSnapshot), the driver must **reset** its
    /// state machine to these snapshot bytes before applying `committed`.
    pub restore: Option<Vec<u8>>,
    /// Messages to send to peers (after persisting).
    pub messages: Vec<Addressed>,
    /// Newly-committed commands to apply to the state machine (after persisting).
    pub committed: Vec<Command>,
}

/// The fixed-membership single-group Raft state machine.
pub struct Raft {
    id: NodeId,
    peers: Vec<NodeId>, // other members (excludes self)
    cfg: RaftConfig,

    // Persistent (durable before acted upon).
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    // Snapshot base: the log's first entry is at index `base_index + 1`; entries at
    // or before `base_index` are compacted into `snapshot` (the serialized state
    // machine at `base_index`, term `base_term`). All zero / None ⇒ no compaction.
    base_index: u64,
    base_term: u64,
    snapshot: Option<Vec<u8>>,

    // Volatile.
    role: Role,
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<NodeId>,
    // Set when an InstallSnapshot replaced our state; `ready()` surfaces the bytes
    // once so the driver resets its state machine before applying newer entries.
    pending_restore: bool,

    // Candidate / pre-candidate vote tally (includes self).
    votes: HashSet<NodeId>,

    // Leader replication bookkeeping.
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    // Timers (ticks).
    election_elapsed: u64,
    heartbeat_elapsed: u64,
    randomized_election_timeout: u64,
    rng: u64,

    // Side-effect buffers (drained by `ready`).
    out: Vec<Addressed>,
    dirty: bool, // hard state / log / commit_index changed since last persist
}

impl Raft {
    /// Construct a member, seeding durable state from [`Persisted`] (empty for a
    /// fresh member). `seed` randomizes the election timeout so peers don't all
    /// time out together.
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        cfg: RaftConfig,
        seed: u64,
        restored: Persisted,
    ) -> Self {
        let mut raft = Raft {
            id,
            peers,
            cfg,
            current_term: restored.current_term,
            voted_for: restored.voted_for,
            log: restored.log,
            base_index: restored.base_index,
            base_term: restored.base_term,
            snapshot: restored.snapshot,
            role: Role::Follower,
            // The snapshot already represents the state machine up to base_index, so
            // commit/apply resume from there and compacted entries are never re-run.
            commit_index: restored.commit_index.max(restored.base_index),
            last_applied: restored.base_index,
            leader_id: None,
            pending_restore: false,
            votes: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout: 0,
            rng: seed | 1, // xorshift needs a non-zero state
            out: Vec::new(),
            dirty: false,
        };
        raft.reset_election_timer();
        raft
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> u64 {
        self.current_term
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn leader(&self) -> Option<&NodeId> {
        self.leader_id.as_ref()
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// The current durable state as a WAL snapshot — the same bytes [`Raft::ready`]
    /// returns in `persist`, but callable on demand. The driver re-reads the
    /// *latest* state through this under its IO lock so that two concurrent
    /// persists can never write an older snapshot over a newer one.
    pub fn persisted_snapshot(&self) -> Persisted {
        Persisted {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            commit_index: self.commit_index,
            log: self.log.clone(),
            base_index: self.base_index,
            base_term: self.base_term,
            snapshot: self.snapshot.clone(),
        }
    }

    fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(self.base_index)
    }
    fn last_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.base_term)
    }
    /// Slab position of 1-based log `index`, or `None` if it is compacted
    /// (≤ `base_index`) or beyond the end of the log.
    fn log_slot(&self, index: u64) -> Option<usize> {
        if index <= self.base_index || index > self.last_index() {
            None
        } else {
            Some((index - self.base_index - 1) as usize)
        }
    }
    /// Term of the entry at 1-based `index`: `base_term` at the snapshot boundary,
    /// the entry's term within the live log, else 0 (compacted or beyond the log).
    fn term_at(&self, index: u64) -> u64 {
        if index == self.base_index {
            self.base_term
        } else {
            self.log_slot(index).map(|i| self.log[i].term).unwrap_or(0)
        }
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }
    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        let span = self.cfg.election_max_ticks - self.cfg.election_min_ticks + 1;
        self.randomized_election_timeout = self.cfg.election_min_ticks + self.next_rand() % span;
    }

    fn send(&mut self, to: &NodeId, msg: RaftMessage) {
        self.out.push(Addressed {
            to: to.clone(),
            msg,
        });
    }

    // ── time ──────────────────────────────────────────────────────────────

    /// Advance one logical tick: leaders heartbeat; everyone else counts down to
    /// an election.
    pub fn tick(&mut self) {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
            return;
        }
        self.election_elapsed += 1;
        if self.election_elapsed >= self.randomized_election_timeout {
            self.start_pre_election();
        }
    }

    // ── elections ─────────────────────────────────────────────────────────

    /// Begin a PreVote round (no term bump, no state change a peer can see).
    fn start_pre_election(&mut self) {
        self.reset_election_timer();
        self.role = Role::PreCandidate;
        self.votes.clear();
        self.votes.insert(self.id.clone());
        let req = RequestVoteReq {
            term: self.current_term + 1, // would-be term
            candidate_id: self.id.clone(),
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
            pre_vote: true,
        };
        let peers = self.peers.clone();
        for p in &peers {
            self.send(p, RaftMessage::RequestVote(req.clone()));
        }
        self.maybe_win_pre_election();
    }

    fn maybe_win_pre_election(&mut self) {
        if self.role == Role::PreCandidate && self.votes.len() >= self.quorum() {
            self.start_election();
        }
    }

    /// Begin a real election: bump the term, vote for self, solicit votes.
    fn start_election(&mut self) {
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        self.dirty = true;
        self.role = Role::Candidate;
        self.leader_id = None;
        self.reset_election_timer();
        self.votes.clear();
        self.votes.insert(self.id.clone());
        let req = RequestVoteReq {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
            pre_vote: false,
        };
        let peers = self.peers.clone();
        for p in &peers {
            self.send(p, RaftMessage::RequestVote(req.clone()));
        }
        self.maybe_win_election();
    }

    fn maybe_win_election(&mut self) {
        if self.role == Role::Candidate && self.votes.len() >= self.quorum() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        let next = self.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for p in self.peers.clone() {
            self.next_index.insert(p.clone(), next);
            self.match_index.insert(p, 0);
        }
        // No-op entry in the new term: lets the leader commit (and thereby commit
        // entries inherited from prior terms) without waiting for a client write.
        let index = self.last_index() + 1;
        self.log.push(LogEntry {
            term: self.current_term,
            index,
            command: None,
        });
        self.dirty = true;
        self.heartbeat_elapsed = 0;
        self.broadcast_append();
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.dirty = true;
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes.clear();
        self.reset_election_timer();
    }

    // ── inbound ─────────────────────────────────────────────────────────────

    /// Handle one inbound message from `from`.
    pub fn step(&mut self, from: NodeId, msg: RaftMessage) {
        match msg {
            RaftMessage::RequestVote(req) => self.handle_request_vote(from, req),
            RaftMessage::RequestVoteResp(resp) => self.handle_vote_resp(from, resp),
            RaftMessage::AppendEntries(req) => self.handle_append(from, req),
            RaftMessage::AppendEntriesResp(resp) => self.handle_append_resp(from, resp),
            RaftMessage::InstallSnapshot(req) => self.handle_install_snapshot(from, req),
            RaftMessage::InstallSnapshotResp(resp) => self.handle_install_snapshot_resp(from, resp),
        }
    }

    fn handle_request_vote(&mut self, from: NodeId, req: RequestVoteReq) {
        // A real (non-pre) vote at a higher term forces us to adopt it and step down.
        if !req.pre_vote && req.term > self.current_term {
            self.become_follower(req.term, None);
        }

        let log_ok = req.last_log_term > self.last_term()
            || (req.last_log_term == self.last_term() && req.last_log_index >= self.last_index());

        let granted = if req.term < self.current_term {
            false
        } else if req.pre_vote {
            // Grant a (state-free) pre-vote only if the candidate's log is
            // up-to-date AND we don't currently trust a leader — otherwise a
            // single flapping member could keep nudging a healthy leader out.
            let no_trusted_leader =
                self.leader_id.is_none() || self.election_elapsed >= self.cfg.election_min_ticks;
            log_ok && no_trusted_leader
        } else {
            let free = self.voted_for.is_none() || self.voted_for.as_deref() == Some(&from);
            if free && log_ok && req.term == self.current_term {
                self.voted_for = Some(from.clone());
                self.dirty = true;
                self.reset_election_timer();
                true
            } else {
                false
            }
        };

        let resp = RequestVoteResp {
            term: self.current_term,
            granted,
            pre_vote: req.pre_vote,
        };
        self.send(&from, RaftMessage::RequestVoteResp(resp));
    }

    fn handle_vote_resp(&mut self, from: NodeId, resp: RequestVoteResp) {
        // A higher term anywhere means we're stale — step down. (For pre-votes the
        // voter reports its own term without adopting ours, so this still holds.)
        if resp.term > self.current_term {
            self.become_follower(resp.term, None);
            return;
        }
        if !resp.granted {
            return;
        }
        if resp.pre_vote {
            if self.role == Role::PreCandidate {
                self.votes.insert(from);
                self.maybe_win_pre_election();
            }
        } else if self.role == Role::Candidate && resp.term == self.current_term {
            self.votes.insert(from);
            self.maybe_win_election();
        }
    }

    fn handle_append(&mut self, from: NodeId, req: AppendEntriesReq) {
        if req.term < self.current_term {
            // Stale leader: reject so it discovers the newer term.
            let resp = AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
            self.send(&from, RaftMessage::AppendEntriesResp(resp));
            return;
        }

        // Valid leader for our term or newer: adopt term if higher, recognize it,
        // and refresh the election timer (we've heard from a leader).
        self.become_follower(req.term, Some(from.clone()));

        // Log-consistency check at prev_log_index.
        let success;
        let match_index;
        if req.prev_log_index > self.last_index() {
            // We're missing entries before these — ask the leader to back up.
            success = false;
            match_index = self.last_index();
        } else if req.prev_log_index > 0 && self.term_at(req.prev_log_index) != req.prev_log_term {
            // Conflict at prev: drop it and everything after, then back up.
            self.truncate_from(req.prev_log_index);
            success = false;
            match_index = req.prev_log_index - 1;
        } else {
            // Prefix matches: splice in the entries, truncating any conflicts.
            for entry in req.entries {
                if entry.index <= self.last_index() {
                    if self.term_at(entry.index) != entry.term {
                        self.truncate_from(entry.index);
                        self.log.push(entry);
                        self.dirty = true;
                    }
                    // else: already have this exact entry — skip.
                } else {
                    self.log.push(entry);
                    self.dirty = true;
                }
            }
            if req.leader_commit > self.commit_index {
                self.commit_index = req.leader_commit.min(self.last_index());
                self.dirty = true;
            }
            success = true;
            match_index = self.last_index();
        }

        let resp = AppendEntriesResp {
            term: self.current_term,
            success,
            match_index,
        };
        self.send(&from, RaftMessage::AppendEntriesResp(resp));
    }

    fn handle_append_resp(&mut self, from: NodeId, resp: AppendEntriesResp) {
        if resp.term > self.current_term {
            self.become_follower(resp.term, None);
            return;
        }
        if self.role != Role::Leader || resp.term != self.current_term {
            return;
        }
        if resp.success {
            self.match_index.insert(from.clone(), resp.match_index);
            self.next_index.insert(from, resp.match_index + 1);
            if self.maybe_advance_commit() {
                // Tell followers the commit point moved so they apply promptly,
                // instead of waiting for the next heartbeat.
                self.broadcast_append();
            }
        } else {
            // Fast-rewind next_index toward the follower's hint and retry.
            let backed = resp.match_index + 1;
            let entry = self.next_index.entry(from.clone()).or_insert(1);
            *entry = backed.max(1).min(*entry);
            self.send_append(&from);
        }
    }

    /// A follower receiving the leader's snapshot: jump the state machine straight
    /// to `last_included_index` instead of replaying log entries the leader no
    /// longer has. Keeps any log suffix consistent with the snapshot boundary.
    fn handle_install_snapshot(&mut self, from: NodeId, req: InstallSnapshotReq) {
        if req.term < self.current_term {
            let resp = InstallSnapshotResp {
                term: self.current_term,
                success: false,
                last_included_index: 0,
            };
            self.send(&from, RaftMessage::InstallSnapshotResp(resp));
            return;
        }
        // Valid leader: adopt its term and recognize it (refreshes the election timer).
        self.become_follower(req.term, Some(from.clone()));

        // Already at or past this snapshot ⇒ ack without applying (it's stale to us).
        if req.last_included_index <= self.base_index
            || req.last_included_index <= self.commit_index
        {
            let resp = InstallSnapshotResp {
                term: self.current_term,
                success: true,
                last_included_index: self.commit_index.max(self.base_index),
            };
            self.send(&from, RaftMessage::InstallSnapshotResp(resp));
            return;
        }

        // Install it. Keep any log suffix consistent with the snapshot boundary
        // (same term at last_included_index); otherwise discard the whole log.
        if self.term_at(req.last_included_index) == req.last_included_term
            && req.last_included_index <= self.last_index()
        {
            let keep_from = (req.last_included_index - self.base_index) as usize;
            self.log = self.log.split_off(keep_from);
        } else {
            self.log.clear();
        }
        self.base_index = req.last_included_index;
        self.base_term = req.last_included_term;
        self.snapshot = Some(req.data);
        self.commit_index = self.commit_index.max(self.base_index);
        self.last_applied = self.base_index;
        self.pending_restore = true;
        self.dirty = true;

        let resp = InstallSnapshotResp {
            term: self.current_term,
            success: true,
            last_included_index: self.base_index,
        };
        self.send(&from, RaftMessage::InstallSnapshotResp(resp));
    }

    fn handle_install_snapshot_resp(&mut self, from: NodeId, resp: InstallSnapshotResp) {
        if resp.term > self.current_term {
            self.become_follower(resp.term, None);
            return;
        }
        if self.role != Role::Leader || resp.term != self.current_term {
            return;
        }
        if resp.success {
            let matched = resp
                .last_included_index
                .max(self.match_index.get(&from).copied().unwrap_or(0));
            self.match_index.insert(from.clone(), matched);
            self.next_index.insert(from, matched + 1);
            if self.maybe_advance_commit() {
                self.broadcast_append();
            }
        }
    }

    /// Drop entries with index ≥ `index`. Never touches committed entries (Raft's
    /// rules guarantee a conflict can only appear above `commit_index`).
    fn truncate_from(&mut self, index: u64) {
        debug_assert!(index > self.commit_index, "would truncate a committed entry");
        if let Some(slot) = self.log_slot(index) {
            self.log.truncate(slot);
            self.dirty = true;
        }
    }

    fn broadcast_append(&mut self) {
        for p in self.peers.clone() {
            self.send_append(&p);
        }
    }

    fn send_append(&mut self, peer: &NodeId) {
        let next = self.next_index.get(peer).copied().unwrap_or(self.last_index() + 1);
        // The follower needs an entry we have already compacted away → ship the
        // snapshot instead of log entries it can no longer receive.
        if next <= self.base_index {
            self.send_snapshot(peer);
            return;
        }
        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index);
        let entries = match self.log_slot(next) {
            Some(slot) => self.log[slot..].to_vec(),
            None => Vec::new(),
        };
        let req = AppendEntriesReq {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };
        self.send(peer, RaftMessage::AppendEntries(req));
    }

    /// Ship the current snapshot to a follower that has fallen behind the log base.
    fn send_snapshot(&mut self, peer: &NodeId) {
        let req = InstallSnapshotReq {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: self.base_index,
            last_included_term: self.base_term,
            data: self.snapshot.clone().unwrap_or_default(),
        };
        self.send(peer, RaftMessage::InstallSnapshot(req));
    }

    fn maybe_advance_commit(&mut self) -> bool {
        let last = self.last_index();
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1)..=last {
            // Raft only commits an entry from the *current* term by counting
            // replicas; earlier-term entries commit transitively beneath it.
            if self.term_at(n) != self.current_term {
                continue;
            }
            let mut count = 1; // self
            for peer in &self.peers {
                if self.match_index.get(peer).copied().unwrap_or(0) >= n {
                    count += 1;
                }
            }
            if count >= self.quorum() {
                new_commit = n;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.dirty = true;
            true
        } else {
            false
        }
    }

    // ── proposals ────────────────────────────────────────────────────────────

    /// Append a command to the log (leader only). Returns its index, or `Err` with
    /// the current leader (if known) so the caller can forward.
    pub fn propose(&mut self, command: Command) -> Result<u64, Option<NodeId>> {
        if self.role != Role::Leader {
            return Err(self.leader_id.clone());
        }
        let index = self.last_index() + 1;
        self.log.push(LogEntry {
            term: self.current_term,
            index,
            command: Some(command),
        });
        self.dirty = true;
        // Single-member group: self is the quorum, so it commits immediately.
        self.maybe_advance_commit();
        self.broadcast_append();
        Ok(index)
    }

    // ── outbound ─────────────────────────────────────────────────────────────

    /// Drain side effects. The driver must persist `ready.persist` (if any)
    /// **before** sending `ready.messages` or applying `ready.committed`.
    pub fn ready(&mut self) -> Ready {
        let messages = std::mem::take(&mut self.out);
        let persist = if self.dirty {
            self.dirty = false;
            Some(Persisted {
                current_term: self.current_term,
                voted_for: self.voted_for.clone(),
                commit_index: self.commit_index,
                log: self.log.clone(),
            })
        } else {
            None
        };
        let mut committed = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(cmd) = &self.log[(self.last_applied - 1) as usize].command {
                committed.push(cmd.clone());
            }
        }
        Ready {
            persist,
            messages,
            committed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ScalePlan;

    fn cfg() -> RaftConfig {
        RaftConfig {
            heartbeat_ticks: 1,
            election_min_ticks: 5,
            election_max_ticks: 10,
        }
    }

    fn plan(n: u32) -> Command {
        Command::SetScalePlan(ScalePlan {
            target_nodes: n,
            replication_factor: 3,
        })
    }

    /// A deterministic in-process cluster: N `Raft`s, per-member in-memory durable
    /// state, an applied-command log per member, and a set of partitioned members.
    /// Honors the persist-before-send rule.
    struct Cluster {
        nodes: HashMap<NodeId, Raft>,
        durable: HashMap<NodeId, Persisted>,
        applied: HashMap<NodeId, Vec<Command>>,
        down: HashSet<NodeId>,
    }

    impl Cluster {
        fn new(n: usize) -> Self {
            let ids: Vec<NodeId> = (0..n).map(|i| format!("brain-{i}")).collect();
            let mut nodes = HashMap::new();
            let mut applied = HashMap::new();
            for (i, id) in ids.iter().enumerate() {
                let peers = ids.iter().filter(|p| *p != id).cloned().collect();
                nodes.insert(
                    id.clone(),
                    Raft::new(id.clone(), peers, cfg(), (i as u64) + 1, Persisted::default()),
                );
                applied.insert(id.clone(), Vec::new());
            }
            Cluster {
                nodes,
                durable: HashMap::new(),
                applied,
                down: HashSet::new(),
            }
        }

        fn ids(&self) -> Vec<NodeId> {
            let mut v: Vec<_> = self.nodes.keys().cloned().collect();
            v.sort();
            v
        }

        /// Drain every reachable node's `ready()`, persist, apply, and deliver
        /// messages, until the cluster is quiescent.
        fn pump(&mut self) {
            for _ in 0..1000 {
                let mut queue: Vec<(NodeId, Addressed)> = Vec::new();
                for id in self.ids() {
                    if self.down.contains(&id) {
                        continue;
                    }
                    let ready = self.nodes.get_mut(&id).unwrap().ready();
                    if let Some(p) = ready.persist {
                        self.durable.insert(id.clone(), p); // persist BEFORE send/apply
                    }
                    self.applied.get_mut(&id).unwrap().extend(ready.committed);
                    for m in ready.messages {
                        queue.push((id.clone(), m));
                    }
                }
                if queue.is_empty() {
                    return;
                }
                for (from, addressed) in queue {
                    if self.down.contains(&from) || self.down.contains(&addressed.to) {
                        continue; // partitioned link drops the message
                    }
                    if let Some(target) = self.nodes.get_mut(&addressed.to) {
                        target.step(from.clone(), addressed.msg);
                    }
                }
            }
            panic!("cluster did not settle");
        }

        fn tick_all(&mut self) {
            for id in self.ids() {
                if !self.down.contains(&id) {
                    self.nodes.get_mut(&id).unwrap().tick();
                }
            }
        }

        /// Tick until some reachable member becomes leader (bounded), settling
        /// message exchange after each tick.
        fn elect(&mut self) -> NodeId {
            for _ in 0..200 {
                self.tick_all();
                self.pump();
                if let Some(l) = self.leader() {
                    return l;
                }
            }
            panic!("no leader elected");
        }

        fn leaders(&self) -> Vec<NodeId> {
            let mut v: Vec<_> = self
                .nodes
                .values()
                .filter(|r| r.is_leader() && !self.down.contains(r.id()))
                .map(|r| r.id().clone())
                .collect();
            v.sort();
            v
        }
        fn leader(&self) -> Option<NodeId> {
            self.leaders().into_iter().next()
        }
        fn node(&mut self, id: &str) -> &mut Raft {
            self.nodes.get_mut(id).unwrap()
        }
    }

    #[test]
    fn three_members_elect_exactly_one_leader() {
        let mut c = Cluster::new(3);
        c.elect();
        assert_eq!(c.leaders().len(), 1, "exactly one leader");
    }

    #[test]
    fn leader_replicates_and_commits_a_command_to_all_members() {
        let mut c = Cluster::new(3);
        let leader = c.elect();
        let idx = c.node(&leader).propose(plan(7)).unwrap();
        c.pump();

        // Committed and applied on every member.
        for id in c.ids() {
            assert!(
                c.applied[&id].iter().any(|cmd| matches!(
                    cmd,
                    Command::SetScalePlan(p) if p.target_nodes == 7
                )),
                "{id} applied the command"
            );
            assert!(c.durable[&id].commit_index >= idx, "{id} persisted commit");
        }
    }

    #[test]
    fn a_minority_partition_cannot_commit() {
        let mut c = Cluster::new(3);
        let leader = c.elect();

        // Isolate two of the three members: the (former) leader, if still up, is
        // now in a minority and cannot commit.
        let others: Vec<NodeId> = c.ids().into_iter().filter(|i| *i != leader).collect();
        c.down.insert(others[0].clone());
        c.down.insert(others[1].clone());

        let before = c.node(&leader).commit_index();
        let _ = c.node(&leader).propose(plan(99));
        c.pump();
        assert_eq!(
            c.node(&leader).commit_index(),
            before,
            "a lone member cannot advance commit"
        );
    }

    #[test]
    fn losing_the_leader_elects_a_new_one_from_the_majority() {
        let mut c = Cluster::new(3);
        let leader = c.elect();
        c.node(&leader).propose(plan(1)).unwrap();
        c.pump();

        // Leader dies; the remaining two are a majority and must elect a new leader.
        c.down.insert(leader.clone());
        let new_leader = c.elect();
        assert_ne!(new_leader, leader);
        assert_eq!(c.leaders().len(), 1);

        // New leader can still commit (its log carried the prior entry forward).
        c.node(&new_leader).propose(plan(2)).unwrap();
        c.pump();
        let up: Vec<NodeId> = c.ids().into_iter().filter(|i| *i != leader).collect();
        for id in up {
            assert!(c.applied[&id]
                .iter()
                .any(|cmd| matches!(cmd, Command::SetScalePlan(p) if p.target_nodes == 2)));
        }
    }

    #[test]
    fn restart_recovers_committed_state_from_persisted_log() {
        let mut c = Cluster::new(3);
        let leader = c.elect();
        c.node(&leader).propose(plan(42)).unwrap();
        c.pump();

        // Restart a follower from its persisted state (fresh volatile state, empty
        // state machine) and confirm it replays the committed command.
        let follower = c.ids().into_iter().find(|i| *i != leader).unwrap();
        let restored = c.durable[&follower].clone();
        assert!(restored.commit_index >= 1, "commit was persisted");
        let peers = c.ids().into_iter().filter(|i| *i != follower).collect();
        let mut revived = Raft::new(follower.clone(), peers, cfg(), 7, restored);
        let ready = revived.ready();
        assert!(
            ready
                .committed
                .iter()
                .any(|cmd| matches!(cmd, Command::SetScalePlan(p) if p.target_nodes == 42)),
            "a restarted member replays its committed log into a fresh state machine"
        );
    }

    #[test]
    fn single_member_group_self_commits() {
        let mut c = Cluster::new(1);
        let leader = c.elect();
        assert_eq!(leader, "brain-0");
        let idx = c.node(&leader).propose(plan(5)).unwrap();
        c.pump();
        assert!(c.node(&leader).commit_index() >= idx);
        assert!(c.applied[&leader]
            .iter()
            .any(|cmd| matches!(cmd, Command::SetScalePlan(p) if p.target_nodes == 5)));
    }
}
