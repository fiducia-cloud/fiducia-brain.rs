#![allow(dead_code)]

// Compile the production membership implementation into this integration-test
// crate. The reference transition system below is intentionally independent.
#[path = "../src/membership.rs"]
mod membership;
#[path = "../src/model.rs"]
mod model;
#[path = "../src/oracle.rs"]
mod oracle;

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use membership::{Membership, MembershipConfig};
use model::{HeartbeatReport, NodeHealth, NodeId, ShardId};
use oracle::{LivenessOracle, PodLiveness};

const NODE: &str = "node-a";
const SUSPECT_AFTER_MS: u64 = 10;
const DEAD_AFTER_MS: u64 = 20;
const MAX_DEPTH: usize = 6;
const MAX_STATES: usize = 10_000;
const MAX_TRANSITIONS: usize = 200_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Health {
    Healthy,
    Suspect,
    Dead,
    Draining,
}

impl From<NodeHealth> for Health {
    fn from(value: NodeHealth) -> Self {
        match value {
            NodeHealth::Healthy => Self::Healthy,
            NodeHealth::Suspect => Self::Suspect,
            NodeHealth::Dead => Self::Dead,
            NodeHealth::Draining => Self::Draining,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Verdict {
    Unknown,
    Running,
    Gone,
}

impl From<Verdict> for PodLiveness {
    fn from(value: Verdict) -> Self {
        match value {
            Verdict::Unknown => Self::Unknown,
            Verdict::Running => Self::Running,
            Verdict::Gone => Self::Gone,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Reference {
    now_ms: u64,
    last_seen_ms: u64,
    health: Health,
    oracle: Verdict,
    last_seq: u64,
    report_version: u8,
    ever_draining: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    Heartbeat { seq: u64, report_version: u8 },
    Sweep,
    AdvanceToSuspect,
    AdvanceToDead,
    OracleAndSweep(Verdict),
    Drain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Heartbeat(Health),
    Sweep { newly_dead: bool },
    Drain { known: bool },
}

impl Reference {
    fn initial() -> Self {
        Self {
            now_ms: 0,
            last_seen_ms: 0,
            health: Health::Healthy,
            oracle: Verdict::Unknown,
            last_seq: 0,
            report_version: 0,
            ever_draining: false,
        }
    }

    fn actions(&self) -> Vec<Action> {
        let mut actions = Vec::new();
        for seq in 0..=3 {
            for report_version in 0..=2 {
                actions.push(Action::Heartbeat {
                    seq,
                    report_version,
                });
            }
        }
        actions.push(Action::Sweep);
        if self.now_ms < self.last_seen_ms.saturating_add(SUSPECT_AFTER_MS) {
            actions.push(Action::AdvanceToSuspect);
        }
        if self.now_ms < self.last_seen_ms.saturating_add(DEAD_AFTER_MS) {
            actions.push(Action::AdvanceToDead);
        }
        actions.extend([
            Action::OracleAndSweep(Verdict::Unknown),
            Action::OracleAndSweep(Verdict::Running),
            Action::OracleAndSweep(Verdict::Gone),
            Action::Drain,
        ]);
        actions
    }

    fn apply(&mut self, action: Action) -> Outcome {
        let outcome = match action {
            Action::Heartbeat {
                seq,
                report_version,
            } => self.heartbeat(seq, report_version),
            Action::Sweep => self.sweep(),
            Action::AdvanceToSuspect => {
                self.now_ms = self
                    .now_ms
                    .max(self.last_seen_ms.saturating_add(SUSPECT_AFTER_MS));
                self.sweep()
            }
            Action::AdvanceToDead => {
                self.now_ms = self
                    .now_ms
                    .max(self.last_seen_ms.saturating_add(DEAD_AFTER_MS));
                self.sweep()
            }
            Action::OracleAndSweep(verdict) => {
                self.oracle = verdict;
                self.sweep()
            }
            Action::Drain => {
                self.health = Health::Draining;
                self.ever_draining = true;
                Outcome::Drain { known: true }
            }
        };
        self.assert_invariants();
        outcome
    }

    fn heartbeat(&mut self, seq: u64, report_version: u8) -> Outcome {
        if seq != 0 && seq <= self.last_seq {
            return Outcome::Heartbeat(self.health);
        }
        self.last_seen_ms = self.now_ms;
        self.last_seq = self.last_seq.max(seq);
        self.report_version = report_version;
        if self.health != Health::Draining {
            self.health = Health::Healthy;
        }
        Outcome::Heartbeat(self.health)
    }

    fn sweep(&mut self) -> Outcome {
        let old = self.health;
        self.health = if old == Health::Draining {
            Health::Draining
        } else {
            let silent_for = self.now_ms.saturating_sub(self.last_seen_ms);
            let by_timeout = if silent_for >= DEAD_AFTER_MS {
                Health::Dead
            } else if silent_for >= SUSPECT_AFTER_MS {
                Health::Suspect
            } else {
                Health::Healthy
            };
            match self.oracle {
                Verdict::Gone => Health::Dead,
                Verdict::Running if by_timeout == Health::Dead => Health::Suspect,
                Verdict::Running | Verdict::Unknown => by_timeout,
            }
        };
        Outcome::Sweep {
            newly_dead: self.health == Health::Dead && old != Health::Dead,
        }
    }

    fn assert_invariants(&self) {
        assert!(
            self.report_version <= 2,
            "report version left finite domain"
        );
        assert!(self.last_seq <= 3, "sequence left finite domain");
        assert!(
            !self.ever_draining || self.health == Health::Draining,
            "draining intent was undone"
        );
        assert!(
            self.last_seen_ms <= self.now_ms,
            "last heartbeat is in the future"
        );
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Projection {
    health: Health,
    last_seen_ms: u64,
    last_seq: u64,
    address: String,
    failure_domain: String,
    hosted_shards: Vec<ShardId>,
    leading_shards: Vec<ShardId>,
}

impl Projection {
    fn from_reference(reference: &Reference) -> Self {
        let report = report(reference.last_seq, reference.report_version);
        Self {
            health: reference.health,
            last_seen_ms: reference.last_seen_ms,
            last_seq: reference.last_seq,
            address: report.address,
            failure_domain: report.failure_domain,
            hosted_shards: report.hosted_shards,
            leading_shards: report.leading_shards,
        }
    }

    fn from_membership(membership: &Membership) -> Self {
        let snapshot = membership.snapshot();
        assert_eq!(snapshot.len(), 1, "formal harness tracks one node");
        let node = &snapshot[0];
        assert_eq!(node.node_id, NODE);
        Self {
            health: node.health.into(),
            last_seen_ms: node.last_seen_ms,
            last_seq: node.last_seq,
            address: node.address.clone(),
            failure_domain: node.failure_domain.clone(),
            hosted_shards: node.hosted_shards.clone(),
            leading_shards: node.leading_shards.clone(),
        }
    }
}

struct MutableOracle {
    verdict: Mutex<PodLiveness>,
}

impl MutableOracle {
    fn new() -> Self {
        Self {
            verdict: Mutex::new(PodLiveness::Unknown),
        }
    }

    fn set(&self, verdict: Verdict) {
        *self.verdict.lock().expect("oracle mutex") = verdict.into();
    }
}

impl LivenessOracle for MutableOracle {
    fn liveness(&self, _node_id: &str) -> PodLiveness {
        *self.verdict.lock().expect("oracle mutex")
    }
}

#[derive(Default, Debug)]
struct Coverage {
    accepted_sequenced: bool,
    stale_ignored: bool,
    legacy_accepted_above_floor: bool,
    suspect_timeout: bool,
    timeout_dead_once: bool,
    dead_not_repeated: bool,
    running_holds_at_suspect: bool,
    gone_is_immediate: bool,
    draining_heartbeat_is_sticky: bool,
    draining_sweep_is_sticky: bool,
    resurrection: bool,
    stale_preserves_report: bool,
}

impl Coverage {
    fn observe(&mut self, parent: &Reference, action: Action, outcome: Outcome, child: &Reference) {
        match (action, outcome) {
            (
                Action::Heartbeat {
                    seq,
                    report_version,
                },
                Outcome::Heartbeat(health),
            ) => {
                let stale = seq != 0 && seq <= parent.last_seq;
                if seq > parent.last_seq && seq != 0 && child.last_seq == seq {
                    self.accepted_sequenced = true;
                }
                if stale && parent == child {
                    self.stale_ignored = true;
                    if child.report_version == parent.report_version {
                        self.stale_preserves_report = true;
                    }
                }
                if seq == 0
                    && parent.last_seq > 0
                    && child.last_seq == parent.last_seq
                    && child.report_version == report_version
                {
                    self.legacy_accepted_above_floor = true;
                }
                if parent.health == Health::Draining
                    && health == Health::Draining
                    && child.health == Health::Draining
                    && child.last_seen_ms == child.now_ms
                {
                    self.draining_heartbeat_is_sticky = true;
                }
                if parent.health == Health::Dead && child.health == Health::Healthy && !stale {
                    self.resurrection = true;
                }
            }
            (Action::Sweep | Action::AdvanceToSuspect, Outcome::Sweep { newly_dead }) => {
                if child.health == Health::Suspect && !newly_dead {
                    self.suspect_timeout = true;
                }
                if parent.health == Health::Dead && child.health == Health::Dead && !newly_dead {
                    self.dead_not_repeated = true;
                }
                if parent.health == Health::Draining && child.health == Health::Draining {
                    self.draining_sweep_is_sticky = true;
                }
            }
            (Action::AdvanceToDead, Outcome::Sweep { newly_dead }) => {
                if parent.oracle == Verdict::Unknown && child.health == Health::Dead && newly_dead {
                    self.timeout_dead_once = true;
                }
                if parent.oracle == Verdict::Running
                    && child.health == Health::Suspect
                    && !newly_dead
                {
                    self.running_holds_at_suspect = true;
                }
            }
            (Action::OracleAndSweep(Verdict::Running), Outcome::Sweep { newly_dead })
                if child.now_ms.saturating_sub(child.last_seen_ms) >= DEAD_AFTER_MS
                    && child.health == Health::Suspect
                    && !newly_dead =>
            {
                self.running_holds_at_suspect = true;
            }
            (Action::OracleAndSweep(Verdict::Gone), Outcome::Sweep { newly_dead })
                if child.health == Health::Dead && newly_dead =>
            {
                self.gone_is_immediate = true;
            }
            _ => {}
        }
    }

    fn assert_complete(&self) {
        assert!(self.accepted_sequenced, "coverage: accepted sequence");
        assert!(self.stale_ignored, "coverage: stale heartbeat ignored");
        assert!(
            self.legacy_accepted_above_floor,
            "coverage: legacy seq=0 compatibility"
        );
        assert!(self.suspect_timeout, "coverage: suspect timeout band");
        assert!(self.timeout_dead_once, "coverage: timeout death");
        assert!(self.dead_not_repeated, "coverage: dead reported once");
        assert!(
            self.running_holds_at_suspect,
            "coverage: running oracle damps a partition"
        );
        assert!(self.gone_is_immediate, "coverage: gone oracle is immediate");
        assert!(
            self.draining_heartbeat_is_sticky,
            "coverage: draining heartbeat"
        );
        assert!(self.draining_sweep_is_sticky, "coverage: draining sweep");
        assert!(self.resurrection, "coverage: fresh heartbeat resurrection");
        assert!(
            self.stale_preserves_report,
            "coverage: stale report preservation"
        );
    }
}

struct Frontier {
    reference: Reference,
    trace: Vec<Action>,
}

#[test]
fn bounded_membership_refinement_matches_rust_implementation() {
    let initial = Reference::initial();
    let mut seen = HashSet::new();
    seen.insert(initial.clone());
    let mut frontier = VecDeque::new();
    frontier.push_back(Frontier {
        reference: initial,
        trace: Vec::new(),
    });

    let mut transitions = 0usize;
    let mut coverage = Coverage::default();

    while let Some(node) = frontier.pop_front() {
        if node.trace.len() >= MAX_DEPTH {
            continue;
        }
        for action in node.reference.actions() {
            transitions += 1;
            assert!(
                transitions <= MAX_TRANSITIONS,
                "membership refinement exceeded {MAX_TRANSITIONS} transitions; states={} trace={:#?}",
                seen.len(),
                node.trace
            );

            let (membership, oracle, replayed) = replay(&node.trace);
            assert_eq!(replayed, node.reference, "reference replay diverged");

            let actual = execute(&membership, &oracle, &replayed, action);
            let mut expected = replayed.clone();
            let expected_outcome = expected.apply(action);
            let mut trace = node.trace.clone();
            trace.push(action);

            assert_eq!(
                actual, expected_outcome,
                "membership output diverged after trace:\n{trace:#?}"
            );
            assert_eq!(
                Projection::from_membership(&membership),
                Projection::from_reference(&expected),
                "membership state diverged after trace:\n{trace:#?}"
            );
            coverage.observe(&replayed, action, actual, &expected);

            if seen.insert(expected.clone()) {
                assert!(
                    seen.len() <= MAX_STATES,
                    "membership refinement exceeded {MAX_STATES} states after trace: {trace:#?}"
                );
                frontier.push_back(Frontier {
                    reference: expected,
                    trace,
                });
            }
        }
    }

    coverage.assert_complete();
    eprintln!(
        "bounded membership refinement explored {} states and {} transitions through depth {}",
        seen.len(),
        transitions,
        MAX_DEPTH
    );
}

fn replay(trace: &[Action]) -> (Membership, Arc<MutableOracle>, Reference) {
    let oracle = Arc::new(MutableOracle::new());
    let membership = Membership::with_oracle(config(), oracle.clone());
    let node_id = node_id();
    let health = membership.heartbeat(&node_id, 0, report(0, 0));
    assert_eq!(Health::from(health), Health::Healthy);

    let mut reference = Reference::initial();
    assert_eq!(
        Projection::from_membership(&membership),
        Projection::from_reference(&reference)
    );

    for (index, action) in trace.iter().copied().enumerate() {
        let actual = execute(&membership, &oracle, &reference, action);
        let expected = reference.apply(action);
        assert_eq!(
            actual, expected,
            "replay output diverged at index {index}: {action:?}"
        );
        assert_eq!(
            Projection::from_membership(&membership),
            Projection::from_reference(&reference),
            "replay state diverged at index {index}: {action:?}"
        );
    }
    (membership, oracle, reference)
}

fn execute(
    membership: &Membership,
    oracle: &MutableOracle,
    reference: &Reference,
    action: Action,
) -> Outcome {
    let node_id = node_id();
    match action {
        Action::Heartbeat {
            seq,
            report_version,
        } => Outcome::Heartbeat(
            membership
                .heartbeat(&node_id, reference.now_ms, report(seq, report_version))
                .into(),
        ),
        Action::Sweep => sweep_outcome(membership, reference.now_ms),
        Action::AdvanceToSuspect => sweep_outcome(
            membership,
            reference
                .now_ms
                .max(reference.last_seen_ms.saturating_add(SUSPECT_AFTER_MS)),
        ),
        Action::AdvanceToDead => sweep_outcome(
            membership,
            reference
                .now_ms
                .max(reference.last_seen_ms.saturating_add(DEAD_AFTER_MS)),
        ),
        Action::OracleAndSweep(verdict) => {
            oracle.set(verdict);
            sweep_outcome(membership, reference.now_ms)
        }
        Action::Drain => Outcome::Drain {
            known: membership.drain(&node_id),
        },
    }
}

fn sweep_outcome(membership: &Membership, now_ms: u64) -> Outcome {
    let newly_dead = membership.sweep(now_ms);
    assert!(
        newly_dead.is_empty() || newly_dead == vec![node_id()],
        "one-node detector returned unexpected deaths: {newly_dead:?}"
    );
    Outcome::Sweep {
        newly_dead: !newly_dead.is_empty(),
    }
}

fn config() -> MembershipConfig {
    MembershipConfig {
        suspect_after_ms: SUSPECT_AFTER_MS,
        dead_after_ms: DEAD_AFTER_MS,
    }
}

fn node_id() -> NodeId {
    NODE.to_string()
}

fn report(seq: u64, report_version: u8) -> HeartbeatReport {
    let shard = ShardId::from(report_version as u32);
    HeartbeatReport {
        address: format!("10.0.0.{report_version}:8090"),
        cloud_provider: "test-cloud".to_string(),
        region: "test-region".to_string(),
        cluster_id: "test-cluster".to_string(),
        failure_domain: format!("fd-{report_version}"),
        hosted_shards: vec![shard],
        leading_shards: vec![shard],
        seq,
    }
}
