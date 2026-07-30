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
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use membership::{Membership, MembershipConfig};
use model::{HeartbeatReport, NodeHealth, NodeId, ShardId};
use oracle::{LivenessOracle, PodLiveness};
use serde_json::Value;

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

#[test]
fn generated_itf_traces_replay_against_membership() {
    let required = std::env::var("FIDUCIA_REQUIRE_MEMBERSHIP_ITF_REPLAY")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let Some(directory) = std::env::var_os("FIDUCIA_MEMBERSHIP_ITF_TRACE_DIR") else {
        assert!(
            !required,
            "FIDUCIA_MEMBERSHIP_ITF_TRACE_DIR is required when replay is mandatory"
        );
        eprintln!("membership ITF replay skipped: FIDUCIA_MEMBERSHIP_ITF_TRACE_DIR is unset");
        return;
    };

    let mut traces = Vec::new();
    collect_itf_traces(&PathBuf::from(&directory), &mut traces);
    traces.sort();
    assert!(
        !traces.is_empty(),
        "no *.itf.json traces found under {}",
        PathBuf::from(&directory).display()
    );

    let mut transitions = 0usize;
    for trace in &traces {
        transitions += replay_itf_trace(trace);
    }
    assert!(
        transitions > 0,
        "membership ITF corpus contained no executable transitions"
    );
    eprintln!(
        "replayed {} Quint membership traces and {} transitions against production Membership",
        traces.len(),
        transitions
    );
}

fn collect_itf_traces(root: &Path, traces: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root)
        .unwrap_or_else(|error| panic!("failed to read ITF directory {}: {error}", root.display()))
    {
        let path = entry.expect("membership ITF directory entry").path();
        if path.is_dir() {
            collect_itf_traces(&path, traces);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".itf.json"))
        {
            traces.push(path);
        }
    }
}

fn replay_itf_trace(path: &Path) -> usize {
    let document: Value =
        serde_json::from_slice(&fs::read(path).unwrap_or_else(|error| {
            panic!("failed to read ITF trace {}: {error}", path.display())
        }))
        .unwrap_or_else(|error| panic!("failed to parse ITF trace {}: {error}", path.display()));
    let states = document["states"]
        .as_array()
        .unwrap_or_else(|| panic!("ITF trace {} has no states array", path.display()));
    assert!(
        !states.is_empty(),
        "ITF trace {} contains no states",
        path.display()
    );

    let (membership, oracle, mut reference) = replay(&[]);
    assert_model_state(&states[0]["s"], &reference, path, 0);

    let mut transitions = 0usize;
    for (index, state) in states.iter().enumerate().skip(1) {
        let action_name = state["mbt::actionTaken"].as_str().unwrap_or_else(|| {
            panic!(
                "ITF trace {} state {index} has no mbt::actionTaken",
                path.display()
            )
        });
        if action_name != "idle" {
            let action = itf_action(action_name, &state["mbt::nondetPicks"], path, index);
            let actual = execute(&membership, &oracle, &reference, action);
            let expected = reference.apply(action);
            assert_eq!(
                actual,
                expected,
                "production output diverged from Quint action {action_name} in {} state {index}",
                path.display()
            );
            transitions += 1;
        }

        assert_eq!(
            Projection::from_membership(&membership),
            Projection::from_reference(&reference),
            "production membership diverged in {} state {index} after {action_name}",
            path.display()
        );
        assert_model_state(&state["s"], &reference, path, index);
    }
    transitions
}

fn itf_action(action: &str, picks: &Value, path: &Path, index: usize) -> Action {
    match action {
        "accept_heartbeat" | "ignore_stale_heartbeat" => Action::Heartbeat {
            seq: itf_bigint(itf_pick(picks, "seq", path, index), path, index, "seq"),
            report_version: u8::try_from(itf_bigint(
                itf_pick(picks, "report", path, index),
                path,
                index,
                "report",
            ))
            .unwrap_or_else(|_| {
                panic!(
                    "ITF report version is outside u8 in {} state {index}",
                    path.display()
                )
            }),
        },
        "sweep" => Action::Sweep,
        "age_and_sweep" => match itf_tag(itf_pick(picks, "age", path, index)) {
            "SuspectAge" => Action::AdvanceToSuspect,
            "DeadAge" => Action::AdvanceToDead,
            age => panic!(
                "unsupported ITF age {age:?} in {} state {index}",
                path.display()
            ),
        },
        "oracle_and_sweep" => {
            Action::OracleAndSweep(match itf_tag(itf_pick(picks, "verdict", path, index)) {
                "OracleUnknown" => Verdict::Unknown,
                "OracleRunning" => Verdict::Running,
                "OracleGone" => Verdict::Gone,
                verdict => panic!(
                    "unsupported ITF oracle {verdict:?} in {} state {index}",
                    path.display()
                ),
            })
        }
        "drain" => Action::Drain,
        other => panic!(
            "unsupported Quint membership action {other:?} in {} state {index}",
            path.display()
        ),
    }
}

fn itf_pick<'a>(picks: &'a Value, name: &str, path: &Path, index: usize) -> &'a Value {
    let pick = &picks[name];
    assert_eq!(
        pick["tag"].as_str(),
        Some("Some"),
        "ITF pick {name} is absent in {} state {index}",
        path.display()
    );
    &pick["value"]
}

fn itf_bigint(value: &Value, path: &Path, index: usize, field: &str) -> u64 {
    value["#bigint"]
        .as_str()
        .and_then(|raw| raw.parse().ok())
        .or_else(|| value.as_u64())
        .unwrap_or_else(|| {
            panic!(
                "ITF field {field} is not a non-negative integer in {} state {index}",
                path.display()
            )
        })
}

fn itf_tag(value: &Value) -> &str {
    value["tag"].as_str().unwrap_or("<missing-tag>")
}

fn assert_model_state(state: &Value, reference: &Reference, path: &Path, index: usize) {
    let model_health = match itf_tag(&state["health"]) {
        "Healthy" => Health::Healthy,
        "Suspect" => Health::Suspect,
        "Dead" => Health::Dead,
        "Draining" => Health::Draining,
        other => panic!(
            "unsupported ITF health {other:?} in {} state {index}",
            path.display()
        ),
    };
    let model_oracle = match itf_tag(&state["oracle"]) {
        "OracleUnknown" => Verdict::Unknown,
        "OracleRunning" => Verdict::Running,
        "OracleGone" => Verdict::Gone,
        other => panic!(
            "unsupported ITF oracle {other:?} in {} state {index}",
            path.display()
        ),
    };
    assert_eq!(
        reference.health,
        model_health,
        "health diverged in {} state {index}",
        path.display()
    );
    assert_eq!(
        reference.oracle,
        model_oracle,
        "oracle diverged in {} state {index}",
        path.display()
    );
    assert_eq!(
        reference.last_seq,
        itf_bigint(&state["last_seq"], path, index, "last_seq"),
        "sequence diverged in {} state {index}",
        path.display()
    );
    assert_eq!(
        u64::from(reference.report_version),
        itf_bigint(&state["report_version"], path, index, "report_version"),
        "report version diverged in {} state {index}",
        path.display()
    );
    assert_eq!(
        reference.ever_draining,
        state["ever_draining"].as_bool().unwrap_or_else(|| {
            panic!(
                "ITF ever_draining is not boolean in {} state {index}",
                path.display()
            )
        }),
        "draining history diverged in {} state {index}",
        path.display()
    );

    let silent_for = reference.now_ms.saturating_sub(reference.last_seen_ms);
    match itf_tag(&state["age"]) {
        "Fresh" => assert!(
            silent_for < SUSPECT_AFTER_MS,
            "Fresh age diverged in {} state {index}",
            path.display()
        ),
        "SuspectAge" => assert!(
            (SUSPECT_AFTER_MS..DEAD_AFTER_MS).contains(&silent_for),
            "SuspectAge diverged in {} state {index}",
            path.display()
        ),
        "DeadAge" => assert!(
            silent_for >= DEAD_AFTER_MS,
            "DeadAge diverged in {} state {index}",
            path.display()
        ),
        other => panic!(
            "unsupported ITF age {other:?} in {} state {index}",
            path.display()
        ),
    }
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
