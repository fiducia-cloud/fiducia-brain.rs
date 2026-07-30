use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::{json, Value};

use crate::cluster::{apply_command, Command, ControlPlane};
use crate::membership::{Membership, MembershipConfig};
use crate::model::{HeartbeatReport, NodeHealth, NodeId, ScalePlan, ShardAssignment};
use crate::placement::Placement;
use crate::scheduler::Scheduler;

const SHARD: u32 = 0;
const A: &str = "a";
const B: &str = "b";
const C: &str = "c";
const D: &str = "d";

pub const REQUIRED_ACTIONS: &[&str] = &[
    "add_d",
    "attempt_forget_a_rejected",
    "become_follower",
    "become_leader",
    "begin_drain_a",
    "evacuate_a",
    "heartbeat_a",
    "lose_a_from_soft_membership",
    "mark_a_dead",
    "prepare_cold_start",
    "reconcile_cold_adopt",
    "reconcile_degraded_hold",
    "reconcile_follower_hold",
    "reconcile_forget_drained_a_after_placement",
    "reconcile_idempotent",
    "reconcile_incomplete_hold",
    "reconcile_replace_and_forget_drained_a",
    "reconcile_replace_dead_a",
    "restore_snapshot_bcd",
];

#[derive(Debug, Clone, Serialize)]
pub struct ReplayMismatch {
    pub trace: PathBuf,
    pub step: Option<u64>,
    pub action: Option<String>,
    pub message: String,
    pub expected: Value,
    pub actual: Value,
}

#[derive(Debug, Clone)]
pub struct ReplaySummary {
    pub traces_total: u64,
    pub traces_passed: u64,
    pub states: usize,
    pub non_idle_transitions: usize,
    pub actions: BTreeSet<String>,
    pub mismatches: Vec<ReplayMismatch>,
}

impl ReplaySummary {
    pub fn success(&self) -> bool {
        self.traces_total > 0
            && self.traces_passed == self.traces_total
            && self.mismatches.is_empty()
    }
}

struct HarnessControlPlane {
    leader: AtomicBool,
    membership: Arc<Membership>,
    placement: Arc<Placement>,
    plan: Arc<Mutex<ScalePlan>>,
    forgotten: Mutex<BTreeSet<u64>>,
}

impl HarnessControlPlane {
    fn new(
        membership: Arc<Membership>,
        placement: Arc<Placement>,
        plan: Arc<Mutex<ScalePlan>>,
    ) -> Self {
        Self {
            leader: AtomicBool::new(true),
            membership,
            placement,
            plan,
            forgotten: Mutex::new(BTreeSet::new()),
        }
    }

    fn set_leader(&self, leader: bool) {
        self.leader.store(leader, Ordering::SeqCst);
    }

    fn forgotten(&self) -> BTreeSet<u64> {
        self.forgotten
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ControlPlane for HarnessControlPlane {
    fn is_available(&self) -> bool {
        true
    }

    fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    fn leader_addr(&self) -> Option<String> {
        (!self.is_leader()).then(|| "http://brain-leader:9095".to_owned())
    }

    fn propose(&self, command: Command) -> bool {
        if !self.is_leader() {
            return false;
        }
        let forget_candidate = match &command {
            Command::ForgetNode(node_id) => node_number(node_id),
            Command::AssignShard(_) | Command::SetScalePlan(_) => None,
        };
        apply_command(&self.membership, &self.placement, &self.plan, command);
        if let Some(node) = forget_candidate {
            let id = node_id(node);
            if self
                .membership
                .snapshot()
                .iter()
                .all(|known| known.node_id != id)
            {
                self.forgotten
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(node);
            }
        }
        true
    }
}

struct Harness {
    membership: Arc<Membership>,
    placement: Arc<Placement>,
    cp: Arc<HarnessControlPlane>,
    scheduler: Scheduler,
}

impl Harness {
    fn initial() -> Self {
        let membership = Arc::new(Membership::new(MembershipConfig {
            suspect_after_ms: 10,
            dead_after_ms: 20,
        }));
        membership.heartbeat(&A.to_owned(), 0, report(1, true, true));
        membership.heartbeat(&B.to_owned(), 0, report(2, true, false));
        membership.heartbeat(&C.to_owned(), 0, report(3, true, false));

        let placement = Arc::new(Placement::new(1));
        placement.assign(assignment(&[1, 2, 3], 1));
        let plan = Arc::new(Mutex::new(ScalePlan {
            target_nodes: 3,
            replication_factor: 3,
        }));
        let cp = Arc::new(HarnessControlPlane::new(
            membership.clone(),
            placement.clone(),
            plan.clone(),
        ));
        let scheduler = Scheduler::new(membership.clone(), placement.clone(), plan, cp.clone());
        Self {
            membership,
            placement,
            cp,
            scheduler,
        }
    }

    fn projection(&self) -> Projection {
        let nodes = self.membership.snapshot();
        let known = nodes
            .iter()
            .filter_map(|node| node_number(&node.node_id))
            .collect();
        let healthy = nodes
            .iter()
            .filter(|node| node.health == NodeHealth::Healthy)
            .filter_map(|node| node_number(&node.node_id))
            .collect();
        let draining = nodes
            .iter()
            .filter(|node| node.health == NodeHealth::Draining)
            .filter_map(|node| node_number(&node.node_id))
            .collect();
        let hosted = nodes
            .iter()
            .filter(|node| node.hosted_shards.contains(&SHARD))
            .filter_map(|node| node_number(&node.node_id))
            .collect();
        let observed_leader = nodes
            .iter()
            .find(|node| node.health == NodeHealth::Healthy && node.leading_shards.contains(&SHARD))
            .and_then(|node| node_number(&node.node_id))
            .unwrap_or(0);
        let assignment = self.placement.get(SHARD);
        let placement = assignment
            .as_ref()
            .map(|assignment| {
                assignment
                    .replicas
                    .iter()
                    .filter_map(|node| node_number(node))
                    .collect()
            })
            .unwrap_or_default();
        let preferred_leader = assignment
            .and_then(|assignment| assignment.preferred_leader)
            .and_then(|node| node_number(&node))
            .unwrap_or(0);

        Projection {
            leader: self.cp.is_leader(),
            known,
            healthy,
            draining,
            hosted,
            observed_leader,
            placement,
            preferred_leader,
            generation: self.placement.generation(),
            forgotten: self.cp.forgotten(),
        }
    }

    fn apply(&self, action: &str) -> Result<(), String> {
        match action {
            "init" | "idle" => Ok(()),
            "become_follower" => {
                self.cp.set_leader(false);
                Ok(())
            }
            "become_leader" => {
                self.cp.set_leader(true);
                Ok(())
            }
            "lose_a_from_soft_membership" => ensure(
                self.membership.forget(&A.to_owned()),
                "A was not present for soft-membership loss",
            ),
            "heartbeat_a" => {
                self.membership
                    .heartbeat(&A.to_owned(), 0, report(1, true, true));
                Ok(())
            }
            "add_d" => {
                self.membership
                    .heartbeat(&D.to_owned(), 0, report(4, false, false));
                Ok(())
            }
            "mark_a_dead" => self.mark_a_dead(),
            "begin_drain_a" => ensure(
                self.membership.drain(&A.to_owned()),
                "A was not present for drain",
            ),
            "evacuate_a" => {
                self.membership
                    .heartbeat(&A.to_owned(), 1, report(1, false, false));
                Ok(())
            }
            "prepare_cold_start" => {
                self.prepare_cold_start();
                Ok(())
            }
            "restore_snapshot_bcd" => {
                self.placement.restore_from(vec![assignment(&[2, 3, 4], 2)]);
                Ok(())
            }
            "reconcile_follower_hold"
            | "reconcile_incomplete_hold"
            | "reconcile_degraded_hold"
            | "reconcile_cold_adopt"
            | "reconcile_replace_dead_a"
            | "reconcile_replace_and_forget_drained_a"
            | "reconcile_forget_drained_a_after_placement"
            | "reconcile_idempotent" => {
                self.scheduler.reconcile();
                Ok(())
            }
            "attempt_forget_a_rejected" => ensure(
                self.cp.propose(Command::ForgetNode(A.to_owned())),
                "leader did not accept the stale ForgetNode proposal for evaluation",
            ),
            other => Err(format!("unsupported Quint scheduler action {other:?}")),
        }
    }

    fn mark_a_dead(&self) -> Result<(), String> {
        let refresh_at = 100;
        for node in self.membership.snapshot() {
            if node.node_id == A || node.health != NodeHealth::Healthy {
                continue;
            }
            self.membership.heartbeat(
                &node.node_id,
                refresh_at,
                HeartbeatReport {
                    address: node.address,
                    cloud_provider: node.cloud_provider,
                    region: node.region,
                    cluster_id: node.cluster_id,
                    failure_domain: node.failure_domain,
                    hosted_shards: node.hosted_shards,
                    leading_shards: node.leading_shards,
                    seq: 0,
                },
            );
        }
        let newly_dead = self.membership.sweep(refresh_at);
        ensure(
            newly_dead == vec![A.to_owned()],
            format!("expected only A to become dead, got {newly_dead:?}"),
        )
    }

    fn prepare_cold_start(&self) {
        self.placement.restore_from(Vec::new());
        self.membership
            .heartbeat(&A.to_owned(), 1, report(1, true, false));
        self.membership
            .heartbeat(&B.to_owned(), 1, report(2, true, true));
        self.membership
            .heartbeat(&C.to_owned(), 1, report(3, true, false));
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct Projection {
    leader: bool,
    known: BTreeSet<u64>,
    healthy: BTreeSet<u64>,
    draining: BTreeSet<u64>,
    hosted: BTreeSet<u64>,
    observed_leader: u64,
    placement: BTreeSet<u64>,
    preferred_leader: u64,
    generation: u64,
    forgotten: BTreeSet<u64>,
}

struct TraceSummary {
    states: usize,
    non_idle_transitions: usize,
    actions: BTreeSet<String>,
}

pub fn replay_paths(paths: &[PathBuf]) -> ReplaySummary {
    let traces_total = u64::try_from(paths.len()).unwrap_or(u64::MAX);
    let mut traces_passed = 0u64;
    let mut states = 0usize;
    let mut non_idle_transitions = 0usize;
    let mut actions = BTreeSet::new();
    let mut mismatches = Vec::new();

    for path in paths {
        match replay_trace(path) {
            Ok(summary) => {
                traces_passed = traces_passed.saturating_add(1);
                states = states.saturating_add(summary.states);
                non_idle_transitions =
                    non_idle_transitions.saturating_add(summary.non_idle_transitions);
                actions.extend(summary.actions);
            }
            Err(mismatch) => mismatches.push(mismatch),
        }
    }

    let missing = REQUIRED_ACTIONS
        .iter()
        .copied()
        .filter(|action| !actions.contains(*action))
        .collect::<Vec<_>>();
    if !missing.is_empty() && !paths.is_empty() {
        if traces_passed == traces_total {
            traces_passed = traces_passed.saturating_sub(1);
        }
        mismatches.push(ReplayMismatch {
            trace: paths[0].clone(),
            step: None,
            action: missing.first().map(|action| (*action).to_owned()),
            message: format!(
                "trace corpus left production scheduler branches untested: {}",
                missing.join(", ")
            ),
            expected: json!(REQUIRED_ACTIONS),
            actual: json!(actions),
        });
    }

    ReplaySummary {
        traces_total,
        traces_passed,
        states,
        non_idle_transitions,
        actions,
        mismatches,
    }
}

fn replay_trace(path: &Path) -> Result<TraceSummary, ReplayMismatch> {
    let bytes = fs::read(path).map_err(|error| mismatch(path, None, None, error.to_string()))?;
    let document: Value = serde_json::from_slice(&bytes)
        .map_err(|error| mismatch(path, None, None, error.to_string()))?;
    let states = document["states"]
        .as_array()
        .ok_or_else(|| mismatch(path, None, None, "ITF trace has no states array".to_owned()))?;
    if states.is_empty() {
        return Err(mismatch(
            path,
            None,
            None,
            "ITF trace contains no states".to_owned(),
        ));
    }

    let harness = Harness::initial();
    let expected = expected_projection(&states[0]["s"])
        .map_err(|message| mismatch(path, Some(0), Some("init"), message))?;
    let actual = harness.projection();
    if actual != expected {
        return Err(projection_mismatch(path, 0, "init", &expected, &actual));
    }

    let mut actions = BTreeSet::from(["init".to_owned()]);
    let mut non_idle_transitions = 0usize;
    for (index, state) in states.iter().enumerate().skip(1) {
        let action = state["mbt::actionTaken"].as_str().ok_or_else(|| {
            mismatch(
                path,
                Some(index as u64),
                None,
                "ITF state has no mbt::actionTaken".to_owned(),
            )
        })?;
        actions.insert(action.to_owned());
        if action != "idle" {
            non_idle_transitions = non_idle_transitions.saturating_add(1);
        }
        if let Err(message) = harness.apply(action) {
            return Err(ReplayMismatch {
                trace: path.to_path_buf(),
                step: Some(index as u64),
                action: Some(action.to_owned()),
                message,
                expected: state["s"].clone(),
                actual: serde_json::to_value(harness.projection()).unwrap_or(Value::Null),
            });
        }
        let expected = expected_projection(&state["s"])
            .map_err(|message| mismatch(path, Some(index as u64), Some(action), message))?;
        let actual = harness.projection();
        if actual != expected {
            return Err(projection_mismatch(path, index, action, &expected, &actual));
        }
    }

    Ok(TraceSummary {
        states: states.len(),
        non_idle_transitions,
        actions,
    })
}

fn expected_projection(state: &Value) -> Result<Projection, String> {
    Ok(Projection {
        leader: itf_tag(&state["role"])? == "Leader",
        known: itf_set(&state["known"], "known")?,
        healthy: itf_set(&state["healthy"], "healthy")?,
        draining: itf_set(&state["draining"], "draining")?,
        hosted: itf_set(&state["hosted"], "hosted")?,
        observed_leader: itf_bigint(&state["observed_leader"], "observed_leader")?,
        placement: itf_set(&state["placement"], "placement")?,
        preferred_leader: itf_bigint(&state["preferred_leader"], "preferred_leader")?,
        generation: itf_bigint(&state["generation"], "generation")?,
        forgotten: itf_set(&state["forgotten"], "forgotten")?,
    })
}

fn projection_mismatch(
    path: &Path,
    index: usize,
    action: &str,
    expected: &Projection,
    actual: &Projection,
) -> ReplayMismatch {
    ReplayMismatch {
        trace: path.to_path_buf(),
        step: Some(index as u64),
        action: Some(action.to_owned()),
        message: "production scheduler observable state diverged from Quint".to_owned(),
        expected: serde_json::to_value(expected).unwrap_or(Value::Null),
        actual: serde_json::to_value(actual).unwrap_or(Value::Null),
    }
}

fn mismatch(
    path: &Path,
    step: Option<u64>,
    action: Option<&str>,
    message: String,
) -> ReplayMismatch {
    ReplayMismatch {
        trace: path.to_path_buf(),
        step,
        action: action.map(ToOwned::to_owned),
        message,
        expected: Value::Null,
        actual: Value::Null,
    }
}

pub fn collect_itf_traces(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut traces = Vec::new();
    collect_itf_traces_into(root, &mut traces)?;
    traces.sort();
    Ok(traces)
}

fn collect_itf_traces_into(root: &Path, traces: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_itf_traces_into(&path, traces)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".itf.json"))
        {
            traces.push(path);
        }
    }
    Ok(())
}

fn report(node: u64, hosts: bool, leads: bool) -> HeartbeatReport {
    debug_assert!(!leads || hosts, "a leader must report hosting the shard");
    let id = node_id(node);
    HeartbeatReport {
        address: format!("10.0.0.{node}:8090"),
        cloud_provider: format!("cloud-{id}"),
        region: format!("region-{id}"),
        cluster_id: format!("cluster-{id}"),
        failure_domain: format!("domain-{id}"),
        hosted_shards: if hosts { vec![SHARD] } else { Vec::new() },
        leading_shards: if leads { vec![SHARD] } else { Vec::new() },
        seq: 0,
    }
}

fn assignment(nodes: &[u64], leader: u64) -> ShardAssignment {
    ShardAssignment {
        shard_id: SHARD,
        replicas: nodes.iter().map(|node| node_id(*node)).collect(),
        preferred_leader: (leader != 0).then(|| node_id(leader)),
        preferred_region: None,
        preferred_cloud_provider: None,
    }
}

fn node_id(node: u64) -> NodeId {
    match node {
        1 => A,
        2 => B,
        3 => C,
        4 => D,
        other => panic!("model node {other} is outside the finite domain"),
    }
    .to_owned()
}

fn node_number(node: &str) -> Option<u64> {
    match node {
        A => Some(1),
        B => Some(2),
        C => Some(3),
        D => Some(4),
        _ => None,
    }
}

fn itf_set(value: &Value, field: &str) -> Result<BTreeSet<u64>, String> {
    value["#set"]
        .as_array()
        .ok_or_else(|| format!("ITF field {field} is not a set"))?
        .iter()
        .map(|entry| itf_bigint(entry, field))
        .collect()
}

fn itf_bigint(value: &Value, field: &str) -> Result<u64, String> {
    value["#bigint"]
        .as_str()
        .and_then(|raw| raw.parse().ok())
        .or_else(|| value.as_u64())
        .ok_or_else(|| format!("ITF field {field} is not a non-negative integer"))
}

fn itf_tag(value: &Value) -> Result<&str, String> {
    value["tag"]
        .as_str()
        .ok_or_else(|| "ITF tagged value has no tag".to_owned())
}

fn ensure(condition: bool, message: impl Into<String>) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
