#![allow(dead_code)]

// Compile the production reconciliation stack into this integration-test crate.
// The Quint model remains independent; this adapter maps its finite actions onto
// public production operations and compares the complete observable projection
// after every transition.
#[path = "../src/cluster.rs"]
mod cluster;
#[path = "../src/leadership.rs"]
mod leadership;
#[path = "../src/membership.rs"]
mod membership;
#[path = "../src/model.rs"]
mod model;
#[path = "../src/oracle.rs"]
mod oracle;
#[path = "../src/placement.rs"]
mod placement;
#[path = "../src/plan.rs"]
mod plan;
#[path = "../src/scheduler.rs"]
mod scheduler;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use cluster::{apply_command, Command, ControlPlane};
use membership::{Membership, MembershipConfig};
use model::{HeartbeatReport, NodeHealth, NodeId, ScalePlan, ShardAssignment};
use placement::Placement;
use scheduler::Scheduler;
use serde_json::Value;

const SHARD: u32 = 0;
const A: &str = "a";
const B: &str = "b";
const C: &str = "c";
const D: &str = "d";

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
        self.forgotten.lock().expect("forgotten mutex").clone()
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
                self.forgotten.lock().expect("forgotten mutex").insert(node);
            }
        }
        true
    }
}

struct Harness {
    membership: Arc<Membership>,
    placement: Arc<Placement>,
    plan: Arc<Mutex<ScalePlan>>,
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
        let scheduler = Scheduler::new(
            membership.clone(),
            placement.clone(),
            plan.clone(),
            cp.clone(),
        );
        Self {
            membership,
            placement,
            plan,
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

    fn apply(&self, action: &str) {
        match action {
            "init" | "idle" => {}
            "become_follower" => self.cp.set_leader(false),
            "become_leader" => self.cp.set_leader(true),
            "lose_a_from_soft_membership" => {
                assert!(self.membership.forget(&A.to_owned()));
            }
            "heartbeat_a" => {
                self.membership
                    .heartbeat(&A.to_owned(), 0, report(1, true, true));
            }
            "add_d" => {
                self.membership
                    .heartbeat(&D.to_owned(), 0, report(4, false, false));
            }
            "mark_a_dead" => self.mark_a_dead(),
            "begin_drain_a" => {
                assert!(self.membership.drain(&A.to_owned()));
            }
            "evacuate_a" => {
                self.membership
                    .heartbeat(&A.to_owned(), 1, report(1, false, false));
            }
            "prepare_cold_start" => self.prepare_cold_start(),
            "restore_snapshot_bcd" => {
                self.placement.restore_from(vec![assignment(&[2, 3, 4], 2)]);
            }
            "reconcile_follower_hold"
            | "reconcile_incomplete_hold"
            | "reconcile_degraded_hold"
            | "reconcile_cold_adopt"
            | "reconcile_replace_dead_a"
            | "reconcile_replace_and_forget_drained_a"
            | "reconcile_idempotent" => self.scheduler.reconcile(),
            "attempt_forget_a_rejected" => {
                assert!(self.cp.propose(Command::ForgetNode(A.to_owned())));
            }
            other => panic!("unsupported Quint scheduler action {other:?}"),
        }
    }

    fn mark_a_dead(&self) {
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
        assert_eq!(newly_dead, vec![A.to_owned()]);
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

#[derive(Debug, PartialEq, Eq)]
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

fn report(node: u64, hosts: bool, leads: bool) -> HeartbeatReport {
    assert!(!leads || hosts, "a leader must report hosting the shard");
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

#[test]
fn generated_scheduler_itf_traces_replay_against_production() {
    let required = std::env::var("FIDUCIA_REQUIRE_SCHEDULER_ITF_REPLAY")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let Some(directory) = std::env::var_os("FIDUCIA_SCHEDULER_ITF_TRACE_DIR") else {
        assert!(
            !required,
            "FIDUCIA_SCHEDULER_ITF_TRACE_DIR is required when replay is mandatory"
        );
        eprintln!("scheduler ITF replay skipped: FIDUCIA_SCHEDULER_ITF_TRACE_DIR is unset");
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
    let mut actions = BTreeSet::new();
    for trace in &traces {
        transitions += replay_itf_trace(trace, &mut actions);
    }
    assert!(
        transitions > 0,
        "scheduler ITF corpus contained no executable transitions"
    );
    eprintln!(
        "replayed {} Quint scheduler traces and {} transitions against production; actions={actions:?}",
        traces.len(),
        transitions
    );
}

fn collect_itf_traces(root: &Path, traces: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root)
        .unwrap_or_else(|error| panic!("failed to read ITF directory {}: {error}", root.display()))
    {
        let path = entry.expect("scheduler ITF directory entry").path();
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

fn replay_itf_trace(path: &Path, actions: &mut BTreeSet<String>) -> usize {
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

    let harness = Harness::initial();
    assert_model_state(&states[0]["s"], &harness.projection(), path, 0);

    let mut transitions = 0usize;
    for (index, state) in states.iter().enumerate().skip(1) {
        let action = state["mbt::actionTaken"].as_str().unwrap_or_else(|| {
            panic!(
                "ITF trace {} state {index} has no mbt::actionTaken",
                path.display()
            )
        });
        actions.insert(action.to_owned());
        harness.apply(action);
        assert_model_state(&state["s"], &harness.projection(), path, index);
        if action != "idle" {
            transitions += 1;
        }
    }
    transitions
}

fn assert_model_state(state: &Value, actual: &Projection, path: &Path, index: usize) {
    let expected = Projection {
        leader: itf_tag(&state["role"]) == "Leader",
        known: itf_set(&state["known"], path, index, "known"),
        healthy: itf_set(&state["healthy"], path, index, "healthy"),
        draining: itf_set(&state["draining"], path, index, "draining"),
        hosted: itf_set(&state["hosted"], path, index, "hosted"),
        observed_leader: itf_bigint(&state["observed_leader"], path, index, "observed_leader"),
        placement: itf_set(&state["placement"], path, index, "placement"),
        preferred_leader: itf_bigint(&state["preferred_leader"], path, index, "preferred_leader"),
        generation: itf_bigint(&state["generation"], path, index, "generation"),
        forgotten: itf_set(&state["forgotten"], path, index, "forgotten"),
    };
    assert_eq!(
        actual,
        &expected,
        "production scheduler diverged from Quint in {} state {index}",
        path.display()
    );
}

fn itf_set(value: &Value, path: &Path, index: usize, field: &str) -> BTreeSet<u64> {
    value["#set"]
        .as_array()
        .unwrap_or_else(|| {
            panic!(
                "ITF field {field} is not a set in {} state {index}",
                path.display()
            )
        })
        .iter()
        .map(|entry| itf_bigint(entry, path, index, field))
        .collect()
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
