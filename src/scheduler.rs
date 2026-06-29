//! Reconciliation loop — the scaling & healing strategy.
//!
//! A control loop that drives the cluster from its *observed* state (membership +
//! reported shard hosting) toward its *desired* state (the [`ScalePlan`]: RF
//! replicas per shard, spread across failure domains, leadership balanced).
//!
//! ## The one invariant that makes scaling cheap
//!
//! **Shard count is fixed; node count is elastic.** Scaling never changes
//! `key → shard` (see [`crate::config`]); it only rewrites `shard → nodes`. So
//! "scale the cluster" == "move some shard replicas/leaders between nodes", an
//! incremental, online operation — never a global rehash.
//!
//! ## One question per shard
//!
//! Every reconciliation phase — heal a failure, absorb a new node, drain a node,
//! rebalance — is the same question: *what should this shard's replicas and leader
//! be?* So each tick we recompute, per shard:
//!
//!   1. **desired replicas** via [`crate::plan::plan_replicas`] (keeps healthy
//!      replicas, drops dead/draining ones, fills to RF on the least-loaded node
//!      in a fresh failure domain), and
//!   2. **desired leader** via [`crate::leadership::desired_leader`] (affinity to a
//!      preferred node, else stickiness to the observed leader, else failover).
//!
//! and write the assignment if it changed. A `Dead` or `Draining` node simply
//! stops being a healthy candidate, so its replicas flow elsewhere automatically.
//!
//! > Safe **execution** of a replica move is still one-at-a-time on the data
//! > plane (add learner → catch up → promote → remove old); the brain publishes
//! > the *target* and nodes reconcile toward it. Throttling/learner sequencing is
//! > the data-plane membership-change work tracked in `fiducia-node`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cluster::{Command, ControlPlane};
use crate::membership::Membership;
use crate::model::{NodeHealth, NodeId, ScalePlan, ShardAssignment, ShardId};
use crate::placement::Placement;
use crate::plan::{plan_replicas, NodeSlot};

/// The reconciler: reads observed state (membership), writes desired state
/// (placement) **through the control plane** so the change is replicated, all
/// mediated by the current [`ScalePlan`].
pub struct Scheduler {
    membership: Arc<Membership>,
    placement: Arc<Placement>,
    /// The live scale intent; `POST /v1/scale` updates this and the loop reads it.
    plan: Arc<Mutex<ScalePlan>>,
    /// The brain's own control plane: writes go through `propose`, and the loop
    /// only acts while `is_leader()`.
    cp: Arc<dyn ControlPlane>,
}

impl Scheduler {
    pub fn new(
        membership: Arc<Membership>,
        placement: Arc<Placement>,
        plan: Arc<Mutex<ScalePlan>>,
        cp: Arc<dyn ControlPlane>,
    ) -> Self {
        Scheduler {
            membership,
            placement,
            plan,
            cp,
        }
    }

    /// One reconciliation tick: recompute every shard's desired replicas + leader
    /// and write the ones that changed.
    pub fn reconcile(&self) {
        let plan = self.plan.lock().unwrap().clone();
        let rf = plan.replication_factor.max(1);
        let target_nodes = plan.target_nodes.max(rf) as usize;
        let nodes = self.membership.snapshot();

        // Healthy placement candidates, by id. If more healthy nodes exist than
        // the scale plan wants, keep the loaded ones active and let light nodes
        // evacuate naturally as assignments are rewritten away from them.
        let domain_of: HashMap<NodeId, String> = nodes
            .iter()
            .map(|n| (n.node_id.clone(), n.failure_domain.clone()))
            .collect();
        let cloud_of: HashMap<NodeId, String> = nodes
            .iter()
            .map(|n| (n.node_id.clone(), n.cloud_provider.clone()))
            .collect();
        let region_of: HashMap<NodeId, String> = nodes
            .iter()
            .map(|n| (n.node_id.clone(), n.region.clone()))
            .collect();
        let all_healthy_ids: Vec<NodeId> = nodes
            .iter()
            .filter(|n| n.health == NodeHealth::Healthy)
            .map(|n| n.node_id.clone())
            .collect();
        // Every node membership currently knows about (at any health). Lets us tell
        // "this replica's node is absent because it hasn't heartbeated to this
        // leader yet" (hold) apart from "membership knows it is Dead/Draining" (act).
        let known: HashSet<NodeId> = nodes.iter().map(|n| n.node_id.clone()).collect();

        // Current per-node replica load (across the existing placement) so fills
        // pick the least-loaded node and spread evens out as we go.
        let assignments = self.placement.snapshot();
        let mut load: HashMap<NodeId, u32> =
            all_healthy_ids.iter().map(|id| (id.clone(), 0)).collect();
        for a in &assignments {
            for r in &a.replicas {
                if let Some(l) = load.get_mut(r) {
                    *l += 1;
                }
            }
        }

<<<<<<< HEAD
        // Observed hosting per shard, from heartbeated `hosted_shards` /
        // `leading_shards`. `observed_replicas` lets a freshly (re)started brain —
        // whose in-memory placement map is empty — ADOPT the data plane's actual
        // layout as its starting point, instead of recomputing a fresh placement
        // and ordering a wave of needless data movement on every brain restart.
=======
        let healthy_ids = active_nodes_for_target(all_healthy_ids, &load, target_nodes);
        let healthy_set: HashSet<NodeId> = healthy_ids.iter().cloned().collect();
        load.retain(|id, _| healthy_set.contains(id));

        let mut leader_load: HashMap<NodeId, u32> =
            healthy_ids.iter().map(|id| (id.clone(), 0)).collect();
        for a in &assignments {
            if let Some(leader) = &a.preferred_leader {
                if let Some(l) = leader_load.get_mut(leader) {
                    *l += 1;
                }
            }
        }

        // Observed leader per shard, from heartbeated `leading_shards`.
>>>>>>> origin/main
        let mut observed_leader: HashMap<ShardId, NodeId> = HashMap::new();
        let mut observed_replicas: HashMap<ShardId, Vec<NodeId>> = HashMap::new();
        for n in &nodes {
            if n.health == NodeHealth::Healthy {
                for s in &n.hosted_shards {
                    observed_replicas
                        .entry(*s)
                        .or_default()
                        .push(n.node_id.clone());
                }
                for s in &n.leading_shards {
                    observed_leader.insert(*s, n.node_id.clone());
                }
            }
        }

        let mut changes = 0u32;
        for shard in 0..self.placement.shard_count() {
            let current = self.placement.get(shard);
            // The brain's own desired state is authoritative; but on a cold start
            // (no desired state yet) fall back to what the nodes actually report
            // hosting, so we reconcile observed → desired rather than blowing the
            // existing layout away and re-placing from scratch.
            let current_replicas: Vec<NodeId> = match &current {
                Some(a) => a.replicas.clone(),
                None => observed_replicas.get(&shard).cloned().unwrap_or_default(),
            };

            // Don't shrink placement on incomplete membership. Just after a leader
            // failover or brain restart the (soft, leader-local) membership is
            // briefly empty while nodes re-heartbeat, even though the replicated
            // placement still references them. Holding a shard whose current replicas
            // aren't all known yet avoids dropping live replicas we simply haven't
            // heard from — a genuinely failed node stays KNOWN as Dead, so real
            // failures still reconcile.
            if !current_replicas.is_empty()
                && !current_replicas.iter().all(|id| known.contains(id))
            {
                continue;
            }

            let slots: Vec<NodeSlot> = healthy_ids
                .iter()
                .map(|id| NodeSlot {
                    node_id: id.clone(),
                    domain: domain_of.get(id).cloned().unwrap_or_default(),
                    load: load.get(id).copied().unwrap_or(0),
                })
                .collect();
            let desired = plan_replicas(&current_replicas, &slots, rf);

            // Maintain load as if this plan is in effect (helps the next shard spread).
            for r in &current_replicas {
                if let Some(l) = load.get_mut(r) {
                    *l = l.saturating_sub(1);
                }
            }
            for r in &desired {
                if let Some(l) = load.get_mut(r) {
                    *l += 1;
                }
            }

            let policy = self.placement.policy(shard);
            let leader_slots: Vec<crate::leadership::LeaderSlot> = desired
                .iter()
                .map(|id| crate::leadership::LeaderSlot {
                    node_id: id.clone(),
                    cloud_provider: cloud_of.get(id).cloned().unwrap_or_default(),
                    region: region_of.get(id).cloned().unwrap_or_default(),
                    leader_load: leader_load.get(id).copied().unwrap_or(0),
                })
                .collect();
            let affinity_target = crate::leadership::preferred_leader_for_policy(
                policy.as_ref(),
                &desired,
                &healthy_set,
                &leader_slots,
            );
            let preferred_leader = crate::leadership::desired_leader(
                affinity_target.as_ref(),
                &desired,
                &healthy_set,
                observed_leader.get(&shard),
            );
            if let Some(previous) = current.as_ref().and_then(|a| a.preferred_leader.as_ref()) {
                if let Some(l) = leader_load.get_mut(previous) {
                    *l = l.saturating_sub(1);
                }
            }
            if let Some(next) = &preferred_leader {
                if let Some(l) = leader_load.get_mut(next) {
                    *l += 1;
                }
            }
            if let (Some(observed), Some(target)) = (observed_leader.get(&shard), &preferred_leader)
            {
                if observed != target {
                    tracing::info!(
                        metric.name = "fiducia.brain.leader_transfer_intent",
                        shard,
                        from = %observed,
                        to = %target,
                        preferred_region = policy
                            .as_ref()
                            .and_then(|p| p.home_region.as_deref())
                            .unwrap_or(""),
                        preferred_cloud_provider = policy
                            .as_ref()
                            .and_then(|p| p.preferred_cloud_provider.as_deref())
                            .unwrap_or(""),
                        "preferred leader differs from observed leader"
                    );
                }
            }

            let changed = match &current {
                None => !desired.is_empty(),
                Some(a) => {
                    a.replicas != desired
                        || a.preferred_leader != preferred_leader
                        || a.preferred_region != policy.as_ref().and_then(|p| p.home_region.clone())
                        || a.preferred_cloud_provider
                            != policy
                                .as_ref()
                                .and_then(|p| p.preferred_cloud_provider.clone())
                }
            };
            if changed {
                tracing::info!(
                    shard,
                    replicas = ?desired,
                    preferred_leader = ?preferred_leader,
                    "scheduler: (re)assigning shard placement"
                );
                changes += 1;
                self.cp.propose(Command::AssignShard(ShardAssignment {
                    shard_id: shard,
                    replicas: desired,
                    preferred_leader,
<<<<<<< HEAD
                }));
=======
                    preferred_region: policy.as_ref().and_then(|p| p.home_region.clone()),
                    preferred_cloud_provider: policy
                        .as_ref()
                        .and_then(|p| p.preferred_cloud_provider.clone()),
                });
>>>>>>> origin/main
            }
        }

        // Scale-down finalize: a drained node that now reports hosting **nothing**
        // has fully evacuated, so remove it from membership (the last step the
        // README promised but nothing did — `DELETE` only *starts* the drain).
        for n in &nodes {
            if n.health == NodeHealth::Draining && n.hosted_shards.is_empty() {
                self.cp.propose(Command::ForgetNode(n.node_id.clone()));
            }
        }

        if changes > 0 {
            tracing::info!(
                changes,
                healthy_nodes = healthy_ids.len(),
                rf,
                "scheduler: reconcile applied placement changes"
            );
        }
    }

    /// Background loop: sweep failures, then reconcile, on an interval. Only the
    /// leader acts — followers stand by so multiple brain replicas don't each
    /// compute (and replicate) a competing placement map.
    pub async fn run(self: Arc<Self>) {
        loop {
            if self.cp.is_leader() {
                let now = now_ms();
                let newly_dead = self.membership.sweep(now);
                if !newly_dead.is_empty() {
                    tracing::warn!(
                        ?newly_dead,
                        "nodes declared dead; re-replicating their shards"
                    );
                }
                self.reconcile();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn active_nodes_for_target(
    mut healthy_ids: Vec<NodeId>,
    load: &HashMap<NodeId, u32>,
    target_nodes: usize,
) -> Vec<NodeId> {
    if healthy_ids.len() <= target_nodes {
        return healthy_ids;
    }

    healthy_ids.sort_by(|a, b| {
        load.get(b)
            .copied()
            .unwrap_or(0)
            .cmp(&load.get(a).copied().unwrap_or(0))
            .then(a.cmp(b))
    });
    healthy_ids.truncate(target_nodes);
    healthy_ids.sort();
    healthy_ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::MembershipConfig;
    use crate::model::{HeartbeatReport, PlacementPolicy};

    fn hb(domain: &str) -> HeartbeatReport {
        HeartbeatReport {
            address: "10.0.0.1:8090".to_string(),
            cloud_provider: String::new(),
            region: String::new(),
            cluster_id: String::new(),
            failure_domain: domain.to_string(),
            hosted_shards: vec![],
            leading_shards: vec![],
            seq: 0,
        }
    }
    fn hb_cloud(cloud_provider: &str, region: &str, cluster_id: &str) -> HeartbeatReport {
        HeartbeatReport {
            address: "10.0.0.1:8090".to_string(),
            cloud_provider: cloud_provider.to_string(),
            region: region.to_string(),
            cluster_id: cluster_id.to_string(),
            failure_domain: String::new(),
            hosted_shards: vec![],
            leading_shards: vec![],
        }
    }

    fn scheduler(shard_count: u32, rf: u32) -> Scheduler {
        let membership = Arc::new(Membership::new(MembershipConfig::default()));
        let placement = Arc::new(Placement::new(shard_count));
        let plan = Arc::new(Mutex::new(ScalePlan {
            target_nodes: 3,
            replication_factor: rf,
        }));
        let cp: Arc<dyn ControlPlane> = Arc::new(crate::cluster::LocalControlPlane::new(
            membership.clone(),
            placement.clone(),
            plan.clone(),
        ));
        Scheduler::new(membership, placement, plan, cp)
    }

    #[test]
    fn reconcile_places_every_shard_at_rf_across_domains() {
        let s = scheduler(8, 3);
        s.membership.heartbeat(&"a".to_string(), 0, hb("gcp"));
        s.membership.heartbeat(&"b".to_string(), 0, hb("aws"));
        s.membership.heartbeat(&"c".to_string(), 0, hb("hetzner"));

        s.reconcile();

        for shard in 0..8 {
            let a = s.placement.get(shard).expect("placed");
            assert_eq!(a.replicas.len(), 3, "shard {shard} at RF");
            assert!(a.preferred_leader.is_some());
        }
        // Replica load is spread evenly: 8 shards × 3 / 3 nodes = 8 each.
        let mut counts: HashMap<String, u32> = HashMap::new();
        for shard in 0..8 {
            for r in s.placement.get(shard).unwrap().replicas {
                *counts.entry(r).or_default() += 1;
            }
        }
        for (_, c) in counts {
            assert_eq!(c, 8, "even replica spread");
        }
    }

    #[test]
    fn reconcile_keeps_healthy_observed_leader_as_preferred_leader() {
        let s = scheduler(1, 3);
        s.membership.heartbeat(
            &"a".to_string(),
            0,
            HeartbeatReport {
                leading_shards: vec![0],
                hosted_shards: vec![0],
                ..hb("gcp")
            },
        );
        s.membership.heartbeat(&"b".to_string(), 0, hb("aws"));
        s.membership.heartbeat(&"c".to_string(), 0, hb("hetzner"));

        s.reconcile();

        let assignment = s.placement.get(0).expect("shard placed");
        assert_eq!(assignment.preferred_leader.as_deref(), Some("a"));
        assert!(assignment.replicas.contains(&"a".to_string()));
    }

    #[test]
<<<<<<< HEAD
    fn drained_and_evacuated_node_is_forgotten_on_reconcile() {
        let s = scheduler(2, 2);
        for (id, dom) in [("a", "gcp"), ("b", "aws"), ("c", "hetzner")] {
            s.membership.heartbeat(&id.to_string(), 0, hb(dom));
        }
        s.reconcile();

        // Operator drains "a"; it evacuates and reports hosting nothing.
        assert!(s.membership.drain(&"a".to_string()));
        s.membership.heartbeat(&"a".to_string(), 1, hb("gcp")); // hb() reports no hosted shards
        s.reconcile();

        assert!(
            s.membership.snapshot().iter().all(|n| n.node_id != "a"),
            "a drained, fully-evacuated node is removed from membership"
        );
    }

    #[test]
    fn holds_placement_when_membership_is_transiently_empty_after_failover() {
        let s = scheduler(2, 3);
        // Replicated placement exists (as if replayed from the Raft log onto a
        // freshly-elected leader)...
        for shard in 0..2 {
            s.placement.assign(ShardAssignment {
                shard_id: shard,
                replicas: vec!["a".to_string(), "b".to_string(), "c".to_string()],
                preferred_leader: Some("a".to_string()),
            });
        }
        // ...but membership is still empty (the nodes haven't heartbeated to this
        // leader yet). Reconcile must NOT wipe the placement to empty.
        s.reconcile();
        for shard in 0..2 {
            let asg = s.placement.get(shard).expect("placement held");
            assert_eq!(
                asg.replicas.len(),
                3,
                "shard {shard} placement held across the membership gap, not wiped"
            );
        }
    }

    #[test]
    fn cold_started_brain_adopts_reported_hosting_instead_of_recomputing() {
        // Data plane was already running (nodes host shard 0, b leads it) before
        // the brain (re)started with an empty placement map. The reconcile must
        // ADOPT the observed layout, not churn the data by re-placing from scratch.
        let s = scheduler(1, 3);
        for (id, dom, leads) in [("a", "gcp", false), ("b", "aws", true), ("c", "hetzner", false)] {
            s.membership.heartbeat(
                &id.to_string(),
                0,
                HeartbeatReport {
                    hosted_shards: vec![0],
                    leading_shards: if leads { vec![0] } else { vec![] },
                    ..hb(dom)
                },
            );
        }

        s.reconcile();

        let a = s.placement.get(0).expect("placed");
        let got: HashSet<String> = a.replicas.into_iter().collect();
        assert_eq!(
            got,
            ["a", "b", "c"].iter().map(|s| s.to_string()).collect::<HashSet<_>>(),
            "keeps exactly the nodes that already host the shard"
        );
        assert_eq!(
            a.preferred_leader.as_deref(),
            Some("b"),
            "adopts the observed leader rather than picking a new one"
        );
=======
    fn rf3_spreads_across_clouds_and_biases_leader_to_policy_region() {
        let s = scheduler(1, 3);
        s.membership.heartbeat(
            &"aws-us".to_string(),
            0,
            hb_cloud("aws", "us-east-1", "aws-prod"),
        );
        s.membership.heartbeat(
            &"gcp-us".to_string(),
            0,
            hb_cloud("gcp", "us-east-1", "gcp-prod"),
        );
        s.membership.heartbeat(
            &"hetzner-eu".to_string(),
            0,
            hb_cloud("hetzner", "nbg1", "hetzner-prod"),
        );
        s.placement.set_policy(PlacementPolicy {
            namespace: "tenant-a".to_string(),
            shard_id: 0,
            home_region: Some("us-east-1".to_string()),
            preferred_cloud_provider: None,
        });

        s.reconcile();

        let assignment = s.placement.get(0).expect("shard placed");
        let nodes = s.membership.snapshot();
        let clouds: HashSet<String> = assignment
            .replicas
            .iter()
            .filter_map(|replica| {
                nodes
                    .iter()
                    .find(|node| &node.node_id == replica)
                    .map(|node| node.cloud_provider.clone())
            })
            .collect();
        let clusters: HashSet<String> = assignment
            .replicas
            .iter()
            .filter_map(|replica| {
                nodes
                    .iter()
                    .find(|node| &node.node_id == replica)
                    .map(|node| node.cluster_id.clone())
            })
            .collect();
        assert_eq!(assignment.replicas.len(), 3);
        assert_eq!(clouds.len(), 3, "one replica per cloud provider");
        assert_eq!(clusters.len(), 3, "one replica per Kubernetes cluster");

        let preferred = assignment.preferred_leader.as_deref().unwrap();
        let preferred_node = nodes
            .iter()
            .find(|node| node.node_id == preferred)
            .expect("preferred leader exists");
        assert_eq!(preferred_node.region, "us-east-1");
        assert_eq!(assignment.preferred_region.as_deref(), Some("us-east-1"));
>>>>>>> origin/main
    }

    #[test]
    fn a_dead_node_is_evacuated_to_a_surviving_node_on_the_next_tick() {
        let s = scheduler(4, 3);
        for (id, dom) in [("a", "gcp"), ("b", "aws"), ("c", "hetzner"), ("d", "gcp")] {
            s.membership.heartbeat(&id.to_string(), 0, hb(dom));
        }
        s.reconcile();

        // Kill node "a": stop heartbeating, and sweep past the dead timeout.
        for (id, dom) in [("b", "aws"), ("c", "hetzner"), ("d", "gcp")] {
            s.membership.heartbeat(&id.to_string(), 1_000_000, hb(dom));
        }
        s.membership.sweep(1_000_000);
        s.reconcile();

        // No shard should still list the dead node, and all stay at RF.
        for shard in 0..4 {
            let a = s.placement.get(shard).unwrap();
            assert!(
                !a.replicas.contains(&"a".to_string()),
                "shard {shard} evacuated a"
            );
            assert_eq!(a.replicas.len(), 3, "shard {shard} restored to RF");
        }
    }

    #[test]
    fn scale_up_rebalance_moves_some_replicas_to_a_new_node() {
        let s = scheduler(12, 3);
        s.plan.lock().unwrap().target_nodes = 4;
        s.membership.heartbeat(&"a".to_string(), 0, hb("gcp"));
        s.membership.heartbeat(&"b".to_string(), 0, hb("aws"));
        s.membership.heartbeat(&"c".to_string(), 0, hb("hetzner"));
        s.reconcile();

        s.membership.heartbeat(&"d".to_string(), 100, hb("gcp"));
        s.reconcile();

        let mut counts: HashMap<String, u32> = HashMap::new();
        for shard in 0..12 {
            let assignment = s.placement.get(shard).expect("placed");
            assert_eq!(assignment.replicas.len(), 3);
            for replica in assignment.replicas {
                *counts.entry(replica).or_default() += 1;
            }
        }
        assert!(
            counts.get("d").copied().unwrap_or(0) > 0,
            "new node should receive replicas during scale-up rebalance"
        );
        assert!(
            counts.get("a").copied().unwrap_or(0) < 12,
            "existing node in the same failure domain should shed some replicas"
        );
    }

    #[test]
    fn target_nodes_excludes_lightest_nodes_from_future_placement() {
        let s = scheduler(6, 3);
        s.plan.lock().unwrap().target_nodes = 4;
        for (id, dom) in [("a", "gcp"), ("b", "aws"), ("c", "hetzner"), ("d", "gcp")] {
            s.membership.heartbeat(&id.to_string(), 0, hb(dom));
        }
        s.reconcile();
        assert!(
            (0..6).any(|shard| s
                .placement
                .get(shard)
                .unwrap()
                .replicas
                .contains(&"d".to_string())),
            "fourth node participates before scale-down"
        );

        s.plan.lock().unwrap().target_nodes = 3;
        s.reconcile();

        for shard in 0..6 {
            let assignment = s.placement.get(shard).unwrap();
            assert_eq!(assignment.replicas.len(), 3);
            assert!(
                !assignment.replicas.contains(&"d".to_string()),
                "lightest extra node should be evacuated from shard {shard}"
            );
        }
    }
}
