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

use crate::membership::Membership;
use crate::model::{NodeHealth, NodeId, ScalePlan, ShardAssignment, ShardId};
use crate::placement::Placement;
use crate::plan::{plan_replicas, NodeSlot};

/// The reconciler: reads observed state (membership), writes desired state
/// (placement), mediated by the current [`ScalePlan`].
pub struct Scheduler {
    membership: Arc<Membership>,
    placement: Arc<Placement>,
    /// The live scale intent; `POST /v1/scale` updates this and the loop reads it.
    plan: Arc<Mutex<ScalePlan>>,
}

impl Scheduler {
    pub fn new(
        membership: Arc<Membership>,
        placement: Arc<Placement>,
        plan: Arc<Mutex<ScalePlan>>,
    ) -> Self {
        Scheduler {
            membership,
            placement,
            plan,
        }
    }

    /// One reconciliation tick: recompute every shard's desired replicas + leader
    /// and write the ones that changed.
    pub fn reconcile(&self) {
        let rf = self.plan.lock().unwrap().replication_factor.max(1);
        let nodes = self.membership.snapshot();

        // Healthy placement candidates, by id.
        let domain_of: HashMap<NodeId, String> = nodes
            .iter()
            .map(|n| (n.node_id.clone(), n.failure_domain.clone()))
            .collect();
        let healthy_ids: Vec<NodeId> = nodes
            .iter()
            .filter(|n| n.health == NodeHealth::Healthy)
            .map(|n| n.node_id.clone())
            .collect();
        let healthy_set: HashSet<NodeId> = healthy_ids.iter().cloned().collect();

        // Current per-node replica load (across the existing placement) so fills
        // pick the least-loaded node and spread evens out as we go.
        let mut load: HashMap<NodeId, u32> = healthy_ids.iter().map(|id| (id.clone(), 0)).collect();
        for a in self.placement.snapshot() {
            for r in &a.replicas {
                if let Some(l) = load.get_mut(r) {
                    *l += 1;
                }
            }
        }

        // Observed leader per shard, from heartbeated `leading_shards`.
        let mut observed_leader: HashMap<ShardId, NodeId> = HashMap::new();
        for n in &nodes {
            if n.health == NodeHealth::Healthy {
                for s in &n.leading_shards {
                    observed_leader.insert(*s, n.node_id.clone());
                }
            }
        }

        for shard in 0..self.placement.shard_count() {
            let current = self.placement.get(shard);
            let current_replicas: Vec<NodeId> = current
                .as_ref()
                .map(|a| a.replicas.clone())
                .unwrap_or_default();

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

            let preferred_leader = crate::leadership::desired_leader(
                current.as_ref().and_then(|a| a.preferred_leader.as_ref()),
                &desired,
                &healthy_set,
                observed_leader.get(&shard),
            );

            let changed = match &current {
                None => !desired.is_empty(),
                Some(a) => a.replicas != desired || a.preferred_leader != preferred_leader,
            };
            if changed {
                self.placement.assign(ShardAssignment {
                    shard_id: shard,
                    replicas: desired,
                    preferred_leader,
                });
            }
        }
    }

    /// Background loop: sweep failures, then reconcile, on an interval.
    pub async fn run(self: Arc<Self>) {
        loop {
            let now = now_ms();
            let newly_dead = self.membership.sweep(now);
            if !newly_dead.is_empty() {
                tracing::warn!(
                    ?newly_dead,
                    "nodes declared dead; re-replicating their shards"
                );
            }
            self.reconcile();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::MembershipConfig;
    use crate::model::HeartbeatReport;

    fn hb(domain: &str) -> HeartbeatReport {
        HeartbeatReport {
            address: "10.0.0.1:8090".to_string(),
            failure_domain: domain.to_string(),
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
        Scheduler::new(membership, placement, plan)
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
}
