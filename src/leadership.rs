//! Leader affinity — which node *should* lead a shard right now.
//!
//! Each shard has a **preferred leader** (its affinity target — chosen for
//! physical proximity / colocation with demand). Normally the preferred node
//! leads. If it fails, a healthy follower takes over (Raft elects it). When the
//! preferred node comes back healthy, leadership should **transfer back** to it.
//!
//! [`desired_leader`] is the pure decision the scheduler drives toward: given the
//! affinity target, the shard's replicas, the set of healthy nodes, and the
//! current leader, it returns who *should* lead. The scheduler then issues a Raft
//! leadership transfer when the desired leader differs from the observed one.

use std::collections::HashSet;

use crate::model::{NodeId, PlacementPolicy};

/// A candidate replica that can lead a shard.
#[derive(Debug, Clone)]
pub struct LeaderSlot {
    pub node_id: NodeId,
    pub cloud_provider: String,
    pub region: String,
    pub leader_load: u32,
}

/// Pick the preferred leader target for a shard.
///
/// Region affinity wins first, provider affinity second, then the least-loaded
/// leader candidate. Ties are broken in favor of the currently-observed leader
/// (`current`) so a brain restart adopts the data plane's existing leader instead
/// of needlessly transferring leadership, and finally by node id for deterministic
/// plans. If no policy exists, this still balances leaders across healthy replicas.
pub fn preferred_leader_for_policy(
    policy: Option<&PlacementPolicy>,
    replicas: &[NodeId],
    healthy: &HashSet<NodeId>,
    slots: &[LeaderSlot],
    current: Option<&NodeId>,
) -> Option<NodeId> {
    let home_region = policy.and_then(|p| p.home_region.as_deref());
    let preferred_cloud = policy.and_then(|p| p.preferred_cloud_provider.as_deref());

    slots
        .iter()
        .filter(|slot| {
            healthy.contains(&slot.node_id) && replicas.iter().any(|r| r == &slot.node_id)
        })
        .min_by(|a, b| {
            let a_region = home_region
                .map(|region| region.eq_ignore_ascii_case(&a.region))
                .unwrap_or(false);
            let b_region = home_region
                .map(|region| region.eq_ignore_ascii_case(&b.region))
                .unwrap_or(false);
            let a_cloud = preferred_cloud
                .map(|cloud| cloud.eq_ignore_ascii_case(&a.cloud_provider))
                .unwrap_or(false);
            let b_cloud = preferred_cloud
                .map(|cloud| cloud.eq_ignore_ascii_case(&b.cloud_provider))
                .unwrap_or(false);
            // On an otherwise-even tie, keep whoever is already leading (stability).
            let a_current = current == Some(&a.node_id);
            let b_current = current == Some(&b.node_id);

            b_region
                .cmp(&a_region)
                .then(b_cloud.cmp(&a_cloud))
                .then(a.leader_load.cmp(&b.leader_load))
                .then(b_current.cmp(&a_current))
                .then(a.node_id.cmp(&b.node_id))
        })
        .map(|slot| slot.node_id.clone())
}

/// Decide the desired leader for a shard.
///
/// Policy:
/// 1. **Affinity** — if the preferred node is a healthy replica, it leads
///    (this is what pulls leadership *back* once it recovers).
/// 2. **Stickiness** — otherwise keep the current leader if it's still a healthy
///    replica (don't churn leadership for no reason).
/// 3. **Failover** — otherwise the first healthy replica (deterministic order).
/// 4. `None` if no replica is healthy.
pub fn desired_leader(
    preferred: Option<&NodeId>,
    replicas: &[NodeId],
    healthy: &HashSet<NodeId>,
    current: Option<&NodeId>,
) -> Option<NodeId> {
    let ok = |n: &NodeId| healthy.contains(n) && replicas.iter().any(|r| r == n);

    if let Some(p) = preferred {
        if ok(p) {
            return Some(p.clone());
        }
    }
    if let Some(c) = current {
        if ok(c) {
            return Some(c.clone());
        }
    }
    replicas.iter().find(|r| healthy.contains(*r)).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(xs: &[&str]) -> HashSet<NodeId> {
        xs.iter().map(|s| s.to_string()).collect()
    }
    fn ids(xs: &[&str]) -> Vec<NodeId> {
        xs.iter().map(|s| s.to_string()).collect()
    }
    fn id(s: &str) -> NodeId {
        s.to_string()
    }
    fn leader_slot(id: &str, cloud: &str, region: &str, leader_load: u32) -> LeaderSlot {
        LeaderSlot {
            node_id: id.to_string(),
            cloud_provider: cloud.to_string(),
            region: region.to_string(),
            leader_load,
        }
    }

    /// The scenario from the design: preferred leader dies, a follower takes
    /// over, then the preferred leader returns and leadership comes back to it.
    #[test]
    fn affinity_failover_then_return() {
        let replicas = ids(&["a", "b", "c"]);
        let pref = id("a");

        // Steady state: preferred (a) is healthy -> a leads.
        assert_eq!(
            desired_leader(Some(&pref), &replicas, &set(&["a", "b", "c"]), None).as_deref(),
            Some("a"),
        );

        // a dies (current leader a is now unhealthy) -> fail over to first healthy (b).
        assert_eq!(
            desired_leader(Some(&pref), &replicas, &set(&["b", "c"]), Some(&id("a"))).as_deref(),
            Some("b"),
        );

        // a recovers -> affinity pulls leadership back to a (even though b leads now).
        assert_eq!(
            desired_leader(
                Some(&pref),
                &replicas,
                &set(&["a", "b", "c"]),
                Some(&id("b"))
            )
            .as_deref(),
            Some("a"),
        );
    }

    #[test]
    fn no_preferred_keeps_current_to_avoid_churn() {
        let replicas = ids(&["a", "b", "c"]);
        assert_eq!(
            desired_leader(None, &replicas, &set(&["a", "b", "c"]), Some(&id("b"))).as_deref(),
            Some("b"),
        );
    }

    #[test]
    fn preferred_that_is_not_a_replica_is_ignored() {
        let replicas = ids(&["a", "b", "c"]);
        let pref = id("z"); // not a replica of this shard
        assert_eq!(
            desired_leader(Some(&pref), &replicas, &set(&["a", "b"]), None).as_deref(),
            Some("a"),
        );
    }

    #[test]
    fn none_when_no_replica_is_healthy() {
        let replicas = ids(&["a", "b"]);
        assert_eq!(
            desired_leader(None, &replicas, &set(&["c"]), Some(&id("a"))),
            None,
        );
    }

    #[test]
    fn policy_prefers_home_region_then_balances_leader_load() {
        let replicas = ids(&["gcp-a", "aws-a", "aws-b", "hetzner-a"]);
        let healthy = set(&["gcp-a", "aws-a", "aws-b", "hetzner-a"]);
        let policy = PlacementPolicy {
            namespace: "tenant-a".to_string(),
            shard_id: 7,
            home_region: Some("us-east-1".to_string()),
            preferred_cloud_provider: None,
        };
        let slots = vec![
            leader_slot("gcp-a", "gcp", "us-central1", 0),
            leader_slot("aws-a", "aws", "us-east-1", 3),
            leader_slot("aws-b", "aws", "us-east-1", 1),
            leader_slot("hetzner-a", "hetzner", "nbg1", 0),
        ];

        assert_eq!(
            preferred_leader_for_policy(Some(&policy), &replicas, &healthy, &slots, None).as_deref(),
            Some("aws-b")
        );
    }

    #[test]
    fn no_policy_picks_least_loaded_replica() {
        let replicas = ids(&["a", "b", "c"]);
        let healthy = set(&["a", "b", "c"]);
        let slots = vec![
            leader_slot("a", "gcp", "us", 5),
            leader_slot("b", "aws", "us", 1),
            leader_slot("c", "hetzner", "eu", 3),
        ];

        assert_eq!(
            preferred_leader_for_policy(None, &replicas, &healthy, &slots, None).as_deref(),
            Some("b")
        );
    }
}
