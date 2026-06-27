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

use crate::model::NodeId;

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
            desired_leader(Some(&pref), &replicas, &set(&["a", "b", "c"]), Some(&id("b"))).as_deref(),
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
}
