//! Placement math — the pure decision the [`crate::scheduler`] drives toward.
//!
//! Given a shard's *current* replicas and the set of *healthy* candidate nodes,
//! [`plan_replicas`] returns the *desired* replica set. One function covers every
//! reconciliation phase, because they're all the same question — "what should
//! this shard's replicas be?" — under different inputs:
//!
//! | Situation | Input | Result |
//! |-----------|-------|--------|
//! | initial placement | `current = []` | RF fresh replicas, spread across domains |
//! | re-replication | a replica is no longer healthy | drop it, add a healthy one |
//! | scale up | a new healthy node (load 0) appears | it wins least-loaded fills |
//! | scale down | a node is `Draining` (not in `healthy`) | it's dropped + replaced |
//!
//! Balance objectives, in priority order: **RF replicas per shard**, spread
//! across **distinct failure domains** (so one domain loss can't take a quorum),
//! then **even replica load** per node, then a stable node-id tiebreak (so the
//! plan is deterministic and doesn't churn).

use crate::model::NodeId;

/// A placement candidate: a healthy node, its failure domain, and how many
/// replicas it already holds (for least-loaded balancing).
#[derive(Debug, Clone)]
pub struct NodeSlot {
    pub node_id: NodeId,
    pub domain: String,
    pub load: u32,
}

/// Compute the desired replica set for one shard.
///
/// Keeps the current replicas that are still healthy (no needless data movement),
/// drops the rest, and fills up to `rf` from the healthy candidates — preferring a
/// fresh failure domain, then the least-loaded node, then the lowest node id.
pub fn plan_replicas(current: &[NodeId], healthy: &[NodeSlot], rf: u32) -> Vec<NodeId> {
    let healthy_ids: std::collections::HashSet<&str> =
        healthy.iter().map(|s| s.node_id.as_str()).collect();

    // Keep still-healthy current replicas (preserve order → stability).
    let mut chosen: Vec<NodeId> = current
        .iter()
        .filter(|id| healthy_ids.contains(id.as_str()))
        .cloned()
        .collect();
    let mut used_domains: std::collections::HashSet<String> = chosen
        .iter()
        .filter_map(|id| healthy.iter().find(|s| &s.node_id == id))
        .map(|s| s.domain.clone())
        .collect();

    // Remaining candidates, not already chosen.
    let mut remaining: Vec<&NodeSlot> = healthy
        .iter()
        .filter(|s| !chosen.contains(&s.node_id))
        .collect();

    while (chosen.len() as u32) < rf && !remaining.is_empty() {
        // Prefer a candidate in a fresh domain; if none, take any (better to be
        // under-spread than under-replicated). Tiebreak: load, then node id.
        let pick = remaining
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                let a_fresh = !used_domains.contains(&a.domain);
                let b_fresh = !used_domains.contains(&b.domain);
                b_fresh
                    .cmp(&a_fresh) // fresh-domain first (true > false)
                    .then(a.load.cmp(&b.load))
                    .then(a.node_id.cmp(&b.node_id))
            })
            .map(|(i, _)| i);
        let Some(i) = pick else { break };
        let slot = remaining.remove(i);
        used_domains.insert(slot.domain.clone());
        chosen.push(slot.node_id.clone());
    }

    // If RF shrank below the kept count, drop the most-loaded extras.
    if (chosen.len() as u32) > rf {
        chosen.sort_by_key(|id| {
            healthy
                .iter()
                .find(|s| &s.node_id == id)
                .map(|s| std::cmp::Reverse(s.load))
                .unwrap_or(std::cmp::Reverse(0))
        });
        chosen.truncate(rf as usize);
    }
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(id: &str, domain: &str, load: u32) -> NodeSlot {
        NodeSlot {
            node_id: id.to_string(),
            domain: domain.to_string(),
            load,
        }
    }
    fn ids(xs: &[&str]) -> Vec<NodeId> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn initial_placement_spreads_rf_across_distinct_domains() {
        // 3 domains, RF=3 → one replica per domain.
        let healthy = vec![
            slot("a1", "gcp", 0),
            slot("a2", "gcp", 0),
            slot("b1", "aws", 0),
            slot("c1", "hetzner", 0),
        ];
        let plan = plan_replicas(&[], &healthy, 3);
        assert_eq!(plan.len(), 3);
        let domains: std::collections::HashSet<_> = plan
            .iter()
            .map(|id| {
                healthy
                    .iter()
                    .find(|s| &s.node_id == id)
                    .unwrap()
                    .domain
                    .clone()
            })
            .collect();
        assert_eq!(domains.len(), 3, "one replica per failure domain");
    }

    #[test]
    fn rereplication_drops_dead_replica_and_keeps_the_healthy_ones() {
        // Shard was on [a,b,c]; b is gone (not in healthy). d is the fresh node.
        let healthy = vec![
            slot("a", "gcp", 5),
            slot("c", "hetzner", 5),
            slot("d", "aws", 0),
        ];
        let plan = plan_replicas(&ids(&["a", "b", "c"]), &healthy, 3);
        assert!(plan.contains(&"a".to_string()));
        assert!(plan.contains(&"c".to_string()));
        assert!(
            plan.contains(&"d".to_string()),
            "the lost replica is replaced"
        );
        assert!(
            !plan.contains(&"b".to_string()),
            "the dead replica is dropped"
        );
    }

    #[test]
    fn fills_prefer_least_loaded_when_domains_are_equal() {
        // All one domain → domain preference is moot; least-loaded wins.
        let healthy = vec![slot("busy", "d", 100), slot("idle", "d", 0)];
        let plan = plan_replicas(&[], &healthy, 1);
        assert_eq!(plan, ids(&["idle"]));
    }

    #[test]
    fn scale_down_evacuates_a_node_no_longer_healthy() {
        // Draining node "a" isn't in `healthy`; its replica moves to "c".
        let healthy = vec![slot("b", "aws", 1), slot("c", "hetzner", 0)];
        let plan = plan_replicas(&ids(&["a", "b"]), &healthy, 2);
        assert!(!plan.contains(&"a".to_string()));
        assert!(plan.contains(&"b".to_string()));
        assert!(plan.contains(&"c".to_string()));
    }

    #[test]
    fn under_capacity_returns_what_it_can_without_panicking() {
        // Only 2 healthy nodes but RF=3 → place 2 now; re-replicate when a 3rd joins.
        let healthy = vec![slot("a", "gcp", 0), slot("b", "aws", 0)];
        let plan = plan_replicas(&[], &healthy, 3);
        assert_eq!(plan.len(), 2);
    }
}
