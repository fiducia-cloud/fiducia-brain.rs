//! Cluster membership + failure detection.
//!
//! The brain tracks every data-plane node: who has joined, who is heartbeating,
//! and who has gone silent. Failure detection here is what triggers shard
//! re-placement — when a node is declared [`NodeHealth::Dead`], the
//! [`crate::scheduler`] re-replicates its shards onto healthy nodes.
//!
//! Liveness is a simple, time-based φ-less detector: a node is `Healthy` while it
//! heartbeats; after `suspect_after` ms of silence it is `Suspect` (placement
//! avoids it but doesn't move data yet — it may just be a blip); after
//! `dead_after` ms it is `Dead` and its replicas are re-placed.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::model::{HeartbeatReport, NodeHealth, NodeId, NodeInfo};

/// Failure-detector timing. Defaults assume a ~1s heartbeat from the sidecar.
#[derive(Debug, Clone, Copy)]
pub struct MembershipConfig {
    /// Silence before a node is demoted Healthy → Suspect.
    pub suspect_after_ms: u64,
    /// Silence before a node is declared Suspect → Dead (and re-replicated).
    pub dead_after_ms: u64,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        MembershipConfig {
            suspect_after_ms: std::env::var("FIDUCIA_SUSPECT_AFTER_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3_000),
            dead_after_ms: std::env::var("FIDUCIA_DEAD_AFTER_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000),
        }
    }
}

/// Registry of known data-plane nodes and their health.
pub struct Membership {
    nodes: Mutex<HashMap<NodeId, NodeInfo>>,
    config: MembershipConfig,
}

impl Membership {
    pub fn new(config: MembershipConfig) -> Self {
        Membership {
            nodes: Mutex::new(HashMap::new()),
            config,
        }
    }

    /// Record a heartbeat from a node: refresh `last_seen`, mark it Healthy, and
    /// store its reported address, failure domain, and shard set (registering it
    /// if new). A `Draining` node that keeps heartbeating stays `Draining` — the
    /// operator's intent to remove it isn't undone by liveness.
    pub fn heartbeat(&self, node_id: &NodeId, now_ms: u64, report: HeartbeatReport) {
        let mut nodes = self.nodes.lock().unwrap();
        let entry = nodes.entry(node_id.clone()).or_insert_with(|| NodeInfo {
            node_id: node_id.clone(),
            address: report.address.clone(),
            health: NodeHealth::Healthy,
            failure_domain: report.failure_domain.clone(),
            last_seen_ms: now_ms,
            hosted_shards: Vec::new(),
            leading_shards: Vec::new(),
        });
        entry.last_seen_ms = now_ms;
        if !report.address.is_empty() {
            entry.address = report.address;
        }
        if !report.failure_domain.is_empty() {
            entry.failure_domain = report.failure_domain;
        }
        entry.hosted_shards = report.hosted_shards;
        entry.leading_shards = report.leading_shards;
        if entry.health != NodeHealth::Draining {
            entry.health = NodeHealth::Healthy;
        }
    }

    /// Begin draining a node ahead of removal (scale-down / maintenance): the
    /// scheduler moves its replicas/leadership away before it leaves. Returns
    /// whether the node was known.
    pub fn drain(&self, node_id: &NodeId) -> bool {
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(info) = nodes.get_mut(node_id) {
            info.health = NodeHealth::Draining;
            true
        } else {
            false
        }
    }

    /// Drop a node from the registry entirely (after it has been drained and holds
    /// nothing). Returns whether it was present. Exposed for the operator
    /// "finalize removal" step once a drained node's replicas have all moved.
    #[allow(dead_code)]
    pub fn forget(&self, node_id: &NodeId) -> bool {
        self.nodes.lock().unwrap().remove(node_id).is_some()
    }

    /// Snapshot of all known nodes.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        let mut v: Vec<NodeInfo> = self.nodes.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }

    /// Periodic failure sweep: demote silent nodes Healthy→Suspect→Dead by elapsed
    /// time since `last_seen`, returning the set **newly** declared Dead so the
    /// scheduler can re-place their shards. `Draining` nodes are left as-is (they
    /// are being removed deliberately, not failing).
    pub fn sweep(&self, now_ms: u64) -> Vec<NodeId> {
        let mut newly_dead = Vec::new();
        let mut nodes = self.nodes.lock().unwrap();
        for info in nodes.values_mut() {
            if info.health == NodeHealth::Draining {
                continue;
            }
            let silent_for = now_ms.saturating_sub(info.last_seen_ms);
            let new_health = if silent_for >= self.config.dead_after_ms {
                NodeHealth::Dead
            } else if silent_for >= self.config.suspect_after_ms {
                NodeHealth::Suspect
            } else {
                NodeHealth::Healthy
            };
            if new_health == NodeHealth::Dead && info.health != NodeHealth::Dead {
                newly_dead.push(info.node_id.clone());
            }
            info.health = new_health;
        }
        newly_dead
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(domain: &str, shards: &[u32]) -> HeartbeatReport {
        HeartbeatReport {
            address: "10.0.0.1:8090".to_string(),
            failure_domain: domain.to_string(),
            hosted_shards: shards.to_vec(),
            leading_shards: shards.to_vec(),
        }
    }

    #[test]
    fn heartbeat_registers_then_silence_walks_healthy_suspect_dead() {
        let m = Membership::new(MembershipConfig {
            suspect_after_ms: 1_000,
            dead_after_ms: 5_000,
        });
        m.heartbeat(&"a".to_string(), 0, report("gcp", &[0, 1]));

        // Still fresh.
        assert!(m.sweep(500).is_empty());
        assert_eq!(m.snapshot()[0].health, NodeHealth::Healthy);

        // Past suspect, before dead.
        assert!(m.sweep(2_000).is_empty());
        assert_eq!(m.snapshot()[0].health, NodeHealth::Suspect);

        // Past dead → reported exactly once.
        assert_eq!(m.sweep(6_000), vec!["a".to_string()]);
        assert_eq!(m.snapshot()[0].health, NodeHealth::Dead);
        assert!(m.sweep(7_000).is_empty(), "dead is reported only on the transition");

        // A fresh heartbeat resurrects it.
        m.heartbeat(&"a".to_string(), 7_000, report("gcp", &[0, 1]));
        assert_eq!(m.snapshot()[0].health, NodeHealth::Healthy);
    }

    #[test]
    fn draining_is_sticky_across_heartbeats_and_sweeps() {
        let m = Membership::new(MembershipConfig::default());
        m.heartbeat(&"a".to_string(), 0, report("aws", &[2]));
        assert!(m.drain(&"a".to_string()));
        // Keeps heartbeating, but stays Draining (operator intent wins).
        m.heartbeat(&"a".to_string(), 100, report("aws", &[2]));
        assert_eq!(m.snapshot()[0].health, NodeHealth::Draining);
        // And the failure sweep never flips a draining node to Dead.
        assert!(m.sweep(1_000_000).is_empty());
        assert_eq!(m.snapshot()[0].health, NodeHealth::Draining);
    }
}
