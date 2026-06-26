//! Cluster membership + failure detection (skeleton).
//!
//! The brain tracks every data-plane node: who has joined, who is heartbeating,
//! and who has gone silent. Failure detection here is what triggers shard
//! re-placement — when a node is declared [`NodeHealth::Dead`], the
//! [`crate::scheduler`] re-replicates its shards onto healthy nodes.
//!
//! Skeleton: holds the node table; the heartbeat-driven liveness transitions and
//! the periodic failure sweep are `TODO`s.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::model::{NodeId, NodeInfo};

/// Registry of known data-plane nodes and their health.
pub struct Membership {
    nodes: Mutex<HashMap<NodeId, NodeInfo>>,
    // TODO(config): heartbeat interval, suspect timeout, dead timeout.
}

impl Membership {
    pub fn new() -> Self {
        Membership {
            nodes: Mutex::new(HashMap::new()),
        }
    }

    /// Record a heartbeat from a node, refreshing its `last_seen` and reported
    /// shard set (registering it if new).
    ///
    /// TODO: upsert NodeInfo, set health Healthy, store hosted/leading shards,
    /// and emit a membership-change so the scheduler can reconcile.
    pub fn heartbeat(&self, _node_id: &NodeId) {
        let _guard = self.nodes.lock().unwrap();
        // TODO
    }

    /// Begin draining a node ahead of removal (scale-down / maintenance):
    /// the scheduler moves its replicas/leadership away before it leaves.
    pub fn drain(&self, _node_id: &NodeId) {
        // TODO: set health = Draining.
    }

    /// Snapshot of all known nodes.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        self.nodes.lock().unwrap().values().cloned().collect()
    }

    /// Periodic failure sweep: demote silent nodes Healthy→Suspect→Dead by
    /// elapsed time since `last_seen`, returning the set newly declared Dead so
    /// the scheduler can re-place their shards.
    ///
    /// TODO: implement the time-based transitions.
    pub fn sweep(&self, _now_ms: u64) -> Vec<NodeId> {
        Vec::new()
    }
}
