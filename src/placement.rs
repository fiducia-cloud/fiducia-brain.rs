//! Shard placement map (skeleton).
//!
//! The authoritative answer to "which nodes hold shard N, and who should lead
//! it?". Data-plane nodes fetch this map and reconcile their hosted replicas
//! toward it; the [`crate::scheduler`] rewrites it as nodes join, fail, or
//! rebalance.
//!
//! In a real deployment this map is itself replicated by the brain's own Raft
//! group, so the control plane survives losing a brain node. Skeleton: an
//! in-memory table with the assignment logic left as `TODO`s.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::model::{ShardAssignment, ShardId};

/// The cluster-wide shard → replicas/leader map.
pub struct Placement {
    shard_count: u32,
    assignments: Mutex<HashMap<ShardId, ShardAssignment>>,
}

impl Placement {
    pub fn new(shard_count: u32) -> Self {
        Placement {
            shard_count,
            assignments: Mutex::new(HashMap::new()),
        }
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Current assignment for one shard, if placed.
    pub fn get(&self, shard: ShardId) -> Option<ShardAssignment> {
        self.assignments.lock().unwrap().get(&shard).cloned()
    }

    /// The full shard map (served to data-plane nodes).
    pub fn snapshot(&self) -> Vec<ShardAssignment> {
        let mut v: Vec<_> = self.assignments.lock().unwrap().values().cloned().collect();
        v.sort_by_key(|a| a.shard_id);
        v
    }

    /// Install a new assignment for a shard (called by the scheduler).
    ///
    /// TODO(cluster): propose this through the brain's Raft group so the
    /// placement map is durable and consistent across brain nodes.
    pub fn assign(&self, assignment: ShardAssignment) {
        self.assignments
            .lock()
            .unwrap()
            .insert(assignment.shard_id, assignment);
    }
}
