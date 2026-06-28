//! Shard placement map (skeleton).
//!
//! The authoritative answer to "which nodes hold shard N, and who should lead
//! it?". Data-plane nodes fetch this map and reconcile their hosted replicas
//! toward it; the [`crate::scheduler`] rewrites it as nodes join, fail, or
//! rebalance.
//!
//! The assignment logic itself lives in [`crate::scheduler`] (it reconciles
//! observed membership toward the desired [`crate::model::ScalePlan`]); this type
//! is just the resulting map. It is authoritative in-memory today. Making it
//! survive losing a brain node — replicating it through the brain's own Raft
//! group — is the remaining HA work (see `assign`).

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
    /// Authoritative in-memory. HA follow-up: propose this through the brain's
    /// own Raft group so the placement map is durable and consistent across
    /// brain nodes (until then a brain restart re-derives it from heartbeats).
    pub fn assign(&self, assignment: ShardAssignment) {
        self.assignments
            .lock()
            .unwrap()
            .insert(assignment.shard_id, assignment);
    }
}
