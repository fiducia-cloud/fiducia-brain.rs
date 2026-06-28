//! Shard placement map.
//!
//! The authoritative answer to "which nodes hold shard N, and who should lead
//! it?". Data-plane nodes fetch this map and reconcile their hosted replicas
//! toward it; the [`crate::scheduler`] rewrites it as nodes join, fail, or
//! rebalance.
//!
//! In this process the table is in-memory; production HA persists/replicates the
//! same assignments through the brain control-plane store.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::model::{PlacementPolicy, ShardAssignment, ShardId};

/// The cluster-wide shard → replicas/leader map.
pub struct Placement {
    shard_count: u32,
    assignments: Mutex<HashMap<ShardId, ShardAssignment>>,
    policies: Mutex<HashMap<ShardId, PlacementPolicy>>,
}

impl Placement {
    pub fn new(shard_count: u32) -> Self {
        Placement {
            shard_count,
            assignments: Mutex::new(HashMap::new()),
            policies: Mutex::new(HashMap::new()),
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

    /// Current placement policy for one shard, if an operator has set one.
    pub fn policy(&self, shard: ShardId) -> Option<PlacementPolicy> {
        self.policies.lock().unwrap().get(&shard).cloned()
    }

    /// All namespace placement policies, sorted for stable API output.
    pub fn policies_snapshot(&self) -> Vec<PlacementPolicy> {
        let mut v: Vec<_> = self.policies.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| {
            a.shard_id
                .cmp(&b.shard_id)
                .then(a.namespace.cmp(&b.namespace))
        });
        v
    }

    /// Set or replace a namespace placement policy for the shard it hashes to.
    pub fn set_policy(&self, policy: PlacementPolicy) {
        self.policies
            .lock()
            .unwrap()
            .insert(policy.shard_id, policy);
    }

    /// Install a new assignment for a shard (called by the scheduler).
    ///
    /// Production HA should commit this through the brain control-plane store so
    /// placement is durable and consistent across brain nodes.
    pub fn assign(&self, assignment: ShardAssignment) {
        self.assignments
            .lock()
            .unwrap()
            .insert(assignment.shard_id, assignment);
    }
}
