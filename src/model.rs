//! Shared control-plane types (skeleton).

use serde::{Deserialize, Serialize};

/// A data-plane node's stable id (matches `FIDUCIA_NODE_ID` on the node).
pub type NodeId = String;

/// A shard id (one independent Raft group in the data plane). Re-exported from
/// the shared routing crate so the type matches the node and load balancer.
pub use fiducia_routing::ShardId;

/// Liveness of a node, as judged by the brain's failure detector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeHealth {
    /// Heartbeating normally.
    Healthy,
    /// Missed recent heartbeats; placement decisions should avoid it.
    Suspect,
    /// Failure-detected; its shard replicas are being re-placed elsewhere.
    Dead,
    /// Administratively draining ahead of a scale-down / removal.
    Draining,
}

/// What the brain knows about one data-plane node.
#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,
    pub health: NodeHealth,
    /// Last heartbeat receipt (ms since epoch).
    pub last_seen_ms: u64,
    /// Shards this node reports hosting, and whether it leads them.
    pub hosted_shards: Vec<ShardId>,
    pub leading_shards: Vec<ShardId>,
}

/// The authoritative placement for one shard: which nodes replicate it and which
/// one the brain wants to lead it. Data-plane nodes reconcile toward this.
#[derive(Debug, Clone, Serialize)]
pub struct ShardAssignment {
    pub shard_id: ShardId,
    /// Nodes that should hold a replica of this shard.
    pub replicas: Vec<NodeId>,
    /// The node the brain prefers as leader (leadership balancing).
    pub preferred_leader: Option<NodeId>,
}

/// A scaling intent the reconciler drives toward.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalePlan {
    /// Desired number of healthy data-plane nodes.
    pub target_nodes: u32,
    /// Replicas per shard (the replication factor).
    pub replication_factor: u32,
}
