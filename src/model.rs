//! Shared control-plane types.

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
    /// Cloud this node belongs to (`aws`, `gcp`, `hetzner`, ...). Kept separate
    /// from region so placement can reason about both provider failure and user
    /// proximity.
    #[serde(default)]
    pub cloud_provider: String,
    /// Cloud/edge region (`us-east-1`, `europe-west1`, `nbg1`, ...).
    #[serde(default)]
    pub region: String,
    /// Kubernetes cluster identity, distinct from provider/region.
    #[serde(default)]
    pub cluster_id: String,
    /// Failure domain (region/AZ/rack) the sidecar reports. The scheduler spreads
    /// a shard's replicas across **distinct** domains so one domain loss can't
    /// take a quorum. Empty string = "unknown" (treated as its own domain).
    #[serde(default)]
    pub failure_domain: String,
    /// Last heartbeat receipt (ms since epoch).
    pub last_seen_ms: u64,
    /// Shards this node reports hosting, and whether it leads them.
    pub hosted_shards: Vec<ShardId>,
    pub leading_shards: Vec<ShardId>,
    /// Highest heartbeat `seq` accepted from this node. A heartbeat whose `seq`
    /// is not strictly greater is a reordered or duplicated delivery and is
    /// ignored, so older in-flight state can never overwrite newer (see
    /// [`HeartbeatReport::seq`]).
    #[serde(default)]
    pub last_seq: u64,
}

/// The body a data-plane node (its sidecar) posts to `/v1/nodes/{id}/heartbeat`.
/// `Serialize` too, so a follower can forward it verbatim to the leader.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatReport {
    /// Where to reach this node (host:port), echoed into the placement redirects.
    #[serde(default)]
    pub address: String,
    /// Cloud provider for placement/failure-domain normalization.
    #[serde(default)]
    pub cloud_provider: String,
    /// Region for leader affinity and latency-aware placement.
    #[serde(default)]
    pub region: String,
    /// Kubernetes cluster identity.
    #[serde(default)]
    pub cluster_id: String,
    /// Failure domain (region/AZ/rack).
    #[serde(default)]
    pub failure_domain: String,
    /// Shards the node currently hosts a replica of.
    #[serde(default)]
    pub hosted_shards: Vec<ShardId>,
    /// Subset of `hosted_shards` this node currently leads.
    #[serde(default)]
    pub leading_shards: Vec<ShardId>,
    /// Monotonic per-node sequence the sidecar stamps on each heartbeat (seeded
    /// from its boot time, incremented per send). The brain ignores any heartbeat
    /// whose `seq` is not strictly greater than the last it accepted, so a
    /// reordered or duplicated POST can't revert newer reported state. `0` (the
    /// default) means the sender doesn't sequence — never rejected on that basis.
    #[serde(default)]
    pub seq: u64,
}

/// The authoritative placement for one shard: which nodes replicate it and which
/// one the brain wants to lead it. Data-plane nodes reconcile toward this.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardAssignment {
    pub shard_id: ShardId,
    /// Nodes that should hold a replica of this shard.
    pub replicas: Vec<NodeId>,
    /// The node the brain prefers as leader (leadership balancing).
    pub preferred_leader: Option<NodeId>,
    /// Region requested by the shard/namespace placement policy, when any.
    #[serde(default)]
    pub preferred_region: Option<String>,
    /// Cloud requested by the shard/namespace placement policy, when any.
    #[serde(default)]
    pub preferred_cloud_provider: Option<String>,
}

/// Region/provider affinity for the shard that owns a namespace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacementPolicy {
    /// Namespace/customer key used to pick the shard.
    pub namespace: String,
    /// Shard affected by this policy (`hash(namespace) % shard_count`).
    pub shard_id: ShardId,
    /// Preferred user/home region for this namespace.
    #[serde(default)]
    pub home_region: Option<String>,
    /// Optional provider affinity, mostly useful when two providers exist in the
    /// same broad geography.
    #[serde(default)]
    pub preferred_cloud_provider: Option<String>,
}

/// Body accepted by `POST /v1/policies`.
#[derive(Debug, Clone, Deserialize)]
pub struct PlacementPolicyUpdate {
    pub namespace: String,
    #[serde(default)]
    pub home_region: Option<String>,
    #[serde(default)]
    pub preferred_cloud_provider: Option<String>,
}

/// A scaling intent the reconciler drives toward.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalePlan {
    /// Desired number of healthy data-plane nodes.
    pub target_nodes: u32,
    /// Replicas per shard (the replication factor).
    pub replication_factor: u32,
}
