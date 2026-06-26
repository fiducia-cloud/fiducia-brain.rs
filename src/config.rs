//! Central cluster configuration + key→shard mapping (skeleton).
//!
//! This is the **authoritative, cluster-wide configuration** every component
//! reads. The single most important rule it encodes:
//!
//! > **Shard count is fixed at cluster creation; node count is elastic.**
//!
//! `shard_count` (and the hash) define `key → shard`, which therefore **never
//! changes** as you add or remove nodes — no key ever has to move because the
//! cluster grew. What *does* change with scaling is the separate `shard → nodes`
//! placement map ([`crate::placement`]). Keep the two layers distinct:
//!
//! ```text
//!   key ──hash(key) % shard_count──▶ shard      (stable; stateless; no lookup)
//!   shard ──placement map──────────▶ nodes      (elastic; central; changes on scale)
//! ```
//!
//! `key → shard` needs no central lookup — node, load balancer, and clients all
//! compute it locally. Only `shard → nodes` requires the central map. This whole
//! config is meant to live in the brain's own Raft group so it is consistent and
//! survives a brain node loss.

use serde::Serialize;

use crate::model::ShardId;

/// Cluster-wide configuration. The shard parameters are effectively immutable
/// for the cluster's life; changing `shard_count` would remap every key and is a
/// full migration, not a scaling operation.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterConfig {
    /// Stable identifier for this cluster.
    pub cluster_id: String,
    /// Number of shards (independent Raft groups). **Fixed at creation.** Pick it
    /// generously (e.g. 256/1024) so there are always enough shards to spread
    /// across the largest node count you expect.
    pub shard_count: u32,
    /// Replicas per shard (Raft group size), e.g. 3. Independent of node count;
    /// the cluster can never have fewer than this many nodes.
    pub replication_factor: u32,
}

impl ClusterConfig {
    pub fn from_env() -> Self {
        ClusterConfig {
            cluster_id: std::env::var("FIDUCIA_CLUSTER_ID")
                .unwrap_or_else(|_| "fiducia-local".to_string()),
            shard_count: std::env::var("FIDUCIA_SHARD_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(16),
            replication_factor: std::env::var("FIDUCIA_REPLICATION_FACTOR")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
        }
    }

    /// Map a key to its shard, via the shared `fiducia-routing` crate — the same
    /// mapping the node and load balancer compute, by construction.
    pub fn shard_for(&self, key: &str) -> ShardId {
        fiducia_routing::shard_for(key, self.shard_count)
    }
}
