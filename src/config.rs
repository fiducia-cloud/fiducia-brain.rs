//! Central cluster configuration + key→shard mapping.
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

pub const SUPPORTED_REPLICATION_FACTOR: u32 = 3;

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
    /// Replicas per shard (Raft group size). Fiducia's multi-cloud baseline is
    /// fixed at RF=3: one voter per Kubernetes cluster / cloud provider.
    pub replication_factor: u32,
    /// Local Kubernetes cluster name (`gcp`, `aws`, `hetzner`, ...), from the
    /// generated `fiducia-cluster` ConfigMap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_cluster: Option<String>,
    /// Cloud provider for the local brain member, when supplied by deployment
    /// config. Defaults to the cluster name if no explicit provider is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_provider: Option<String>,
    /// Physical cloud/edge region of the local brain member, if supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Other brain members this process should replicate control-plane state with.
    #[serde(default)]
    pub brain_peers: Vec<String>,
}

impl ClusterConfig {
    pub fn from_env() -> Self {
        let local_cluster = non_empty_env("FIDUCIA_CLUSTER");
        ClusterConfig {
            cluster_id: std::env::var("FIDUCIA_CLUSTER_ID")
                .unwrap_or_else(|_| "fiducia-local".to_string()),
            shard_count: positive_u32_env("FIDUCIA_SHARD_COUNT", 16),
            replication_factor: SUPPORTED_REPLICATION_FACTOR,
            cloud_provider: non_empty_env("FIDUCIA_CLOUD_PROVIDER")
                .or_else(|| non_empty_env("FIDUCIA_PLATFORM"))
                .or_else(|| local_cluster.clone()),
            region: non_empty_env("FIDUCIA_REGION"),
            local_cluster,
            brain_peers: csv_env("FIDUCIA_BRAIN_PEERS"),
        }
    }

    /// Map a key to its shard, via the shared `fiducia-routing` crate — the same
    /// mapping the node and load balancer compute, by construction.
    pub fn shard_for(&self, key: &str) -> ShardId {
        fiducia_routing::shard_for(key, self.shard_count.max(1))
    }
}

fn positive_u32_env(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().and_then(|s| {
        let value = s.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
}

fn csv_env(name: &str) -> Vec<String> {
    std::env::var(name)
        .ok()
        .map(|s| parse_csv(&s))
        .unwrap_or_default()
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                None
            } else {
                Some(part.to_string())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_keeps_supported_replication_factor_at_three() {
        let config = ClusterConfig::from_env();
        assert_eq!(config.replication_factor, SUPPORTED_REPLICATION_FACTOR);
        assert_eq!(SUPPORTED_REPLICATION_FACTOR, 3);
    }

    #[test]
    fn shard_for_is_safe_even_if_constructed_with_zero_shards() {
        let config = ClusterConfig {
            cluster_id: "test".to_string(),
            shard_count: 0,
            replication_factor: SUPPORTED_REPLICATION_FACTOR,
            local_cluster: None,
            cloud_provider: None,
            region: None,
            brain_peers: Vec::new(),
        };

        assert_eq!(config.shard_for("orders/42"), 0);
    }

    #[test]
    fn parse_csv_trims_and_drops_empty_peers() {
        assert_eq!(
            parse_csv("brain.gcp:9095, , brain.aws:9095"),
            vec!["brain.gcp:9095".to_string(), "brain.aws:9095".to_string()]
        );
    }
}
