//! Control-plane HTTP API.
//!
//! Two audiences:
//!   * **data-plane nodes** heartbeat in and fetch the placement map they should
//!     reconcile toward;
//!   * **operators / orchestration** view membership and adjust the scale plan.
//!
//! Routes (mounted under `/v1`):
//!   * `GET    /v1/nodes`                     — cluster membership view
//!   * `POST   /v1/nodes/{id}/heartbeat`      — node liveness + reported shards
//!   * `DELETE /v1/nodes/{id}`                — drain + remove a node (scale-down)
//!   * `GET    /v1/placement`                 — full shard map (nodes poll this)
//!   * `GET    /v1/placement/{shard}`         — assignment for one shard
//!   * `POST   /v1/scale`                     — set the desired `ScalePlan`
//!   * `GET    /v1/status`                    — control-plane status

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::{ClusterConfig, SUPPORTED_REPLICATION_FACTOR};
use crate::membership::Membership;
use crate::model::{
    HeartbeatReport, NodeHealth, PlacementPolicy, PlacementPolicyUpdate, ScalePlan,
};
use crate::placement::Placement;

/// Shared control-plane state handed to handlers.
#[derive(Clone)]
pub struct BrainState {
    pub config: ClusterConfig,
    pub membership: Arc<Membership>,
    pub placement: Arc<Placement>,
    /// The live scale intent the reconciler drives toward (`POST /v1/scale`).
    pub plan: Arc<Mutex<ScalePlan>>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn router(state: BrainState) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/config", get(config))
        .route("/route", get(route_key))
        .route("/nodes", get(list_nodes))
        .route("/nodes/:id/heartbeat", post(heartbeat))
        .route("/nodes/:id", axum::routing::delete(remove_node))
        .route("/placement", get(placement))
        .route("/placement/:shard", get(placement_shard))
        .route("/policies", get(list_policies).post(set_policy))
        .route("/scale", post(set_scale))
        .with_state(state)
}

/// `GET /v1/status` — control-plane summary.
async fn status(State(s): State<BrainState>) -> Json<Value> {
    let nodes = s.membership.snapshot();
    let placement = s.placement.snapshot();
    let plan = s.plan.lock().unwrap().clone();
    let rf = SUPPORTED_REPLICATION_FACTOR as usize;

    let mut nodes_by_health: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut nodes_by_failure_domain: BTreeMap<String, usize> = BTreeMap::new();
    let mut health_by_node: HashMap<String, NodeHealth> = HashMap::new();
    let mut healthy_failure_domains = HashSet::new();
    let mut healthy_clouds = HashSet::new();
    let mut healthy_clusters = HashSet::new();

    for node in &nodes {
        *nodes_by_health.entry(health_name(node.health)).or_default() += 1;
        *nodes_by_failure_domain
            .entry(non_empty_or_unknown(&node.failure_domain))
            .or_default() += 1;
        health_by_node.insert(node.node_id.clone(), node.health);

        if node.health == NodeHealth::Healthy {
            healthy_failure_domains.insert(non_empty_or_unknown(&node.failure_domain));
            if !node.cloud_provider.trim().is_empty() {
                healthy_clouds.insert(node.cloud_provider.trim().to_ascii_lowercase());
            }
            let cluster = if !node.cluster_id.trim().is_empty() {
                node.cluster_id.trim().to_ascii_lowercase()
            } else {
                non_empty_or_unknown(&node.failure_domain)
            };
            healthy_clusters.insert(cluster);
        }
    }

    let placed_shards = placement.len();
    let unplaced_shards = (s.config.shard_count as usize).saturating_sub(placed_shards);
    let under_replicated_shards = placement
        .iter()
        .filter(|assignment| assignment.replicas.len() < rf)
        .count()
        + unplaced_shards;
    let leaderless_shards = placement
        .iter()
        .filter(|assignment| assignment.preferred_leader.is_none())
        .count()
        + unplaced_shards;
    let shards_with_unhealthy_replicas = placement
        .iter()
        .filter(|assignment| {
            assignment
                .replicas
                .iter()
                .any(|node_id| health_by_node.get(node_id) != Some(&NodeHealth::Healthy))
        })
        .count();
    let scale_target_met = nodes
        .iter()
        .filter(|node| node.health == NodeHealth::Healthy)
        .count()
        >= plan.target_nodes.max(SUPPORTED_REPLICATION_FACTOR) as usize;
    let topology_ready = healthy_failure_domains.len() >= rf;
    let placement_ready = unplaced_shards == 0
        && under_replicated_shards == 0
        && leaderless_shards == 0
        && shards_with_unhealthy_replicas == 0;

    Json(json!({
        "service": "fiducia-brain",
        "version": env!("CARGO_PKG_VERSION"),
        "cluster_id": s.config.cluster_id,
        "local_cluster": s.config.local_cluster,
        "nodes": s.membership.snapshot().len(),
        "shard_count": s.config.shard_count,
        "replication_factor": s.config.replication_factor,
        "scale_plan": plan,
        "ready": topology_ready && placement_ready,
        "topology": {
            "required_failure_domains": rf,
            "healthy_failure_domains": healthy_failure_domains.len(),
            "healthy_cloud_providers": healthy_clouds.len(),
            "healthy_kubernetes_clusters": healthy_clusters.len(),
            "nodes_by_health": nodes_by_health,
            "nodes_by_failure_domain": nodes_by_failure_domain,
            "scale_target_met": scale_target_met,
        },
        "placement": {
            "placed_shards": placed_shards,
            "unplaced_shards": unplaced_shards,
            "under_replicated_shards": under_replicated_shards,
            "leaderless_shards": leaderless_shards,
            "shards_with_unhealthy_replicas": shards_with_unhealthy_replicas,
        },
        "brain_cluster": {
            "local_peer": s.config.local_cluster,
            "configured_remote_peers": s.config.brain_peers,
            "configured_members": s.config.brain_peers.len() + 1,
            "ha_configured": s.config.brain_peers.len() + 1 >= rf,
        },
    }))
}

/// `GET /v1/config` — the authoritative cluster configuration. Nodes, the load
/// balancer, and clients read this to learn `shard_count` (so they can compute
/// `key → shard` locally) and the replication factor.
async fn config(State(s): State<BrainState>) -> Json<Value> {
    Json(json!(s.config))
}

#[derive(Debug, Deserialize)]
struct RouteQuery {
    key: String,
}

/// `GET /v1/route?key=orders/checkout` — resolve a key all the way to its shard
/// and that shard's placement. `key → shard` is a local hash (no lookup);
/// `shard → nodes` comes from the central placement map.
async fn route_key(State(s): State<BrainState>, Query(q): Query<RouteQuery>) -> Json<Value> {
    let shard = s.config.shard_for(&q.key);
    Json(json!({
        "key": q.key,
        "shard": shard,
        "assignment": s.placement.get(shard),
    }))
}

/// `GET /v1/nodes` — membership view.
async fn list_nodes(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({ "nodes": s.membership.snapshot() }))
}

/// `POST /v1/nodes/{id}/heartbeat` — a data-plane node checks in with its
/// address, failure domain, and the shards it hosts/leads. Refreshes liveness.
async fn heartbeat(
    State(s): State<BrainState>,
    Path(id): Path<String>,
    report: Option<Json<HeartbeatReport>>,
) -> Json<Value> {
    let report = report.map(|Json(r)| r).unwrap_or_default();
    s.membership.heartbeat(&id, now_ms(), report);
    let health = s
        .membership
        .snapshot()
        .into_iter()
        .find(|n| n.node_id == id)
        .map(|n| n.health);
    Json(json!({ "ok": true, "node_id": id, "health": health }))
}

/// `DELETE /v1/nodes/{id}` — begin draining a node. The reconciler evacuates its
/// replicas/leadership onto healthy nodes; the operator removes it once empty.
async fn remove_node(State(s): State<BrainState>, Path(id): Path<String>) -> Json<Value> {
    let known = s.membership.drain(&id);
    Json(json!({ "draining": known, "node_id": id }))
}

/// `GET /v1/placement` — full shard map for nodes to reconcile against.
async fn placement(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({
        "shards": s.placement.snapshot(),
        "policies": s.placement.policies_snapshot(),
    }))
}

/// `GET /v1/placement/{shard}` — one shard's assignment.
async fn placement_shard(State(s): State<BrainState>, Path(shard): Path<u32>) -> Json<Value> {
    match s.placement.get(shard) {
        Some(a) => Json(json!(a)),
        None => Json(json!({ "error": "not_found", "shard": shard })),
    }
}

/// `GET /v1/policies` — namespace home-region/provider placement policies.
async fn list_policies(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({ "policies": s.placement.policies_snapshot() }))
}

/// `POST /v1/policies` — set the preferred leader region/provider for the shard
/// that owns a namespace.
async fn set_policy(
    State(s): State<BrainState>,
    Json(update): Json<PlacementPolicyUpdate>,
) -> Json<Value> {
    let namespace = update.namespace.trim().to_string();
    if namespace.is_empty() {
        return Json(json!({ "ok": false, "error": "namespace_required" }));
    }

    let policy = PlacementPolicy {
        shard_id: s.config.shard_for(&namespace),
        namespace,
        home_region: update.home_region.filter(|r| !r.trim().is_empty()),
        preferred_cloud_provider: update
            .preferred_cloud_provider
            .filter(|p| !p.trim().is_empty()),
    };
    s.placement.set_policy(policy.clone());
    Json(json!({ "ok": true, "policy": policy }))
}

/// `POST /v1/scale` — set the desired scale plan; the reconciler picks it up on
/// its next tick. `replication_factor` is fixed at RF=3 for the multi-cloud
/// baseline.
async fn set_scale(State(s): State<BrainState>, Json(mut plan): Json<ScalePlan>) -> Json<Value> {
    plan.replication_factor = SUPPORTED_REPLICATION_FACTOR;
    plan.target_nodes = plan.target_nodes.max(SUPPORTED_REPLICATION_FACTOR);
    *s.plan.lock().unwrap() = plan.clone();
    Json(json!({ "ok": true, "plan": plan }))
}

fn health_name(health: NodeHealth) -> &'static str {
    match health {
        NodeHealth::Healthy => "healthy",
        NodeHealth::Suspect => "suspect",
        NodeHealth::Dead => "dead",
        NodeHealth::Draining => "draining",
    }
}

fn non_empty_or_unknown(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        "unknown".to_string()
    } else {
        value.to_ascii_lowercase()
    }
}
