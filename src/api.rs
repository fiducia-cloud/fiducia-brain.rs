//! Control-plane HTTP API (skeleton handlers).
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

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::cluster::{Command, ControlPlane};
use crate::config::ClusterConfig;
use crate::membership::Membership;
use crate::model::{HeartbeatReport, NodeHealth, ScalePlan};
use crate::placement::Placement;

/// Shared control-plane state handed to handlers.
#[derive(Clone)]
pub struct BrainState {
    pub config: ClusterConfig,
    pub membership: Arc<Membership>,
    pub placement: Arc<Placement>,
    /// The live scale intent the reconciler drives toward (`POST /v1/scale`).
    pub plan: Arc<Mutex<ScalePlan>>,
    /// The brain's own control plane; durable writes (`/v1/scale`) go through it.
    pub control_plane: Arc<dyn ControlPlane>,
    /// Client for forwarding writes/heartbeats from a follower to the leader.
    pub http: reqwest::Client,
}

/// Forward a request to the leader and relay its JSON response. Used when a
/// non-leader receives a write or a heartbeat (liveness + durable state live on
/// the leader). Keeps the data-plane sidecar dumb: it heartbeats any member and
/// the brain routes internally.
async fn forwarded(req: reqwest::RequestBuilder) -> Json<Value> {
    match req.send().await {
        Ok(resp) => Json(
            resp.json::<Value>()
                .await
                .unwrap_or_else(|_| json!({ "ok": true, "forwarded": true })),
        ),
        Err(err) => Json(json!({
            "ok": false,
            "error": "leader_forward_failed",
            "detail": err.to_string(),
        })),
    }
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
        .route("/scale", post(set_scale))
        .with_state(state)
}

/// `GET /v1/status` — control-plane summary + placement health. Surfaces the gap
/// between *desired* (`ScalePlan`) and *observed* (membership) so operators can
/// see at a glance whether the cluster is converged or under-replicated.
async fn status(State(s): State<BrainState>) -> Json<Value> {
    let nodes = s.membership.snapshot();
    let healthy = nodes
        .iter()
        .filter(|n| n.health == NodeHealth::Healthy)
        .count();
    let plan = s.plan.lock().unwrap().clone();
    let rf = plan.replication_factor.max(1);
    let placed = s.placement.snapshot();
    let under_replicated = placed
        .iter()
        .filter(|a| (a.replicas.len() as u32) < rf)
        .count();
    Json(json!({
        "service": "fiducia-brain",
        "version": env!("CARGO_PKG_VERSION"),
        "cluster_id": s.config.cluster_id,
        "shard_count": s.config.shard_count,
        "replication_factor": rf,
        "nodes": nodes.len(),
        "healthy_nodes": healthy,
        "desired_nodes": plan.target_nodes,
        "placed_shards": placed.len(),
        "under_replicated_shards": under_replicated,
        "placement_generation": s.placement.generation(),
        // Brain's own control plane: which member is driving reconciliation.
        "is_leader": s.control_plane.is_leader(),
        "leader": s.control_plane.leader_addr(),
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
    let health = s.membership.heartbeat(&id, now_ms(), report);
    Json(json!({ "ok": true, "node_id": id, "health": health }))
}

/// `DELETE /v1/nodes/{id}` — begin draining a node. The reconciler evacuates its
/// replicas/leadership onto healthy nodes; the operator removes it once empty.
async fn remove_node(State(s): State<BrainState>, Path(id): Path<String>) -> Json<Value> {
    let known = s.membership.drain(&id);
    Json(json!({ "draining": known, "node_id": id }))
}

/// `GET /v1/placement` — full shard map for nodes to reconcile against. The
/// `generation` lets pollers skip re-diffing the map when nothing changed.
async fn placement(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({
        "generation": s.placement.generation(),
        "shards": s.placement.snapshot(),
    }))
}

/// `GET /v1/placement/{shard}` — one shard's assignment (404 if unplaced).
async fn placement_shard(State(s): State<BrainState>, Path(shard): Path<u32>) -> impl IntoResponse {
    match s.placement.get(shard) {
        Some(a) => (StatusCode::OK, Json(json!(a))),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "not_found", "shard": shard })),
        ),
    }
}

/// `POST /v1/scale` — set the desired scale plan; the reconciler picks it up on
/// its next tick. `replication_factor` is clamped to ≥ 1.
async fn set_scale(State(s): State<BrainState>, Json(mut plan): Json<ScalePlan>) -> Json<Value> {
    plan.replication_factor = plan.replication_factor.max(1);
    // Operator intent is durable state: route it through the control plane so it
    // replicates. Only the leader accepts a write; a follower reports where the
    // leader is so the caller can retry there (transparent forwarding is the
    // sidecar-era follow-up).
    if s.control_plane.propose(Command::SetScalePlan(plan.clone())) {
        Json(json!({ "ok": true, "plan": plan }))
    } else {
        Json(json!({
            "ok": false,
            "error": "not_leader",
            "leader": s.control_plane.leader_addr(),
        }))
    }
}
