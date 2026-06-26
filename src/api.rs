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

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::ClusterConfig;
use crate::membership::Membership;
use crate::placement::Placement;

/// Shared control-plane state handed to handlers.
#[derive(Clone)]
pub struct BrainState {
    pub config: ClusterConfig,
    pub membership: Arc<Membership>,
    pub placement: Arc<Placement>,
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

/// `GET /v1/status` — control-plane summary.
async fn status(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({
        "service": "fiducia-brain",
        "version": env!("CARGO_PKG_VERSION"),
        "cluster_id": s.config.cluster_id,
        "nodes": s.membership.snapshot().len(),
        "shard_count": s.config.shard_count,
        "replication_factor": s.config.replication_factor,
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

/// `POST /v1/nodes/{id}/heartbeat` — a data-plane node checks in.
async fn heartbeat(State(_s): State<BrainState>, Path(_id): Path<String>) -> Json<Value> {
    // TODO: parse reported shard status from the body; membership.heartbeat(id).
    not_implemented("brain.heartbeat")
}

/// `DELETE /v1/nodes/{id}` — drain and remove a node.
async fn remove_node(State(_s): State<BrainState>, Path(_id): Path<String>) -> Json<Value> {
    // TODO: membership.drain(id); scheduler moves replicas off before removal.
    not_implemented("brain.remove_node")
}

/// `GET /v1/placement` — full shard map for nodes to reconcile against.
async fn placement(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({ "shards": s.placement.snapshot() }))
}

/// `GET /v1/placement/{shard}` — one shard's assignment.
async fn placement_shard(State(s): State<BrainState>, Path(shard): Path<u32>) -> Json<Value> {
    match s.placement.get(shard) {
        Some(a) => Json(json!(a)),
        None => Json(json!({ "error": "not_found", "shard": shard })),
    }
}

/// `POST /v1/scale` — set the desired scale plan.
async fn set_scale(State(_s): State<BrainState>) -> Json<Value> {
    // TODO: parse ScalePlan from the body; store it for the scheduler loop.
    not_implemented("brain.set_scale")
}

fn not_implemented(op: &str) -> Json<Value> {
    Json(json!({ "error": "not_implemented", "op": op }))
}
