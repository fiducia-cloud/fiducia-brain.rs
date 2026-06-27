//! fiducia-brain — the control plane.
//!
//! A small, highly-available cluster that sits *inside* the larger Fiducia
//! deployment and runs the cluster: it tracks node membership, detects failures,
//! owns the authoritative shard-placement map, and reconciles the data plane
//! toward a desired scale (nodes × replication factor). Data-plane
//! [`fiducia-node`] processes heartbeat to the brain and fetch the placement map
//! they should host.
//!
//! This is a **skeleton**: the API surface, the membership/placement stores, and
//! the reconciliation loop are wired up; the failure-detection, placement math,
//! and scaling actions are marked with `TODO`s.

mod api;
mod config;
mod leadership;
mod membership;
mod model;
mod placement;
mod scheduler;

use std::net::SocketAddr;
use std::sync::Arc;

use std::time::Duration;

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer,
    trace::TraceLayer,
};

use api::BrainState;
use membership::Membership;
use model::ScalePlan;
use placement::Placement;
use scheduler::Scheduler;

const SERVICE: &str = "fiducia-brain";

/// Bound request handling time (slow-loris / hung-upstream protection).
const REQUEST_TIMEOUT_SECS: u64 = 30;
/// Cap request bodies; control-plane payloads are small JSON.
const MAX_BODY_BYTES: usize = 256 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fiducia_telemetry::init(SERVICE);

    // Authoritative cluster configuration: shard_count (fixed) + replication
    // factor. Everything else reads this.
    let cluster = config::ClusterConfig::from_env();

    // Desired cluster shape. In a real deployment this is persisted in the
    // brain's own Raft group and adjusted via POST /v1/scale.
    let plan = ScalePlan {
        target_nodes: std::env::var("FIDUCIA_TARGET_NODES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3),
        replication_factor: cluster.replication_factor,
    };

    let membership = Arc::new(Membership::new());
    let placement = Arc::new(Placement::new(cluster.shard_count));
    let scheduler = Arc::new(Scheduler::new(membership.clone(), placement.clone()));

    // Kick off the reconciliation loop.
    tokio::spawn(scheduler.clone().run(plan.clone()));

    let state = BrainState {
        config: cluster.clone(),
        membership,
        placement,
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .nest("/v1", api::router(state))
        // Hardening stack (outermost last): catch handler panics → 500, bound
        // request time, and cap body size.
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(Duration::from_secs(REQUEST_TIMEOUT_SECS)))
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(CatchPanicLayer::new());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8095);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    tracing::info!(
        "{SERVICE} listening on http://{addr} (cluster={}, shards={}, target_nodes={}, rf={})",
        cluster.cluster_id,
        cluster.shard_count,
        plan.target_nodes,
        plan.replication_factor
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": SERVICE }))
}
