//! fiducia-brain — the control plane.
//!
//! A small, highly-available cluster manager that sits *inside* the larger
//! Fiducia deployment. It does not serve customer coordination operations
//! directly; [`fiducia-node`] does. The brain tracks node membership, detects
//! failures, owns the authoritative shard-placement map, manages preferred
//! leaders, and reconciles the data plane toward a desired scale (nodes ×
//! replication factor). Data-plane [`fiducia-node`] processes heartbeat to the
//! brain and fetch the placement map they should host.
//!
//! Failure detection (Healthy→Suspect→Dead), the placement math
//! ([`plan`]), and the reconciliation loop are implemented; what remains is
//! replicating the brain's *own* state in its own Raft group (HA), tracked below.

mod api;
mod config;
mod leadership;
mod membership;
mod model;
mod placement;
mod plan;
mod scheduler;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

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

    // Desired cluster shape. Shared (so `POST /v1/scale` can adjust it live); in a
    // real deployment this is persisted in the brain's own Raft group.
    let plan = Arc::new(Mutex::new(ScalePlan {
        target_nodes: std::env::var("FIDUCIA_TARGET_NODES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3),
        replication_factor: cluster.replication_factor,
    }));

    let membership = Arc::new(Membership::new(membership::MembershipConfig::default()));
    let placement = Arc::new(Placement::new(cluster.shard_count));
    let scheduler = Arc::new(Scheduler::new(
        membership.clone(),
        placement.clone(),
        plan.clone(),
    ));

    // Kick off the reconciliation loop (sweeps failures, then reconciles).
    tokio::spawn(scheduler.clone().run());

    let state = BrainState {
        config: cluster.clone(),
        membership,
        placement,
        plan: plan.clone(),
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

    let shape = plan.lock().unwrap().clone();
    tracing::info!(
        "{SERVICE} listening on http://{addr} (cluster={}, shards={}, target_nodes={}, rf={})",
        cluster.cluster_id,
        cluster.shard_count,
        shape.target_nodes,
        shape.replication_factor
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": SERVICE }))
}

#[cfg(test)]
mod interface_contract_tests {
    use fiducia_interfaces::{LockAcquireManyRequest, ProposeErrorReason};

    #[test]
    fn generated_interfaces_are_importable() {
        let request = LockAcquireManyRequest {
            keys: vec!["orders/42".to_string(), "inventory/sku-7".to_string()],
            holder: Some("worker-a".to_string()),
            ttl_ms: Some(30_000),
            wait: Some(false),
        };

        assert_eq!(request.keys.len(), 2);
        assert!(matches!(
            ProposeErrorReason::NotLeader,
            ProposeErrorReason::NotLeader
        ));
    }
}
