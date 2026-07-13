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
mod cluster;
mod config;
mod internal_auth;
mod leadership;
mod membership;
mod model;
mod oracle;
mod placement;
mod plan;
// The brain's own Raft: the pure engine, its WAL, and the async driver that wires
// it into `ControlPlane`. `allow(dead_code)` on the engine — it exposes a fuller
// accessor API (role/term/commit_index) than the driver currently consumes.
#[allow(dead_code)]
mod raft;
mod raft_driver;
mod raft_store;
mod scheduler;

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use std::time::Duration;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer,
    trace::TraceLayer,
};

use api::BrainState;
use cluster::{ControlPlane, LocalControlPlane};
use membership::Membership;
use model::ScalePlan;
use placement::Placement;
use raft::RaftConfig;
use raft_driver::RaftControlPlane;
use raft_store::RaftStore;
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
    let target_nodes = std::env::var("FIDUCIA_TARGET_NODES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(3)
        .max(cluster.replication_factor);
    let plan = Arc::new(Mutex::new(ScalePlan {
        target_nodes,
        replication_factor: cluster.replication_factor,
    }));

    // Liveness oracle: in-cluster, the `KubeOracle` confirms deaths and damps
    // WAN blips from the k8s API; otherwise (local dev / no RBAC) the `NullOracle`
    // keeps pure-timeout behavior.
    let oracle: Arc<dyn oracle::LivenessOracle> = match oracle::KubeOracle::spawn() {
        Some(kube) => {
            tracing::info!("liveness: k8s KubeOracle active (confirmed-gone + blip damping)");
            kube
        }
        None => {
            tracing::info!("liveness: NullOracle (not in-cluster / no RBAC) — pure timeouts");
            Arc::new(oracle::NullOracle)
        }
    };
    let membership = Arc::new(Membership::with_oracle(
        membership::MembershipConfig::default(),
        oracle,
    ));
    let placement = Arc::new(Placement::new(cluster.shard_count));

    // Where this brain member listens (and the default for its own address).
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8095);

    // The brain's own control plane. With `FIDUCIA_BRAIN_PEERS` set, run the
    // replicated Raft (one member per cloud): durable state in `FIDUCIA_DATA_DIR`,
    // and a single elected leader that alone reconciles. Unset ⇒ a single-member
    // `LocalControlPlane` (local dev / one box) — always leader, no replication.
    let peers: Vec<String> = std::env::var("FIDUCIA_BRAIN_PEERS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let (control_plane, raft): (Arc<dyn ControlPlane>, Option<Arc<RaftControlPlane>>) =
        if peers.is_empty() {
            tracing::info!("control plane: single-member (FIDUCIA_BRAIN_PEERS unset) — no replication");
            (
                Arc::new(LocalControlPlane::new(
                    membership.clone(),
                    placement.clone(),
                    plan.clone(),
                )),
                None,
            )
        } else {
            let id = std::env::var("FIDUCIA_BRAIN_ID")
                .unwrap_or_else(|_| format!("http://localhost:{port}"));
            // Peers must EXCLUDE self. Operators commonly list every member
            // (including this one) in FIDUCIA_BRAIN_PEERS, so filter our own id out
            // — otherwise quorum is inflated (a 3-member group would wrongly need
            // all 3, losing fault tolerance) and the member would RPC itself.
            let peers: Vec<String> = peers.into_iter().filter(|p| p != &id).collect();
            let data_dir = std::env::var("FIDUCIA_DATA_DIR")
                .unwrap_or_else(|_| "/tmp/fiducia-brain".to_string());
            // Fail closed: if we can't open our durable Raft home, we must not run.
            let (store, restored) = RaftStore::open(&data_dir)?;
            tracing::info!(
                %id, ?peers, %data_dir,
                "control plane: Raft ({} members) — replicating placement + scale plan",
                peers.len() + 1
            );
            let rcp = RaftControlPlane::new(
                id,
                peers,
                RaftConfig::default(),
                Some(store),
                restored,
                raft_driver::Transport::http(),
                membership.clone(),
                placement.clone(),
                plan.clone(),
            );
            rcp.spawn();
            (rcp.clone(), Some(rcp))
        };

    let scheduler = Arc::new(Scheduler::new(
        membership.clone(),
        placement.clone(),
        plan.clone(),
        control_plane.clone(),
    ));

    // Kick off the reconciliation loop (sweeps failures, then reconciles) — it
    // only acts while this member is the leader.
    tokio::spawn(scheduler.clone().run());

    let state = BrainState {
        config: cluster.clone(),
        membership,
        placement,
        plan: plan.clone(),
        control_plane,
        // Short-timeout client for forwarding follower writes/heartbeats to the leader.
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap_or_default(),
    };

    // Reusable hardening stack (outermost last): catch handler panics → 500,
    // bound request time, and cap body size.
    fn harden(router: Router) -> Router {
        router
            .layer(TraceLayer::new_for_http())
            .layer(TimeoutLayer::new(Duration::from_secs(REQUEST_TIMEOUT_SECS)))
            .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
            .layer(CatchPanicLayer::new())
    }

    // Control plane (:PORT, 8095): health + /v1 — the sidecar heartbeat, the LB's
    // placement/membership reads, and admin. A NetworkPolicy locks this to
    // in-namespace (see fiducia-infra).
    // Refuse to bind the control plane unless its trusted-hop boundary is enforced.
    internal_auth::init_and_log()?;

    // `/v1` is cluster-internal and requires the trusted-hop header (attached by
    // the node sidecar, LB, admin, and the brain's own follower→leader forwarding).
    // Health probes stay open for k8s.
    let control_app = harden(
        Router::new()
            .route("/healthz", get(health))
            .route("/readyz", get(health))
            .nest(
                "/v1",
                api::router(state).layer(middleware::from_fn(internal_auth::guard)),
            ),
    );

    let shape = plan.lock().unwrap().clone();
    let control_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let control_listener = tokio::net::TcpListener::bind(control_addr).await?;

    match raft {
        // Replicated: serve peer-facing Raft RPC on a SEPARATE port (the declared
        // brain_endpoint / FIDUCIA_BRAIN_PEERS port, default 9095), reachable
        // cross-cluster, while the control plane stays in-namespace. /raft is
        // additionally bearer-authenticated by FIDUCIA_BRAIN_RAFT_SECRET.
        // Previously /raft was merged onto :8095, so cross-cluster RPCs to :9095
        // hit nothing.
        Some(rcp) => {
            let peer_port: u16 = std::env::var("FIDUCIA_BRAIN_PEER_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(9095);
            let peer_app = harden(
                Router::new()
                    .route("/healthz", get(health))
                    .merge(raft_driver::raft_router(rcp)),
            );
            let peer_addr = SocketAddr::from(([0, 0, 0, 0], peer_port));
            let peer_listener = tokio::net::TcpListener::bind(peer_addr).await?;
            tracing::info!(
                "{SERVICE} control plane on http://{control_addr} (/v1), brain-Raft peer plane on \
                 http://{peer_addr} (/raft) (cluster={}, shards={}, target_nodes={}, rf={})",
                cluster.cluster_id,
                cluster.shard_count,
                shape.target_nodes,
                shape.replication_factor
            );
            tokio::try_join!(
                axum::serve(control_listener, control_app).into_future(),
                axum::serve(peer_listener, peer_app).into_future(),
            )?;
        }
        // Single-member (no peers): no Raft router, one listener — unchanged.
        None => {
            tracing::info!(
                "{SERVICE} listening on http://{control_addr} (cluster={}, shards={}, target_nodes={}, rf={})",
                cluster.cluster_id,
                cluster.shard_count,
                shape.target_nodes,
                shape.replication_factor
            );
            axum::serve(control_listener, control_app).await?;
        }
    }
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
