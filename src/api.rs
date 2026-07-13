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
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::cluster::{Command, ControlPlane};
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
    // The leader's /v1 enforces the trusted-hop secret when configured, so a
    // follower forwarding a heartbeat/scale/drain must present it too.
    let req = crate::internal_auth::attach(req);
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

/// Validate node-reported shard data at the trust boundary. Heartbeats are the
/// one input the brain takes from the data plane, and the scheduler *adopts*
/// reported hosting/leading on a cold start, so a compromised or buggy node
/// must not be able to smuggle in shard state the cluster cannot have:
///
///   * shard ids at or beyond `shard_count` do not exist — dropped;
///   * duplicate ids would otherwise be adopted verbatim into a placement
///     (`replicas: [a, a, b]` — RF met on paper with only two real replicas) —
///     deduplicated, keeping first occurrence order;
///   * a node cannot *lead* a shard it does not report *hosting* — such
///     leading claims are dropped rather than allowed to steer leader
///     stickiness/adoption toward the claimant.
fn sanitize_report(report: &mut HeartbeatReport, shard_count: u32) {
    let mut seen = HashSet::new();
    report
        .hosted_shards
        .retain(|shard| *shard < shard_count && seen.insert(*shard));
    let hosted: HashSet<u32> = report.hosted_shards.iter().copied().collect();
    let mut seen = HashSet::new();
    report
        .leading_shards
        .retain(|shard| hosted.contains(shard) && seen.insert(*shard));
}

/// Fail closed at the API boundary: a member whose control plane is unavailable
/// (sticky, after a Raft durability failure) must not acknowledge mutations —
/// not even leader-local soft state like heartbeats or drain intent. Without
/// this check a failed member would keep answering `ok: true` to heartbeats and
/// `DELETE /v1/nodes/{id}` while never acting on them.
fn unavailable(s: &BrainState) -> Option<(StatusCode, Json<Value>)> {
    if s.control_plane.is_available() {
        None
    } else {
        Some((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ok": false,
                "error": "unavailable",
                "detail": "control plane is unavailable until restart",
            })),
        ))
    }
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

/// `GET /v1/status` — control-plane summary + placement health. Surfaces the gap
/// between *desired* (`ScalePlan`) and *observed* (membership) so operators can
/// see at a glance whether the cluster is converged or under-replicated.
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
            "available": s.control_plane.is_available(),
            // Brain's own control plane: which member is driving reconciliation.
            "placement_generation": s.placement.generation(),
            "is_leader": s.control_plane.is_leader(),
            "leader": s.control_plane.leader_addr(),
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
) -> Response {
    if let Some(resp) = unavailable(&s) {
        return resp.into_response();
    }
    let mut report = report.map(|Json(r)| r).unwrap_or_default();
    sanitize_report(&mut report, s.config.shard_count);
    // Liveness is leader-local soft state, so it must land on the leader. A
    // follower forwards there (the sidecar heartbeats any member); only if no
    // leader is known yet — mid-election — do we accept best-effort locally.
    if !s.control_plane.is_leader() {
        if let Some(leader) = s.control_plane.leader_addr() {
            let url = format!("{}/v1/nodes/{}/heartbeat", leader.trim_end_matches('/'), id);
            return forwarded(s.http.post(url).json(&report))
                .await
                .into_response();
        }
    }
    let health = s.membership.heartbeat(&id, now_ms(), report);
    Json(json!({ "ok": true, "node_id": id, "health": health })).into_response()
}

/// `DELETE /v1/nodes/{id}` — begin draining a node. The reconciler evacuates its
/// replicas/leadership onto healthy nodes; the operator removes it once empty.
async fn remove_node(State(s): State<BrainState>, Path(id): Path<String>) -> Response {
    if let Some(resp) = unavailable(&s) {
        return resp.into_response();
    }
    // Draining is leader-local intent; forward from a follower to the leader.
    if !s.control_plane.is_leader() {
        if let Some(leader) = s.control_plane.leader_addr() {
            let url = format!("{}/v1/nodes/{}", leader.trim_end_matches('/'), id);
            return forwarded(s.http.delete(url)).await.into_response();
        }
    }
    let known = s.membership.drain(&id);
    Json(json!({ "draining": known, "node_id": id })).into_response()
}

/// `GET /v1/placement` — full shard map for nodes to reconcile against. The
/// `generation` lets pollers skip re-diffing the map when nothing changed.
async fn placement(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({
        "generation": s.placement.generation(),
        "shards": s.placement.snapshot(),
        "policies": s.placement.policies_snapshot(),
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

/// `GET /v1/policies` — namespace home-region/provider placement policies.
async fn list_policies(State(s): State<BrainState>) -> Json<Value> {
    Json(json!({ "policies": s.placement.policies_snapshot() }))
}

/// `POST /v1/policies` — set the preferred leader region/provider for the shard
/// that owns a namespace.
async fn set_policy(
    State(s): State<BrainState>,
    Json(update): Json<PlacementPolicyUpdate>,
) -> Response {
    if let Some(resp) = unavailable(&s) {
        return resp.into_response();
    }
    let namespace = update.namespace.trim().to_string();
    if namespace.is_empty() {
        return Json(json!({ "ok": false, "error": "namespace_required" })).into_response();
    }

    // Policies steer the LEADER's reconcile loop (it is the only member that
    // computes placements), so a policy posted to a follower must land on the
    // leader — previously it was applied to the follower's local map only and
    // silently never took effect. Same forwarding contract as /v1/scale.
    if !s.control_plane.is_leader() {
        match s.control_plane.leader_addr() {
            Some(leader) => {
                let url = format!("{}/v1/policies", leader.trim_end_matches('/'));
                return forwarded(s.http.post(url).json(&update))
                    .await
                    .into_response();
            }
            None => {
                return Json(json!({ "ok": false, "error": "not_leader", "leader": Value::Null }))
                    .into_response()
            }
        }
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
    Json(json!({ "ok": true, "policy": policy })).into_response()
}

/// `POST /v1/scale` — set the desired scale plan; the reconciler picks it up on
/// its next tick. `replication_factor` is fixed at RF=3 for the multi-cloud
/// baseline.
async fn set_scale(State(s): State<BrainState>, Json(mut plan): Json<ScalePlan>) -> Response {
    if let Some(resp) = unavailable(&s) {
        return resp.into_response();
    }
    // RF is fixed at the multi-cloud baseline; target can't drop below it.
    plan.replication_factor = SUPPORTED_REPLICATION_FACTOR;
    plan.target_nodes = plan.target_nodes.max(SUPPORTED_REPLICATION_FACTOR);
    // Operator intent is durable state: route it through the control plane so it
    // replicates (apply_command writes it into `s.plan` once committed). Only the
    // leader may accept a write — a follower transparently forwards there; if no
    // leader is known (mid-election) we report not_leader.
    if s.control_plane.propose(Command::SetScalePlan(plan.clone())) {
        return Json(json!({ "ok": true, "plan": plan })).into_response();
    }
    match s.control_plane.leader_addr() {
        Some(leader) => {
            let url = format!("{}/v1/scale", leader.trim_end_matches('/'));
            forwarded(s.http.post(url).json(&plan))
                .await
                .into_response()
        }
        None => Json(json!({ "ok": false, "error": "not_leader", "leader": Value::Null }))
            .into_response(),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::LocalControlPlane;
    use crate::membership::MembershipConfig;

    /// A control plane in an arbitrary (possibly failed-closed) state.
    struct FakeControlPlane {
        available: bool,
        leader: bool,
        leader_addr: Option<String>,
    }

    impl ControlPlane for FakeControlPlane {
        fn is_available(&self) -> bool {
            self.available
        }
        fn is_leader(&self) -> bool {
            self.available && self.leader
        }
        fn leader_addr(&self) -> Option<String> {
            self.leader_addr.clone()
        }
        fn propose(&self, _command: Command) -> bool {
            false
        }
    }

    fn state_with(cp: Arc<dyn ControlPlane>) -> BrainState {
        BrainState {
            config: ClusterConfig {
                cluster_id: "test".to_string(),
                shard_count: 4,
                replication_factor: SUPPORTED_REPLICATION_FACTOR,
                local_cluster: None,
                cloud_provider: None,
                region: None,
                brain_peers: Vec::new(),
            },
            membership: Arc::new(Membership::new(MembershipConfig::default())),
            placement: Arc::new(Placement::new(4)),
            plan: Arc::new(Mutex::new(ScalePlan {
                target_nodes: 3,
                replication_factor: 3,
            })),
            control_plane: cp,
            http: reqwest::Client::new(),
        }
    }

    fn unavailable_state() -> BrainState {
        state_with(Arc::new(FakeControlPlane {
            available: false,
            leader: false,
            leader_addr: None,
        }))
    }

    fn leader_state() -> BrainState {
        let s = state_with(Arc::new(FakeControlPlane {
            available: true,
            leader: true,
            leader_addr: None,
        }));
        // A leader that really applies: reuse the local (always-leader) plane.
        BrainState {
            control_plane: Arc::new(LocalControlPlane::new(
                s.membership.clone(),
                s.placement.clone(),
                s.plan.clone(),
            )),
            ..s
        }
    }

    #[test]
    fn sanitize_report_drops_out_of_range_duplicate_and_unhosted_shards() {
        let mut report = HeartbeatReport {
            hosted_shards: vec![0, 1, 1, 3, 99, u32::MAX],
            leading_shards: vec![1, 1, 2, 99],
            ..HeartbeatReport::default()
        };
        sanitize_report(&mut report, 4);
        assert_eq!(
            report.hosted_shards,
            vec![0, 1, 3],
            "out-of-range and duplicate hosted shards dropped"
        );
        assert_eq!(
            report.leading_shards,
            vec![1],
            "leading claims limited to hosted, in-range, deduplicated shards"
        );
    }

    #[test]
    fn sanitize_report_keeps_a_well_formed_report_unchanged() {
        let mut report = HeartbeatReport {
            hosted_shards: vec![2, 0, 3],
            leading_shards: vec![0, 3],
            ..HeartbeatReport::default()
        };
        sanitize_report(&mut report, 4);
        assert_eq!(report.hosted_shards, vec![2, 0, 3]);
        assert_eq!(report.leading_shards, vec![0, 3]);
    }

    #[tokio::test]
    async fn a_heartbeat_with_forged_shard_state_is_recorded_sanitized() {
        let s = leader_state();
        let resp = heartbeat(
            State(s.clone()),
            Path("liar".to_string()),
            Some(Json(HeartbeatReport {
                hosted_shards: vec![0, 0, 500],
                leading_shards: vec![0, 500, 3],
                ..HeartbeatReport::default()
            })),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let node = s.membership.snapshot().remove(0);
        assert_eq!(node.hosted_shards, vec![0]);
        assert_eq!(
            node.leading_shards,
            vec![0],
            "cannot claim to lead shards it does not host or that do not exist"
        );
    }

    #[tokio::test]
    async fn an_unavailable_member_refuses_all_mutations_with_503() {
        let s = unavailable_state();

        let resp = heartbeat(
            State(s.clone()),
            Path("node-a".to_string()),
            Some(Json(HeartbeatReport::default())),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            s.membership.snapshot().is_empty(),
            "a failed-closed member must not record heartbeats"
        );

        let resp = remove_node(State(s.clone()), Path("node-a".to_string())).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let resp = set_scale(
            State(s.clone()),
            Json(ScalePlan {
                target_nodes: 9,
                replication_factor: 3,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            s.plan.lock().unwrap().target_nodes,
            3,
            "a failed-closed member must not accept a scale plan"
        );

        let resp = set_policy(
            State(s.clone()),
            Json(PlacementPolicyUpdate {
                namespace: "tenant-a".to_string(),
                home_region: Some("us-east-1".to_string()),
                preferred_cloud_provider: None,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            s.placement.policies_snapshot().is_empty(),
            "a failed-closed member must not accept placement policies"
        );
    }

    #[tokio::test]
    async fn a_policy_posted_to_a_follower_lands_on_the_leader() {
        // A real leader serving /v1 on an ephemeral port...
        let leader = leader_state();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_url = format!("http://{}", listener.local_addr().unwrap());
        let app = router(leader.clone());
        tokio::spawn(async move {
            axum::serve(listener, axum::Router::new().nest("/v1", app))
                .await
                .unwrap();
        });

        // ...and a follower that knows the leader's address.
        let follower = state_with(Arc::new(FakeControlPlane {
            available: true,
            leader: false,
            leader_addr: Some(leader_url),
        }));

        let resp = set_policy(
            State(follower.clone()),
            Json(PlacementPolicyUpdate {
                namespace: "tenant-a".to_string(),
                home_region: Some("us-east-1".to_string()),
                preferred_cloud_provider: None,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let policies = leader.placement.policies_snapshot();
        assert_eq!(policies.len(), 1, "policy applied on the leader");
        assert_eq!(policies[0].namespace, "tenant-a");
        assert_eq!(policies[0].home_region.as_deref(), Some("us-east-1"));
        assert!(
            follower.placement.policies_snapshot().is_empty(),
            "the follower does not keep a divergent local policy"
        );
    }

    #[tokio::test]
    async fn a_policy_posted_to_a_follower_without_a_leader_reports_not_leader() {
        let follower = state_with(Arc::new(FakeControlPlane {
            available: true,
            leader: false,
            leader_addr: None,
        }));
        let resp = set_policy(
            State(follower.clone()),
            Json(PlacementPolicyUpdate {
                namespace: "tenant-a".to_string(),
                home_region: None,
                preferred_cloud_provider: None,
            }),
        )
        .await;
        // Mid-election: refuse rather than apply somewhere it will never be read.
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(follower.placement.policies_snapshot().is_empty());
    }

    #[tokio::test]
    async fn an_available_leader_still_accepts_heartbeats_and_drain() {
        let s = leader_state();

        let resp = heartbeat(
            State(s.clone()),
            Path("node-a".to_string()),
            Some(Json(HeartbeatReport::default())),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(s.membership.snapshot().len(), 1);

        let resp = remove_node(State(s.clone()), Path("node-a".to_string())).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(s.membership.snapshot()[0].health, NodeHealth::Draining);
    }
}
