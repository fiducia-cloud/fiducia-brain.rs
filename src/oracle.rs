//! Liveness/topology oracle — the optional **k8s "read" side** of the hybrid
//! failure detector.
//!
//! The brain stays heartbeat-driven (it never *mutates* k8s), but it can consult
//! an external source of truth to make its failure detector smarter than a bare
//! timeout — which matters a great deal across three clouds, where a transient
//! WAN partition must NOT be mistaken for a dead node and trigger a
//! re-replication storm:
//!
//!   * a pod that k8s reports **gone / CrashLooping** is dead *now* — declare it
//!     without waiting out `dead_after`;
//!   * a pod that is **Running** but briefly silent is a network blip, not a
//!     failure — hold it at `Suspect` instead of escalating to `Dead`;
//!   * `Unknown` (no oracle, or the pod isn't found) falls back to pure timeouts,
//!     i.e. exactly the prior behavior.
//!
//! The default [`NullOracle`] answers `Unknown` for everything, so a brain built
//! without a k8s client behaves byte-for-byte as before. [`KubeOracle`] is the
//! real implementation: a lightweight poller over the k8s API (no kube-rs
//! dependency — just the in-cluster service-account token + CA over reqwest),
//! caching pod status so `liveness()` is a cheap in-memory lookup.
//!
//! It is deliberately **conservative**: it reports `Gone` only on *positive*
//! evidence (a pod that is present but `Failed`/`CrashLoopBackOff`/terminating)
//! and `Running` only for a pod that is present and running. A pod merely
//! *absent* from the (possibly slightly stale) cache is `Unknown`, never `Gone` —
//! so a brand-new node can't be killed by a cache that predates it.

/// What an external oracle believes about a node's backing pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodLiveness {
    /// The pod exists and is `Running` — silence is a blip, not death.
    Running,
    /// The pod is deleted / `Failed` / wedged in `CrashLoopBackOff` — dead now.
    Gone,
    /// No external signal (no oracle, pod not found) — use timeouts.
    Unknown,
}

/// An external liveness signal the failure detector can consult. Read-only: the
/// brain never asks the oracle to *change* anything (that keeps the layering
/// clean — k8s schedules pods, the brain places shards).
pub trait LivenessOracle: Send + Sync {
    /// What does the external source of truth say about this node's pod?
    fn liveness(&self, node_id: &str) -> PodLiveness;
}

/// No external oracle: every node is `Unknown`, so the detector uses timeouts
/// alone. This is the default, and keeps a k8s-less build identical to before.
pub struct NullOracle;

impl LivenessOracle for NullOracle {
    fn liveness(&self, _node_id: &str) -> PodLiveness {
        PodLiveness::Unknown
    }
}

// ── KubeOracle: poll the k8s API for data-plane pod status ──────────────────

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use serde_json::Value;

const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
/// The data-plane pods we track (`node_id` == pod name; see the sidecar wiring).
const NODE_LABEL_SELECTOR: &str = "app=fiducia-node";
/// How often to refresh the pod cache.
const POLL_INTERVAL: Duration = Duration::from_secs(3);
/// Treat the cache as `Unknown` (fall back to timeouts) once it's older than this,
/// so a prolonged API outage can't leave us acting on a stale view.
const STALE_AFTER: Duration = Duration::from_secs(15);

struct Cache {
    pods: HashMap<String, PodLiveness>,
    updated_at: Option<Instant>,
}

/// Liveness oracle backed by the k8s API. Holds an in-memory cache of data-plane
/// pod status, refreshed by a background poller; `liveness()` is a cheap lookup.
pub struct KubeOracle {
    cache: RwLock<Cache>,
}

impl KubeOracle {
    /// Build the oracle and spawn its poller — but only when running in-cluster
    /// (a service-account token is mounted). Returns `None` otherwise (local dev,
    /// tests, or no RBAC), so the caller falls back to [`NullOracle`].
    pub fn spawn() -> Option<Arc<KubeOracle>> {
        let api = KubeApi::in_cluster()?;
        let oracle = Arc::new(KubeOracle {
            cache: RwLock::new(Cache {
                pods: HashMap::new(),
                updated_at: None,
            }),
        });
        let driver = oracle.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(POLL_INTERVAL);
            loop {
                tick.tick().await;
                if let Some(body) = api.list_node_pods().await {
                    let pods = parse_pod_list(&body);
                    let mut cache = driver.cache.write().unwrap();
                    cache.pods = pods;
                    cache.updated_at = Some(Instant::now());
                }
                // On failure we simply don't refresh; the cache ages out via
                // STALE_AFTER and `liveness()` reverts to Unknown (timeouts).
            }
        });
        Some(oracle)
    }
}

impl LivenessOracle for KubeOracle {
    fn liveness(&self, node_id: &str) -> PodLiveness {
        let cache = self.cache.read().unwrap();
        match cache.updated_at {
            Some(at) if at.elapsed() <= STALE_AFTER => {
                // Present ⇒ its computed status; absent ⇒ Unknown (NOT Gone — could
                // be a brand-new pod the cache hasn't seen yet, or a label change).
                cache.pods.get(node_id).copied().unwrap_or(PodLiveness::Unknown)
            }
            _ => PodLiveness::Unknown,
        }
    }
}

/// Pure: a single pod object → our liveness verdict (positive evidence only).
fn pod_liveness(pod: &Value) -> PodLiveness {
    if pod["metadata"]["deletionTimestamp"].is_string() {
        return PodLiveness::Gone; // terminating
    }
    let phase = pod["status"]["phase"].as_str().unwrap_or("");
    if phase == "Failed" {
        return PodLiveness::Gone;
    }
    if let Some(statuses) = pod["status"]["containerStatuses"].as_array() {
        let crash_looping = statuses.iter().any(|c| {
            c["state"]["waiting"]["reason"].as_str() == Some("CrashLoopBackOff")
        });
        if crash_looping {
            return PodLiveness::Gone;
        }
    }
    if phase == "Running" {
        return PodLiveness::Running;
    }
    PodLiveness::Unknown // Pending / no status yet — don't decide
}

/// Pure: a k8s `PodList` JSON → pod-name → liveness.
fn parse_pod_list(list: &Value) -> HashMap<String, PodLiveness> {
    let mut map = HashMap::new();
    if let Some(items) = list["items"].as_array() {
        for pod in items {
            if let Some(name) = pod["metadata"]["name"].as_str() {
                map.insert(name.to_string(), pod_liveness(pod));
            }
        }
    }
    map
}

/// Minimal in-cluster k8s API client (no kube-rs): SA bearer token + CA over HTTPS.
struct KubeApi {
    client: reqwest::Client,
    base: String,
    namespace: String,
}

impl KubeApi {
    fn in_cluster() -> Option<KubeApi> {
        // Confirm we're in-cluster: the token must be readable now (re-read per
        // request below). The CA, by contrast, is stable for the cluster's life.
        std::fs::read_to_string(format!("{SA_DIR}/token")).ok()?;
        let ca = std::fs::read(format!("{SA_DIR}/ca.crt")).ok()?;
        let namespace = std::fs::read_to_string(format!("{SA_DIR}/namespace"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "fiducia".to_string());
        let cert = reqwest::Certificate::from_pem(&ca).ok()?;
        let client = reqwest::Client::builder()
            .add_root_certificate(cert)
            .timeout(Duration::from_secs(5))
            .build()
            .ok()?;
        Some(KubeApi {
            client,
            base: "https://kubernetes.default.svc".to_string(),
            namespace,
        })
    }

    async fn list_node_pods(&self) -> Option<Value> {
        // Re-read the token each poll: projected bound SA tokens rotate (~1h) and
        // the kubelet refreshes the file, so caching it would 401 after expiry and
        // silently kill the oracle. A local file read per 3s poll is negligible.
        let token = std::fs::read_to_string(format!("{SA_DIR}/token")).ok()?;
        let url = format!(
            "{}/api/v1/namespaces/{}/pods?labelSelector={}",
            self.base,
            self.namespace,
            NODE_LABEL_SELECTOR.replace('=', "%3D"),
        );
        self.client
            .get(url)
            .bearer_auth(token.trim())
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn null_oracle_is_always_unknown() {
        assert_eq!(NullOracle.liveness("anything"), PodLiveness::Unknown);
    }

    #[test]
    fn pod_status_maps_to_liveness_on_positive_evidence_only() {
        let running = json!({ "status": { "phase": "Running" } });
        assert_eq!(pod_liveness(&running), PodLiveness::Running);

        let failed = json!({ "status": { "phase": "Failed" } });
        assert_eq!(pod_liveness(&failed), PodLiveness::Gone);

        let terminating = json!({
            "metadata": { "deletionTimestamp": "2026-06-28T00:00:00Z" },
            "status": { "phase": "Running" }
        });
        assert_eq!(pod_liveness(&terminating), PodLiveness::Gone, "terminating is gone");

        let crashloop = json!({
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    { "state": { "waiting": { "reason": "CrashLoopBackOff" } } }
                ]
            }
        });
        assert_eq!(pod_liveness(&crashloop), PodLiveness::Gone, "crashloop is gone");

        let pending = json!({ "status": { "phase": "Pending" } });
        assert_eq!(pod_liveness(&pending), PodLiveness::Unknown, "transitional is unknown");
    }

    #[test]
    fn parse_pod_list_keys_by_pod_name() {
        let list = json!({
            "items": [
                { "metadata": { "name": "fiducia-node-0" }, "status": { "phase": "Running" } },
                { "metadata": { "name": "fiducia-node-1" }, "status": { "phase": "Failed" } }
            ]
        });
        let map = parse_pod_list(&list);
        assert_eq!(map.get("fiducia-node-0"), Some(&PodLiveness::Running));
        assert_eq!(map.get("fiducia-node-1"), Some(&PodLiveness::Gone));
        assert_eq!(map.get("fiducia-node-2"), None);
    }

    #[test]
    fn kube_oracle_cache_is_conservative_about_absent_and_stale() {
        let oracle = KubeOracle {
            cache: RwLock::new(Cache {
                pods: HashMap::new(),
                updated_at: None,
            }),
        };
        // Never-populated cache ⇒ Unknown (fall back to timeouts).
        assert_eq!(oracle.liveness("fiducia-node-0"), PodLiveness::Unknown);

        // Fresh cache: present ⇒ its status; absent ⇒ Unknown (not Gone).
        {
            let mut c = oracle.cache.write().unwrap();
            c.pods
                .insert("fiducia-node-0".to_string(), PodLiveness::Running);
            c.updated_at = Some(Instant::now());
        }
        assert_eq!(oracle.liveness("fiducia-node-0"), PodLiveness::Running);
        assert_eq!(oracle.liveness("fiducia-node-9"), PodLiveness::Unknown);

        // Stale cache ⇒ Unknown regardless of contents.
        {
            let mut c = oracle.cache.write().unwrap();
            c.updated_at = Some(Instant::now() - STALE_AFTER - Duration::from_secs(1));
        }
        assert_eq!(oracle.liveness("fiducia-node-0"), PodLiveness::Unknown);
    }
}
