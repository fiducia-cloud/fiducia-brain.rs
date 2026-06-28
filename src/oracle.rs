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
//! without a k8s client behaves byte-for-byte as before. The real
//! [`KubeOracle`] (kube-rs Pod watch + node topology labels) is Milestone 3 — its
//! shape is documented at the bottom of this file.

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

// ─────────────────────────────────────────────────────────────────────────────
// Milestone 3 — `KubeOracle` (sketch, not yet built):
//
//   struct KubeOracle { pods: Store<Pod>, /* reflector-backed cache */ }
//
//   * Spawn a kube-rs `reflector`/`watcher` over Pods with label
//     `app=fiducia-node` in the `fiducia` namespace; keep a local `Store` so
//     `liveness()` is a cheap in-memory lookup (no API call on the hot path).
//   * Map our `node_id` (the node's advertised URL) → pod via a stable label the
//     sidecar/pod sets (e.g. `fiducia.dev/node-id`), or via pod DNS name.
//   * `Running` if `pod.status.phase == Running` and not in CrashLoopBackOff;
//     `Gone` if the pod is missing from the store or `Failed`; else `Unknown`.
//   * Topology: read the *node's* `topology.kubernetes.io/region` / `zone`
//     labels (+ cluster name) as the authoritative failure domain, overriding
//     the self-reported one. Across 3 clusters this needs one watch per cluster
//     (a kubeconfig per cloud) — the same multi-cluster reach the brain's own
//     Raft requires.
//   * RBAC: a ServiceAccount with `get/list/watch` on `pods` and `nodes`
//     (read-only — no `patch`/`delete`, by design).
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_oracle_is_always_unknown() {
        assert_eq!(NullOracle.liveness("anything"), PodLiveness::Unknown);
    }
}
