//! Replication & leadership seam — where the brain's **own Raft** plugs in.
//!
//! All *durable* brain state — the shard placement map and the operator's scale
//! plan, plus node-removal decisions — lives in the brain's own Raft group (a
//! 3-member group, one per cloud) so the control plane survives losing a brain
//! node and can see membership across all three clusters. This module is the
//! socket that engine drops into; the rest of the brain talks to it, never to a
//! consensus library directly.
//!
//! Two ideas:
//!
//!   * [`Command`] — the deterministic, replicated state mutations. Note what is
//!     *absent*: raw heartbeats. Liveness is high-frequency **soft state** and is
//!     deliberately kept leader-local (re-derived from heartbeats), exactly as
//!     TiKV's PD keeps store heartbeats out of its Raft. Only *decisions* are
//!     replicated.
//!   * [`ControlPlane`] — what the API and reconciler call: `propose` a command
//!     (the leader applies + replicates; a follower forwards to the leader) and
//!     ask `is_leader()` so **only the leader reconciles** (this is what makes
//!     running more than one brain replica safe instead of split-brain).
//!
//! Both implementations are live today, selected at startup by
//! `FIDUCIA_BRAIN_PEERS` (see `main.rs`):
//! [`crate::raft_driver::RaftControlPlane`] is the replicated engine — a
//! purpose-built brain Raft (`crate::raft`; deliberately a *separate*
//! implementation from `fiducia-node`'s multi-Raft, so the two codebases evolve
//! independently) with a durable WAL. [`LocalControlPlane`] is the single-member
//! fallback for local dev / one-box runs: a degenerate one-node Raft — always
//! leader, applies synchronously in-process. Same trait, same `Command`.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::membership::Membership;
use crate::model::{ScalePlan, ShardAssignment};
use crate::placement::Placement;

/// A durable, replicated state mutation. (Liveness heartbeats are intentionally
/// *not* here — see the module docs.) `Serialize`/`Deserialize` so it can be
/// written to the Raft WAL and shipped between brain members.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Install/replace a shard's authoritative placement.
    AssignShard(ShardAssignment),
    /// Set the desired scale plan (operator intent, from `POST /v1/scale`).
    SetScalePlan(ScalePlan),
    /// Finalize removal of a fully-evacuated, drained node (scale-down's last step).
    ForgetNode(String),
}

/// The seam the API and reconciler use instead of mutating state directly. The
/// real Raft engine and the local single-member engine both implement this.
pub trait ControlPlane: Send + Sync {
    /// Whether this member can safely participate in the control plane. A
    /// replicated member becomes unavailable after a durability failure and
    /// must not acknowledge or forward further work until it is restarted.
    fn is_available(&self) -> bool;

    /// Is this brain member the current leader? Only the leader runs the
    /// reconcile loop and applies writes; followers stand by / forward.
    fn is_leader(&self) -> bool;

    /// The current leader's address, for write-forwarding from a follower.
    /// `None` for a single-member control plane (there is nowhere to forward).
    fn leader_addr(&self) -> Option<String>;

    /// Propose a durable mutation. On the leader this applies it (and, in the
    /// replicated engine, commits it through Raft before returning). On a
    /// follower the caller should instead forward the originating request to
    /// [`ControlPlane::leader_addr`]. Returns whether it was applied here.
    fn propose(&self, command: Command) -> bool;
}

/// Single-member control plane: always leader, applies synchronously to the
/// in-process state. This is the brain running as a one-node Raft — correct by
/// itself, and the exact shape the replicated engine replaces.
pub struct LocalControlPlane {
    membership: Arc<Membership>,
    placement: Arc<Placement>,
    plan: Arc<Mutex<ScalePlan>>,
}

impl LocalControlPlane {
    pub fn new(
        membership: Arc<Membership>,
        placement: Arc<Placement>,
        plan: Arc<Mutex<ScalePlan>>,
    ) -> Self {
        LocalControlPlane {
            membership,
            placement,
            plan,
        }
    }

    /// Apply a committed command to the state machine. In the replicated engine
    /// this runs on every member after the entry commits; here it runs inline.
    fn apply(&self, command: Command) {
        apply_command(&self.membership, &self.placement, &self.plan, command);
    }
}

/// Apply one committed [`Command`] to the brain's state machine. This is the
/// single definition of how the replicated log mutates state — used both by the
/// in-process [`LocalControlPlane`] and the Raft driver, and replayed at boot to
/// rebuild the state machine from the persisted log.
pub fn apply_command(
    membership: &Membership,
    placement: &Placement,
    plan: &Mutex<ScalePlan>,
    command: Command,
) {
    match command {
        Command::AssignShard(assignment) => placement.assign(assignment),
        Command::SetScalePlan(new_plan) => *plan.lock().unwrap() = new_plan,
        Command::ForgetNode(node_id) => {
            // A node may report that it has evacuated before the authoritative
            // placement update which removes its final replica has committed. The
            // command log is the last safety boundary: never erase membership for a
            // node that any current assignment still requires. This also makes stale
            // or duplicated ForgetNode proposals harmless across leader failover.
            let still_assigned = placement
                .snapshot()
                .iter()
                .any(|assignment| assignment.replicas.iter().any(|id| id == &node_id));
            if still_assigned {
                tracing::warn!(
                    node = %node_id,
                    "control-plane: refusing to forget node still referenced by placement"
                );
            } else if membership.forget(&node_id) {
                tracing::info!(node = %node_id, "control-plane: forgot drained, evacuated node");
            }
        }
    }
}

impl ControlPlane for LocalControlPlane {
    fn is_available(&self) -> bool {
        true
    }

    fn is_leader(&self) -> bool {
        true
    }

    fn leader_addr(&self) -> Option<String> {
        None
    }

    fn propose(&self, command: Command) -> bool {
        self.apply(command);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::MembershipConfig;
    use crate::model::{HeartbeatReport, ShardId};

    fn local() -> (
        LocalControlPlane,
        Arc<Membership>,
        Arc<Placement>,
        Arc<Mutex<ScalePlan>>,
    ) {
        let membership = Arc::new(Membership::new(MembershipConfig::default()));
        let placement = Arc::new(Placement::new(4));
        let plan = Arc::new(Mutex::new(ScalePlan {
            target_nodes: 3,
            replication_factor: 3,
        }));
        let cp = LocalControlPlane::new(membership.clone(), placement.clone(), plan.clone());
        (cp, membership, placement, plan)
    }

    fn heartbeat() -> HeartbeatReport {
        HeartbeatReport {
            address: "10.0.0.1:8090".to_owned(),
            ..HeartbeatReport::default()
        }
    }

    #[test]
    fn local_control_plane_is_always_leader_and_applies_on_propose() {
        let (cp, _, placement, plan) = local();
        assert!(cp.is_leader());
        assert_eq!(cp.leader_addr(), None);

        let shard: ShardId = 0;
        cp.propose(Command::AssignShard(ShardAssignment {
            shard_id: shard,
            replicas: vec!["a".into(), "b".into()],
            preferred_leader: Some("a".into()),
            preferred_region: None,
            preferred_cloud_provider: None,
        }));
        assert_eq!(placement.get(shard).unwrap().replicas, vec!["a", "b"]);

        cp.propose(Command::SetScalePlan(ScalePlan {
            target_nodes: 5,
            replication_factor: 3,
        }));
        assert_eq!(plan.lock().unwrap().target_nodes, 5);
    }

    #[test]
    fn forget_node_is_rejected_while_authoritative_placement_references_it() {
        let (cp, membership, placement, _) = local();
        let node = "a".to_owned();
        membership.heartbeat(&node, 0, heartbeat());
        assert!(membership.drain(&node));
        placement.assign(ShardAssignment {
            shard_id: 0,
            replicas: vec![node.clone(), "b".into(), "c".into()],
            preferred_leader: Some("b".into()),
            preferred_region: None,
            preferred_cloud_provider: None,
        });

        cp.propose(Command::ForgetNode(node.clone()));

        assert!(
            membership
                .snapshot()
                .iter()
                .any(|known| known.node_id == node),
            "a stale ForgetNode must not orphan an authoritative assignment"
        );
    }

    #[test]
    fn forget_node_succeeds_after_last_assignment_is_removed() {
        let (cp, membership, placement, _) = local();
        let node = "a".to_owned();
        membership.heartbeat(&node, 0, heartbeat());
        assert!(membership.drain(&node));
        placement.assign(ShardAssignment {
            shard_id: 0,
            replicas: vec!["b".into(), "c".into(), "d".into()],
            preferred_leader: Some("b".into()),
            preferred_region: None,
            preferred_cloud_provider: None,
        });

        cp.propose(Command::ForgetNode(node.clone()));

        assert!(
            membership
                .snapshot()
                .iter()
                .all(|known| known.node_id != node),
            "a drained node is forgettable once placement no longer requires it"
        );
    }
}
