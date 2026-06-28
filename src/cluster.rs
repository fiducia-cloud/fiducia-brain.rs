//! Replication & leadership seam — where the brain's **own Raft** plugs in.
//!
//! All *durable* brain state — the shard placement map and the operator's scale
//! plan, plus node-removal decisions — is meant to live in the brain's own Raft
//! group (a 3-member group, one per cloud) so the control plane survives losing a
//! brain node and can see membership across all three clusters. This module is the
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
//! [`LocalControlPlane`] is the single-member implementation in use today: a
//! degenerate one-node Raft — always leader, applies synchronously in-process.
//! It is correct on its own, and swapping in the replicated engine extracted from
//! `fiducia-node` is a drop-in (same trait, same `Command`).

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
        match command {
            Command::AssignShard(assignment) => self.placement.assign(assignment),
            Command::SetScalePlan(plan) => *self.plan.lock().unwrap() = plan,
            Command::ForgetNode(node_id) => {
                if self.membership.forget(&node_id) {
                    tracing::info!(node = %node_id, "control-plane: forgot drained, evacuated node");
                }
            }
        }
    }
}

impl ControlPlane for LocalControlPlane {
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
    use crate::model::ShardId;

    fn local() -> (LocalControlPlane, Arc<Placement>, Arc<Mutex<ScalePlan>>) {
        let membership = Arc::new(Membership::new(MembershipConfig::default()));
        let placement = Arc::new(Placement::new(4));
        let plan = Arc::new(Mutex::new(ScalePlan {
            target_nodes: 3,
            replication_factor: 3,
        }));
        let cp = LocalControlPlane::new(membership, placement.clone(), plan.clone());
        (cp, placement, plan)
    }

    #[test]
    fn local_control_plane_is_always_leader_and_applies_on_propose() {
        let (cp, placement, plan) = local();
        assert!(cp.is_leader());
        assert_eq!(cp.leader_addr(), None);

        let shard: ShardId = 0;
        cp.propose(Command::AssignShard(ShardAssignment {
            shard_id: shard,
            replicas: vec!["a".into(), "b".into()],
            preferred_leader: Some("a".into()),
        }));
        assert_eq!(placement.get(shard).unwrap().replicas, vec!["a", "b"]);

        cp.propose(Command::SetScalePlan(ScalePlan {
            target_nodes: 5,
            replication_factor: 3,
        }));
        assert_eq!(plan.lock().unwrap().target_nodes, 5);
    }
}
