//! Reconciliation loop — the scaling & healing strategy (skeleton).
//!
//! A control loop that drives the cluster from its *observed* state (membership +
//! reported shard hosting) toward its *desired* state (the [`ScalePlan`]: N
//! healthy nodes, RF replicas per shard, leadership spread evenly).
//!
//! ## The one invariant that makes scaling cheap
//!
//! **Shard count is fixed; node count is elastic.** Scaling never changes
//! `key → shard` (see [`crate::config`]); it only rewrites `shard → nodes`. So
//! "scale the cluster" == "move some shard replicas/leaders between nodes", which
//! is an incremental, online operation — never a global rehash.
//!
//! ## Safe membership change (used by every phase below)
//!
//! A shard is a Raft group, so adding/removing a replica is a Raft configuration
//! change, done **one replica at a time**:
//!
//!   1. **add learner** — new node joins the shard as a non-voting learner and
//!      catches up via snapshot + log tail (doesn't count toward quorum, so it
//!      can lag without stalling commits);
//!   2. **promote** — once caught up, promote learner → voter;
//!   3. **(optional) transfer leadership** — if we want this node to lead;
//!   4. **remove** — demote/remove the old voter.
//!
//! Never add a far-behind voter (it would stall the quorum), and never drop below
//! a quorum. Moves are **throttled** (a few shards at a time) so rebalancing
//! doesn't saturate the network with snapshots.
//!
//! ## Balance objectives (what "balanced" means)
//!
//!   * exactly RF replicas per shard;
//!   * replicas spread across failure domains (don't put 2 of 3 in one AZ);
//!   * even replica count per node;
//!   * even **leader** count per node (leaders do the writes — the real hotspot).
//!
//! Skeleton: the loop and the four phases are sketched as methods; the placement
//! math inside each is left as `TODO`.

use std::collections::HashSet;
use std::sync::Arc;

use crate::membership::Membership;
use crate::model::{NodeHealth, NodeId, ScalePlan};
use crate::placement::Placement;

/// The reconciler: reads observed state (membership), writes desired state
/// (placement), mediated by the current [`ScalePlan`].
pub struct Scheduler {
    membership: Arc<Membership>,
    placement: Arc<Placement>,
}

impl Scheduler {
    pub fn new(membership: Arc<Membership>, placement: Arc<Placement>) -> Self {
        Scheduler {
            membership,
            placement,
        }
    }

    /// One reconciliation tick. Ordering matters: heal first (urgent), then
    /// shed/absorb capacity, then fine-tune balance.
    pub fn reconcile(&self, plan: &ScalePlan) {
        let nodes = self.membership.snapshot();
        let healthy = nodes.iter().filter(|n| n.health == NodeHealth::Healthy).count() as u32;

        // 1. Heal failures first — restore RF for under-replicated shards.
        self.rereplicate_failed(plan);
        // 2. Absorb new capacity — bleed replicas onto healthy new nodes.
        if healthy < plan.target_nodes {
            self.scale_up(plan);
        }
        // 3. Shed capacity — drain the lightest nodes down to target.
        if healthy > plan.target_nodes {
            self.scale_down(plan);
        }
        // 4. Fine-tune — even out leadership so no node is a write hotspot.
        self.balance_leadership();
    }

    /// **Node failure / under-replication.** For every shard with a Dead or
    /// Draining replica (or simply fewer than RF live replicas), add a learner on
    /// the least-loaded healthy node, promote it, and drop the dead replica.
    /// Highest priority — a shard at RF-1 has no redundancy left.
    ///
    /// TODO: scan placement vs. live membership; emit `Placement::assign` moves.
    fn rereplicate_failed(&self, _plan: &ScalePlan) {
        let _ = (&self.membership, &self.placement);
        // TODO
    }

    /// **Scale up.** A new node joins owning nothing. Pick the shards whose move
    /// most improves balance and migrate replicas/leadership onto it (learner →
    /// voter → optional leadership transfer), throttled, until replica/leader
    /// counts level out.
    ///
    /// TODO: choose source shards; emit throttled moves.
    fn scale_up(&self, _plan: &ScalePlan) {
        // TODO
    }

    /// **Scale down.** Above target: mark the lightest nodes `Draining`, move
    /// every replica/leadership they hold onto other healthy nodes (replacement
    /// learner → voter, then remove the draining replica), and once a node holds
    /// nothing, release it. Refuses to go below `replication_factor` nodes — you
    /// can't keep RF copies with fewer than RF nodes.
    ///
    /// TODO: pick drain victims; sequence the evacuations.
    fn scale_down(&self, plan: &ScalePlan) {
        let _floor = plan.replication_factor; // hard lower bound on node count
        // TODO
    }

    /// **Leadership balancing + affinity.** Drive each shard's leader toward its
    /// affinity target ([`crate::leadership::desired_leader`]): the preferred
    /// node when healthy (so leadership returns to it after it recovers),
    /// otherwise the current leader, otherwise a healthy replica.
    ///
    /// TODO: feed the observed Raft leader (reported via heartbeats) as `current`
    /// and issue a leadership transfer when it differs from the desired leader.
    fn balance_leadership(&self) {
        let healthy: HashSet<NodeId> = self
            .membership
            .snapshot()
            .into_iter()
            .filter(|n| n.health == NodeHealth::Healthy)
            .map(|n| n.node_id)
            .collect();
        for a in self.placement.snapshot() {
            let _desired = crate::leadership::desired_leader(
                a.preferred_leader.as_ref(),
                &a.replicas,
                &healthy,
                None, // TODO: observed leader from heartbeats
            );
            // TODO: if _desired differs from the observed leader, transfer.
        }
    }

    /// Background loop driving `reconcile` on an interval.
    ///
    /// TODO: also wake on membership/heartbeat events instead of pure polling.
    pub async fn run(self: Arc<Self>, plan: ScalePlan) {
        loop {
            self.reconcile(&plan);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
