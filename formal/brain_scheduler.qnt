// Finite executable model of fiducia-brain's desired-placement reconciler.
//
// This is intentionally separate from brain_reconfiguration.qnt. That model proves
// the data-plane learner/catch-up/promote/remove sequence. This model proves the
// brain-side decision boundary: only the elected leader may publish placement,
// incomplete soft membership and degraded capacity hold the last authoritative
// assignment, cold starts adopt observed hosting, snapshots replace stale state,
// repeated stable reconciliation is idempotent, and drain finalization cannot
// forget a node while placement still requires it.
module brain_scheduler {
  type Role = Leader | Follower

  type Event =
    | InitEvent
    | BecameLeader
    | BecameFollower
    | SoftMembershipLost
    | HeartbeatRestored
    | NodeAdded
    | NodeDied
    | DrainStarted
    | DrainEvacuated
    | ColdStartPrepared
    | SnapshotRestored
    | FollowerHeld
    | IncompleteHeld
    | DegradedHeld
    | ForgetRejected
    | ColdAdopted
    | DeadReplaced
    | DrainForgotten
    | IdempotentHeld

  pure val NODES = Set(1, 2, 3, 4)
  pure val ABC = Set(1, 2, 3)
  pure val BCD = Set(2, 3, 4)
  pure val RF = 3
  pure val NO_NODE = 0

  type State = {
    role: Role,
    known: Set[int],
    healthy: Set[int],
    draining: Set[int],
    hosted: Set[int],
    observed_leader: int,
    placement: Set[int],
    preferred_leader: int,
    generation: int,
    forgotten: Set[int],
    last_event: Event,

    // A pre-action projection used to state that every hold/rejection is actually
    // non-mutating rather than merely ending in another superficially safe state.
    before_role: Role,
    before_known: Set[int],
    before_healthy: Set[int],
    before_draining: Set[int],
    before_hosted: Set[int],
    before_observed_leader: int,
    before_placement: Set[int],
    before_preferred_leader: int,
    before_generation: int,
    before_forgotten: Set[int],
  }

  var s: State

  action init = {
    s' = {
      role: Leader,
      known: ABC,
      healthy: ABC,
      draining: Set(),
      hosted: ABC,
      observed_leader: 1,
      placement: ABC,
      preferred_leader: 1,
      generation: 1,
      forgotten: Set(),
      last_event: InitEvent,
      before_role: Leader,
      before_known: ABC,
      before_healthy: ABC,
      before_draining: Set(),
      before_hosted: ABC,
      before_observed_leader: 1,
      before_placement: ABC,
      before_preferred_leader: 1,
      before_generation: 1,
      before_forgotten: Set(),
    }
  }

  action become_follower = all {
    s.role == Leader,
    s' = { ...s, role: Follower, last_event: BecameFollower },
  }

  action become_leader = all {
    s.role == Follower,
    s' = { ...s, role: Leader, last_event: BecameLeader },
  }

  // Leader-local membership can be empty/incomplete after failover even while the
  // replicated placement still names a live node. This is not a durable forget.
  action lose_a_from_soft_membership = all {
    s.known.contains(1),
    not(s.forgotten.contains(1)),
    s.observed_leader == 1,
    s' = {
      ...s,
      known: s.known.exclude(Set(1)),
      healthy: s.healthy.exclude(Set(1)),
      draining: s.draining.exclude(Set(1)),
      hosted: s.hosted.exclude(Set(1)),
      observed_leader: NO_NODE,
      last_event: SoftMembershipLost,
    },
  }

  action heartbeat_a = all {
    not(s.forgotten.contains(1)),
    not(s.known.contains(1)),
    s' = {
      ...s,
      known: s.known.union(Set(1)),
      healthy: s.healthy.union(Set(1)),
      hosted: s.hosted.union(Set(1)),
      observed_leader: 1,
      last_event: HeartbeatRestored,
    },
  }

  action add_d = all {
    not(s.known.contains(4)),
    s' = {
      ...s,
      known: s.known.union(Set(4)),
      healthy: s.healthy.union(Set(4)),
      last_event: NodeAdded,
    },
  }

  // A dead node remains KNOWN, which distinguishes a confirmed failure from an
  // incomplete leader-local membership view.
  action mark_a_dead = all {
    s.known.contains(1),
    s.healthy.contains(1),
    not(s.draining.contains(1)),
    s' = {
      ...s,
      healthy: s.healthy.exclude(Set(1)),
      observed_leader: if (s.observed_leader == 1) NO_NODE else s.observed_leader,
      last_event: NodeDied,
    },
  }

  action begin_drain_a = all {
    s.known.contains(1),
    s.healthy.contains(1),
    not(s.forgotten.contains(1)),
    s' = {
      ...s,
      healthy: s.healthy.exclude(Set(1)),
      draining: s.draining.union(Set(1)),
      last_event: DrainStarted,
    },
  }

  action evacuate_a = all {
    s.draining.contains(1),
    s.hosted.contains(1),
    s' = {
      ...s,
      hosted: s.hosted.exclude(Set(1)),
      observed_leader: if (s.observed_leader == 1) NO_NODE else s.observed_leader,
      last_event: DrainEvacuated,
    },
  }

  // A cold brain has no desired map yet, but receives healthy data-plane reports.
  // Placement.restore_from([]) advances generation before reconciliation adopts
  // the observed ABC layout and observed leader B.
  action prepare_cold_start = all {
    s.role == Leader,
    s.generation == 1,
    s.known == ABC,
    s.healthy == ABC,
    s.draining.size() == 0,
    s.placement == ABC,
    s.forgotten.size() == 0,
    s' = {
      ...s,
      placement: Set(),
      preferred_leader: NO_NODE,
      hosted: ABC,
      observed_leader: 2,
      generation: s.generation + 1,
      last_event: ColdStartPrepared,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  // A replicated snapshot is authoritative: it replaces ABC with BCD even when
  // the leader-local soft membership has not heard from D yet.
  action restore_snapshot_bcd = all {
    s.role == Leader,
    s.generation == 1,
    s.placement == ABC,
    s.forgotten.size() == 0,
    s' = {
      ...s,
      placement: BCD,
      preferred_leader: 2,
      generation: s.generation + 1,
      last_event: SnapshotRestored,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  action reconcile_follower_hold = all {
    s.role == Follower,
    s' = {
      ...s,
      last_event: FollowerHeld,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  action reconcile_incomplete_hold = all {
    s.role == Leader,
    s.placement.size() > 0,
    not(s.placement.subseteq(s.known)),
    s' = {
      ...s,
      last_event: IncompleteHeld,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  // With only B and C healthy, planning would return two replicas. The degraded
  // floor therefore retains ABC and, critically, retains A in membership too.
  action reconcile_degraded_hold = all {
    s.role == Leader,
    s.placement == ABC,
    s.placement.subseteq(s.known),
    s.healthy == Set(2, 3),
    s.draining.contains(1),
    not(s.hosted.contains(1)),
    s' = {
      ...s,
      last_event: DegradedHeld,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  // Even a directly proposed durable ForgetNode is rejected while placement names A.
  action attempt_forget_a_rejected = all {
    s.role == Leader,
    s.known.contains(1),
    s.draining.contains(1),
    not(s.hosted.contains(1)),
    s.placement.contains(1),
    s' = {
      ...s,
      last_event: ForgetRejected,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  action reconcile_cold_adopt = all {
    s.role == Leader,
    s.placement.size() == 0,
    s.known == ABC,
    s.healthy == ABC,
    s.hosted == ABC,
    s.observed_leader == 2,
    s' = {
      ...s,
      placement: ABC,
      preferred_leader: 2,
      generation: s.generation + 1,
      last_event: ColdAdopted,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  action reconcile_replace_dead_a = all {
    s.role == Leader,
    s.known == NODES,
    s.healthy == BCD,
    s.draining.size() == 0,
    s.placement == ABC,
    s.observed_leader == NO_NODE,
    s' = {
      ...s,
      placement: BCD,
      preferred_leader: 2,
      generation: s.generation + 1,
      last_event: DeadReplaced,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  // Once D is healthy, reconcile can publish BCD and then safely finalize A in the
  // same leader tick because the committed assignment no longer references A.
  action reconcile_replace_and_forget_drained_a = all {
    s.role == Leader,
    s.known == NODES,
    s.healthy == BCD,
    s.draining == Set(1),
    not(s.hosted.contains(1)),
    s.placement == ABC,
    s.observed_leader == NO_NODE,
    s' = {
      ...s,
      known: BCD,
      draining: Set(),
      placement: BCD,
      preferred_leader: 2,
      generation: s.generation + 1,
      forgotten: s.forgotten.union(Set(1)),
      last_event: DrainForgotten,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  // These are the stable one-shard fixed points of the production planner under
  // target_nodes=RF=3. A second tick must not rewrite placement or rotate leaders.
  action reconcile_idempotent = all {
    s.role == Leader,
    s.placement.subseteq(s.known),
    s.placement.subseteq(s.healthy),
    or {
      and { s.placement == ABC, s.preferred_leader == 1, s.observed_leader == 1 },
      and { s.placement == ABC, s.preferred_leader == 2, s.observed_leader == 2 },
      and { s.placement == BCD, s.preferred_leader == 2, s.observed_leader == NO_NODE },
    },
    s' = {
      ...s,
      last_event: IdempotentHeld,
      before_role: s.role,
      before_known: s.known,
      before_healthy: s.healthy,
      before_draining: s.draining,
      before_hosted: s.hosted,
      before_observed_leader: s.observed_leader,
      before_placement: s.placement,
      before_preferred_leader: s.preferred_leader,
      before_generation: s.generation,
      before_forgotten: s.forgotten,
    },
  }

  action idle = s' = s

  action step = any {
    become_follower,
    become_leader,
    lose_a_from_soft_membership,
    heartbeat_a,
    add_d,
    mark_a_dead,
    begin_drain_a,
    evacuate_a,
    prepare_cold_start,
    restore_snapshot_bcd,
    reconcile_follower_hold,
    reconcile_incomplete_hold,
    reconcile_degraded_hold,
    attempt_forget_a_rejected,
    reconcile_cold_adopt,
    reconcile_replace_dead_a,
    reconcile_replace_and_forget_drained_a,
    reconcile_idempotent,
    idle,
  }

  val identifiers_are_bounded = and {
    s.known.subseteq(NODES),
    s.healthy.subseteq(NODES),
    s.draining.subseteq(NODES),
    s.hosted.subseteq(NODES),
    s.placement.subseteq(NODES),
    s.forgotten.subseteq(NODES),
    s.healthy.subseteq(s.known),
    s.draining.subseteq(s.known),
    s.hosted.subseteq(s.known),
    s.healthy.intersect(s.draining).size() == 0,
    s.forgotten.intersect(s.known).size() == 0,
    or { s.observed_leader == NO_NODE, NODES.contains(s.observed_leader) },
    or { s.preferred_leader == NO_NODE, NODES.contains(s.preferred_leader) },
  }

  val placement_shape = and {
    s.placement.size() <= RF,
    or {
      s.preferred_leader == NO_NODE,
      s.placement.contains(s.preferred_leader),
    },
    s.forgotten.intersect(s.placement).size() == 0,
  }

  val observed_leader_is_eligible = or {
    s.observed_leader == NO_NODE,
    and {
      s.known.contains(s.observed_leader),
      s.healthy.contains(s.observed_leader),
      s.hosted.contains(s.observed_leader),
    },
  }

  val generation_is_finite_and_positive = and {
    s.generation >= 1,
    s.generation <= 3,
  }

  val hold_or_rejection_preserves_observable_state =
    not(or {
      s.last_event == FollowerHeld,
      s.last_event == IncompleteHeld,
      s.last_event == DegradedHeld,
      s.last_event == ForgetRejected,
      s.last_event == IdempotentHeld,
    }) or and {
      s.role == s.before_role,
      s.known == s.before_known,
      s.healthy == s.before_healthy,
      s.draining == s.before_draining,
      s.hosted == s.before_hosted,
      s.observed_leader == s.before_observed_leader,
      s.placement == s.before_placement,
      s.preferred_leader == s.before_preferred_leader,
      s.generation == s.before_generation,
      s.forgotten == s.before_forgotten,
    }

  val changed_assignment_is_leader_only =
    not(or {
      s.last_event == ColdAdopted,
      s.last_event == DeadReplaced,
      s.last_event == DrainForgotten,
      s.last_event == SnapshotRestored,
      s.last_event == ColdStartPrepared,
    }) or s.before_role == Leader

  val assignment_changes_advance_generation_once =
    not(or {
      s.last_event == ColdAdopted,
      s.last_event == DeadReplaced,
      s.last_event == DrainForgotten,
      s.last_event == SnapshotRestored,
      s.last_event == ColdStartPrepared,
    }) or s.generation == s.before_generation + 1

  val drain_finalization_is_safe =
    s.last_event != DrainForgotten or and {
      s.placement == BCD,
      not(s.known.contains(1)),
      not(s.draining.contains(1)),
      s.forgotten.contains(1),
    }

  val snapshot_is_authoritative =
    s.last_event != SnapshotRestored or and {
      s.placement == BCD,
      s.preferred_leader == 2,
      s.generation == s.before_generation + 1,
    }

  val scheduler_safety = and {
    identifiers_are_bounded,
    placement_shape,
    observed_leader_is_eligible,
    generation_is_finite_and_positive,
    hold_or_rejection_preserves_observable_state,
    changed_assignment_is_leader_only,
    assignment_changes_advance_generation_once,
    drain_finalization_is_safe,
    snapshot_is_authoritative,
  }

  val follower_hold_reached = s.last_event == FollowerHeld
  val incomplete_membership_hold_reached = s.last_event == IncompleteHeld
  val degraded_membership_hold_reached = s.last_event == DegradedHeld
  val direct_forget_rejected_reached = s.last_event == ForgetRejected
  val cold_adoption_reached = s.last_event == ColdAdopted
  val dead_replacement_reached = s.last_event == DeadReplaced
  val drain_finalization_reached = s.last_event == DrainForgotten
  val idempotent_reconcile_reached = s.last_event == IdempotentHeld
  val snapshot_restore_reached = s.last_event == SnapshotRestored
}
