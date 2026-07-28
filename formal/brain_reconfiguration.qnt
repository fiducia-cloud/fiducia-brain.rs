// Executable model of one fiducia-brain shard placement being enacted by the
// fiducia-node data plane.
//
// The brain publishes a target replica set, but safe execution is deliberately
// staged one membership change at a time:
//
//   plan -> add learner -> catch up -> promote -> transfer leadership -> remove
//
// The model also includes the scheduler's two critical hold conditions:
// incomplete soft membership after a brain failover, and fewer Healthy candidates
// than the replication factor. Target failure may abort before promotion or roll
// back the newly promoted voter without ever crossing the quorum floor.
module brain_reconfiguration {
  type Health = Healthy | Suspect | Dead | Draining
  type Phase = Stable | Planned | LearnerAdded | CaughtUp | Promoted
  type Event =
    | InitEvent
    | MembershipLost
    | MembershipRestored
    | HealthChanged
    | IncompleteHeld
    | DegradedHeld
    | MovePlanned
    | LearnerAddedEvent
    | CaughtUpEvent
    | PromotedEvent
    | LeaderTransferred
    | OldRemoved
    | UnpromotedAborted
    | PromotedRolledBack

  pure val NODES = Set(1, 2, 3, 4)
  pure val HEALTHS = Set(Healthy, Suspect, Dead, Draining)
  pure val RF = 3
  pure val NO_NODE = 0

  type State = {
    known: Set[int],
    health: int -> Health,
    voters: Set[int],
    learners: Set[int],
    leader: int,
    phase: Phase,
    move_from: int,
    move_to: int,
    voters_before_move: Set[int],
    plan_started_with_complete_membership: bool,
    plan_started_with_capacity: bool,
    last_event: Event,
  }

  var s: State

  val healthy_nodes =
    NODES.filter(node => s.health.get(node) == Healthy)

  val membership_complete_for_current_placement =
    s.voters.subseteq(s.known)

  action init = {
    s' = {
      known: NODES,
      health: NODES.mapBy(_ => Healthy),
      voters: Set(1, 2, 3),
      learners: Set(),
      leader: 1,
      phase: Stable,
      move_from: NO_NODE,
      move_to: NO_NODE,
      voters_before_move: Set(1, 2, 3),
      plan_started_with_complete_membership: true,
      plan_started_with_capacity: true,
      last_event: InitEvent,
    }
  }

  // Brain liveness membership is leader-local soft state. A newly elected brain
  // may temporarily not know a still-live voter; the scheduler must hold rather
  // than interpret absence as failure and shrink placement.
  action lose_membership(node) = all {
    s.phase == Stable,
    s.known.contains(node),
    s' = {
      ...s,
      known: s.known.exclude(Set(node)),
      last_event: MembershipLost,
    },
  }

  action restore_membership(node) = all {
    s.phase == Stable,
    NODES.contains(node),
    not(s.known.contains(node)),
    s' = {
      ...s,
      known: s.known.union(Set(node)),
      last_event: MembershipRestored,
    },
  }

  // Faults may occur while a move is in progress. Progress guards below require
  // a Healthy target; otherwise the safe exits are abort or rollback.
  action change_health(node, health) = all {
    NODES.contains(node),
    HEALTHS.contains(health),
    s' = {
      ...s,
      health: s.health.set(node, health),
      last_event: HealthChanged,
    },
  }

  action hold_incomplete_membership = all {
    s.phase == Stable,
    not(membership_complete_for_current_placement),
    s' = { ...s, last_event: IncompleteHeld },
  }

  action hold_degraded_membership = all {
    s.phase == Stable,
    membership_complete_for_current_placement,
    healthy_nodes.size() < RF,
    s' = { ...s, last_event: DegradedHeld },
  }

  // A target may be published only from a complete membership view with at least
  // RF Healthy candidates. Exactly one existing voter is replaced.
  action plan_move(from, to) = all {
    s.phase == Stable,
    membership_complete_for_current_placement,
    healthy_nodes.size() >= RF,
    s.voters.contains(from),
    s.known.contains(to),
    not(s.voters.contains(to)),
    s.health.get(to) == Healthy,
    s' = {
      ...s,
      phase: Planned,
      move_from: from,
      move_to: to,
      voters_before_move: s.voters,
      plan_started_with_complete_membership: true,
      plan_started_with_capacity: true,
      last_event: MovePlanned,
    },
  }

  action add_learner = all {
    s.phase == Planned,
    s.known.contains(s.move_to),
    s.health.get(s.move_to) == Healthy,
    s' = {
      ...s,
      learners: s.learners.union(Set(s.move_to)),
      phase: LearnerAdded,
      last_event: LearnerAddedEvent,
    },
  }

  action catch_up_learner = all {
    s.phase == LearnerAdded,
    s.learners.contains(s.move_to),
    s.health.get(s.move_to) == Healthy,
    s' = {
      ...s,
      phase: CaughtUp,
      last_event: CaughtUpEvent,
    },
  }

  // Promotion happens before removing the old voter, temporarily creating RF+1
  // voters. This is the key quorum-preserving ordering property.
  action promote_learner = all {
    s.phase == CaughtUp,
    s.learners.contains(s.move_to),
    s.health.get(s.move_to) == Healthy,
    s' = {
      ...s,
      voters: s.voters.union(Set(s.move_to)),
      learners: s.learners.exclude(Set(s.move_to)),
      phase: Promoted,
      last_event: PromotedEvent,
    },
  }

  // Leadership may transfer at any phase to a Healthy existing voter. Removal and
  // rollback guards ensure the current leader is never deleted from membership.
  action transfer_leader(candidate) = all {
    s.voters.contains(candidate),
    s.health.get(candidate) == Healthy,
    candidate != s.leader,
    s' = {
      ...s,
      leader: candidate,
      last_event: LeaderTransferred,
    },
  }

  action remove_old_voter = all {
    s.phase == Promoted,
    s.voters.contains(s.move_from),
    s.voters.contains(s.move_to),
    s.health.get(s.move_to) == Healthy,
    s.leader != s.move_from,
    s' = {
      ...s,
      voters: s.voters.exclude(Set(s.move_from)),
      phase: Stable,
      move_from: NO_NODE,
      move_to: NO_NODE,
      last_event: OldRemoved,
    },
  }

  // Before promotion, aborting removes only a learner. The original voter set is
  // untouched, regardless of why the target became unsuitable.
  action abort_unpromoted = all {
    or {
      s.phase == Planned,
      s.phase == LearnerAdded,
      s.phase == CaughtUp,
    },
    s' = {
      ...s,
      voters: s.voters_before_move,
      learners: Set(),
      phase: Stable,
      move_from: NO_NODE,
      move_to: NO_NODE,
      last_event: UnpromotedAborted,
    },
  }

  // After promotion, rollback removes the new voter and retains the old one. If
  // the new voter currently leads, leadership must first move elsewhere.
  action rollback_promoted = all {
    s.phase == Promoted,
    s.voters.contains(s.move_from),
    s.voters.contains(s.move_to),
    s.leader != s.move_to,
    s' = {
      ...s,
      voters: s.voters.exclude(Set(s.move_to)),
      learners: Set(),
      phase: Stable,
      move_from: NO_NODE,
      move_to: NO_NODE,
      last_event: PromotedRolledBack,
    },
  }

  action idle = s' = s

  action step = {
    nondet node = NODES.oneOf()
    nondet other = NODES.oneOf()
    nondet health = HEALTHS.oneOf()
    any {
      lose_membership(node),
      restore_membership(node),
      change_health(node, health),
      hold_incomplete_membership,
      hold_degraded_membership,
      plan_move(node, other),
      add_learner,
      catch_up_learner,
      promote_learner,
      transfer_leader(node),
      remove_old_voter,
      abort_unpromoted,
      rollback_promoted,
      idle,
    }
  }

  val identifiers_are_bounded = and {
    s.known.subseteq(NODES),
    s.voters.subseteq(NODES),
    s.learners.subseteq(NODES),
    NODES.contains(s.leader),
    or { s.move_from == NO_NODE, NODES.contains(s.move_from) },
    or { s.move_to == NO_NODE, NODES.contains(s.move_to) },
  }

  val quorum_floor_is_never_crossed = and {
    s.voters.size() >= RF,
    s.voters.size() <= RF + 1,
  }

  val leader_is_always_a_voter =
    s.voters.contains(s.leader)

  val learners_are_not_voters =
    s.learners.intersect(s.voters).size() == 0

  val stable_shape =
    s.phase != Stable or and {
      s.voters.size() == RF,
      s.learners.size() == 0,
      s.move_from == NO_NODE,
      s.move_to == NO_NODE,
    }

  val planned_shape =
    s.phase != Planned or and {
      s.voters == s.voters_before_move,
      s.voters.size() == RF,
      s.learners.size() == 0,
      s.voters.contains(s.move_from),
      not(s.voters.contains(s.move_to)),
      s.move_from != s.move_to,
    }

  val learner_shape =
    not(s.phase == LearnerAdded or s.phase == CaughtUp) or and {
      s.voters == s.voters_before_move,
      s.voters.size() == RF,
      s.learners == Set(s.move_to),
      s.voters.contains(s.move_from),
      not(s.voters.contains(s.move_to)),
    }

  val promoted_shape =
    s.phase != Promoted or and {
      s.voters == s.voters_before_move.union(Set(s.move_to)),
      s.voters.size() == RF + 1,
      s.learners.size() == 0,
      s.voters.contains(s.move_from),
      s.voters.contains(s.move_to),
      s.move_from != s.move_to,
    }

  val every_active_plan_started_safely =
    s.phase == Stable or and {
      s.plan_started_with_complete_membership,
      s.plan_started_with_capacity,
      s.voters_before_move.size() == RF,
    }

  val reconfiguration_safety = and {
    identifiers_are_bounded,
    quorum_floor_is_never_crossed,
    leader_is_always_a_voter,
    learners_are_not_voters,
    stable_shape,
    planned_shape,
    learner_shape,
    promoted_shape,
    every_active_plan_started_safely,
  }

  // Reachability witnesses show that both hold guards, the successful path, the
  // rollback path, and leadership transfer remain executable.
  val incomplete_membership_hold_reached = s.last_event == IncompleteHeld
  val degraded_membership_hold_reached = s.last_event == DegradedHeld
  val completed_move_reached = s.last_event == OldRemoved
  val rollback_reached = s.last_event == PromotedRolledBack
  val leader_transfer_reached = s.last_event == LeaderTransferred
}
