// One fiducia-brain shard placement enacted by the fiducia-node data plane.
//
// Safe replacement is staged:
//   plan -> add learner -> catch up -> promote -> transfer leader -> remove old
//
// The scheduler observes only whether a node is Healthy; Suspect, Dead, and
// Draining are all excluded candidates. Representing that exact predicate as a set
// avoids multiplying equivalent states while preserving placement behavior.
module brain_reconfiguration {
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
  pure val RF = 3
  pure val NO_NODE = 0

  type State = {
    known: Set[int],
    healthy: Set[int],
    voters: Set[int],
    learners: Set[int],
    leader: int,
    phase: Phase,
    move_from: int,
    move_to: int,
    voters_before_move: Set[int],
    known_at_plan: Set[int],
    healthy_at_plan: Set[int],
    last_event: Event,
  }

  var s: State

  val membership_complete =
    s.voters.subseteq(s.known)

  action init = {
    s' = {
      known: NODES,
      healthy: NODES,
      voters: Set(1, 2, 3),
      learners: Set(),
      leader: 1,
      phase: Stable,
      move_from: NO_NODE,
      move_to: NO_NODE,
      voters_before_move: Set(1, 2, 3),
      known_at_plan: NODES,
      healthy_at_plan: NODES,
      last_event: InitEvent,
    }
  }

  // Membership is leader-local soft state. On brain failover, missing knowledge of
  // a current voter must hold placement rather than shrink it.
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

  action mark_healthy(node) = all {
    NODES.contains(node),
    s' = {
      ...s,
      healthy: s.healthy.union(Set(node)),
      last_event: HealthChanged,
    },
  }

  action mark_unhealthy(node) = all {
    NODES.contains(node),
    s' = {
      ...s,
      healthy: s.healthy.exclude(Set(node)),
      last_event: HealthChanged,
    },
  }

  action hold_incomplete_membership = all {
    s.phase == Stable,
    not(membership_complete),
    s' = { ...s, last_event: IncompleteHeld },
  }

  action hold_degraded_membership = all {
    s.phase == Stable,
    membership_complete,
    s.healthy.size() < RF,
    s' = { ...s, last_event: DegradedHeld },
  }

  // Exactly one voter is selected for replacement from a complete view with at
  // least RF Healthy candidates. Snapshots preserve the start preconditions even
  // if health changes while the data plane executes the move.
  action plan_move(source, target) = all {
    s.phase == Stable,
    membership_complete,
    s.healthy.size() >= RF,
    s.voters.contains(source),
    s.known.contains(target),
    not(s.voters.contains(target)),
    s.healthy.contains(target),
    s' = {
      ...s,
      phase: Planned,
      move_from: source,
      move_to: target,
      voters_before_move: s.voters,
      known_at_plan: s.known,
      healthy_at_plan: s.healthy,
      last_event: MovePlanned,
    },
  }

  action add_learner = all {
    s.phase == Planned,
    s.known.contains(s.move_to),
    s.healthy.contains(s.move_to),
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
    s.healthy.contains(s.move_to),
    s' = {
      ...s,
      phase: CaughtUp,
      last_event: CaughtUpEvent,
    },
  }

  // Promotion precedes removal, temporarily creating RF+1 voters.
  action promote_learner = all {
    s.phase == CaughtUp,
    s.learners.contains(s.move_to),
    s.healthy.contains(s.move_to),
    s' = {
      ...s,
      voters: s.voters.union(Set(s.move_to)),
      learners: s.learners.exclude(Set(s.move_to)),
      phase: Promoted,
      last_event: PromotedEvent,
    },
  }

  action transfer_leader(candidate) = all {
    s.voters.contains(candidate),
    s.healthy.contains(candidate),
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
    s.healthy.contains(s.move_to),
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

  // Before promotion, aborting removes at most the non-voting learner.
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
  // the new voter leads, leadership must first transfer elsewhere.
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
    any {
      lose_membership(node),
      restore_membership(node),
      mark_healthy(node),
      mark_unhealthy(node),
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
    s.healthy.subseteq(NODES),
    s.voters.subseteq(NODES),
    s.learners.subseteq(NODES),
    s.known_at_plan.subseteq(NODES),
    s.healthy_at_plan.subseteq(NODES),
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
      s.voters_before_move.size() == RF,
      s.voters_before_move.subseteq(s.known_at_plan),
      s.healthy_at_plan.size() >= RF,
      s.known_at_plan.contains(s.move_to),
      s.healthy_at_plan.contains(s.move_to),
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

  val incomplete_membership_hold_reached = s.last_event == IncompleteHeld
  val degraded_membership_hold_reached = s.last_event == DegradedHeld
  val completed_move_reached = s.last_event == OldRemoved
  val rollback_reached = s.last_event == PromotedRolledBack
  val leader_transfer_reached = s.last_event == LeaderTransferred
}
