// Finite composition model for the desired-placement scheduler and staged
// learner/catch-up/promote/leader-transfer/remove data-plane protocol.
//
// The model allows one target failure, before or immediately after promotion.
// Once that finite fault budget is consumed, a fair scheduler that eventually
// takes enabled progress actions has an acyclic path to the requested BCD
// placement. Stuttering remains available and is not claimed as temporal
// liveness without the stated fairness assumption.
module brain_composition {
  type Phase =
    | StableABC
    | PlanIssued
    | LearnerAdded
    | LearnerCaughtUp
    | LearnerPromoted
    | LeaderTransferred
    | StableBCD

  pure val NODES = Set(1, 2, 3, 4)
  pure val ABC = Set(1, 2, 3)
  pure val BCD = Set(2, 3, 4)
  pure val ABCD = Set(1, 2, 3, 4)
  pure val RF = 3
  pure val NO_GENERATION = 0

  type State = {
    phase: Phase,
    desired_bcd: bool,
    voters: Set[int],
    learners: Set[int],
    leader: int,
    generation: int,
    active_generation: int,
    failure_available: bool,
    saw_move_started: bool,
    saw_active_reconcile_hold: bool,
    saw_pre_promotion_rollback: bool,
    saw_post_promotion_rollback: bool,
    saw_retry: bool,
    saw_promotion: bool,
    saw_leader_transfer: bool,
    saw_convergence: bool,
    saw_idempotent_stable_replay: bool,
  }

  var s: State

  action init = {
    s' = {
      phase: StableABC,
      desired_bcd: false,
      voters: ABC,
      learners: Set(),
      leader: 1,
      generation: 1,
      active_generation: NO_GENERATION,
      failure_available: true,
      saw_move_started: false,
      saw_active_reconcile_hold: false,
      saw_pre_promotion_rollback: false,
      saw_post_promotion_rollback: false,
      saw_retry: false,
      saw_promotion: false,
      saw_leader_transfer: false,
      saw_convergence: false,
      saw_idempotent_stable_replay: false,
    }
  }

  action request_bcd = all {
    s.phase == StableABC,
    not(s.desired_bcd),
    s' = {
      ...s,
      phase: PlanIssued,
      desired_bcd: true,
      generation: s.generation + 1,
      active_generation: s.generation + 1,
      saw_move_started: true,
    },
  }

  action retry_bcd = all {
    s.phase == StableABC,
    s.desired_bcd,
    s.active_generation == NO_GENERATION,
    s' = {
      ...s,
      phase: PlanIssued,
      generation: s.generation + 1,
      active_generation: s.generation + 1,
      saw_retry: true,
    },
  }

  action add_learner_d = all {
    s.phase == PlanIssued,
    s' = {
      ...s,
      phase: LearnerAdded,
      learners: Set(4),
    },
  }

  action catch_up_learner_d = all {
    s.phase == LearnerAdded,
    s.learners == Set(4),
    s' = { ...s, phase: LearnerCaughtUp },
  }

  action fail_before_promotion = all {
    s.failure_available,
    or {
      s.phase == LearnerAdded,
      s.phase == LearnerCaughtUp,
    },
    s' = {
      ...s,
      phase: StableABC,
      learners: Set(),
      active_generation: NO_GENERATION,
      failure_available: false,
      saw_pre_promotion_rollback: true,
    },
  }

  action promote_learner_d = all {
    s.phase == LearnerCaughtUp,
    s.learners == Set(4),
    s' = {
      ...s,
      phase: LearnerPromoted,
      voters: ABCD,
      learners: Set(),
      saw_promotion: true,
    },
  }

  // The target may fail after promotion while A is still leader. Removing D
  // restores ABC and retains RF=3; the scheduler may then issue one new
  // generation because the finite fault budget is consumed.
  action fail_after_promotion = all {
    s.phase == LearnerPromoted,
    s.failure_available,
    s.leader == 1,
    s' = {
      ...s,
      phase: StableABC,
      voters: ABC,
      active_generation: NO_GENERATION,
      failure_available: false,
      saw_post_promotion_rollback: true,
    },
  }

  action transfer_leader_to_b = all {
    s.phase == LearnerPromoted,
    s.voters == ABCD,
    s.leader == 1,
    s' = {
      ...s,
      phase: LeaderTransferred,
      leader: 2,
      saw_leader_transfer: true,
    },
  }

  action remove_old_voter_a = all {
    s.phase == LeaderTransferred,
    s.voters == ABCD,
    s.leader == 2,
    s' = {
      ...s,
      phase: StableBCD,
      voters: BCD,
      active_generation: NO_GENERATION,
      saw_convergence: true,
    },
  }

  // Brain reconciliation while a data-plane generation is active must hold the
  // same generation rather than overlap a second replacement.
  action reconcile_active_hold = all {
    s.active_generation != NO_GENERATION,
    s' = { ...s, saw_active_reconcile_hold: true },
  }

  action replay_stable = all {
    s.phase == StableBCD,
    s' = { ...s, saw_idempotent_stable_replay: true },
  }

  action idle = s' = s

  action step = any {
    request_bcd,
    retry_bcd,
    add_learner_d,
    catch_up_learner_d,
    fail_before_promotion,
    promote_learner_d,
    fail_after_promotion,
    transfer_leader_to_b,
    remove_old_voter_a,
    reconcile_active_hold,
    replay_stable,
    idle,
  }

  val identities_are_bounded = and {
    s.voters.subseteq(NODES),
    s.learners.subseteq(NODES),
    s.learners.forall(node => not(s.voters.contains(node))),
    s.learners.size() <= 1,
  }

  val quorum_floor_is_preserved = s.voters.size() >= RF
  val leader_is_always_a_voter = s.voters.contains(s.leader)

  val phase_shape_is_consistent = or {
    and {
      s.phase == StableABC,
      s.voters == ABC,
      s.learners.size() == 0,
      s.leader == 1,
      s.active_generation == NO_GENERATION,
    },
    and {
      s.phase == PlanIssued,
      s.voters == ABC,
      s.learners.size() == 0,
      s.leader == 1,
      s.active_generation == s.generation,
      s.desired_bcd,
    },
    and {
      or { s.phase == LearnerAdded, s.phase == LearnerCaughtUp },
      s.voters == ABC,
      s.learners == Set(4),
      s.leader == 1,
      s.active_generation == s.generation,
      s.desired_bcd,
    },
    and {
      s.phase == LearnerPromoted,
      s.voters == ABCD,
      s.learners.size() == 0,
      s.leader == 1,
      s.active_generation == s.generation,
      s.desired_bcd,
    },
    and {
      s.phase == LeaderTransferred,
      s.voters == ABCD,
      s.learners.size() == 0,
      s.leader == 2,
      s.active_generation == s.generation,
      s.desired_bcd,
    },
    and {
      s.phase == StableBCD,
      s.voters == BCD,
      s.learners.size() == 0,
      s.leader == 2,
      s.active_generation == NO_GENERATION,
      s.desired_bcd,
    },
  }

  val one_generation_is_active = or {
    s.active_generation == NO_GENERATION,
    s.active_generation == s.generation,
  }

  // One initial plan plus at most one retry after the single modeled target
  // failure bounds generation growth and rules out overlapping retry storms.
  val generation_is_bounded = and {
    s.generation >= 1,
    s.generation <= 3,
  }

  val request_is_monotonic = or {
    s.desired_bcd,
    and {
      s.phase == StableABC,
      s.generation == 1,
      s.failure_available,
    },
  }

  val rollback_consumes_fault_budget = or {
    not(or {
      s.saw_pre_promotion_rollback,
      s.saw_post_promotion_rollback,
    }),
    not(s.failure_available),
  }

  val old_voter_is_removed_only_after_transfer = or {
    s.voters.contains(1),
    and {
      s.phase == StableBCD,
      s.leader == 2,
    },
  }

  val composition_safety = and {
    identities_are_bounded,
    quorum_floor_is_preserved,
    leader_is_always_a_voter,
    phase_shape_is_consistent,
    one_generation_is_active,
    generation_is_bounded,
    request_is_monotonic,
    rollback_consumes_fault_budget,
    old_voter_is_removed_only_after_transfer,
  }

  val move_started_reached = s.saw_move_started
  val active_reconcile_hold_reached = s.saw_active_reconcile_hold
  val pre_promotion_rollback_reached = s.saw_pre_promotion_rollback
  val post_promotion_rollback_reached = s.saw_post_promotion_rollback
  val retry_reached = s.saw_retry
  val promotion_reached = s.saw_promotion
  val leader_transfer_reached = s.saw_leader_transfer
  val convergence_reached = s.saw_convergence
  val idempotent_stable_replay_reached = s.saw_idempotent_stable_replay
}
