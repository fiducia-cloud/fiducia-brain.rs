// Executable model of fiducia-brain's hybrid membership/failure detector.
//
// The implementation combines heartbeat age with a Kubernetes-style oracle:
//   * Healthy -> Suspect -> Dead by elapsed heartbeat age;
//   * a Gone pod is Dead immediately on the next sweep;
//   * a Running pod is held at Suspect rather than declared Dead on silence;
//   * Unknown trusts the timeout verdict;
//   * Draining is operator intent and remains sticky;
//   * sequenced stale/duplicate heartbeats cannot overwrite newer reports;
//   * seq == 0 remains a legacy, always-accepted heartbeat.
//
// Nodes are independent for this detector, so a single representative node is
// sufficient for exhaustive checking and avoids multiplying an identical state
// machine across a Cartesian product.
module brain_membership {
  type Health = Healthy | Suspect | Dead | Draining
  type Age = Fresh | SuspectAge | DeadAge
  type Oracle = OracleUnknown | OracleRunning | OracleGone
  type Event =
    | InitEvent
    | HeartbeatAccepted
    | HeartbeatIgnored
    | SweepEvent
    | DrainEvent

  pure val NODES = Set(1)
  pure val SEQS = Set(0, 1, 2, 3)
  pure val REPORT_VERSIONS = Set(0, 1, 2)
  pure val NO_NODE = 0

  type State = {
    health: int -> Health,
    age: int -> Age,
    oracle: int -> Oracle,
    last_seq: int -> int,
    report_version: int -> int,
    ever_draining: Set[int],
    last_event: Event,
    last_node: int,
    ignored_seq_baseline: int,
    ignored_report_baseline: int,
    ignored_health_baseline: Health,
    ignored_age_baseline: Age,
    saw_stale_ignored: bool,
    saw_running_hold: bool,
    saw_gone_dead: bool,
    saw_timeout_dead: bool,
    saw_draining_heartbeat: bool,
    saw_resurrection: bool,
  }

  var s: State

  pure def swept_health(current: Health, age: Age, oracle: Oracle): Health =
    if (current == Draining) Draining
    else if (oracle == OracleGone) Dead
    else if (oracle == OracleRunning and age == DeadAge) Suspect
    else if (age == DeadAge) Dead
    else if (age == SuspectAge) Suspect
    else Healthy

  pure def next_age(current: Age, candidate: Age): bool = or {
    current == Fresh and candidate == SuspectAge,
    current == SuspectAge and candidate == DeadAge,
  }

  action init = {
    s' = {
      health: NODES.mapBy(_ => Healthy),
      age: NODES.mapBy(_ => Fresh),
      oracle: NODES.mapBy(_ => OracleUnknown),
      last_seq: NODES.mapBy(_ => 0),
      report_version: NODES.mapBy(_ => 0),
      ever_draining: Set(),
      last_event: InitEvent,
      last_node: NO_NODE,
      ignored_seq_baseline: 0,
      ignored_report_baseline: 0,
      ignored_health_baseline: Healthy,
      ignored_age_baseline: Fresh,
      saw_stale_ignored: false,
      saw_running_hold: false,
      saw_gone_dead: false,
      saw_timeout_dead: false,
      saw_draining_heartbeat: false,
      saw_resurrection: false,
    }
  }

  // A sequenced heartbeat is accepted only when strictly newer. The legacy
  // unsequenced form (seq == 0) is always accepted but never lowers last_seq.
  action accept_heartbeat(node, seq, report) = all {
    NODES.contains(node),
    SEQS.contains(seq),
    REPORT_VERSIONS.contains(report),
    seq == 0 or seq > s.last_seq.get(node),
    s' = {
      ...s,
      health: s.health.set(
        node,
        if (s.health.get(node) == Draining) Draining else Healthy
      ),
      age: s.age.set(node, Fresh),
      last_seq: s.last_seq.set(
        node,
        if (seq == 0) s.last_seq.get(node) else seq
      ),
      report_version: s.report_version.set(node, report),
      last_event: HeartbeatAccepted,
      last_node: node,
      saw_draining_heartbeat:
        s.saw_draining_heartbeat or s.health.get(node) == Draining,
      saw_resurrection:
        s.saw_resurrection or s.health.get(node) == Dead,
    },
  }

  // A delayed or duplicated sequenced heartbeat refreshes nothing and cannot
  // revert the address/shard report represented here by report_version.
  action ignore_stale_heartbeat(node, seq, report) = all {
    NODES.contains(node),
    SEQS.contains(seq),
    REPORT_VERSIONS.contains(report),
    seq > 0,
    seq <= s.last_seq.get(node),
    s' = {
      ...s,
      last_event: HeartbeatIgnored,
      last_node: node,
      ignored_seq_baseline: s.last_seq.get(node),
      ignored_report_baseline: s.report_version.get(node),
      ignored_health_baseline: s.health.get(node),
      ignored_age_baseline: s.age.get(node),
      saw_stale_ignored: true,
    },
  }

  // A periodic sweep applies the exact timeout/oracle merge used by membership.rs.
  action sweep(node) = all {
    NODES.contains(node),
    s' = {
      ...s,
      health: s.health.set(
        node,
        swept_health(
          s.health.get(node),
          s.age.get(node),
          s.oracle.get(node)
        )
      ),
      last_event: SweepEvent,
      last_node: node,
      saw_running_hold: s.saw_running_hold or and {
        s.oracle.get(node) == OracleRunning,
        s.age.get(node) == DeadAge,
        s.health.get(node) != Draining,
      },
      saw_gone_dead: s.saw_gone_dead or and {
        s.oracle.get(node) == OracleGone,
        s.health.get(node) != Draining,
      },
      saw_timeout_dead: s.saw_timeout_dead or and {
        s.oracle.get(node) == OracleUnknown,
        s.age.get(node) == DeadAge,
        s.health.get(node) != Draining,
      },
    },
  }

  // Time is abstracted into the only two threshold crossings that affect the
  // detector. Crossing a threshold and running the periodic sweep is one model
  // action; arbitrary millisecond values add no behavior but greatly enlarge TLC.
  action age_and_sweep(node, candidate: Age) = all {
    NODES.contains(node),
    next_age(s.age.get(node), candidate),
    s' = {
      ...s,
      age: s.age.set(node, candidate),
      health: s.health.set(
        node,
        swept_health(
          s.health.get(node),
          candidate,
          s.oracle.get(node)
        )
      ),
      last_event: SweepEvent,
      last_node: node,
      saw_running_hold: s.saw_running_hold or and {
        s.oracle.get(node) == OracleRunning,
        candidate == DeadAge,
        s.health.get(node) != Draining,
      },
      saw_gone_dead: s.saw_gone_dead or and {
        s.oracle.get(node) == OracleGone,
        s.health.get(node) != Draining,
      },
      saw_timeout_dead: s.saw_timeout_dead or and {
        s.oracle.get(node) == OracleUnknown,
        candidate == DeadAge,
        s.health.get(node) != Draining,
      },
    },
  }

  // Oracle refresh and sweep are atomic at this abstraction boundary. This models
  // the membership loop consuming one coherent oracle verdict for the node.
  action oracle_and_sweep(node, verdict: Oracle) = all {
    NODES.contains(node),
    s' = {
      ...s,
      oracle: s.oracle.set(node, verdict),
      health: s.health.set(
        node,
        swept_health(s.health.get(node), s.age.get(node), verdict)
      ),
      last_event: SweepEvent,
      last_node: node,
      saw_running_hold: s.saw_running_hold or and {
        verdict == OracleRunning,
        s.age.get(node) == DeadAge,
        s.health.get(node) != Draining,
      },
      saw_gone_dead: s.saw_gone_dead or and {
        verdict == OracleGone,
        s.health.get(node) != Draining,
      },
      saw_timeout_dead: s.saw_timeout_dead or and {
        verdict == OracleUnknown,
        s.age.get(node) == DeadAge,
        s.health.get(node) != Draining,
      },
    },
  }

  action drain(node) = all {
    NODES.contains(node),
    s' = {
      ...s,
      health: s.health.set(node, Draining),
      ever_draining: s.ever_draining.union(Set(node)),
      last_event: DrainEvent,
      last_node: node,
    },
  }

  action idle = s' = s

  action step = {
    nondet node = NODES.oneOf()
    nondet seq = SEQS.oneOf()
    nondet report = REPORT_VERSIONS.oneOf()
    nondet age = Set(SuspectAge, DeadAge).oneOf()
    nondet verdict = Set(OracleUnknown, OracleRunning, OracleGone).oneOf()
    any {
      accept_heartbeat(node, seq, report),
      ignore_stale_heartbeat(node, seq, report),
      sweep(node),
      age_and_sweep(node, age),
      oracle_and_sweep(node, verdict),
      drain(node),
      idle,
    }
  }

  val values_are_bounded = NODES.forall(node => and {
    s.last_seq.get(node) >= 0,
    s.last_seq.get(node) <= 3,
    REPORT_VERSIONS.contains(s.report_version.get(node)),
  })

  // Operator drain intent is never undone by a heartbeat or a failure sweep.
  val draining_is_sticky =
    s.ever_draining.forall(node => s.health.get(node) == Draining)

  // The ignored message may contain any old report, but every accepted field from
  // the newer report is unchanged.
  val stale_heartbeat_preserves_newer_state =
    s.last_event != HeartbeatIgnored or and {
      NODES.contains(s.last_node),
      s.last_seq.get(s.last_node) == s.ignored_seq_baseline,
      s.report_version.get(s.last_node) == s.ignored_report_baseline,
      s.health.get(s.last_node) == s.ignored_health_baseline,
      s.age.get(s.last_node) == s.ignored_age_baseline,
    }

  val accepted_heartbeat_has_expected_state =
    s.last_event != HeartbeatAccepted or and {
      NODES.contains(s.last_node),
      s.age.get(s.last_node) == Fresh,
      or {
        s.health.get(s.last_node) == Healthy,
        s.health.get(s.last_node) == Draining,
      },
    }

  // After each sweep, the stored verdict is exactly the hybrid detector function.
  // swept_health is idempotent, so this remains checkable from the resulting state.
  val sweep_applies_hybrid_verdict =
    s.last_event != SweepEvent or and {
      NODES.contains(s.last_node),
      s.health.get(s.last_node) == swept_health(
        s.health.get(s.last_node),
        s.age.get(s.last_node),
        s.oracle.get(s.last_node)
      ),
    }

  val membership_safety = and {
    values_are_bounded,
    draining_is_sticky,
    stale_heartbeat_preserves_newer_state,
    accepted_heartbeat_has_expected_state,
    sweep_applies_hybrid_verdict,
  }

  // Reachability witnesses prevent the model from becoming safely vacuous.
  val stale_delivery_reached = s.saw_stale_ignored
  val running_oracle_hold_reached = s.saw_running_hold
  val gone_oracle_dead_reached = s.saw_gone_dead
  val timeout_dead_reached = s.saw_timeout_dead
  val draining_heartbeat_reached = s.saw_draining_heartbeat
  val resurrection_reached = s.saw_resurrection
}
