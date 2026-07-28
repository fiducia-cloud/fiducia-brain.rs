// Per-node executable model of fiducia-brain's hybrid failure detector.
//
// Membership nodes do not transition jointly, so one scalar machine is sufficient
// for exhaustive checking. This keeps TLC focused on behavior rather than a
// Cartesian product of independent copies.
module brain_membership {
  type Health = Healthy | Suspect | Dead | Draining
  type Age = Fresh | SuspectAge | DeadAge
  type Oracle = OracleUnknown | OracleRunning | OracleGone
  type Event =
    | InitEvent
    | HeartbeatAccepted
    | StaleIgnored
    | SweepOther
    | RunningHeld
    | GoneDead
    | TimeoutDead
    | DrainEvent
    | DrainingHeartbeat
    | Resurrected

  pure val SEQS = Set(0, 1, 2, 3)
  pure val REPORT_VERSIONS = Set(0, 1, 2)

  type State = {
    health: Health,
    age: Age,
    oracle: Oracle,
    last_seq: int,
    report_version: int,
    ever_draining: bool,
    last_event: Event,
    ignored_seq_baseline: int,
    ignored_report_baseline: int,
    ignored_health_baseline: Health,
    ignored_age_baseline: Age,
  }

  var s: State

  pure def swept_health(current: Health, age: Age, oracle: Oracle): Health =
    if (current == Draining) Draining
    else if (oracle == OracleGone) Dead
    else if (oracle == OracleRunning and age == DeadAge) Suspect
    else if (age == DeadAge) Dead
    else if (age == SuspectAge) Suspect
    else Healthy

  pure def sweep_event(current: Health, age: Age, oracle: Oracle): Event =
    if (current == Draining) SweepOther
    else if (oracle == OracleGone) GoneDead
    else if (oracle == OracleRunning and age == DeadAge) RunningHeld
    else if (oracle == OracleUnknown and age == DeadAge) TimeoutDead
    else SweepOther

  pure def next_age(current: Age, candidate: Age): bool = or {
    current == Fresh and candidate == SuspectAge,
    current == SuspectAge and candidate == DeadAge,
  }

  action init = {
    s' = {
      health: Healthy,
      age: Fresh,
      oracle: OracleUnknown,
      last_seq: 0,
      report_version: 0,
      ever_draining: false,
      last_event: InitEvent,
      ignored_seq_baseline: 0,
      ignored_report_baseline: 0,
      ignored_health_baseline: Healthy,
      ignored_age_baseline: Fresh,
    }
  }

  // Positive sequences must be strictly newer. Legacy seq == 0 is always
  // accepted, but cannot lower the accepted sequence floor.
  action accept_heartbeat(seq, report) = all {
    SEQS.contains(seq),
    REPORT_VERSIONS.contains(report),
    seq == 0 or seq > s.last_seq,
    s' = {
      ...s,
      health: if (s.health == Draining) Draining else Healthy,
      age: Fresh,
      last_seq: if (seq == 0) s.last_seq else seq,
      report_version: report,
      last_event:
        if (s.health == Draining) DrainingHeartbeat
        else if (s.health == Dead) Resurrected
        else HeartbeatAccepted,
    },
  }

  // Delayed/duplicate sequenced reports refresh nothing and cannot overwrite the
  // report accepted with a newer sequence.
  action ignore_stale_heartbeat(seq, report) = all {
    SEQS.contains(seq),
    REPORT_VERSIONS.contains(report),
    seq > 0,
    seq <= s.last_seq,
    s' = {
      ...s,
      last_event: StaleIgnored,
      ignored_seq_baseline: s.last_seq,
      ignored_report_baseline: s.report_version,
      ignored_health_baseline: s.health,
      ignored_age_baseline: s.age,
    },
  }

  action sweep = {
    s' = {
      ...s,
      health: swept_health(s.health, s.age, s.oracle),
      last_event: sweep_event(s.health, s.age, s.oracle),
    }
  }

  // Milliseconds are abstracted to the only two threshold crossings that change
  // behavior. Crossing and applying the periodic sweep is one atomic model step.
  action age_and_sweep(candidate) = all {
    next_age(s.age, candidate),
    s' = {
      ...s,
      age: candidate,
      health: swept_health(s.health, candidate, s.oracle),
      last_event: sweep_event(s.health, candidate, s.oracle),
    },
  }

  // The membership loop consumes one coherent oracle verdict per sweep.
  action oracle_and_sweep(verdict) = {
    s' = {
      ...s,
      oracle: verdict,
      health: swept_health(s.health, s.age, verdict),
      last_event: sweep_event(s.health, s.age, verdict),
    }
  }

  action drain = {
    s' = {
      ...s,
      health: Draining,
      ever_draining: true,
      last_event: DrainEvent,
    }
  }

  action idle = s' = s

  action step = {
    nondet seq = SEQS.oneOf()
    nondet report = REPORT_VERSIONS.oneOf()
    nondet age = Set(SuspectAge, DeadAge).oneOf()
    nondet verdict = Set(OracleUnknown, OracleRunning, OracleGone).oneOf()
    any {
      accept_heartbeat(seq, report),
      ignore_stale_heartbeat(seq, report),
      sweep,
      age_and_sweep(age),
      oracle_and_sweep(verdict),
      drain,
      idle,
    }
  }

  val values_are_bounded = and {
    s.last_seq >= 0,
    s.last_seq <= 3,
    REPORT_VERSIONS.contains(s.report_version),
  }

  val draining_is_sticky =
    not(s.ever_draining) or s.health == Draining

  val stale_heartbeat_preserves_newer_state =
    s.last_event != StaleIgnored or and {
      s.last_seq == s.ignored_seq_baseline,
      s.report_version == s.ignored_report_baseline,
      s.health == s.ignored_health_baseline,
      s.age == s.ignored_age_baseline,
    }

  val heartbeat_state_is_expected = and {
    not(s.last_event == HeartbeatAccepted or s.last_event == Resurrected)
      or and { s.health == Healthy, s.age == Fresh },
    s.last_event != DrainingHeartbeat
      or and { s.health == Draining, s.age == Fresh },
  }

  // swept_health is idempotent, so the resulting state can validate the exact
  // timeout/oracle merge without retaining a second copy of pre-sweep state.
  val sweep_state_is_expected =
    not(Set(SweepOther, RunningHeld, GoneDead, TimeoutDead).contains(s.last_event))
      or s.health == swept_health(s.health, s.age, s.oracle)

  val event_classification_is_exact = and {
    s.last_event != RunningHeld or and {
      s.oracle == OracleRunning,
      s.age == DeadAge,
      s.health == Suspect,
    },
    s.last_event != GoneDead or and {
      s.oracle == OracleGone,
      s.health == Dead,
    },
    s.last_event != TimeoutDead or and {
      s.oracle == OracleUnknown,
      s.age == DeadAge,
      s.health == Dead,
    },
  }

  val membership_safety = and {
    values_are_bounded,
    draining_is_sticky,
    stale_heartbeat_preserves_newer_state,
    heartbeat_state_is_expected,
    sweep_state_is_expected,
    event_classification_is_exact,
  }

  val stale_delivery_reached = s.last_event == StaleIgnored
  val running_oracle_hold_reached = s.last_event == RunningHeld
  val gone_oracle_dead_reached = s.last_event == GoneDead
  val timeout_dead_reached = s.last_event == TimeoutDead
  val draining_heartbeat_reached = s.last_event == DrainingHeartbeat
  val resurrection_reached = s.last_event == Resurrected
}
