# Formal-methods change procedure

This file governs how correctness claims are proposed, checked, reviewed, and maintained for `fiducia-brain`. Existing executable models and reproduction commands remain documented in [`README.md`](README.md). This procedure does **not** broaden those models' current proof boundary.

## Why this repository needs a procedure

The brain turns soft, delayed observations into authoritative cluster placement. A small semantic regression can remove a live replica, resurrect a draining node, admit an unsafe reconfiguration, or regress replicated placement state. Example tests remain necessary, but they do not by themselves enumerate heartbeat reordering, leader changes, clock movement, incomplete membership after restart, and degraded-replication schedules.

The machine-readable review inventory is [`procedure.toml`](procedure.toml).

## Change procedure

1. **Identify the affected machine before implementation.** A change to a listed source path, state enum, timeout, health predicate, placement rule, snapshot, generation, or retry rule requires reviewing `procedure.toml` in the same pull request.
2. **State the property before selecting the tool.** Express safety as behavior that must never occur. Express liveness together with clock, fairness, eventual-delivery, and healthy-capacity assumptions.
3. **Choose the smallest sound method.** Quint/TLC or Apalache is appropriate for membership and reconfiguration schedules. A Rust refinement harness establishes implementation replay. Loom/Shuttle may check implementation interleavings but do not replace the protocol model.
4. **Keep pull-request and scheduled profiles distinct.** Pull requests run deterministic traces and a documented small state space. Scheduled checks may increase node count, step depth, clock range, and fault schedules.
5. **Compare canonical state.** Refinement adapters compare accepted heartbeat sequence, health, desired replicas, preferred leader, phase, and generation—not map order, diagnostics, or incidental storage layout.
6. **Review the claim as carefully as the code.** A reviewer must be able to state what was checked, under which exact bounds and assumptions, and what remains outside the result.

## Claim language

Every CI or pull-request claim must use one of these classes:

- **typechecked specification** — the model parses and type-checks;
- **randomized exploration** — no violation was found for the recorded seed, samples, and steps;
- **bounded exhaustive verification** — every behavior inside the stated finite domain and depth was checked;
- **implementation replay** — production Rust matched the supplied model traces;
- **differential replay** — two implementations produced the same canonical observations;
- **unbounded proof** — used only when the method genuinely establishes one under explicit assumptions.

Never shorten a bounded result to “proved correct.” Reports must record model hash, implementation revision, tool versions, constants, state/step limits, seed where applicable, timeout/resource limits, and assumptions.

## Counterexamples

A counterexample is a product artifact:

1. preserve the original trace and provenance;
2. minimize it without changing the failure;
3. classify model defect, implementation defect, or assumption mismatch;
4. add a deterministic Rust regression test when implementation behavior is implicated;
5. keep the minimized trace under `formal/regressions/`; and
6. update the affected invariant, abstraction, or assumption before closing the finding.

A failing trace must not be hidden by deleting a reachable action or merely increasing a timeout.

## Required review triggers

Formal review is mandatory when a pull request changes heartbeat acceptance, health transitions, timeout/oracle reconciliation, drain behavior, reconfiguration phases, candidate eligibility, replication-factor behavior, observed-state adoption, leader transfer, snapshot/generation semantics, or command commit/application ordering.

Full Raft verification remains a separate refinement. Any model that assumes one stable elected leader must print that assumption in its result rather than allowing readers to infer that Raft itself was checked.
