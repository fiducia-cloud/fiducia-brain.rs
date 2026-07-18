# fiducia-brain — next steps (2026-07-18 deep-dive)

Concrete follow-ups found by the platform deep-dive. Full context + cross-repo
sequencing: `fiducia-monorepo/docs/discovery-backlog-2026-07.md` and
`.../hardening-roadmap-2026-07.md`. Ordered by value.

## P0 — do first
- **C3 (CRITICAL, ~S) — empty-shard-wipe on total node loss.** `scheduler.rs:181-184,262-291`.
  When every node is Dead-but-known, the absent-only shrink guard passes and
  `plan_replicas(...,[],rf)` returns `[]` for every shard → the empty placement commits and
  serves at `GET /v1/placement` → fleet-wide unavailability.
  **Fix:** before the per-shard loop, if `healthy_ids.len() < rf` treat the tick as incomplete
  membership; never propose a replica set smaller than `min(current.len(), rf)`.
  **Test first:** all-Dead-but-known membership + one reconcile → assert no `AssignShard{replicas:[]}`
  and `generation()` unchanged.

## P1
- **H1 (~S) — failure detection uses wall-clock, not monotonic.** `scheduler.rs:334-339`,
  `membership.rs:184`, `api.rs:111-116`. A forward NTP/VM-migration jump > `dead_after_ms`
  marks the whole fleet Dead in one sweep → triggers C3. Track liveness on `Instant`; keep
  wall-clock only for display.
- **H10 (~S, infra-side) — member id must be dialable.** Code contract at `main.rs:145`
  requires the member id be this member's dialable peer-plane URL; infra sets
  `$(POD_NAME).$(CLUSTER)` (portless, not cross-cluster resolvable). Fix is in
  `fiducia-infra` (`base/components/brain/statefulset.yaml:67` → set from `topology.toml`
  `brain_endpoint`); brain-side, ensure `normalize_member_url` rejects/upgrades a portless id.
- **H11 (~M) — placement `generation` not monotonic across restart/failover/snapshot.**
  `placement.rs:22-26,96`, `main.rs:107`, `raft_driver.rs:678`. Derive it from the Raft commit
  index; confirm the node/LB poller compares `!=` not `>`.
- **H17 (infra) — no PodDisruptionBudget** for the brain StatefulSet → quorum loss on drain.

## P2 — brain-Raft rigor cluster (port node's persistence discipline; one workstream)
- **M5** checksum on persisted files + inverse `base_index`/snapshot check (`raft_store.rs:26,165`).
- **M7** reject gapped / at-or-below-`base_index` AppendEntries (`raft.rs:588`) — node validates, brain doesn't.
- **M8** derive `match_index` leader-side instead of trusting the follower report (`raft.rs:606,625,800`).
- **M9** replace the `debug_assert!` committed-truncation guard (compiled out in release) with a hard runtime guard + re-clamp `commit_index` (`raft.rs:719`).
- **M10** torn-vs-corrupt log-record distinction on load (`raft_store.rs:85`) — port `node/persist.rs:150`.
- **M12** `ForgetNode` Dead-and-shardless nodes after a grace window (`scheduler.rs:297`, `membership.rs:177`) — tombstone leak + enabler of C3.
- **M13** preserve ≥RF failure domains when trimming for scale-down (`scheduler.rs:341`).
- **M23** read `FIDUCIA_RAFT_*` + `FIDUCIA_REPLICATION_FACTOR` (brain is the cross-cloud Raft group) or drop them from its env surface.

## Test-coverage gaps to backfill (guardrails)
- P3-4: `AssignShard` through Raft commit + snapshot round-trip (all current tests use `SetScalePlan`; `placement.rs` has no test module).
- P3-5: `reconcile()`/`plan_replicas` anti-churn fixed point + at-most-one-replica bound.

Toolchain: pins 1.95.0 — build/test with `RUSTC`+`RUSTDOC` set to the rustup 1.95.0 binaries
(a Homebrew 1.96.1 on PATH shadows rustup and breaks doctests). See the hardening-roadmap doc.
