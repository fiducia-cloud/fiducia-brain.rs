# Scheduler and data-plane composition model

Linear: DEN-1516, child of DEN-569

## Why this model exists

DEN-569 already verifies the `fiducia-brain` scheduler's membership and placement
reconciliation. That proof ends when the brain produces a placement generation. Production
safety also depends on how nodes receive that generation, stage data, acknowledge readiness,
activate the complete generation, survive restart, and admit writes after leader failover.

`tools/scheduler-data-plane-composition` models that next boundary without duplicating the
existing Quint scheduler model or claiming to re-prove Raft.

## Authority layers

| Layer | Authority modeled here |
| --- | --- |
| Raft | Supplies the fact that one exact placement generation committed. Full term/log/snapshot correctness remains DEN-80 work. |
| Brain scheduler | Only a leader publishes generations; generations are sequential; only the committed generation receives a write fence. |
| Node staging | A node derives its shard set from the complete snapshot, stages a newer generation, and records per-shard copy/readiness acknowledgements. |
| Node activation | A generation activates atomically only after every shard assigned to that node is acknowledged. A removal generation may activate an empty local shard set. |
| Write admission | A request must match the node's active generation, current observed leader term, installed write fence, and assigned shard. |

## Safety invariants

1. **Leader-only publication.** Followers cannot publish a placement or issue write authority.
2. **Sequential commit.** Generation `n + 1` cannot become committed before generation `n`.
3. **Canonical replica sets.** Empty placements, empty replica sets, and duplicate nodes are rejected.
4. **No rollback.** Delayed snapshots and acknowledgements cannot replace a newer staged or active generation.
5. **No partial activation.** A node keeps the complete staged snapshot and activates only after every locally assigned shard is ready.
6. **Fail closed during rollout.** Staging a newer generation revokes the previous write fence. The old active data can remain for recovery/read policy, but this model admits no writes until activation and a new fence.
7. **Term fencing.** Observing a higher leader term immediately revokes old write authority, even when the committed placement generation itself does not change.
8. **Restart safety.** Active and staged generation metadata survive restart; volatile copy acknowledgements and write authority do not. Lost state reduces availability but cannot create a partial activation or stale write.
9. **Safe removal.** A node removed by the next generation activates an empty shard set before receiving generation-matched authority.
10. **Idempotent replay.** Identical staged snapshots and acknowledgements replay without changing the result; conflicting contents for one generation fail closed.

## Publication and activation sequence

```text
brain leader publishes generation G
             │
             ▼
Raft commits exact snapshot G          (trusted input from DEN-80 boundary)
             │
             ├──────────────► brain may derive write fence(term, G)
             │
             ▼
node stages complete G and revokes prior write fence
             │
             ├── shard copy / recovery / verification
             ├── acknowledge every shard assigned to this node
             ▼
node atomically activates G
             │
             ▼
node installs write fence(term, G)
             │
             ▼
write(term, G, shard) is accepted only when shard is active locally
```

The fence is intentionally separate from placement contents. After a leader change, the new
leader can reauthorize the already committed generation in a higher term without manufacturing
a new placement. Nodes first observe the higher term, which revokes old authority; only then may
they install the replacement fence.

## Failure and replay scenarios

The executable tests cover:

- follower publication and authorization rejection;
- sequential publication and out-of-order commit rejection;
- malformed replica-set rejection;
- duplicate delivery and same-generation conflict;
- partial-copy activation denial;
- uncommitted write-authority denial;
- exact term/generation/shard write matching;
- write revocation as soon as a newer generation stages;
- same-generation reauthorization after leader failover;
- restart with durable generation metadata but lost volatile acknowledgements/fence;
- delayed generation and delayed acknowledgement rejection;
- safe removal of a node from a shard;
- all six acknowledgement orders for a three-shard node; and
- higher-term replay of the active generation.

Run locally or in CI:

```sh
cargo fmt --manifest-path tools/scheduler-data-plane-composition/Cargo.toml --all -- --check
cargo clippy --manifest-path tools/scheduler-data-plane-composition/Cargo.toml --all-targets --locked -- -D warnings
cargo test --manifest-path tools/scheduler-data-plane-composition/Cargo.toml --locked
```

## Assumptions and non-goals

This is a finite executable refinement contract, not a complete distributed-system proof.
Liveness requires a stable leader, fair scheduling, eventual heartbeat/snapshot delivery,
eventual Raft commit, and enough healthy capacity to copy every assigned shard. Network
identity/authentication, disk corruption, actual shard-copy bytes, quorum durability, Raft log
matching, and snapshot installation remain production and DEN-80 boundaries.

A future integration step should emit traces from the existing Quint scheduler model into this
composition state machine and then replay the accepted activation/fence sequence against the
real node API. This PR establishes the safety vocabulary and deterministic regression corpus for
that work.
