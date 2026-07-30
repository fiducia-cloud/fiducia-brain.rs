# fiducia-brain formal verification

This directory contains executable, finite Quint models for the three control-plane
state machines most likely to turn a transient failure into lost quorum or unsafe
placement.

The models are intentionally small. Each abstraction has a stated boundary and is
checked independently so a counterexample identifies the contract that failed.
They complement the Rust unit/integration tests; they do not claim to prove the
entire process, networking stack, Raft implementation, or Kubernetes deployment.

## 1. Hybrid membership and failure detection

[`brain_membership.qnt`](brain_membership.qnt) mirrors the behavior in
`src/membership.rs`:

- heartbeat age crosses only the behaviorally relevant `Fresh`, `SuspectAge`, and
  `DeadAge` bands;
- `OracleGone` declares a non-draining node dead on the next sweep;
- `OracleRunning` holds a silent node at `Suspect` rather than declaring it dead;
- `OracleUnknown` trusts the timeout result;
- `Draining` is sticky across both heartbeats and sweeps;
- a positive heartbeat sequence must be strictly newer;
- delayed/duplicate sequenced reports preserve the accepted shard/report state;
- legacy `seq == 0` heartbeats remain accepted and never lower the sequence floor;
- a fresh heartbeat can resurrect a timeout-dead node.

The detector is per-node and has no cross-node transition. The exhaustive model
therefore uses one representative node rather than multiplying the same automaton
across an irrelevant Cartesian product.

Primary invariant: `membership_safety`.

Reachability witnesses ensure CI actually explores stale delivery, both oracle
branches, timeout death, a heartbeat while draining, and resurrection.

## 2. Quorum-preserving shard membership change

[`brain_reconfiguration.qnt`](brain_reconfiguration.qnt) models one shard with
replication factor three over four candidate nodes. It joins the brain scheduler's
placement guards to the data-plane sequence documented by the implementation:

```text
plan target
  -> add non-voting learner
  -> catch learner up
  -> promote learner
  -> transfer leadership when required
  -> remove old voter
```

It verifies:

- no move starts from an incomplete leader-local membership view;
- no move starts while fewer than RF candidates are Healthy;
- only one replacement is active;
- the new node is a learner before it becomes a voter;
- promotion occurs before removal, temporarily producing RF+1 voters;
- the current leader cannot be removed;
- target failure before promotion aborts without changing voters;
- target failure after promotion can roll back the new voter;
- every reachable state retains at least RF voters;
- learners and voters remain disjoint.

Primary invariant: `reconfiguration_safety`.

The model permits node health changes while a move is active. It abstracts the
Raft joint-consensus/log-catch-up implementation to the explicit `CaughtUp` phase;
proving the underlying replication algorithm and snapshot transport is separate
`fiducia-node` work.

## 3. Production scheduler and placement reconciliation

[`brain_scheduler.qnt`](brain_scheduler.qnt) models the leader-side desired-state
reconciler for one RF3 shard over four nodes. Unlike the staged data-plane model,
this model is replayed against the real `Scheduler`, `Membership`, `Placement`,
`plan_replicas`, leadership policy, and replicated `apply_command` boundary.

It verifies:

- a follower reconciliation call cannot publish a placement or forget command;
- a replicated assignment is held while leader-local membership is incomplete;
- degraded capacity cannot shrink the last authoritative RF3 assignment to two or
  zero replicas;
- an empty cold-start placement adopts healthy reported hosting and the observed
  leader instead of generating needless movement;
- a restored placement snapshot replaces stale local desired state;
- a confirmed-dead node is replaced once a full healthy candidate set exists;
- repeated stable reconciliation is idempotent and does not rotate preferred
  leadership because of the shard's own load contribution;
- an evacuated draining node remains known while any assignment still names it;
- a direct stale `ForgetNode` proposal is rejected at the replicated state-machine
  boundary;
- drain finalization becomes legal only after the committed assignment no longer
  references the node.

Primary invariant: `scheduler_safety`.

`tests/formal_scheduler_refinement.rs` consumes generated ITF traces and compares
the complete observable production projection after every model action: role,
known/healthy/draining membership, reported hosting, observed leader, authoritative
replicas, preferred leader, placement generation, and finalized removals.

## Reproduce locally

Quint and the Java runtime used by CI are pinned in `fm.toml`,
`fm-reconfiguration.toml`, `fm-scheduler.toml`, and the workflows. The membership
manifest is the default discovered by `fmctl`; pass an explicit manifest for the
other models.

```bash
QUINT='npx --yes --package=@informalsystems/quint@0.32.0 quint'

$QUINT typecheck formal/brain_membership.qnt
$QUINT run formal/brain_membership.qnt \
  --max-samples=10000 \
  --max-steps=35 \
  --invariant=membership_safety \
  --witnesses \
    stale_delivery_reached \
    running_oracle_hold_reached \
    gone_oracle_dead_reached \
    timeout_dead_reached \
    draining_heartbeat_reached \
    resurrection_reached
$QUINT verify formal/brain_membership.qnt \
  --backend=tlc \
  --invariant=membership_safety

$QUINT typecheck formal/brain_reconfiguration.qnt
$QUINT run formal/brain_reconfiguration.qnt \
  --max-samples=20000 \
  --max-steps=55 \
  --invariant=reconfiguration_safety \
  --witnesses \
    incomplete_membership_hold_reached \
    degraded_membership_hold_reached \
    completed_move_reached \
    rollback_reached \
    leader_transfer_reached
$QUINT verify formal/brain_reconfiguration.qnt \
  --backend=tlc \
  --invariant=reconfiguration_safety

$QUINT typecheck formal/brain_scheduler.qnt
$QUINT run formal/brain_scheduler.qnt \
  --max-samples=20000 \
  --max-steps=35 \
  --invariant=scheduler_safety \
  --witnesses \
    follower_hold_reached \
    incomplete_membership_hold_reached \
    degraded_membership_hold_reached \
    direct_forget_rejected_reached \
    cold_adoption_reached \
    dead_replacement_reached \
    drain_finalization_reached \
    idempotent_reconcile_reached \
    snapshot_restore_reached
$QUINT verify formal/brain_scheduler.qnt \
  --backend=tlc \
  --invariant=scheduler_safety
```

CI exports model-based testing traces in Informal Trace Format. The membership and
scheduler models both fail closed when their expected corpus is absent and replay
every generated transition against production Rust. The reconfiguration model
remains a design-level contract for the staged data-plane learner sequence until a
`fiducia-node` implementation adapter owns those actual membership operations.

## Deliberate limits and next refinements

These models do not yet cover:

1. replicated `fiducia-brain` Raft term/vote/log/snapshot behavior;
2. multiple shards, global load balancing, or all policy-affinity combinations;
3. real learner log indexes and snapshot installation;
4. joint-consensus details inside `fiducia-node`;
5. partitions between the brain target and data-plane observations;
6. temporal liveness under explicit fairness assumptions.

The next highest-value refinements and models are:

- Raft stale-leader, log-commit, WAL recovery, and snapshot-install safety;
- a `fiducia-node` adapter for learner/catch-up/promote/remove traces;
- effects escrow and idempotency across duplicate delivery and failover;
- task claim/reclaim and cron-fire ownership.
