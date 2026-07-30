# Rust membership refinement checks

`tests/formal_membership_refinement.rs` executes the real Rust `Membership`
implementation against an independent finite reference transition system. It is
the implementation-facing companion to `brain_membership.qnt`.

## Explored behavior

The breadth-first harness covers one representative node, matching the scalar
Quint model, with:

- heartbeat sequences `0..=3`;
- three distinct report payload versions;
- `Healthy`, `Suspect`, `Dead`, and sticky `Draining` health;
- `Unknown`, `Running`, and `Gone` external-oracle verdicts;
- exact suspect/dead threshold crossings;
- accepted, duplicate, reordered, and legacy unsequenced heartbeats;
- timeout death, one-shot newly-dead reporting, and fresh-heartbeat resurrection.

After every transition it compares the returned health/death signal and the
complete observable node record: health, `last_seen_ms`, highest accepted
sequence, address, failure domain, hosted shards, and leading shards. This makes
stale-report preservation executable rather than merely asserting the health
enum.

Coverage assertions require the exploration to reach every critical class:
sequenced acceptance, stale rejection, `seq == 0` compatibility above a positive
sequence floor, both timeout bands, one-shot death notification, Kubernetes
`Running` partition damping, immediate `Gone` death, sticky draining across both
heartbeats and sweeps, and resurrection.

The same test binary also consumes Quint's generated ITF corpus. It decodes each
model action and nondeterministic pick, executes the corresponding operation on
the real `Membership`, and compares health, oracle state, timeout band, sequence,
report payload, and draining history after every step. The dedicated formal
workflow sets `FIDUCIA_REQUIRE_MEMBERSHIP_ITF_REPLAY=1`, so missing traces fail
the job instead of silently skipping conformance.

## Bounds and claim strength

The run is capped at depth 6, 10,000 unique reference states, and 200,000 checked
transitions. Reaching a resource cap fails CI; it does not silently truncate.

This proves bounded refinement of the per-node membership/failure-detector
surface. It does not prove multi-node scheduler behavior, Raft durability, or the
quorum-preserving shard-reconfiguration model. Those remain separate layers in
DEN-80.

## Run locally

```bash
cargo test --test formal_membership_refinement --locked -- --nocapture

FIDUCIA_MEMBERSHIP_ITF_TRACE_DIR=/path/to/traces \
FIDUCIA_REQUIRE_MEMBERSHIP_ITF_REPLAY=1 \
  cargo test --locked --test formal_membership_refinement \
    generated_itf_traces_replay_against_membership -- --nocapture
```

The ordinary Rust CI executes the same test through `cargo test --all-targets
--all-features --locked`, while the formal workflow typechecks, simulates,
exhaustively checks, emits ITF traces, and replays the membership corpus against
production Rust.
