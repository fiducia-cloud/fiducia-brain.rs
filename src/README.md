# src — fiducia-brain control plane

The Rust source for the brain: a small, highly-available cluster manager that
decides shard placement and leadership for the data-plane nodes and drives the
cluster toward that plan. Durable control-plane state is replicated by the
brain's own Raft group (one member per cloud).

Key files:

- `main.rs` — process entry point and wiring.
- `api.rs` — control-plane HTTP API (`/v1` for nodes, plus operator/dashboard).
- `config.rs` — authoritative cluster-wide configuration and key→shard mapping.
- `membership.rs` / `oracle.rs` — node membership tracking and the hybrid
  heartbeat + Kubernetes failure detector.
- `placement.rs` / `leadership.rs` — the shard→nodes map and per-shard preferred
  leader (affinity).
- `plan.rs` / `scheduler.rs` — pure placement math and the reconciliation loop
  that scales and heals the cluster toward it.
- `cluster.rs` — the replication/leadership seam where Raft plugs in.
- `raft.rs`, `raft_driver.rs`, `raft_store.rs` — the brain's own consensus
  engine, its async network driver, and crash-safe persistence.
- `internal_auth.rs` — trusted-hop auth for the control plane.
- `model.rs` — shared control-plane types.
