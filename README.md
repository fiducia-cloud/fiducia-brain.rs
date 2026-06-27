# fiducia-brain

The **control plane** for [fiducia.cloud](https://fiducia.cloud) — a small,
highly-available cluster manager that runs *inside* the larger deployment.
`fiducia-brain` does not serve customer coordination operations directly;
`fiducia-node` does. The brain manages the cluster itself: membership, shard
placement, leader affinity, failover, scale, and rebalance. This repository is
currently a **skeleton**: the architecture and HTTP surface are in place; the
failure-detection, placement, and scaling internals are stubbed with `TODO`s`.

## What the brain does

The data plane ([`fiducia-node`](https://github.com/fiducia-cloud/fiducia-node.rs))
stores and replicates coordination state in sharded Raft groups. Every VM or
bare-metal machine runs a node process; each node can lead some shards and
follow others. Nodes deliberately do **not** decide *which* machines host
*which* shards — that is the brain's job:

- **Membership & failure detection** — every node heartbeats to the brain;
  silent nodes go Healthy → Suspect → Dead.
- **Shard placement** — owns the authoritative shard → (replicas, preferred
  leader) map that data-plane nodes fetch and reconcile toward.
- **Node-failure handling** — when a node is declared Dead, its shard replicas
  are re-placed onto healthy nodes to restore the replication factor.
- **Scale up / down** — drives the cluster toward a desired `ScalePlan`
  (target node count × replication factor): bleeds shards onto new nodes,
  drains and removes nodes on scale-down.
- **Rebalancing** — keeps replica counts and leadership spread evenly so no
  node becomes a write hotspot; after failover, the brain can restore leader
  affinity when the original, preferred leader is healthy again.

This mirrors the "placement driver" pattern: TiKV's **PD** and CockroachDB's
control plane do the same thing for their range/region maps.

```
        ┌──────────────────── fiducia-brain (control plane, HA) ────────────────────┐
        │  membership + failure detector   shard-placement map   reconcile loop      │
        └───────▲───────────────────────────────────┬───────────────────────────────┘
       heartbeats│  (node liveness + reported shards) │ placement map (desired state)
                 │                                    ▼
        ┌────────┴───────── fiducia-node × N (data plane, sharded multi-Raft) ────────┐
        │   node-a            node-b            node-c            …                    │
        │   shards it leads / follows, reconciled toward the brain's placement map     │
        └─────────────────────────────────────────────────────────────────────────────┘
```

## API (`/v1`)

| Route                              | Audience    | Purpose                                  |
|------------------------------------|-------------|------------------------------------------|
| `GET  /v1/config`                  | all         | authoritative cluster config (`shard_count`, RF) |
| `GET  /v1/route?key=...`           | all         | resolve a key → shard → assignment       |
| `GET  /v1/nodes`                   | operators   | membership view                          |
| `POST /v1/nodes/{id}/heartbeat`    | data plane  | liveness + reported shard status         |
| `DELETE /v1/nodes/{id}`            | operators   | drain + remove a node (scale-down)       |
| `GET  /v1/placement`               | data plane  | full shard map to reconcile toward       |
| `GET  /v1/placement/{shard}`       | data plane  | one shard's assignment                   |
| `POST /v1/scale`                   | operators   | set the desired `ScalePlan`              |
| `GET  /v1/status`                  | all         | control-plane status + placement health |

Plus `/healthz`, `/readyz`.

## Sharding & scaling strategy

**Two layers, kept separate** (this is the whole trick):

```text
  key ──hash(key) % shard_count──▶ shard      stable · stateless · no lookup
  shard ──placement map──────────▶ nodes      elastic · central · changes on scale
```

- **`shard_count` is fixed at cluster creation** ([`config.rs`](src/config.rs)) and
  generously sized (e.g. 256/1024). It defines `key → shard`, so **no key ever
  moves when you add/remove nodes**. Every component computes `key → shard`
  locally (same `fnv1a`); only `shard → nodes` needs the central map.
- **Node count is elastic.** Scaling rewrites only the `shard → nodes` placement.

**Central configuration** = the brain's replicated state: the immutable
`ClusterConfig` + the mutable `shard → {replicas, preferred_leader}` placement
map ([`placement.rs`](src/placement.rs)). Meant to live in the brain's own Raft
group so it is consistent and survives a brain-node loss.

**Membership change is one replica at a time** (every scaling phase uses this):
`add learner` (non-voting, catches up via snapshot + log) → `promote` to voter →
optional `transfer leadership` → `remove` old replica. Never add a far-behind
voter; never drop below quorum; throttle concurrent moves.

The reconciler ([`scheduler.rs`](src/scheduler.rs)) runs four phases each tick:

| Phase | Trigger | Action |
|-------|---------|--------|
| **re-replicate** | node Dead / shard below RF | add replica on least-loaded healthy node (highest priority) |
| **scale up** | healthy nodes < target | bleed replicas/leadership onto new nodes until balanced |
| **scale down** | healthy nodes > target | drain lightest nodes, evacuate replicas, release (floor = RF) |
| **balance leadership** | lopsided leader counts | transfer leadership toward `shard_count / nodes` per node |

Balance objectives: RF replicas/shard · spread across failure domains · even
replicas per node · even **leaders** per node (the real write hotspot).

## Layout

| File               | Responsibility                                            |
|--------------------|-----------------------------------------------------------|
| `src/main.rs`      | axum wiring, scheduler spawn, config                      |
| `src/config.rs`    | central `ClusterConfig` + `key → shard` mapping           |
| `src/api.rs`       | control-plane HTTP handlers                               |
| `src/membership.rs`| node registry + failure detection                        |
| `src/placement.rs` | authoritative shard → replicas/leader map                |
| `src/scheduler.rs` | reconciliation loop (failure / scale / rebalance)        |
| `src/model.rs`     | shared types                                              |

> HA note: the brain's own state (membership + placement) is meant to be
> replicated by the brain's *own* Raft group (a 3–5 node "brain cluster"), so the
> control plane survives losing a brain node. That replication is a `TODO`.

## Run locally

```bash
cargo run     # listens on :8095 (override PORT)
# env: FIDUCIA_SHARD_COUNT, FIDUCIA_TARGET_NODES, FIDUCIA_REPLICATION_FACTOR
curl localhost:8095/v1/status
```

## Related

- [`fiducia-node.rs`](https://github.com/fiducia-cloud/fiducia-node.rs) — data plane (runs on each node; hosts shard leaders/followers).
- [`fiducia-backend.rs`](https://github.com/fiducia-cloud/fiducia-backend.rs) — the website webserver.
- [`fiducia-ui.web`](https://github.com/fiducia-cloud/fiducia-ui.web) — the website frontend.
