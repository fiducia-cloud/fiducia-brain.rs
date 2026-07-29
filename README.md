# fiducia-brain

The **control plane** for [fiducia.cloud](https://fiducia.cloud) — a small,
highly-available cluster manager that runs *inside* the larger deployment.
`fiducia-brain` does not serve customer coordination operations directly;
`fiducia-node` does. The brain manages the cluster itself: membership, shard
placement, leader affinity, failover, scale, and rebalance. The control logic is
**implemented**: time-based failure detection (Healthy→Suspect→Dead), the
placement math ([`src/plan.rs`](src/plan.rs): keep healthy replicas, drop
dead/draining ones, fill to RF on the least-loaded node in a fresh failure
domain), leadership affinity/failover, and the reconciliation loop + HTTP API are
all live and unit-tested. Placement and scale intent are replicated in the
brain's **own Raft group**, so the durable control plane survives losing a
brain node; liveness membership remains leader-local soft state rebuilt from
heartbeats.

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
- **Observable liveness fallback** — Kubernetes oracle refresh failures age to
  `Unknown` rather than declaring nodes dead; the first failure, sustained
  failures, and recovery are emitted through shared telemetry.

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
| `GET/POST /v1/policies`            | operators   | namespace home-region/provider policies |
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
- **RF is fixed at 3 for the multi-cloud baseline.** Each shard gets one voter
  per Kubernetes cluster / cloud provider (AWS, GCP, Hetzner). `/v1/scale`
  preserves RF=3 even if a caller submits another replication factor.
- **Leader placement is policy-driven.** `/v1/policies` can pin a namespace's
  home region/provider so the scheduler prefers a nearby leader while preserving
  the three-cloud replica spread. Policy writes received by a follower are
  forwarded to the elected leader; during an election they fail closed instead
  of creating follower-local intent that reconciliation would ignore.

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
| `src/membership.rs`| node registry + time-based failure detection             |
| `src/placement.rs` | authoritative shard → replicas/leader map                |
| `src/plan.rs`      | **pure placement math** (`plan_replicas`) + tests        |
| `src/leadership.rs`| leader affinity / failover decision (`desired_leader`)   |
| `src/scheduler.rs` | reconciliation loop (sweep failures → recompute placement)|
| `src/model.rs`     | shared types                                              |

> HA note: replicated mode durably stores placement and scale intent in a small
> brain Raft cluster. Membership heartbeats are intentionally soft state and
> are rebuilt at the elected leader.

> **Invariant: the brain's Raft has no message-bus dependency.** Brain↔brain
> RPC is JSON-over-HTTP on the `/raft` peer plane (`FIDUCIA_BRAIN_PEERS`,
> bearer-authenticated); node heartbeats arrive over HTTP via the sidecar; the
> liveness oracle talks to the Kubernetes API. NATS delivers application events
> elsewhere in the platform and is deliberately absent here — a broker outage
> must never stall control-plane consensus or placement decisions.

## Configuration (env surface)

Everything is configured through environment variables. Secrets are marked; see
[Security](#security) for the trust-boundary rules.

| Variable | Type | Default | Secret? | Meaning |
|----------|------|---------|:-------:|---------|
| `FIDUCIA_INTERNAL_SECRET`   | string  | *(required)*          | **yes** | Trusted-hop secret for the `/v1` control plane. **Startup fails closed if unset** ([`internal_auth.rs`](src/internal_auth.rs)). |
| `FIDUCIA_BRAIN_RAFT_SECRET` | string  | *(required in replicated mode)* | **yes** | Bearer secret for brain↔brain `/raft` RPC. When `FIDUCIA_BRAIN_PEERS` is non-empty, startup fails before binding if this is unset or blank. Single-member mode exposes no `/raft` plane and does not require it. |
| `PORT`                      | integer | `8095`                | no      | Control-plane (`/v1`, health) listen port. |
| `FIDUCIA_CLUSTER_ID`        | string  | `fiducia-local`       | no      | Stable identifier for this cluster. |
| `FIDUCIA_SHARD_COUNT`       | integer | `16`                  | no      | Number of shards; **fixed at cluster creation** (defines `key → shard`). |
| `FIDUCIA_TARGET_NODES`      | integer | `3` (floored at RF=3) | no      | Desired data-plane node count for the scale plan. |
| `FIDUCIA_BRAIN_PEERS`       | string  | *(unset ⇒ single-member)* | no  | Comma-separated brain member addresses; set ⇒ replicated Raft control plane. |
| `FIDUCIA_BRAIN_ID`          | string  | `http://localhost:$PORT` | no   | This member's addressable id (must be excluded from `FIDUCIA_BRAIN_PEERS`). |
| `FIDUCIA_BRAIN_PEER_PORT`   | integer | `9095`                | no      | Port for the peer-facing `/raft` plane (replicated mode only). |
| `FIDUCIA_DATA_DIR`          | string  | `/tmp/fiducia-brain`  | no      | Durable home for the brain's Raft WAL + snapshots (replicated mode). |
| `FIDUCIA_CLUSTER`           | string  | *(none)*              | no      | Local Kubernetes cluster name (`gcp`, `aws`, `hetzner`, …). |
| `FIDUCIA_CLOUD_PROVIDER` / `FIDUCIA_PLATFORM` | string | *(falls back to `FIDUCIA_CLUSTER`)* | no | Cloud provider for the local brain member. |
| `FIDUCIA_REGION`            | string  | *(none)*              | no      | Physical region of the local brain member. |
| `FIDUCIA_SUSPECT_AFTER_MS`  | integer | `6000`                | no      | Silence before a node goes Healthy → Suspect. |
| `FIDUCIA_DEAD_AFTER_MS`     | integer | `30000`               | no      | Silence before a Suspect node is declared Dead. |

### Deriving env from CLI flags (flags-2-env)

Non-secret settings can be mapped to the `FIDUCIA_*`/`PORT` env vars above through the pinned
[`ORESoftware/flags-2-env`](https://github.com/ORESoftware/flags-2-env) parser
(schema in [`.cli-flags.toml`](.cli-flags.toml), audited in CI by
`.github/workflows/cli-flags.yml`):

```bash
git submodule update --init --recursive
make -B -C vendor/flags-2-env all
scripts/with-flags2env.sh --port=8095 --cluster-id=fiducia-local -- cargo run --locked
```

`FIDUCIA_INTERNAL_SECRET` and `FIDUCIA_BRAIN_RAFT_SECRET` are deliberately
excluded from the CLI schema. Inject them through the environment or a secret
store so they cannot leak through shell history or process listings.

## Run locally

The control plane fails closed, so set the trusted-hop secret even for a
single-node dev run. `FIDUCIA_BRAIN_PEERS` unset ⇒ single-member mode (always
leader, no replication, no `/raft` port):

```bash
FIDUCIA_INTERNAL_SECRET=dev-secret cargo run --locked   # listens on :8095 (override PORT)
curl -H 'x-fiducia-internal-auth: dev-secret' localhost:8095/v1/status
curl localhost:8095/healthz                    # health probes stay open
```

## Reproducible build inputs

CI and the container build use Rust 1.95.0, the committed `Cargo.lock`, and
immutable sibling revisions for the local path dependencies:

- `fiducia-interfaces` at
  `bd718cd72d72aa330534f3688f8fb1ce90c19d10`
- `fiducia-routing.rs` at
  `c694bc5c58587bec12989a347e926c0040aacada`

When either shared contract changes, update the checkout refs in
`.github/workflows/ci.yml`, the build arguments in `.github/workflows/docker.yml`,
and the defaults in `Dockerfile` together. Cargo formatting, clippy, tests, and
release builds run with the lockfile enforced; dependency-audit failures block
CI. Docker build and runtime bases are pinned by multi-platform manifest digest,
and the final image runs as the distroless non-root uid/gid `65532:65532`.

## Security

Trust boundaries and the hardening applied to this crate:

- **`/v1` control plane fails closed.** `FIDUCIA_INTERNAL_SECRET` is **required**;
  the process refuses to bind `/v1` if it is unset ([`main.rs`](src/main.rs)
  `internal_auth::init_and_log()`, [`internal_auth.rs`](src/internal_auth.rs)).
  Every `/v1` request must carry a matching `x-fiducia-internal-auth` header,
  compared in **constant time**. Only `/healthz` and `/readyz` are open.
- **`/raft` peer plane.** Guarded by a `FIDUCIA_BRAIN_RAFT_SECRET` bearer token,
  now compared in **constant time** (`subtle::ConstantTimeEq`) so the secret
  can't be recovered byte-by-byte via response timing. Replicated mode has no
  unauthenticated fallback: an unset or whitespace-only secret aborts startup
  before the cross-cluster peer plane binds. Keep `/raft` (default `:9095`)
  reachable only by peer brains.
- **Durability fails closed.** A WAL or snapshot persistence failure blocks the
  current batch before it is applied, routed, acknowledged, or compacted. A
  persistence or Raft outbox handoff failure makes the replicated member
  unavailable before it acknowledges further work. `/readyz` then returns `503`
  while `/healthz` remains live for diagnosis. Recovery accepts a torn tail only
  when it is wholly uncommitted; it refuses to canonicalize a log that would
  lose an entry named by the durable `commit_index`.
- **Request hardening.** All routers wrap a shared stack ([`main.rs`](src/main.rs)):
  request-body cap (256 KiB), 30s request timeout (slow-loris protection), and a
  catch-panic layer that turns a handler panic into a 500 instead of dropping the
  connection.
- **No `unsafe`.** The crate contains no `unsafe` blocks. Reachable `unwrap()`s are
  limited to `Mutex` lock poisoning (contained by the catch-panic layer) and test
  code; request bodies deserialize into small fixed structs.
- **Dependencies.** `cargo audit` is **clean** (0 advisories across 201
  dependencies at the last scan). No accepted/ignored advisories.

## Related

- [`fiducia-node.rs`](https://github.com/fiducia-cloud/fiducia-node.rs) — data plane (runs on each node; hosts shard leaders/followers).
- [`fiducia-customer.rs`](https://github.com/fiducia-cloud/fiducia-customer.rs) — the website webserver.
- [`fiducia-marketing.web`](https://github.com/fiducia-cloud/fiducia-marketing.web) — the website frontend.
