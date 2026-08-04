# Raft safety boundaries

The brain Raft engine is a crash-fault-tolerant replicated log. It assumes authenticated, non-Byzantine configured members and a driver that persists every `Ready.persist` value before sending messages or applying commands. Recovery rejects malformed complete WAL records, missing hard state beside durable data, zero or descending terms, non-contiguous indexes, snapshot/base mismatches, and commit pointers beyond durable state.

A successful `AppendEntries` response acknowledges only the highest index carried or matched by that request. Existing follower suffixes are not implicit acknowledgements. Leader replication indexes are monotonic and bounded by the leader log, and no-progress failures wait for the next heartbeat rather than creating a tight retry loop. Snapshot responses acknowledge at most the specific snapshot boundary recorded for that peer.

The current shared bearer secret authenticates membership in the peer plane, not an individual member identity: any holder can claim another configured body identity. The pure engine requires the transport sender and body identity to agree, which creates the correct seam for per-member mTLS or SPIFFE identities. Deployments must continue to treat the present protocol as crash-fault, not Byzantine-fault, until that transport work lands.

The data-plane lock queue is a separate deterministic state-machine concern. Its container mechanics may remain orthogonal to Raft, but enqueue, cancellation, expiry, grant, and promotion become authoritative only through committed commands and are persisted through Raft log replay or snapshots.
