#!/usr/bin/env python3
"""Real HTTP sidecar-style heartbeat -> 3 brain members -> elected leader.

Run only in a DISPOSABLE nonproduction three-brain cluster. Adds an ephemeral
membership record; DELETE initiates draining, not immediate hard deletion.
FIDUCIA_E2E_BRAIN_URLS=http://127.0.0.1:18095,http://127.0.0.1:18096,http://127.0.0.1:18097 \
FIDUCIA_E2E_ALLOW_MEMBERSHIP_WRITES=1 FIDUCIA_E2E_INTERNAL_SECRET=... \
python3 tests/live_heartbeat_forwarding_e2e.py
"""
import json
import os
import time
import urllib.parse
import urllib.request
import uuid


def brains_from_env():
    urls = [x.strip().rstrip("/") for x in os.getenv("FIDUCIA_E2E_BRAIN_URLS", "").split(",") if x.strip()]
    if len(urls) != 3 or len(set(urls)) != 3:
        raise RuntimeError("three independent brain HTTP endpoints are required")
    for url in urls:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.path not in ("", "/"):
            raise ValueError("brain URL must be a HTTP(S) origin")
    if os.getenv("FIDUCIA_E2E_ALLOW_MEMBERSHIP_WRITES") != "1":
        raise RuntimeError("disposable nonproduction membership-write opt-in is required")
    if not os.getenv("FIDUCIA_E2E_INTERNAL_SECRET"):
        raise RuntimeError("real trusted-hop secret is required")
    return urls


def call(origin, path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"accept": "application/json",
               "x-fiducia-internal-auth": os.environ["FIDUCIA_E2E_INTERNAL_SECRET"]}
    if data is not None:
        headers["content-type"] = "application/json"
    request = urllib.request.Request(origin + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=8) as res:
        assert res.status == 200, (path, res.status)
        return json.load(res)


def find(brain, node_id):
    members = call(brain, "/v1/nodes")["nodes"]
    matching = [record for record in members if record.get("node_id") == node_id]
    assert len(matching) <= 1, "duplicate membership identity"
    return matching[0] if matching else None


def converge(brains, node_id, predicate, label):
    deadline = time.monotonic() + 22
    last = []
    while time.monotonic() < deadline:
        try:
            last = [find(brain, node_id) for brain in brains]
            if all(record and predicate(record) for record in last):
                return last
        except (OSError, KeyError, ValueError):
            pass
        time.sleep(0.25)
    raise AssertionError("three-brain %s did not converge: %r" % (label, last))


def heartbeat(brain, node_id, seq, address, hosted, leading):
    return call(brain, "/v1/nodes/" + urllib.parse.quote(node_id, safe="") + "/heartbeat",
                "POST", {"seq": seq, "address": address,
                         "region": "e2e-nonprod",
                         "cluster_id": "e2e-disposable",
                         "failure_domain": "e2e-disposable",
                         "hosted_shards": hosted, "leading_shards": leading})


def main():
    brains = brains_from_env()
    for brain in brains:
        state = call(brain, "/v1/status")
        assert state["brain_cluster"]["available"] is True, "brain is unavailable"
    node = "e2e-heartbeat-" + uuid.uuid4().hex
    seq = int(time.time() * 1000) + 100
    old_address, new_address = "http://127.0.0.1:18500", "http://127.0.0.1:18501"
    try:
        assert heartbeat(brains[0], node, seq, old_address, [], [])["ok"] is True
        converge(brains, node, lambda row: row["last_seq"] == seq and
                 row["address"] == old_address, "initial heartbeat")
        # Posting through another brain must update the SAME leader-authoritative
        # membership (followers forward to /forward/v1 on the peer plane).
        assert heartbeat(brains[1], node, seq + 2, new_address, [], [])["ok"] is True
        fresh = converge(brains, node, lambda row: row["last_seq"] == seq + 2 and
                         row["address"] == new_address, "forwarded heartbeat")
        seen = min(row["last_seen_ms"] for row in fresh)
        # Old and duplicate writes arrive from other paths AFTER the newer
        # receipt. Neither may resurrect the false leader/hosted state.
        heartbeat(brains[2], node, seq + 1, old_address, [0], [0])
        heartbeat(brains[0], node, seq + 2, old_address, [0], [0])
        last = converge(brains, node, lambda row:
                        row["last_seq"] == seq + 2
                        and row["address"] == new_address
                        and row["hosted_shards"] == []
                        and row["leading_shards"] == [], "stale replay suppression")
        assert all(row["last_seen_ms"] >= seen for row in last)
        print("PASS: 3 brains agree; forwarded and replayed heartbeat authority fenced")
    finally:
        try:
            call(brains[0], "/v1/nodes/" + node, "DELETE")
        except OSError:
            pass


if __name__ == "__main__":
    main()
