# scripts

Helper scripts for working with the crate.

- `with-flags2env.sh` — bridges non-secret CLI flags to the `FIDUCIA_*`
  environment variables the `fiducia-brain` binary reads. It runs the pinned
  `flags2env` parser against the `.cli-flags.toml` schema, exports the resulting
  env map, then execs the given command (for example, `cargo run --locked`).
  Trusted-hop and Raft secrets remain environment-only.
