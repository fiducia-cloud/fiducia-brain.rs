# workflows

GitHub Actions pipelines for fiducia-brain:

- `ci.yml` — blocking formatting, clippy, all-target tests, CLI flag-contract,
  and dependency-audit gates on Rust 1.95.0. Sibling interface and routing
  sources are checked out at the immutable revisions documented in the root
  README.
- `docker.yml` — build and push the non-root service container image on push to
  `main`, using those same immutable sibling revisions.
- `deploy-test.yml` — secret-gated deploy to the TEST environment; a no-op when
  the `KUBE_CONFIG_TEST` secret is absent (validation only), but a configured
  deployment fails if the target is missing or the rollout does not complete.
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.
