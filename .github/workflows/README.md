# workflows

GitHub Actions pipelines for fiducia-brain:

- `ci.yml` — build, test, and lint (rustfmt/clippy) on push and pull request.
- `docker.yml` — build and push the service container image on push to `main`.
- `deploy-test.yml` — secret-gated deploy to the TEST environment; a no-op when
  the `KUBE_CONFIG_TEST` secret is absent (validation only).
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.
