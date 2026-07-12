# .nix

Nix flake defining the reproducible development environment (Rust toolchain and
tooling) for this repo. `flake.nix` declares the dev shell; `flake.lock` pins
input revisions. The top-level `./shell` helper enters this shell via
`nix develop ./.nix`.
