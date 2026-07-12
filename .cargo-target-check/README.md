# .cargo-target-check

Cargo build cache/target directory that was accidentally committed. These are
generated build artifacts (fingerprints, dep-info, compiled outputs) — not
source. Nothing here should be edited by hand, and the directory is a candidate
for removal from version control (add it to `.gitignore`).
