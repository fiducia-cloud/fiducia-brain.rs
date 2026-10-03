#![forbid(unsafe_code)]

//! Operational integration surface for fiducia-brain. Consensus, placement,
//! and scheduler internals remain in the binary; external orchestration lock
//! identities are reusable by deployment/admin tooling without invoking
//! Fiducia's own coordination API.

pub mod external_locks;
