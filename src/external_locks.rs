//! External orchestration locks for fiducia-brain.
//!
//! Raft remains authoritative for brain leadership and replicated placement
//! decisions. These keys fence application of external side effects around
//! those decisions and may be backed by Cloudflare Durable Objects while the
//! Fiducia control plane itself is unhealthy or bootstrapping.

use ores_locks_and_leases::LockKey;

pub fn plan_apply(plan_id: &str) -> LockKey {
    fiducia_lib_core::locks::brain_plan_apply(plan_id)
}

pub fn migration() -> LockKey {
    fiducia_lib_core::locks::migration("brain")
}

pub fn maintenance(job: &str) -> LockKey {
    fiducia_lib_core::locks::singleton_job(job)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_plan_key_is_separate_from_raft_leadership() {
        assert_eq!(
            plan_apply("plan-42").as_str(),
            "fiducia-cloud/brain/plan-apply:plan-42"
        );
        assert_eq!(migration().as_str(), "fiducia-cloud/migrations/brain");
    }
}
