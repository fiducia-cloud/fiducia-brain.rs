#![allow(dead_code)]

#[path = "../src/cluster.rs"]
mod cluster;
#[path = "../src/leadership.rs"]
mod leadership;
#[path = "../src/membership.rs"]
mod membership;
#[path = "../src/model.rs"]
mod model;
#[path = "../src/oracle.rs"]
mod oracle;
#[path = "../src/placement.rs"]
mod placement;
#[path = "../src/plan.rs"]
mod plan;
#[path = "../src/scheduler.rs"]
mod scheduler;
#[path = "../formal/scheduler_replay_common.rs"]
mod scheduler_replay_common;

use std::path::PathBuf;

use scheduler_replay_common::{collect_itf_traces, replay_paths};

#[test]
fn generated_scheduler_itf_traces_replay_against_production() {
    let required = std::env::var("FIDUCIA_REQUIRE_SCHEDULER_ITF_REPLAY")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let Some(directory) = std::env::var_os("FIDUCIA_SCHEDULER_ITF_TRACE_DIR") else {
        assert!(
            !required,
            "FIDUCIA_SCHEDULER_ITF_TRACE_DIR is required when replay is mandatory"
        );
        eprintln!("scheduler ITF replay skipped: FIDUCIA_SCHEDULER_ITF_TRACE_DIR is unset");
        return;
    };

    let directory = PathBuf::from(directory);
    let traces = collect_itf_traces(&directory).unwrap_or_else(|error| {
        panic!(
            "failed to collect scheduler ITF traces from {}: {error}",
            directory.display()
        )
    });
    assert!(
        !traces.is_empty(),
        "no *.itf.json traces found under {}",
        directory.display()
    );

    let summary = replay_paths(&traces);
    assert!(
        summary.success(),
        "scheduler refinement mismatches:\n{}",
        serde_json::to_string_pretty(&summary.mismatches)
            .unwrap_or_else(|_| format!("{:?}", summary.mismatches))
    );
    eprintln!(
        "replayed {} Quint scheduler traces, {} states, and {} non-idle transitions against production; actions={:?}",
        summary.traces_total,
        summary.states,
        summary.non_idle_transitions,
        summary.actions
    );
}
