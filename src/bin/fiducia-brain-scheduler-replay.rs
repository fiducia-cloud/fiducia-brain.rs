#![allow(dead_code)]

#[path = "../cluster.rs"]
mod cluster;
#[path = "../leadership.rs"]
mod leadership;
#[path = "../membership.rs"]
mod membership;
#[path = "../model.rs"]
mod model;
#[path = "../oracle.rs"]
mod oracle;
#[path = "../placement.rs"]
mod placement;
#[path = "../plan.rs"]
mod plan;
#[path = "../scheduler.rs"]
mod scheduler;
#[path = "../../formal/scheduler_replay_common.rs"]
mod scheduler_replay_common;

use std::error::Error;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use scheduler_replay_common::{replay_paths, ReplayMismatch};
use serde::{Deserialize, Serialize};

const ADAPTER_PROTOCOL: &str = "fmctl.adapter.v1";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayRequest {
    protocol: String,
    project: String,
    model: String,
    adapter: String,
    specification: PathBuf,
    traces: Vec<PathBuf>,
}

#[derive(Debug, Serialize)]
struct ReplayResponse {
    protocol: &'static str,
    success: bool,
    traces_total: u64,
    traces_passed: u64,
    mismatches: Vec<ReplayMismatch>,
    implementation: Implementation,
}

#[derive(Debug, Serialize)]
struct Implementation {
    language: &'static str,
    name: &'static str,
    version: &'static str,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut paths = std::env::args_os()
        .skip(1)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        run_protocol()
    } else {
        paths.sort();
        run_human(paths)
    }
}

fn run_protocol() -> Result<(), Box<dyn Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let request: ReplayRequest = serde_json::from_str(&input)?;
    ensure(
        request.protocol == ADAPTER_PROTOCOL,
        format!("unsupported adapter protocol {:?}", request.protocol),
    )?;
    ensure(
        request.adapter == "rust",
        "request selected a non-Rust adapter",
    )?;
    ensure(
        request.project == "fiducia-brain.rs",
        format!("unexpected project {:?}", request.project),
    )?;
    ensure(
        request.model == "brain-scheduler",
        format!("unexpected model {:?}", request.model),
    )?;
    ensure(
        request.specification.is_file(),
        format!(
            "request specification is not a file: {}",
            request.specification.display()
        ),
    )?;
    ensure(!request.traces.is_empty(), "request contains no traces")?;
    for trace in &request.traces {
        ensure(
            trace.is_file(),
            format!("request trace is not a file: {}", trace.display()),
        )?;
    }

    let summary = replay_paths(&request.traces);
    eprintln!(
        "replayed {} Quint scheduler traces, {} states, and {} non-idle transitions; actions={:?}",
        summary.traces_total, summary.states, summary.non_idle_transitions, summary.actions
    );
    let response = ReplayResponse {
        protocol: ADAPTER_PROTOCOL,
        success: summary.success(),
        traces_total: summary.traces_total,
        traces_passed: summary.traces_passed,
        mismatches: summary.mismatches,
        implementation: Implementation {
            language: "rust",
            name: "fiducia-brain Scheduler",
            version: env!("CARGO_PKG_VERSION"),
        },
    };
    serde_json::to_writer(io::stdout().lock(), &response)?;
    Ok(())
}

fn run_human(paths: Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for path in &paths {
        ensure(
            fs::metadata(path).is_ok_and(|metadata| metadata.is_file()),
            format!("trace is not a file: {}", path.display()),
        )?;
    }
    let summary = replay_paths(&paths);
    if !summary.success() {
        let rendered = serde_json::to_string_pretty(&summary.mismatches)?;
        return Err(invalid(format!(
            "scheduler refinement failed for {} of {} traces:\n{rendered}",
            summary.traces_total.saturating_sub(summary.traces_passed),
            summary.traces_total
        )));
    }
    println!(
        "fiducia-brain Scheduler conformed to {} states and {} non-idle transitions across {} Quint ITF traces covering all {} required actions",
        summary.states,
        summary.non_idle_transitions,
        summary.traces_total,
        scheduler_replay_common::REQUIRED_ACTIONS.len()
    );
    Ok(())
}

fn ensure(condition: bool, message: impl Into<String>) -> Result<(), Box<dyn Error>> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message))
    }
}

fn invalid(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}
