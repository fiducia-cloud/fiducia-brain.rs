//! Trusted-hop authentication for the brain's **control plane** (`/v1`).
//!
//! The brain's `/v1` mutates the control plane: node heartbeats (liveness),
//! `DELETE /v1/nodes/{id}` (drain + scale-down), `POST /v1/scale`, and policy
//! changes. None of it is user-authenticated — it trusts that only in-cluster
//! components reach it. If the brain's port is reachable directly, an attacker
//! could forge heartbeats, drain nodes, or rescale the cluster.
//!
//! This middleware closes that gap with the same cluster secret the node uses for
//! its `/v1` and `/raft` planes. Every `/v1` request must carry a matching
//! [`INTERNAL_AUTH_HEADER`] (constant-time);
//! the node sidecar, the load balancer, admin, and the brain's own follower→leader
//! forwarding attach it. Missing configuration is a startup error.
//!
//! The brain's peer plane (`/raft`, on a separate port) keeps its own bearer
//! secret (`FIDUCIA_BRAIN_RAFT_SECRET`); this guards only the control plane.

use std::sync::OnceLock;

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Header carrying the shared internal secret between trusted hops (same name the
/// node uses, so one secret covers node `/v1`+`/raft` and brain `/v1`).
pub const INTERNAL_AUTH_HEADER: &str = "x-fiducia-internal-auth";

static SECRET: OnceLock<Option<String>> = OnceLock::new();

fn configured() -> &'static Option<String> {
    SECRET.get_or_init(|| {
        std::env::var("FIDUCIA_INTERNAL_SECRET")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
}

/// Validate the trusted-hop secret before binding the control-plane port.
pub fn init_and_log() -> Result<(), std::io::Error> {
    if configured().is_some() {
        tracing::info!(
            "internal-auth: enforcing FIDUCIA_INTERNAL_SECRET on the brain /v1 control plane"
        );
        Ok(())
    } else {
        Err(std::io::Error::other(
            "FIDUCIA_INTERNAL_SECRET must be configured for the brain control plane",
        ))
    }
}

/// Attach the trusted-hop header to an outbound control-plane request (the
/// brain's own follower→leader forwarding) when the secret is configured.
pub fn attach(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match configured().as_deref() {
        Some(secret) => builder.header(INTERNAL_AUTH_HEADER, secret),
        None => builder,
    }
}

/// Axum middleware guarding the `/v1` control plane.
pub async fn guard(request: Request, next: Next) -> Response {
    let provided = request
        .headers()
        .get(INTERNAL_AUTH_HEADER)
        .and_then(|v| v.to_str().ok());
    if !authorized(configured().as_deref(), provided) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "unauthorized",
                "detail": "missing or invalid internal auth; the brain control plane is cluster-internal"
            })),
        )
            .into_response();
    }
    next.run(request).await
}

/// Pure authorization decision (testable without env/process state).
pub fn authorized(expected: Option<&str>, provided: Option<&str>) -> bool {
    match expected {
        None => false,
        Some(secret) => provided
            .map(|p| constant_time_eq(p.as_bytes(), secret.as_bytes()))
            .unwrap_or(false),
    }
}

/// Length-then-content compare that doesn't short-circuit on the first differing
/// byte, so the secret can't be recovered a byte at a time via response timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_configuration_fails_closed() {
        assert!(!authorized(None, None));
        assert!(!authorized(None, Some("whatever")));
    }

    #[test]
    fn enabled_guard_requires_exact_secret() {
        assert!(authorized(Some("s3cret"), Some("s3cret")));
        assert!(!authorized(Some("s3cret"), Some("s3cre7")));
        assert!(!authorized(Some("s3cret"), Some("s3cret-extra")));
        assert!(!authorized(Some("s3cret"), None));
    }
}
