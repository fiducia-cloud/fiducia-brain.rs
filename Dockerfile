# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-brain. Clones the pinned fiducia-routing crate as
# a sibling so the path dependency resolves.
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG ROUTING_REF=v0.1.0
RUN git clone --depth 1 --branch "$ROUTING_REF" \
    https://github.com/fiducia-cloud/fiducia-routing.rs.git fiducia-routing.rs
COPY . fiducia-brain.rs
WORKDIR /build/fiducia-brain.rs
RUN cargo build --release && strip target/release/fiducia-brain

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && useradd --uid 10001 --user-group --home-dir /nonexistent --shell /usr/sbin/nologin fiducia
COPY --from=build --chown=10001:10001 /build/fiducia-brain.rs/target/release/fiducia-brain /usr/local/bin/fiducia-brain
EXPOSE 8095 9095
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/fiducia-brain"]
