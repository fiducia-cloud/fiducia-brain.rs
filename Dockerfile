# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-brain. Clones sibling path dependencies so Cargo
# resolves the same layout as local development.
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG ROUTING_REF=main
ARG INTERFACES_REF=main
RUN git clone --depth 1 --branch "$ROUTING_REF" \
    https://github.com/fiducia-cloud/fiducia-routing.rs.git fiducia-routing.rs
RUN git clone --depth 1 --branch "$INTERFACES_REF" \
    https://github.com/fiducia-cloud/fiducia-interfaces.git fiducia-interfaces
COPY . fiducia-brain.rs
WORKDIR /build/fiducia-brain.rs
RUN cargo build --release && strip target/release/fiducia-brain

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build --chown=65532:65532 /build/fiducia-brain.rs/target/release/fiducia-brain /usr/local/bin/fiducia-brain
EXPOSE 8095 9095
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-brain"]
