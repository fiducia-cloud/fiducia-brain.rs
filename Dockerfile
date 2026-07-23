# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-brain. Clones sibling path dependencies so Cargo
# resolves the same layout as local development.
FROM rust:1.97.0-slim-bookworm@sha256:6d220bf85c74e842a79da63997af8d2e74455c0b8847d8bb3a5888572334991d AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG ROUTING_REF=6106b4f79a5559699a64c931dbcb472f42274266
ARG INTERFACES_REF=6e20a3f4df2e52b99a0ad6add83d4528262b5dbc
RUN git init fiducia-routing.rs \
    && git -C fiducia-routing.rs remote add origin https://github.com/fiducia-cloud/fiducia-routing.rs.git \
    && git -C fiducia-routing.rs fetch --depth 1 origin "$ROUTING_REF" \
    && test "$(git -C fiducia-routing.rs rev-parse FETCH_HEAD)" = "$ROUTING_REF" \
    && git -C fiducia-routing.rs checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-routing.rs rev-parse HEAD)" = "$ROUTING_REF"
RUN git init fiducia-interfaces \
    && git -C fiducia-interfaces remote add origin https://github.com/fiducia-cloud/fiducia-interfaces.git \
    && git -C fiducia-interfaces fetch --depth 1 origin "$INTERFACES_REF" \
    && test "$(git -C fiducia-interfaces rev-parse FETCH_HEAD)" = "$INTERFACES_REF" \
    && git -C fiducia-interfaces checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-interfaces rev-parse HEAD)" = "$INTERFACES_REF"
COPY . fiducia-brain.rs
WORKDIR /build/fiducia-brain.rs
RUN cargo build --locked --release && strip target/release/fiducia-brain

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:fccdbb0a547c14e23fcf4ce8ad62ca5d43b4faae8d22cd292f490fef9946c96e
COPY --from=build --chown=65532:65532 /build/fiducia-brain.rs/target/release/fiducia-brain /usr/local/bin/fiducia-brain
EXPOSE 8095 9095
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-brain"]
