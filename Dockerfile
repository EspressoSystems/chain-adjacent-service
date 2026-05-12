# syntax=docker/dockerfile:1
FROM rust:1.93.1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Dep-cache layer: dummy crate compiles all dependencies so source-only
# changes skip the expensive dep-resolution step.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "pub fn _dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

COPY src/ src/
RUN cargo build --release --bin chain-adjacent-service

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 cas && \
    useradd --uid 1000 --gid cas --create-home cas

COPY --from=builder /build/target/release/chain-adjacent-service /usr/local/bin/

USER cas
WORKDIR /home/cas

ENTRYPOINT ["chain-adjacent-service"]
CMD ["--config", "/config/cas.json"]
