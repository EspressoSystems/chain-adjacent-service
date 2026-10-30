# syntax=docker/dockerfile:1
FROM rust:1.94.1-bookworm AS builder

# Select the Nitro variant with --build-arg CARGO_FEATURES=nitro-v3_12_1.
ARG CARGO_FEATURES=nitro-v3_9_9

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# All Nitro variants share dependencies. Cache them with a dummy crate;
# the final build selects CARGO_FEATURES.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "pub fn _dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --locked --release --no-default-features --features "nitro-v3_9_9" && \
    cargo clean --release -p chain-adjacent-service && \
    rm -rf src

COPY src/ src/
RUN cargo build --locked --release --no-default-features --features "$CARGO_FEATURES" --bin chain-adjacent-service

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        libcurl4 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 user && \
    useradd --uid 1000 --gid user --create-home user

COPY --from=builder /build/target/release/chain-adjacent-service /usr/local/bin/

USER user
WORKDIR /home/user

ENTRYPOINT ["chain-adjacent-service"]
CMD ["--config", "/etc/cas/config.json"]
