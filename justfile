# E2E test setup and execution for CAS (Chain Agnostic Service)
# Prerequisites: just, Rust, Docker
# Run: just test-e2e

# List available recipes
default:
    @just --list

# ─── Preflight check ──────────────────────────────────────────────────────────

# Verify all prerequisites are in place before running tests
check:
    #!/usr/bin/env bash
    set -euo pipefail
    all_ok=true

    require() {
        local cmd="$1"
        if command -v "$cmd" &>/dev/null; then
            echo "  [ok] $cmd"
        else
            echo "  [missing] $cmd"
            all_ok=false
        fi
    }

    echo "Checking required binaries..."
    require cargo
    require docker

    echo ""
    echo "Checking Docker daemon..."
    if docker info &>/dev/null; then
        echo "  [ok] Docker daemon is running"
    else
        echo "  [missing] Docker daemon is not running — start Docker and retry"
        all_ok=false
    fi

    echo ""
    echo "Checking e2e compose file..."
    if [ -f "e2e/nitro/docker-compose.yml" ]; then
        echo "  [ok] e2e/nitro/docker-compose.yml exists"
    else
        echo "  [missing] e2e/nitro/docker-compose.yml not found"
        all_ok=false
    fi

    echo ""
    if $all_ok; then
        echo "All prerequisites met. Run 'just test-e2e' to run the tests."
    else
        echo "Some prerequisites are missing."
        exit 1
    fi

# ─── Build ────────────────────────────────────────────────────────────────────

build:
    cargo build --release --bin chain-adjacent-service

docker-build tag="chain-adjacent-service:latest":
    docker build -t {{tag}} .

# ─── Formatting & linting ─────────────────────────────────────────────────────

# Check formatting across the entire workspace.
fmt:
    cargo fmt --all --check

# Run clippy for Nitro v3.9.9.
clippy-v3_9_9:
    cargo clippy --all-targets --no-default-features --features nitro-v3_9_9 -- -D warnings

# Run clippy for Nitro v3.10.
clippy-v3_10:
    cargo clippy --all-targets --no-default-features --features nitro-v3_10 -- -D warnings

# ─── Running tests ────────────────────────────────────────────────────────────

# Run unit/integration tests against a single Nitro version (default v3.10).
# `--all-features` can't be used: nitro-v3_9_9 and nitro-v3_10 are mutually exclusive.
test-v3_10:
    RUST_BACKTRACE=1 cargo test --no-default-features --features nitro-v3_10 -- --test-threads=1

test-v3_9_9:
    RUST_BACKTRACE=1 cargo test --no-default-features --features nitro-v3_9_9 -- --test-threads=1

# Shared v3.9.9 env. Used by every recipe that needs to talk to the v3.9.9 stack.
v3_9_9_env := "COMPOSE_FILE=docker-compose.yml:docker-compose.v3_9_9.yml " + \
    "NITRO_IMAGE=ghcr.io/espressosystems/nitro-espresso-integration/nitro-node:support-espresso-v3.9.9 " + \
    "CONFIG_DIR=generated-config-v3_9_9 " + \
    "NITRO_E2E_VERSION=v3_9_9"

# Run all E2E tests against Nitro v3.10 (tests must run sequentially).
# Pass an optional libtest filter, e.g. `just test-e2e test_e2e_anytrust`.
test-e2e filter="":
    RUST_BACKTRACE=1 cargo test --test nitro --no-default-features --features e2e,nitro-v3_10 {{filter}} -- --test-threads=1

# Run E2E tests against Nitro v3.9.9. Requires `just generate-l1-state-v3_9_9`
# to have produced e2e/nitro/generated-config-v3_9_9/ first.
# Pass an optional libtest filter, e.g. `just test-e2e-v3_9_9 test_e2e_anytrust`.
test-e2e-v3_9_9 filter="":
    {{v3_9_9_env}} \
    RUST_BACKTRACE=1 cargo test --test nitro --no-default-features --features e2e,nitro-v3_9_9 {{filter}} -- --test-threads=1

test: test-v3_10 test-v3_9_9

# ─── L1 state generation ──────────────────────────────────────────────────────

# (Re)generate the pre-deployed L1 state used by e2e tests (Nitro v3.10).
# Run this after updating nitro-contracts or the rollup-creator image.
generate-l1-state:
    ./e2e/nitro/generate-l1-state.sh

# (Re)generate the pre-deployed L1 state for Nitro v3.9.9.
# Outputs to e2e/nitro/generated-config-v3_9_9/.
generate-l1-state-v3_9_9:
    ./e2e/nitro/generate-l1-state.sh .env.v3_9_9

# ─── Docker compose helpers ───────────────────────────────────────────────────

# Bring up the e2e Nitro stack (without poster). v3.10 default.
e2e-up:
    docker compose -f e2e/nitro/docker-compose.yml up -d --wait

# Tear down the e2e Nitro stack. v3.10 default.
e2e-down:
    docker compose -f e2e/nitro/docker-compose.yml --profile poster down -v --remove-orphans

# Bring up the e2e Nitro stack (v3.9.9). Sources .env then .env.v3_9_9 so
# shared values stay in .env and only differences live in the overlay.
e2e-up-v3_9_9:
    #!/usr/bin/env bash
    set -euo pipefail
    set -a
    source e2e/nitro/.env
    source e2e/nitro/.env.v3_9_9
    set +a
    docker compose -f e2e/nitro/docker-compose.yml -f e2e/nitro/docker-compose.v3_9_9.yml \
        up -d --wait

# Tear down the e2e Nitro stack (v3.9.9).
e2e-down-v3_9_9:
    #!/usr/bin/env bash
    set -euo pipefail
    set -a
    source e2e/nitro/.env
    source e2e/nitro/.env.v3_9_9
    set +a
    docker compose -f e2e/nitro/docker-compose.yml -f e2e/nitro/docker-compose.v3_9_9.yml \
        --profile poster down -v --remove-orphans

# ─── Cleanup ──────────────────────────────────────────────────────────────────

# Tear down leftover E2E state (containers) after a crash or Ctrl-C.
# Brings down both the v3.10 and v3.9.9 stacks since either may be live.
clean:
    #!/usr/bin/env bash
    set -u
    echo "Stopping e2e Nitro stack (v3.10)..."
    (docker compose -f e2e/nitro/docker-compose.yml --profile poster down -v --remove-orphans) || true

    echo "Stopping e2e Nitro stack (v3.9.9)..."
    (
        set -a
        source e2e/nitro/.env
        source e2e/nitro/.env.v3_9_9
        set +a
        docker compose -f e2e/nitro/docker-compose.yml -f e2e/nitro/docker-compose.v3_9_9.yml \
            --profile poster down -v --remove-orphans
    ) || true

    echo "Stopping espresso dev-node containers..."
    docker compose -f src/espresso_e2e/docker-compose.yml down -v --remove-orphans || true

    echo "Done."
