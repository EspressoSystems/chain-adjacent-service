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

# ─── Running tests ────────────────────────────────────────────────────────────

# Run all E2E tests (tests must run sequentially)
test-e2e:
    RUST_BACKTRACE=1 cargo test --test nitro --features e2e -- --test-threads=1

test:
    RUST_BACKTRACE=1 cargo test --all-features -- --test-threads=1

# ─── L1 state generation ──────────────────────────────────────────────────────

# (Re)generate the pre-deployed L1 state used by e2e tests.
# Run this after updating nitro-contracts or the rollup-creator image.
generate-l1-state:
    ./e2e/nitro/generate-l1-state.sh

# ─── Docker compose helpers ───────────────────────────────────────────────────

# Bring up the e2e Nitro stack (without poster)
e2e-up:
    docker compose -f e2e/nitro/docker-compose.yml up -d --wait

# Tear down the e2e Nitro stack
e2e-down:
    docker compose -f e2e/nitro/docker-compose.yml --profile poster down -v --remove-orphans

# ─── Cleanup ──────────────────────────────────────────────────────────────────

# Tear down leftover E2E state (containers) after a crash or Ctrl-C
clean:
    #!/usr/bin/env bash
    set -u
    echo "Stopping e2e Nitro stack..."
    (docker compose -f e2e/nitro/docker-compose.yml --profile poster down -v --remove-orphans) || true

    echo "Stopping espresso dev-node containers..."
    docker compose -f src/espresso_e2e/docker-compose.yml down -v --remove-orphans || true

    echo "Done."
