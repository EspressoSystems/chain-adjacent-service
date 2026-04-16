# E2E test setup and execution for CAS (Chain Agnostic Service)
# Prerequisites: just, Rust, Go, Docker (Celestia binaries provided by the nix devshell)
# First time on a fresh machine: just setup && just test-e2e

# List available recipes
default:
    @just --list

# ─── Setup ────────────────────────────────────────────────────────────────────

# Install all prerequisites and initialize submodules (run once on a fresh machine)
setup: setup-submodules
    @echo ""
    @echo "Setup complete. Run 'just check' to verify, then 'just test-e2e' to run tests."

# Initialize git submodules (nitro-testnode)
setup-submodules:
    git submodule update --init --recursive

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
    require go
    require docker
    require celestia-appd
    require celestia

    echo ""
    echo "Checking Docker daemon..."
    if docker info &>/dev/null; then
        echo "  [ok] Docker daemon is running"
    else
        echo "  [missing] Docker daemon is not running — start Docker and retry"
        all_ok=false
    fi

    echo ""
    echo "Checking git submodules..."
    if [ -n "$(ls -A nitro-testnode 2>/dev/null)" ]; then
        echo "  [ok] nitro-testnode submodule is initialized"
    else
        echo "  [missing] nitro-testnode submodule is empty — run: just setup-submodules"
        all_ok=false
    fi

    echo ""
    if $all_ok; then
        echo "All prerequisites met. Run 'just test-e2e' to run the tests."
    else
        echo "Some prerequisites are missing. Run 'just setup' to install them."
        exit 1
    fi

# ─── Running tests ────────────────────────────────────────────────────────────

# Run all E2E tests (tests must run sequentially)
test-e2e:
    RUST_BACKTRACE=1 cargo test --test nitro --features e2e -- --test-threads=1

test:
    RUST_BACKTRACE=1 cargo test --all-features -- --test-threads=1