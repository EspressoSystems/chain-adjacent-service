# E2E test setup and execution for CAS (Chain Agnostic Service)
# Prerequisites: just, rustup, Go, Docker
# First time on a fresh machine: just setup && just test-e2e

# List available recipes
default:
    @just --list

# ─── Setup ────────────────────────────────────────────────────────────────────

# Install all prerequisites and initialize submodules (run once on a fresh machine)
setup: setup-submodules setup-rust setup-celestia
    @echo ""
    @echo "Setup complete. Run 'just check' to verify, then 'just test-e2e' to run tests."

# Initialize git submodules (nitro-testnode)
setup-submodules:
    git submodule update --init --recursive

# Ensure the pinned Rust toolchain is installed (version from rust-toolchain.toml)
setup-rust:
    rustup show

# Install both Celestia binaries
setup-celestia: _install-celestia-app _install-celestia-node

_install-celestia-app:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v celestia-appd &>/dev/null; then
        echo "celestia-appd already installed ($(celestia-appd version 2>/dev/null || echo unknown))"
        exit 0
    fi
    echo "Installing celestia-appd..."
    curl -L https://raw.githubusercontent.com/celestiaorg/docs/main/public/celestia-app.sh | bash
    sudo mv "$HOME/celestia-app-temp/celestia-appd" /usr/local/bin/
    rm -rf "$HOME/celestia-app-temp"
    echo "celestia-appd installed"

_install-celestia-node:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v celestia &>/dev/null; then
        echo "celestia already installed ($(celestia version 2>/dev/null || echo unknown))"
        exit 0
    fi
    echo "Installing celestia node..."
    curl -L https://raw.githubusercontent.com/celestiaorg/docs/main/public/celestia-node.sh | bash
    sudo mv "$HOME/celestia-node-temp/celestia" /usr/local/bin/
    rm -rf "$HOME/celestia-node-temp"
    echo "celestia installed"

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
    require rustup
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

# Run all E2E tests (tests must run sequentially
test-e2e:
    RUST_BACKTRACE=1 cargo test --test nitro --features e2e -- --test-threads=1

test:
    RUST_BACKTRACE=1 cargo test --all-features -- --test-threads=1
