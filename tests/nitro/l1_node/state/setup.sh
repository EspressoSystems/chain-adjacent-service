#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
L1_NODE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$L1_NODE_DIR/../../.." && pwd)"
NITRO_TESTNODE_DIR="$PROJECT_ROOT/nitro-testnode"
STATE_DIR="$SCRIPT_DIR"
STATE_FILE="$STATE_DIR/anvil-state.json"
CHAIN_INFO_FILE="$STATE_DIR/deployed_chain_info.json"
DEPLOYMENT_FILE="$STATE_DIR/deployment.json"
WRITE_OVERRIDE="$L1_NODE_DIR/../write-override.sh"
OVERRIDE_FILE="$NITRO_TESTNODE_DIR/docker-compose.override.yml"

FORCE_REBOOTSTRAP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --init-force)
            FORCE_REBOOTSTRAP=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Build the anvil L1 image, override nitro-testnode's geth service,"
            echo "run the testnode, and dump the resulting L1 anvil state."
            echo ""
            echo "On re-runs, the existing anvil state and deployed_chain_info.json"
            echo "are reused: L2 volumes are wiped via --init-force, but L1 contracts"
            echo "are not re-deployed (rollupcreator is overridden to use the saved"
            echo "chain info)."
            echo ""
            echo "Options:"
            echo "  --init-force   Delete saved state files and re-bootstrap from scratch"
            echo "  --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run '$0 --help' for usage."
            exit 1
            ;;
    esac
done

if [ ! -d "$NITRO_TESTNODE_DIR" ]; then
    echo "Error: nitro-testnode directory not found at $NITRO_TESTNODE_DIR"
    exit 1
fi

if $FORCE_REBOOTSTRAP; then
    echo "== --init-force: removing saved state files"
    rm -f "$STATE_FILE" "$CHAIN_INFO_FILE" "$DEPLOYMENT_FILE"
fi

STATE_AVAILABLE=false
if [ -f "$STATE_FILE" ] && [ -f "$CHAIN_INFO_FILE" ] && [ -f "$DEPLOYMENT_FILE" ]; then
    STATE_AVAILABLE=true
fi

echo "== Building L1 anvil image from $L1_NODE_DIR/anvil-l1.Dockerfile"
docker build -f "$L1_NODE_DIR/anvil-l1.Dockerfile" -t nitro-l1-anvil "$L1_NODE_DIR"

if $STATE_AVAILABLE; then
    echo "== Saved state found — writing reuse-mode override"
    "$WRITE_OVERRIDE" reuse
else
    echo "== No saved state — writing bootstrap-mode override; state will be dumped at the end"
    "$WRITE_OVERRIDE" bootstrap
fi

echo "== Running nitro testnode with anvil L1 (this may take a few minutes)"
cd "$NITRO_TESTNODE_DIR"
./test-node.bash --no-simple --detach --init-force

export https_proxy=""
export http_proxy=""
export all_proxy=""
echo "== Waiting for sequencer RPC to be ready"
for i in $(seq 1 60); do
    if cast rpc eth_blockNumber --rpc-url http://localhost:8547 > /dev/null 2>&1; then
        echo "Sequencer is ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "Warning: sequencer did not become ready in time, continuing anyway"
    fi
    sleep 2
done

if ! $STATE_AVAILABLE; then
    echo "== Dumping anvil L1 state"
    cast rpc anvil_dumpState --rpc-url http://localhost:8545 > "$STATE_FILE"

    echo "== Saving deployed_chain_info.json and deployment.json"
    docker compose run --rm --entrypoint sh rollupcreator -c 'cat /config/deployed_chain_info.json' > "$CHAIN_INFO_FILE"
    docker compose run --rm --entrypoint sh rollupcreator -c 'cat /config/deployment.json' > "$DEPLOYMENT_FILE"

    echo "== Rewriting override to reuse-mode so subsequent test-node.bash runs load the saved state"
    "$WRITE_OVERRIDE" reuse
fi

L1_BLOCK=$(cast rpc eth_blockNumber --rpc-url http://localhost:8545 | tr -d '"')
L2_BLOCK=$(cast rpc eth_blockNumber --rpc-url http://localhost:8547 | tr -d '"')
echo "== Done."
echo "   Anvil L1 state:          $STATE_FILE (L1 block: $L1_BLOCK)"
echo "   Deployed chain info:     $CHAIN_INFO_FILE"
echo "   Deployment info:         $DEPLOYMENT_FILE"
echo "   Sequencer L2 block:      $L2_BLOCK"
echo ""
echo "To restore original geth: rm $OVERRIDE_FILE"

echo "stopping nitro testnode and cleaning up..."
cd "$NITRO_TESTNODE_DIR" && docker compose down
