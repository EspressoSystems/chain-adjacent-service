#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
L1_NODE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$L1_NODE_DIR/../../.." && pwd)"
NITRO_TESTNODE_DIR="$PROJECT_ROOT/nitro-testnode"
STATE_DIR="$SCRIPT_DIR"

INIT_FORCE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --init-force)
            INIT_FORCE="--init-force"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Build the anvil L1 image, override nitro-testnode's geth service,"
            echo "run the testnode, and dump the resulting L1 anvil state."
            echo ""
            echo "Options:"
            echo "  --init-force   Remove old data and force a fresh rollup deployment"
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

echo "== Building L1 anvil image from $L1_NODE_DIR"
docker build -t nitro-l1-anvil "$L1_NODE_DIR"

echo "== Placing docker-compose override in $NITRO_TESTNODE_DIR"
cat > "$NITRO_TESTNODE_DIR/docker-compose.override.yml" << 'EOF'
services:
  geth:
    image: nitro-l1-anvil
    user: root
    command: []
    entrypoint: ["/app/entrypoint.sh"]
EOF

echo "== Running nitro testnode with anvil L1 (this may take a few minutes)"
cd "$NITRO_TESTNODE_DIR"
./test-node.bash --simple --detach $INIT_FORCE

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

echo "== Dumping anvil L1 state"
cast rpc anvil_dumpState --rpc-url http://localhost:8545 > "$STATE_DIR/anvil-state.json"

L1_BLOCK=$(cast rpc eth_blockNumber --rpc-url http://localhost:8545 | tr -d '"')
L2_BLOCK=$(cast rpc eth_blockNumber --rpc-url http://localhost:8547 | tr -d '"')
echo "== Done."
echo "   Anvil L1 state: $STATE_DIR/anvil-state.json (L1 block: $L1_BLOCK)"
echo "   Sequencer L2 block: $L2_BLOCK"
echo ""
echo "To restore original geth: rm $NITRO_TESTNODE_DIR/docker-compose.override.yml"

echo "stopping nitro testnode and cleaning up..."
cd $NITRO_TESTNODE_DIR && docker compose down
