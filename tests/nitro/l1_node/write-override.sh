#!/usr/bin/env bash
# Writes nitro-testnode's docker-compose.override.yml in one of two modes:
#
#   bootstrap  Fresh anvil (SKIP_STATE_LOAD=1); rollupcreator builds from our
#              Dockerfile and deploys contracts normally. Used by setup.sh on
#              the first run to generate the saved state.
#
#   reuse      Anvil loads state from $STATE_DIR/anvil-state.json. Rollupcreator
#              still builds from our Dockerfile, but its entrypoint is replaced
#              with rollupcreator-wrapper.sh, which short-circuits
#              `create-rollup-testnode` by copying saved deployed_chain_info.json.
#              Used by setup.sh after bootstrap, and by the integration test
#              harness before spawning test-node.bash.
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
NITRO_TESTNODE_DIR="$PROJECT_ROOT/nitro-testnode"
STATE_DIR="$SCRIPT_DIR/state"
WRAPPER_SCRIPT="$SCRIPT_DIR/rollupcreator-wrapper.sh"
OVERRIDE_FILE="$NITRO_TESTNODE_DIR/docker-compose.override.yml"

MODE="${1:-}"

case "$MODE" in
    bootstrap)
        cat > "$OVERRIDE_FILE" << EOF
services:
  geth:
    image: nitro-l1-anvil
    build:
      context: $SCRIPT_DIR
      dockerfile: anvil-l1.Dockerfile
    user: root
    command: []
    entrypoint: ["/app/entrypoint.sh"]
    environment:
      - SKIP_STATE_LOAD=1
    volumes:
      - "$STATE_DIR:/app/state"
  rollupcreator:
    build:
      context: $SCRIPT_DIR
      dockerfile: rollupcreator.Dockerfile
EOF
        ;;
    reuse)
        cat > "$OVERRIDE_FILE" << EOF
services:
  geth:
    image: nitro-l1-anvil
    build:
      context: $SCRIPT_DIR
      dockerfile: anvil-l1.Dockerfile
    user: root
    command: []
    entrypoint: ["/app/entrypoint.sh"]
    volumes:
      - "$STATE_DIR:/app/state"
  rollupcreator:
    build:
      context: $SCRIPT_DIR
      dockerfile: rollupcreator.Dockerfile
    entrypoint: ["/app/rollupcreator-wrapper.sh"]
    volumes:
      - "$WRAPPER_SCRIPT:/app/rollupcreator-wrapper.sh:ro"
      - "$STATE_DIR:/app/state:ro"
EOF
        ;;
    *)
        echo "Usage: $0 {bootstrap|reuse}" >&2
        exit 1
        ;;
esac

echo "Wrote docker-compose override ($MODE mode) to $OVERRIDE_FILE"
