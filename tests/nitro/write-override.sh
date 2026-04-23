#!/usr/bin/env bash
# Writes nitro-testnode's docker-compose.override.yml in one of two modes:
#
#   bootstrap  Fresh anvil (SKIP_STATE_LOAD=1); rollupcreator builds from our
#              Dockerfile and deploys contracts normally. Used by setup.sh on
#              the first run to generate the saved state.
#
#   reuse      Anvil loads state from l1_node/state/anvil-state.json. Rollupcreator
#              still builds from our Dockerfile, but its entrypoint is replaced
#              with rollupcreator-wrapper.sh, which short-circuits
#              `create-rollup-testnode` by copying saved deployment artifacts.
#              Used by setup.sh after bootstrap, and by the integration test
#              harness before spawning test-node.bash.
#
# Env vars (optional):
#   CAS_FEED_URL  If set, the poster service is overridden to subscribe to this
#                 URL via --node.feed.input.url, so batches posted by nitro come
#                 from our CAS instead of the in-testnode sequencer feed.
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
L1_NODE_DIR="$SCRIPT_DIR/l1_node"
STATE_DIR="$L1_NODE_DIR/state"
WRAPPER_SCRIPT="$L1_NODE_DIR/rollupcreator-wrapper.sh"
NITRO_TESTNODE_DIR="$PROJECT_ROOT/nitro-testnode"
OVERRIDE_FILE="$NITRO_TESTNODE_DIR/docker-compose.override.yml"

MODE="${1:-}"

case "$MODE" in
    bootstrap)
        cat > "$OVERRIDE_FILE" << EOF
services:
  geth:
    image: nitro-l1-anvil
    build:
      context: $L1_NODE_DIR
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
      context: $L1_NODE_DIR
      dockerfile: rollupcreator.Dockerfile
EOF
        ;;
    reuse)
        cat > "$OVERRIDE_FILE" << EOF
services:
  geth:
    image: nitro-l1-anvil
    build:
      context: $L1_NODE_DIR
      dockerfile: anvil-l1.Dockerfile
    user: root
    command: []
    entrypoint: ["/app/entrypoint.sh"]
    volumes:
      - "$STATE_DIR:/app/state"
  rollupcreator:
    build:
      context: $L1_NODE_DIR
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

if [ -n "${CAS_FEED_URL:-}" ]; then
    cat >> "$OVERRIDE_FILE" << EOF
  poster:
    command:
      - --conf.file
      - /config/poster_config.json
      - '--node.feed.input.url=[\"$CAS_FEED_URL\"]'
EOF
fi

echo "Wrote docker-compose override ($MODE mode${CAS_FEED_URL:+, poster→$CAS_FEED_URL}) to $OVERRIDE_FILE"
