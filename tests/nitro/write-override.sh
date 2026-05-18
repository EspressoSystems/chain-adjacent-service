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
#   CAS_RPC_URL   If set, the poster's DA provider is configured to use
#                 this URL as its DA RPC endpoint. Required if CAS_FEED_URL is set
#                 and the poster is enabled, since the poster needs to fetch batch
#                 data from the DA provider in order to post batches.
#   ANYTRUST      If set to "1", in `reuse` mode adds a `daprovider-anytrust`
#                 service running `/usr/local/bin/daprovider --mode anytrust`.
#                 CAS forwards anytrust requests to it instead of doing
#                 aggregation itself. The committee backend pubkeys are read
#                 from the das-keys directory and baked into the sidecar's
#                 config file (written to STATE_DIR).
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
L1_NODE_DIR="$SCRIPT_DIR/l1_node"
STATE_DIR="$L1_NODE_DIR/state"
WRAPPER_SCRIPT="$L1_NODE_DIR/rollupcreator-wrapper.sh"
DATOOL_WRAPPER_SCRIPT="$L1_NODE_DIR/datool-wrapper.sh"
SCRIPTS_WRAPPER_SCRIPT="$L1_NODE_DIR/scripts-wrapper.sh"
DAS_KEYS_DIR="$STATE_DIR/das-keys"
NITRO_TESTNODE_DIR="$PROJECT_ROOT/nitro-testnode"
OVERRIDE_FILE="$NITRO_TESTNODE_DIR/docker-compose.override.yml"

# Path used by the anytrust sidecar; only written when ANYTRUST=1.
ANYTRUST_DAPROVIDER_CONFIG="$STATE_DIR/anytrust_daprovider.json"
# Host-side port the sidecar binds to (also the in-container port — we
# match them to keep the URL the same on both sides).
ANYTRUST_DAPROVIDER_PORT=9881

# Sequencer inbox address from the saved L1 state (matches the constant in
# tests/nitro/test_e2e.rs).
SEQUENCER_INBOX_ADDRESS="0xE44f73d4e7b3C008b71CF273000703F5B6380119"

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
    environment:
      - ENABLE_ESPRESSO_CAS=1
      - TEE_VERIFIER_INFO=/config/tee_verifier_address.txt
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
  datool:
    volumes:
      - "$DATOOL_WRAPPER_SCRIPT:/usr/local/bin/datool:ro"
      - "$DAS_KEYS_DIR:/app/state/das-keys:ro"
  scripts:
    entrypoint: ["/app/scripts-wrapper.sh"]
    volumes:
      - "$SCRIPTS_WRAPPER_SCRIPT:/app/scripts-wrapper.sh:ro"
  das-committee-a:
    # 3.10.0: this line can be removed after nitro-testnode upgrades to v3.10.0
    entrypoint: ["/usr/local/bin/anytrustserver"]
    command:
      - --conf.file=/config/l2_das_committee.json
      - --data-availability.disable-signature-checking=true
      - --log-level=debug

  das-committee-b:
    # 3.10.0: this line can be removed after nitro-testnode upgrades to v3.10.0
    entrypoint: ["/usr/local/bin/anytrustserver"]
    command:
      - --conf.file=/config/l2_das_committee.json
      - --data-availability.disable-signature-checking=true
      - --log-level=debug
  das-mirror:
    entrypoint: ["/usr/local/bin/anytrustserver"]
EOF
        ;;
    *)
        echo "Usage: $0 {bootstrap|reuse}" >&2
        exit 1
        ;;
esac

if [ "${ANYTRUST:-}" = "1" ] && [ "$MODE" = "reuse" ]; then
    # Generate the anytrust sidecar's config: it talks to the same
    # das-committee-a/b that the test brings up, and falls back to
    # das-mirror's REST endpoint for recover/preimage queries. BLS
    # pubkeys are baked in from the committed das-keys files so the
    # sidecar's view of the committee matches the keyset the saved L1
    # state has already committed.
    bls_a="$(tr -d '\n' < "$DAS_KEYS_DIR/a/das_bls.pub")"
    bls_b="$(tr -d '\n' < "$DAS_KEYS_DIR/b/das_bls.pub")"
    cat > "$ANYTRUST_DAPROVIDER_CONFIG" << EOF
{
  "mode": "anytrust",
  "parent-chain": {
    "node-url": "ws://geth:8546",
    "sequencer-inbox-address": "$SEQUENCER_INBOX_ADDRESS"
  },
  "anytrust": {
    "enable": true,
    "max-batch-size": 1000000,
    "request-timeout": "30s",
    "rpc-aggregator": {
      "enable": true,
      "assumed-honest": 1,
      "backends": [
        {"url": "http://das-committee-a:9876", "pubkey": "$bls_a"},
        {"url": "http://das-committee-b:9876", "pubkey": "$bls_b"}
      ]
    },
    "rest-aggregator": {
      "enable": true,
      "urls": ["http://das-mirror:9877"]
    }
  },
  "with-data-signer": true,
  "data-signer-wallet": {
    "private-key": "b6b15c8cb491557369f3c7d2c287b053eb229daa9c22138887752191c9520659"
  },
  "provider-server": {
    "addr": "0.0.0.0",
    "port": $ANYTRUST_DAPROVIDER_PORT,
    "enable-da-writer": true
  },
  "log-level": "DEBUG"
}
EOF

    cat >> "$OVERRIDE_FILE" << EOF
  daprovider-anytrust:
    pid: host
    image: nitro-node-dev-testnode
    entrypoint: /usr/local/bin/daprovider
    ports:
      - "127.0.0.1:$ANYTRUST_DAPROVIDER_PORT:$ANYTRUST_DAPROVIDER_PORT"
    volumes:
      - "$ANYTRUST_DAPROVIDER_CONFIG:/config/anytrust_daprovider.json:ro"
    command:
      - --conf.file=/config/anytrust_daprovider.json
    depends_on:
      - das-committee-a
      - das-committee-b
      - das-mirror
      - geth
EOF
fi

if [ -n "${CAS_FEED_URL:-}" ]; then
    cat >> "$OVERRIDE_FILE" << EOF
  poster:
    command:
      - --conf.file
      - /config/poster_config.json
      - --node.da.external-provider.enable=true
      - --node.da.external-provider.with-writer=true
      - --node.da.external-provider.rpc.url=$CAS_RPC_URL
      - --node.feed.input.url=$CAS_FEED_URL
      - --node.seq-coordinator.enable=false
      - --node.dangerous.no-sequencer-coordinator=true
    extra_hosts:
      - "host.docker.internal:host-gateway"
  sequencer:
    command:
      - --conf.file=/config/sequencer_config.json
      - --node.feed.output.enable
      - --node.feed.output.port=9642
      - --node.feed.output.signed=true
      - --http.api=net,web3,eth,txpool,debug,timeboost,auctioneer
      - --node.seq-coordinator.enable=false
      - --node.dangerous.no-sequencer-coordinator=true
EOF
fi

echo "Wrote docker-compose override ($MODE mode${CAS_FEED_URL:+, poster→$CAS_FEED_URL}${ANYTRUST:+, anytrust sidecar enabled}) to $OVERRIDE_FILE"
