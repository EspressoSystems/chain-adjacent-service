#!/usr/bin/env bash
# (Re)generate the pre-deployed L1 state used by e2e tests.
# Run this after updating nitro-contracts or the rollup-creator image.
#
# Usage: just generate-l1-state   (or run directly: ./e2e/nitro/generate-l1-state.sh)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BASE="$SCRIPT_DIR/docker-compose.yml"
OVERRIDE="$SCRIPT_DIR/docker-compose.generate.yml"
CONFIG_DIR="$SCRIPT_DIR/generated-config"
DAS_KEYS_DIR="$SCRIPT_DIR/das-keys"
DC="docker compose -f $BASE -f $OVERRIDE"

# Source .env for NITRO_IMAGE, DEPLOYER_PRIVATE_KEY, etc.
source "$SCRIPT_DIR/.env"

# ── Teardown ──────────────────────────────────────────────────────────────────

echo "==> Tearing down any leftover containers..."
$DC --profile deploy down -v --remove-orphans 2>/dev/null || true

rm -rf "$CONFIG_DIR"
mkdir -p "$CONFIG_DIR"

# ── Deploy contracts ──────────────────────────────────────────────────────────

echo "==> Starting fresh Anvil L1..."
$DC up -d --wait l1-anvil

echo "==> Deploying rollup contracts via rollup-creator..."
$DC --profile deploy up rollup-creator

# ── Register DAS keyset ───────────────────────────────────────────────────────

echo "==> Registering DAS keyset on SequencerInbox..."

BLS_A=$(tr -d '\n' < "$DAS_KEYS_DIR/a/das_bls.pub")
BLS_B=$(tr -d '\n' < "$DAS_KEYS_DIR/b/das_bls.pub")

# Dump serialized keyset via anytrusttool
DUMP_CONFIG=$(mktemp)
trap 'rm -f "$DUMP_CONFIG"' EXIT
cat > "$DUMP_CONFIG" <<EOF
{
  "keyset": {
    "assumed-honest": 1,
    "backends": [
      {"url": "http://das-committee-a:9876", "pubkey": "$BLS_A"},
      {"url": "http://das-committee-b:9876", "pubkey": "$BLS_B"}
    ]
  }
}
EOF

KEYSET_HEX=$(docker run --rm \
    --entrypoint /usr/local/bin/anytrusttool \
    -v "$DUMP_CONFIG:/config.json:ro" \
    "$NITRO_IMAGE" \
    dumpkeyset --conf.file /config.json \
    | grep '^Keyset: ' | sed 's/^Keyset: //')

if [ -z "$KEYSET_HEX" ]; then
    echo "ERROR: Failed to dump DAS keyset"
    exit 1
fi

# Read contract addresses from deployment.json
SEQ_INBOX=$(jq -r '.["sequencer-inbox"]' "$CONFIG_DIR/deployment.json")
UPGRADE_EXEC=$(jq -r '.["upgrade-executor"]' "$CONFIG_DIR/deployment.json")

# Encode setValidKeyset(bytes) and send via upgrade-executor
INNER_CALLDATA=$(docker run --rm \
    --entrypoint cast \
    ghcr.io/foundry-rs/foundry:latest \
    calldata "setValidKeyset(bytes)" "$KEYSET_HEX")

docker run --rm \
    --network nitro_default \
    --entrypoint cast \
    ghcr.io/foundry-rs/foundry:latest \
    send \
    --rpc-url http://l1-anvil:8545 \
    --private-key "$DEPLOYER_PRIVATE_KEY" \
    "$UPGRADE_EXEC" \
    "executeCall(address,bytes)" \
    "$SEQ_INBOX" \
    "$INNER_CALLDATA"

echo "DAS keyset registered on SequencerInbox $SEQ_INBOX"

# ── Snapshot & cleanup ────────────────────────────────────────────────────────

# The generate override starts Anvil with `--dump-state /state/l1-state.json`.
# When Anvil receives SIGTERM (via `docker compose stop`), it writes the
# full chain state to that path before exiting.
echo "==> Stopping Anvil (triggers state dump)..."
$DC stop l1-anvil

echo "==> Cleaning up containers..."
$DC --profile deploy down -v --remove-orphans

echo ""
echo "Generated files:"
ls -lh "$CONFIG_DIR"
echo ""
echo "Done. Commit the files in $CONFIG_DIR to the repo."
