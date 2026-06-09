#!/usr/bin/env bash
# (Re)generate the pre-deployed L1 state used by e2e tests.
# Run this after updating nitro-contracts or the rollup-creator image.
#
# Usage:
#   ./e2e/nitro/generate-l1-state.sh                      # default (v3.10)
#   ./e2e/nitro/generate-l1-state.sh .env.v3_9_9          # v3.9.9 overlay
#
# `.env` is always sourced first for shared values (ports, accounts,
# espresso config). If an overlay file is passed, it is sourced afterwards
# so its values win. A sibling `docker-compose.<overlay-suffix>.yml` (e.g.
# `docker-compose.v3_9_9.yml`) is layered on top of the base compose file
# automatically.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OVERLAY_FILE="${1:-}"

# Always source the base .env, then the overlay (if any) so it can win.
# Variables are exported into the shell env so docker compose picks them
# up via its standard env inheritance — no --env-file needed.
set -a
# shellcheck disable=SC1091
source "$SCRIPT_DIR/.env"
if [ -n "$OVERLAY_FILE" ]; then
    OVERLAY_PATH="$SCRIPT_DIR/$OVERLAY_FILE"
    if [ ! -f "$OVERLAY_PATH" ]; then
        echo "ERROR: overlay env file not found: $OVERLAY_PATH" >&2
        exit 1
    fi
    # shellcheck disable=SC1090
    source "$OVERLAY_PATH"
fi
set +a

: "${CONFIG_DIR:?CONFIG_DIR must be set}"
: "${ANYTRUST_TOOL_BIN:?ANYTRUST_TOOL_BIN must be set}"

BASE="$SCRIPT_DIR/docker-compose.yml"
GENERATE_OVERRIDE="$SCRIPT_DIR/docker-compose.generate.yml"
CONFIG_PATH="$SCRIPT_DIR/$CONFIG_DIR"
DAS_KEYS_DIR="$SCRIPT_DIR/das-keys"

DC_FILES=(-f "$BASE" -f "$GENERATE_OVERRIDE")

# Layer version-specific compose override when the overlay name implies one.
# `.env.v3_9_9` -> docker-compose.v3_9_9.yml
if [ -n "$OVERLAY_FILE" ]; then
    OVERLAY_SUFFIX="${OVERLAY_FILE#.env}"
    OVERLAY_SUFFIX="${OVERLAY_SUFFIX#.}"
    if [ -n "$OVERLAY_SUFFIX" ]; then
        VERSION_OVERRIDE="$SCRIPT_DIR/docker-compose.$OVERLAY_SUFFIX.yml"
        if [ -f "$VERSION_OVERRIDE" ]; then
            DC_FILES+=(-f "$VERSION_OVERRIDE")
        fi
    fi
fi

DC="docker compose ${DC_FILES[*]}"

# ── Teardown ──────────────────────────────────────────────────────────────────

echo "==> Tearing down any leftover containers..."
$DC --profile deploy down -v --remove-orphans 2>/dev/null || true

rm -rf "$CONFIG_PATH"
mkdir -p "$CONFIG_PATH"

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
    --platform linux/amd64 \
    --entrypoint "/usr/local/bin/$ANYTRUST_TOOL_BIN" \
    -v "$DUMP_CONFIG:/config.json:ro" \
    "$NITRO_IMAGE" \
    dumpkeyset --conf.file /config.json \
    | grep '^Keyset: ' | sed 's/^Keyset: //')

if [ -z "$KEYSET_HEX" ]; then
    echo "ERROR: Failed to dump DAS keyset"
    exit 1
fi

# Read contract addresses from deployment.json
SEQ_INBOX=$(jq -r '.["sequencer-inbox"]' "$CONFIG_PATH/deployment.json")
UPGRADE_EXEC=$(jq -r '.["upgrade-executor"]' "$CONFIG_PATH/deployment.json")

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

# ── Authorize validator EOA ───────────────────────────────────────────────────

echo "==> Authorizing validator EOA on rollup..."

ROLLUP=$(jq -r '.rollup' "$CONFIG_PATH/deployment.json")
VALIDATOR_ADDRESS=$(docker run --rm \
    --entrypoint cast \
    ghcr.io/foundry-rs/foundry:latest \
    wallet address --private-key "0x${VALIDATOR_PRIVATE_KEY}")

INNER_CALLDATA=$(docker run --rm \
    --entrypoint cast \
    ghcr.io/foundry-rs/foundry:latest \
    calldata "setValidator(address[],bool[])" "[$VALIDATOR_ADDRESS]" "[true]")

docker run --rm \
    --network nitro_default \
    --entrypoint cast \
    ghcr.io/foundry-rs/foundry:latest \
    send \
    --rpc-url http://l1-anvil:8545 \
    --private-key "$DEPLOYER_PRIVATE_KEY" \
    "$UPGRADE_EXEC" \
    "executeCall(address,bytes)" \
    "$ROLLUP" \
    "$INNER_CALLDATA"

echo "Validator $VALIDATOR_ADDRESS authorized on rollup $ROLLUP"

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
ls -lh "$CONFIG_PATH"
echo ""
echo "Done. Commit the files in $CONFIG_PATH to the repo."
