#!/usr/bin/env bash
# (Re)generate the pre-deployed L1 state used by e2e tests.
# Run this after updating nitro-contracts or the rollup-creator image.
#
# Usage:
#   just generate-l1-state           # default (v3.10.0)
#   just generate-l1-state v3.9.9    # specific version
# Optional env overrides:
#   ROLLUP_CREATOR_IMAGE=ghcr.io/.../rollup-creator:<tag>
#   ROLLUP_CREATOR_NITRO_CONTRACTS_BRANCH=<branch>
#   ANYTRUST_TOOL_IMAGE=offchainlabs/nitro-node:<tag>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

VERSION="${1:-}"

BASE="$SCRIPT_DIR/docker-compose.yml"
OVERRIDE="$SCRIPT_DIR/docker-compose.generate.yml"
DAS_KEYS_DIR="$SCRIPT_DIR/das-keys"

# Source .env for shared vars (DEPLOYER_PRIVATE_KEY, ports, etc.)
source "$SCRIPT_DIR/.env"

build_rollup_creator_image() {
    local contracts_branch="$1"
    local build_root
    local image_tag

    build_root="$(mktemp -d)"
    trap 'rm -rf "$build_root"' RETURN

    image_tag="local/rollup-creator:${contracts_branch//[^a-zA-Z0-9_.-]/-}"

    echo "==> Building rollup-creator with nitro-contracts branch '$contracts_branch'..."
    git clone --depth 1 --branch integrate-cas \
        https://github.com/EspressoSystems/nitro-testnode.git \
        "$build_root/nitro-testnode"

    docker build \
        -t "$image_tag" \
        --build-arg "NITRO_CONTRACTS_BRANCH=$contracts_branch" \
        -f "$build_root/nitro-testnode/rollupcreator/Dockerfile" \
        "$build_root/nitro-testnode/rollupcreator"

    export ROLLUP_CREATOR_IMAGE="$image_tag"
    echo "==> Using rollup-creator image $ROLLUP_CREATOR_IMAGE"
}

resolve_anytrust_tool_image() {
    if [ -n "${ANYTRUST_TOOL_IMAGE:-}" ]; then
        return
    fi

    if docker run --rm --entrypoint /bin/sh "$NITRO_IMAGE" -lc "test -x /usr/local/bin/anytrusttool" >/dev/null 2>&1; then
        ANYTRUST_TOOL_IMAGE="$NITRO_IMAGE"
    else
        ANYTRUST_TOOL_IMAGE="offchainlabs/nitro-node:v3.10.0-b1cf6db"
        echo "==> $NITRO_IMAGE does not ship anytrusttool; falling back to $ANYTRUST_TOOL_IMAGE for keyset generation"
    fi

    export ANYTRUST_TOOL_IMAGE
}

# Version-specific overrides
if [ -n "$VERSION" ]; then
    VERSION_DIR="$SCRIPT_DIR/versions/$VERSION"
    if [ ! -d "$VERSION_DIR" ]; then
        echo "ERROR: version directory not found: $VERSION_DIR"
        exit 1
    fi
    CONFIG_DIR="$VERSION_DIR/generated-config"
    export VERSION_CONFIG_DIR="versions/$VERSION/generated-config"
    export VERSION_NITRO_CONFIG_DIR="versions/$VERSION/nitro-config"

    # Version-specific image and WASM root
    case "$VERSION" in
        v3.9.9)
            export NITRO_IMAGE="offchainlabs/nitro-node:v3.9.9-6b0af88"
            export WASM_MODULE_ROOT="0xc2c02df561d4afaf9a1d6785f70098ec3874765c638e3cb6dbe8d3c83333e14c"
            ;;
        v3.10.0)
            export NITRO_IMAGE="offchainlabs/nitro-node:v3.10.0-b1cf6db"
            export WASM_MODULE_ROOT="0xc2c02df561d4afaf9a1d6785f70098ec3874765c638e3cb6dbe8d3c83333e14c"
            ;;
        *)
            echo "ERROR: unknown version '$VERSION'. Add a case to generate-l1-state.sh."
            exit 1
            ;;
    esac
    echo "Generating L1 state for version $VERSION (image: $NITRO_IMAGE)"
else
    CONFIG_DIR="$SCRIPT_DIR/generated-config"
    echo "Generating L1 state for default version (image: $NITRO_IMAGE)"
fi

if [ -n "${ROLLUP_CREATOR_NITRO_CONTRACTS_BRANCH:-}" ] && [ -n "${ROLLUP_CREATOR_IMAGE:-}" ]; then
    echo "ERROR: set either ROLLUP_CREATOR_IMAGE or ROLLUP_CREATOR_NITRO_CONTRACTS_BRANCH, not both"
    exit 1
fi

if [ -n "${ROLLUP_CREATOR_NITRO_CONTRACTS_BRANCH:-}" ]; then
    build_rollup_creator_image "$ROLLUP_CREATOR_NITRO_CONTRACTS_BRANCH"
fi

resolve_anytrust_tool_image

DC="docker compose -f $BASE -f $OVERRIDE"

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
    "$ANYTRUST_TOOL_IMAGE" \
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

# ── Authorize validator EOA ───────────────────────────────────────────────────

echo "==> Authorizing validator EOA on rollup..."

ROLLUP=$(jq -r '.rollup' "$CONFIG_DIR/deployment.json")
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
ls -lh "$CONFIG_DIR"
echo ""
echo "Done. Commit the files in $CONFIG_DIR to the repo."
