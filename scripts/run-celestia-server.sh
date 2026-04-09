#!/usr/bin/env bash

set -euo pipefail

# -------------------------------
# Config
# -------------------------------
CHAIN_ID="${CHAIN_ID:-test}"
APP_HOME="${APP_HOME:-$HOME/.celestia-app}"
BRIDGE_HOME="${BRIDGE_HOME:-$HOME/.celestia-bridge-test}"
KEY_NAME="${KEY_NAME:-my_celes_key}"

CONSENSUS_RPC_ENDPOINT="${RPC_ENDPOINT:-http://localhost:26657}"
BRIDGE_RPC_ENDPOINT="${RPC_ENDPOINT:-http://localhost:26658}"
DOCKER_BRIDGE_RPC_ENDPOINT="http://host.docker.internal:26658/"
SERVER_PORT="${SERVER_PORT:-8080}"
NAMESPACE_ID="${NAMESPACE_ID:-65636c69707365}"

LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "$LOG_DIR"

# -------------------------------
# Cleanup
# -------------------------------
cleanup() {
  echo "🧹 Cleaning up..."

  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
  fi
  pkill -9 -f "celestia-server" 2>/dev/null || true

  kill "${NODE_PID:-}" "${BRIDGE_PID:-}" 2>/dev/null || true
  sleep 1
  pkill -9 -f celestia-appd     2>/dev/null || true
  pkill -9 -f "celestia bridge" 2>/dev/null || true

  wait "${SERVER_PID:-}" "${NODE_PID:-}" "${BRIDGE_PID:-}" 2>/dev/null || true
  # docker rm -f celestia-das 2>/dev/null || true
}
trap cleanup EXIT

# -------------------------------
# 1. Start Consensus Node
# -------------------------------
echo "🚀 Starting consensus node..."
echo "y" | ./scripts/single-node.sh > "$LOG_DIR/node.log" 2>&1 &
NODE_PID=$!

echo "⏳ Waiting for consensus node..."
echo $CONSENSUS_RPC_ENDPOINT
for i in {1..40}; do
  if curl -s "$CONSENSUS_RPC_ENDPOINT/status" >/dev/null 2>&1; then
    echo "✅ Consensus node ready"
    break
  fi

  if ! kill -0 $NODE_PID 2>/dev/null; then
    echo "❌ Node crashed"
    tail -n 50 "$LOG_DIR/node.log"
    exit 1
  fi

  sleep 3
done

# -------------------------------
# 2. Start Bridge Node
# -------------------------------
echo "🌉 Starting bridge node..."
./scripts/single-node-bridge.sh > "$LOG_DIR/bridge.log" 2>&1 &
BRIDGE_PID=$!

# -------------------------------
# 3. Get Bridge Address
# -------------------------------
echo "🔍 Extracting bridge address from logs..."

BRIDGE_ADDR=""

for i in {1..40}; do
  if [ -f "$LOG_DIR/bridge.log" ]; then
    BRIDGE_ADDR=$(grep -oE "ADDRESS: celestia1[0-9a-z]+" "$LOG_DIR/bridge.log" | awk '{print $2}' | tail -n 1 || true)
  fi

  if [[ -n "$BRIDGE_ADDR" ]]; then
    echo "✅ Found bridge address"
    break
  fi

  sleep 2
done

if [[ -z "$BRIDGE_ADDR" ]]; then
  echo "❌ Failed to extract bridge address"
  echo "---- bridge logs ----"
  tail -n 100 "$LOG_DIR/bridge.log"
  exit 1
fi

echo "📬 Bridge Address: $BRIDGE_ADDR"

# -------------------------------
# 4. Fund Bridge Signer
# -------------------------------
echo "💸 Funding bridge signer..."

celestia-appd tx bank send validator "$BRIDGE_ADDR" 10000000utia \
  --chain-id "$CHAIN_ID" \
  --keyring-backend test \
  --home "$APP_HOME" \
  --fees 800utia \
  -y > "$LOG_DIR/fund.log" 2>&1

echo "✅ Funded"

# -------------------------------
# 5. Get Auth Token
# -------------------------------
echo "🔑 Fetching auth token..."

AUTH_TOKEN=$(celestia bridge auth write \
  --node.store "$BRIDGE_HOME" | tr -d '\n')

if [[ -z "$AUTH_TOKEN" ]]; then
  echo "❌ Failed to get auth token"
  exit 1
fi

echo "✅ Auth token acquired"
echo $AUTH_TOKEN

# -------------------------------
# 6. Start DAS Server
# -------------------------------
echo "🖥️ Starting DAS server(Submodule)..."

DAS_DIR="${DAS_DIR:-./nitro-das-celestia}"


if [ ! -f "$DAS_DIR/cmd/celestia-server" ]; then
  echo "🔨 Building DAS server..."
  pushd "$DAS_DIR/cmd" > /dev/null
  go build -o celestia-server
  popd > /dev/null
fi

echo "🚀 Starting DAS server..."

"$DAS_DIR/cmd/celestia-server" \
  --enable-rpc \
  --rpc-addr 0.0.0.0 \
  --rpc-port "$SERVER_PORT" \
  --celestia.with-writer \
  --celestia.rpc "$BRIDGE_RPC_ENDPOINT/" \
  --celestia.auth-token "$AUTH_TOKEN" \
  --celestia.namespace-id "$NAMESPACE_ID" \
  > "$LOG_DIR/server.log" 2>&1 &

SERVER_PID=$!

echo "⏳ Waiting for DAS server..."

for i in {1..30}; do
  if curl -s "http://localhost:$SERVER_PORT" >/dev/null 2>&1; then
    echo "✅ DAS server ready"
    break
  fi

  # detect crash early
  if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "❌ DAS server crashed"
    tail -n 50 "$LOG_DIR/server.log"
    exit 1
  fi

  sleep 2
done

echo "🎉 FULL FLOW COMPLETE"

echo ""
echo "----------------------------------"
echo "Consensus RPC:        $CONSENSUS_RPC_ENDPOINT"
echo "Bridge RPC:           $BRIDGE_RPC_ENDPOINT"
echo "DAS Server:           http://localhost:$SERVER_PORT"
echo "Bridge:               $BRIDGE_ADDR"
echo "DAS server:           $SERVER_PORT"
echo "----------------------------------"

wait $NODE_PID $BRIDGE_PID $SERVER_PID