#!/bin/sh
set -e

# Compatibility wrapper for nitro-testnode which calls `docker compose run geth init ...`
if [ "${1:-}" = "init" ]; then
    echo "Skipping geth init (anvil does not require genesis initialization)"
    exit 0
fi

RPC_URL="http://localhost:8545"

# Start anvil in background with nitro-testnode L1 configuration.
# Matches the genesis config from nitro-testnode/scripts/config.ts
# (chainId 1337, gasLimit 0x1C9C380 = 30000000, baseFee 0x3B9ACA00 = 1 Gwei).
anvil \
  --chain-id 1337 \
  --gas-limit 30000000 \
  --base-fee 1000000000 \
  --mnemonic "indoor dish desk flag debris potato excuse depart ticket judge file exit" \
  --derivation-path "m/44'/60'/0'/0/" \
  --accounts 10 \
  --balance 10000 \
  --host 0.0.0.0 \
  --port 8545 \
  --block-time 10 \
  &

ANVIL_PID=$!

# Anvil serves HTTP and WS on the same port, but nitro-testnode expects a
# separate WS endpoint on 8546. Forward 8546 -> 8545 so clients using the
# classic geth port layout can connect.
socat TCP-LISTEN:8546,fork,reuseaddr TCP:127.0.0.1:8545 &
SOCAT_PID=$!

# Wait for anvil RPC to be ready
for i in $(seq 1 30); do
  if cast rpc eth_blockNumber > /dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

# Pre-fund accounts to match nitro-testnode geth genesis alloc.
# This lets tests use hardhat default accounts and special accounts.
BALANCE_10K="0x21e19e0c9bab2400000"   # 10000 ETH in wei
BALANCE_20K="0x43c33c1937564800000"   # 20000 ETH in wei
BALANCE_FUNNEL="0x314dc6448d9338c15b0a00000000"  # massive genesis balance for funnel

# Hardhat default accounts (used by nitro tests)
HARDHAT_ADDRS="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 0x70997970C51812dc3A010C7d01b50e0d17dc79C8 0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC 0x90F79bf6EB2c4f870365E785982E1f101E93b906 0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65 0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc 0x976EA74026E726554dB657fA54763abd0C3a0aa9 0x14dC79964da2C08b23698B3D3cc7Ca32193d9955 0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f 0xa0Ee7A142d267C1f36714E4a8F75612F20a79720 0xBcd4042DE499D14e55001CcbB24a551F3b954096 0x71bE63f3384f5fb98995898A86B02Fb2426c5788 0xFABB0ac9d68B0B445fB7357272Ff202C5651694a 0x1CBd3b2770909D4e10f157cABC84C7264073C9Ec 0xdF3e18d64BC6A983f673Ab319CCaE4f1a57C7097 0xcd3B766CCDd6AE721141F452C550Ca635964ce71 0x2546BcD3c84621e976D8185a91A922aE77ECEc30 0xbDA5747bFD65F08deb54cb465eB87D40e51B197E 0xdD2FD4581271e230360230F9337D5c0430Bf44C0 0x8626f6940E2eb28930eFb4CeF49B2d1F2C9C1199"

for addr in $HARDHAT_ADDRS; do
  cast rpc anvil_setBalance "$addr" "$BALANCE_10K" > /dev/null 2>&1
done

# Extra genesis accounts with 20k ETH
cast rpc anvil_setBalance "0x123463a4B065722E99115D6c222f267d9cABb524" "$BALANCE_20K" > /dev/null 2>&1
cast rpc anvil_setBalance "0x5678E9E827B3be0E3d4b910126a64a697a148267" "$BALANCE_20K" > /dev/null 2>&1

# Funnel (specialAccount 0) gets the massive genesis balance used to fund validators/sequencers
cast rpc anvil_setBalance "0x3f1Eae7D46d88F08fc2F8ed27FCb2AB183EB2d0E" "$BALANCE_FUNNEL" > /dev/null 2>&1

echo "Anvil L1 node ready on $RPC_URL (chain-id: 1337, gas-limit: 30000000, base-fee: 1000000000)"

# Keep anvil in foreground; if it exits, tear down the socat forwarder too.
wait $ANVIL_PID
kill $SOCAT_PID 2>/dev/null || true
