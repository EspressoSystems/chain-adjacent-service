#!/bin/sh
# Short-circuits `yarn create-rollup-testnode` when saved deployment artifacts
# are mounted at /app/state/. The saved files are copied into /config/ so
# downstream steps (e.g. bridge-funds reading deployment.json, sequencer
# reading deployed_chain_info.json) behave as if the rollup had just been
# deployed.
#
# Any other command falls through to the original `yarn` entrypoint.
set -e

SAVED_CHAIN_INFO=/app/state/deployed_chain_info.json
SAVED_DEPLOYMENT=/app/state/deployment.json

if [ "${1:-}" = "create-rollup-testnode" ] \
    && [ -f "$SAVED_CHAIN_INFO" ] \
    && [ -f "$SAVED_DEPLOYMENT" ]; then
    echo "== Reusing saved rollup deployment from /app/state"
    cp "$SAVED_CHAIN_INFO" /config/deployed_chain_info.json
    cp "$SAVED_DEPLOYMENT" /config/deployment.json
    exit 0
fi

exec yarn "$@"
