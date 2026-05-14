#!/bin/sh
# Short-circuits `node index.js set-valid-keyset` in reuse mode. The anytrust
# keyset was already committed to L1 when the saved anvil state was generated,
# and SequencerInbox.setValidKeyset reverts on re-register (custom error
# AlreadyValidDASKeyset). Without this short-circuit, test-node.bash aborts at
# line 588 with set -eu, leaving sequencer_config.json unwritten and the
# sequencer dies on startup with "/config/sequencer_config.json: no such file".
#
# Any other subcommand falls through to `node index.js`.
set -e

if [ "${1:-}" = "set-valid-keyset" ]; then
    echo "== scripts-wrapper: skipping set-valid-keyset (keyset is already valid in saved L1 state)"
    exit 0
fi

exec node index.js "$@"
