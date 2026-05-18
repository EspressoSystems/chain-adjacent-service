#!/bin/sh
# Short-circuits `datool keygen --dir /das-committee-{a,b}/keys` when saved
# BLS keys are mounted at /app/state/das-keys/{a,b}/. The saved keys are
# copied into the destination dir so downstream steps (writing the keyset
# config, starting daservers, set-valid-keyset) behave as if datool had just
# generated them — but the resulting keyset hash matches what bootstrap
# already committed to L1.
#
# Mounted at /usr/local/bin/datool so calls like `sh -c "/usr/local/bin/datool
# dumpkeyset ..."` in test-node.bash also route through here. Nitro 3.10
# renamed the binary to anytrusttool, so unhandled commands forward there.
set -e

SAVED_KEYS_DIR=/app/state/das-keys

case "$*" in
    "keygen --dir /das-committee-a/keys"*)
        if [ -d "$SAVED_KEYS_DIR/a" ]; then
            echo "== Restoring saved DAS keys (committee-a) from $SAVED_KEYS_DIR/a"
            cp "$SAVED_KEYS_DIR/a"/* /das-committee-a/keys/
            exit 0
        fi
        ;;
    "keygen --dir /das-committee-b/keys"*)
        if [ -d "$SAVED_KEYS_DIR/b" ]; then
            echo "== Restoring saved DAS keys (committee-b) from $SAVED_KEYS_DIR/b"
            cp "$SAVED_KEYS_DIR/b"/* /das-committee-b/keys/
            exit 0
        fi
        ;;
esac

exec /usr/local/bin/anytrusttool "$@"
