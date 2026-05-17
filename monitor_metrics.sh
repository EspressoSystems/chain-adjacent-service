#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="https://espresso-mainnet.alt.technology/v1/status/metrics"
LOG_FILE="${1:-metrics_history.csv}"
INTERVAL=60

prev_libp2p=""
prev_cdn=""

extract_metric() {
    local name="$1"
    local data="$2"
    # Handles both bare metric and labeled metric (Prometheus format)
    echo "$data" | grep -E "^${name}(\{[^}]*\})? " | tail -1 | awk '{print $NF}'
}

echo "Logging to: $LOG_FILE"
echo "Polling every ${INTERVAL}s. Press Ctrl+C to stop."
echo ""

# Write CSV header if file doesn't exist yet
if [[ ! -f "$LOG_FILE" ]]; then
    echo "timestamp,libp2p_failed,cdn_failed,libp2p_delta,cdn_delta" > "$LOG_FILE"
fi

while true; do
    ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    raw=$(curl -sf --max-time 10 "$ENDPOINT" 2>/dev/null) || {
        echo "[$ts] ERROR: failed to fetch endpoint (curl exit $?)"
        sleep "$INTERVAL"
        continue
    }

    libp2p=$(extract_metric "consensus_libp2p_num_failed_messages" "$raw")
    cdn=$(extract_metric "consensus_cdn_num_failed_messages" "$raw")

    if [[ -z "$libp2p" || -z "$cdn" ]]; then
        echo "[$ts] ERROR: could not parse metrics from response"
        sleep "$INTERVAL"
        continue
    fi

    if [[ -z "$prev_libp2p" ]]; then
        libp2p_delta="N/A (first reading)"
        cdn_delta="N/A (first reading)"
        libp2p_delta_csv=""
        cdn_delta_csv=""
    else
        libp2p_delta=$(echo "$libp2p $prev_libp2p" | awk '{printf "%+.0f", $1-$2}')
        cdn_delta=$(echo "$cdn $prev_cdn" | awk '{printf "%+.0f", $1-$2}')
        libp2p_delta_csv=$(echo "$libp2p $prev_libp2p" | awk '{print $1-$2}')
        cdn_delta_csv=$(echo "$cdn $prev_cdn" | awk '{print $1-$2}')
    fi

    echo "[$ts]"
    echo "  consensus_libp2p_num_failed_messages : $libp2p  (delta: $libp2p_delta)"
    echo "  consensus_cdn_num_failed_messages    : $cdn  (delta: $cdn_delta)"
    echo ""

    echo "${ts},${libp2p},${cdn},${libp2p_delta_csv},${cdn_delta_csv}" >> "$LOG_FILE"

    prev_libp2p="$libp2p"
    prev_cdn="$cdn"

    sleep "$INTERVAL"
done
