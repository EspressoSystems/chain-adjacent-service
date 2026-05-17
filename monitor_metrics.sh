#!/usr/bin/env bash
set -euo pipefail

CDN_URL="https://espresso-mainnet.alt.technology/v1/status/metrics"
LIBP2P_URL="https://query-0.main.net.espresso.network/v1/status/metrics"
CSV="metrics_history.csv"
INTERVAL=60
COMMIT_EVERY=5  # commit after every N samples

fetch_metric() {
    local url="$1"
    local metric="$2"
    curl -sf --max-time 15 "$url" 2>/dev/null \
        | grep -E "^${metric} " \
        | awk '{print $2}' \
        | head -1
}

prev_cdn=""
prev_libp2p=""

# Seed prev values from last CSV row so deltas are correct on resume
if [[ -f "$CSV" ]]; then
    last=$(tail -1 "$CSV")
    prev_cdn=$(echo "$last" | cut -d',' -f2)
    prev_libp2p=$(echo "$last" | cut -d',' -f4)
fi

sample_count=0

while true; do
    cdn=$(fetch_metric "$CDN_URL" "consensus_cdn_num_failed_messages")
    libp2p=$(fetch_metric "$LIBP2P_URL" "consensus_finalized_bytes_count")

    if [[ -z "$cdn" || -z "$libp2p" ]]; then
        echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') WARN: failed to fetch metrics (cdn='$cdn' libp2p='$libp2p')"
        sleep "$INTERVAL"
        continue
    fi

    cdn_delta=0
    libp2p_delta=0
    if [[ -n "$prev_cdn" ]]; then
        cdn_delta=$(( cdn - prev_cdn ))
    fi
    if [[ -n "$prev_libp2p" ]]; then
        libp2p_delta=$(( libp2p - prev_libp2p ))
    fi

    ts=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
    echo "${ts},${cdn},${cdn_delta},${libp2p},${libp2p_delta}" >> "$CSV"
    echo "${ts}  cdn=${cdn} (+${cdn_delta})  libp2p=${libp2p} (+${libp2p_delta})"

    prev_cdn="$cdn"
    prev_libp2p="$libp2p"
    sample_count=$(( sample_count + 1 ))

    if (( sample_count % COMMIT_EVERY == 0 )); then
        git add "$CSV"
        git commit -m "Update metrics history log" \
            --author="Claude <noreply@anthropic.com>" 2>/dev/null || true
        git push -u origin claude/resume-metrics-monitoring-u0h1U 2>/dev/null || true
        echo "--- committed and pushed (sample $sample_count) ---"
    fi

    sleep "$INTERVAL"
done
