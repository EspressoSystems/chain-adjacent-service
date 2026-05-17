#!/usr/bin/env bash
set -euo pipefail

ALTTECH_URL="https://espresso-mainnet.alt.technology/v1/status/metrics"
QUERY_URL="https://q0-kx268rjgdiwhzue.main.net.espresso.network/v1/status/metrics"
CSV="metrics_history.csv"
INTERVAL=60
COMMIT_EVERY=1

fetch_metric() {
    local url="$1"
    local metric="$2"
    curl -sf --max-time 15 "$url" 2>/dev/null \
        | grep -E "^${metric} " \
        | awk '{print $2}' \
        | head -1
}

prev_alttech_cdn=""
prev_alttech_libp2p=""
prev_query_cdn=""
prev_query_libp2p=""

if [[ -f "$CSV" ]]; then
    header=$(head -1 "$CSV")
    if [[ "$header" == "timestamp,alttech_cdn_total,alttech_cdn_delta,alttech_libp2p_total,alttech_libp2p_delta,query_cdn_total,query_cdn_delta,query_libp2p_total,query_libp2p_delta" ]]; then
        last=$(tail -1 "$CSV")
        prev_alttech_cdn=$(echo "$last"   | cut -d',' -f2)
        prev_alttech_libp2p=$(echo "$last" | cut -d',' -f4)
        prev_query_cdn=$(echo "$last"     | cut -d',' -f6)
        prev_query_libp2p=$(echo "$last"  | cut -d',' -f8)
    fi
fi

if [[ ! -f "$CSV" ]] || [[ $(head -1 "$CSV") != "timestamp,alttech_cdn_total,alttech_cdn_delta,alttech_libp2p_total,alttech_libp2p_delta,query_cdn_total,query_cdn_delta,query_libp2p_total,query_libp2p_delta" ]]; then
    echo "timestamp,alttech_cdn_total,alttech_cdn_delta,alttech_libp2p_total,alttech_libp2p_delta,query_cdn_total,query_cdn_delta,query_libp2p_total,query_libp2p_delta" > "$CSV"
fi

sample_count=0

while true; do
    alttech_cdn=$(fetch_metric    "$ALTTECH_URL" "consensus_cdn_num_failed_messages")    || true
    alttech_libp2p=$(fetch_metric "$ALTTECH_URL" "consensus_libp2p_num_failed_messages") || true
    query_cdn=$(fetch_metric      "$QUERY_URL"   "consensus_cdn_num_failed_messages")    || true
    query_libp2p=$(fetch_metric   "$QUERY_URL"   "consensus_libp2p_num_failed_messages") || true

    if [[ -z "$alttech_cdn" || -z "$alttech_libp2p" || -z "$query_cdn" || -z "$query_libp2p" ]]; then
        echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') WARN: incomplete fetch (alttech_cdn='$alttech_cdn' alttech_libp2p='$alttech_libp2p' query_cdn='$query_cdn' query_libp2p='$query_libp2p')"
        sleep "$INTERVAL"
        continue
    fi

    delta() { [[ -n "$1" ]] && echo $(( $2 - $1 )) || echo 0; }
    alttech_cdn_delta=$(delta    "$prev_alttech_cdn"   "$alttech_cdn")
    alttech_libp2p_delta=$(delta "$prev_alttech_libp2p" "$alttech_libp2p")
    query_cdn_delta=$(delta      "$prev_query_cdn"     "$query_cdn")
    query_libp2p_delta=$(delta   "$prev_query_libp2p"  "$query_libp2p")

    ts=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
    echo "${ts},${alttech_cdn},${alttech_cdn_delta},${alttech_libp2p},${alttech_libp2p_delta},${query_cdn},${query_cdn_delta},${query_libp2p},${query_libp2p_delta}" >> "$CSV"
    echo "${ts}  alttech: cdn=${alttech_cdn}(+${alttech_cdn_delta}) libp2p=${alttech_libp2p}(+${alttech_libp2p_delta})  query: cdn=${query_cdn}(+${query_cdn_delta}) libp2p=${query_libp2p}(+${query_libp2p_delta})"

    prev_alttech_cdn="$alttech_cdn"
    prev_alttech_libp2p="$alttech_libp2p"
    prev_query_cdn="$query_cdn"
    prev_query_libp2p="$query_libp2p"
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
