#!/usr/bin/env bash
while true; do
    bash monitor_metrics.sh
    echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') monitor_metrics.sh exited (code $?), restarting in 5s..."
    sleep 5
done
