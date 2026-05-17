#!/usr/bin/env python3
"""
Poll espresso-mainnet metrics every minute.
Appends all readings to metrics_history.csv and prints per-poll deltas.
"""

import csv
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import urllib.request

URL = "https://espresso-mainnet.alt.technology/v1/status/metrics"
METRICS = [
    "consensus_libp2p_num_failed_messages",
    "consensus_cdn_num_failed_messages",
]
CSV_PATH = Path("metrics_history.csv")
POLL_INTERVAL = 60  # seconds


def fetch_metrics(url: str) -> dict[str, float]:
    with urllib.request.urlopen(url, timeout=15) as resp:
        body = resp.read().decode()
    values = {}
    for line in body.splitlines():
        if line.startswith("#"):
            continue
        for name in METRICS:
            if line.startswith(name + " ") or line.startswith(name + "{"):
                parts = line.split()
                try:
                    values[name] = float(parts[-1])
                except ValueError:
                    pass
    return values


def ensure_csv_header():
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp_utc"] + METRICS + [f"delta_{m}" for m in METRICS])


def append_row(ts: str, values: dict, deltas: dict):
    with CSV_PATH.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [ts]
            + [values.get(m, "") for m in METRICS]
            + [deltas.get(m, "") for m in METRICS]
        )


def main():
    ensure_csv_header()
    prev: dict[str, float] = {}

    def handle_sigint(sig, frame):
        print(f"\nStopped. History saved to {CSV_PATH.resolve()}")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    print(f"Polling {URL} every {POLL_INTERVAL}s — Ctrl+C to stop.")
    print(f"Saving history to {CSV_PATH.resolve()}\n")

    while True:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            values = fetch_metrics(URL)
        except Exception as e:
            print(f"[{ts}] ERROR fetching metrics: {e}")
            time.sleep(POLL_INTERVAL)
            continue

        if not values:
            print(f"[{ts}] WARNING: neither metric found in response")
            time.sleep(POLL_INTERVAL)
            continue

        deltas: dict[str, float | str] = {}
        lines = [f"[{ts}]"]
        for m in METRICS:
            v = values.get(m)
            if v is None:
                lines.append(f"  {m}: (not found)")
                deltas[m] = ""
                continue
            if m in prev:
                d = v - prev[m]
                deltas[m] = d
                lines.append(f"  {m}: +{d:g}  (total {v:g})")
            else:
                deltas[m] = ""
                lines.append(f"  {m}: {v:g}  (first reading)")

        print("\n".join(lines))
        append_row(ts, values, deltas)
        prev = {m: values[m] for m in METRICS if m in values}

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
