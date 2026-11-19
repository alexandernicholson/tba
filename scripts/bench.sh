#!/usr/bin/env bash
# Benchmarks the release binary with hyperfine and writes docs/benchmarks.md.
# Usage: scripts/bench.sh [extra hyperfine args]
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release --locked
mkdir -p bench
for count in 100 1000 10000; do
  [[ -s bench/tba-$count.yaml ]] || scripts/gen-fixtures.sh "$count" yaml > "bench/tba-$count.yaml"
  [[ -s bench/tba-$count.json ]] || scripts/gen-fixtures.sh "$count" json > "bench/tba-$count.json"
done

bin=./target/release/tba
at=2026-01-05T12:00:00Z
hyperfine --shell=none --warmup 5 --min-runs 20 "$@" \
  --export-markdown docs/benchmarks.md \
  --export-json target/benchmarks.json \
  -n 'startup (crd)' "$bin crd" \
  -n 'evaluate 100 (yaml)' "$bin evaluate -f bench/tba-100.yaml --at $at" \
  -n 'evaluate 1000 (yaml)' "$bin evaluate -f bench/tba-1000.yaml --at $at" \
  -n 'evaluate 10000 (yaml)' "$bin evaluate -f bench/tba-10000.yaml --at $at" \
  -n 'evaluate 1000 (json)' "$bin evaluate -f bench/tba-1000.json --at $at" \
  -n 'evaluate 10000 (json)' "$bin evaluate -f bench/tba-10000.json --at $at" \
  -n 'evaluate 10000 (json, Europe/Berlin)' "$bin evaluate -f bench/tba-10000.json --at $at --timezone Europe/Berlin"
