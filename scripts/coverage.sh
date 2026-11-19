#!/usr/bin/env bash
# Runs every test (unit, controller and binary end-to-end) under coverage
# instrumentation and fails unless line and function coverage are 100%.
# Extra arguments are passed to `cargo llvm-cov report` (e.g. --html).
set -euo pipefail
cd "$(dirname "$0")/.."

# Homebrew's rustc ships without llvm-tools; use the matching Homebrew LLVM.
if [[ -z "${LLVM_PROFDATA:-}" ]] && command -v brew >/dev/null && [[ "$(command -v rustc)" == "$(brew --prefix)"/* ]]; then
  export LLVM_COV="$(brew --prefix llvm)/bin/llvm-cov" LLVM_PROFDATA="$(brew --prefix llvm)/bin/llvm-profdata"
fi

# Stale objects from earlier builds corrupt the mapping; always start clean.
rm -rf target/llvm-cov-target
cargo llvm-cov --all-targets --no-report
cargo llvm-cov report --fail-under-lines 100 --fail-under-functions 100 --show-missing-lines "$@"
