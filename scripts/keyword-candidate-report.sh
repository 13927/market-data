#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/keyword-candidate-report.sh [--rebuild] <config_path> <symbol> [--keyword KW] [--max N]
#
# Examples:
#   bash scripts/keyword-candidate-report.sh config.toml 1000SATSUSDT
#   bash scripts/keyword-candidate-report.sh --rebuild config.toml 1000SATSUSDT --keyword SATS --max 200

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REBUILD=0
if [[ "${1:-}" == "--rebuild" ]]; then
  REBUILD=1
  shift
fi

CFG="${1:-}"
SYMBOL="${2:-}"
if [[ -z "$CFG" || -z "$SYMBOL" ]]; then
  echo "missing args"
  echo "usage: bash scripts/keyword-candidate-report.sh [--rebuild] <config_path> <symbol> [--keyword KW] [--max N]"
  exit 2
fi
shift 2

BIN="./target/release/keyword_candidate_report"
SRC="src/bin/keyword_candidate_report.rs"

NEED_BUILD=0
if [[ "$REBUILD" -eq 1 || ! -x "$BIN" ]]; then
  NEED_BUILD=1
elif [[ -f "$SRC" && "$SRC" -nt "$BIN" ]]; then
  NEED_BUILD=1
elif [[ -f "Cargo.toml" && "Cargo.toml" -nt "$BIN" ]]; then
  NEED_BUILD=1
elif [[ -f "Cargo.lock" && "Cargo.lock" -nt "$BIN" ]]; then
  NEED_BUILD=1
fi

if [[ "$NEED_BUILD" -eq 1 ]]; then
  echo "building $BIN ..."
  cargo build --release --bin keyword_candidate_report
fi

exec "$BIN" "$CFG" "$SYMBOL" "$@"

