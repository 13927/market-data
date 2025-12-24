#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/keyword-candidate-report-all.sh [--rebuild] <config_path> [--max N]
#
# Examples:
#   bash scripts/keyword-candidate-report-all.sh config.toml
#   bash scripts/keyword-candidate-report-all.sh --rebuild config.toml --max 50

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REBUILD=0
if [[ "${1:-}" == "--rebuild" ]]; then
  REBUILD=1
  shift
fi

CFG="${1:-}"
if [[ -z "$CFG" ]]; then
  echo "missing args"
  echo "usage: bash scripts/keyword-candidate-report-all.sh [--rebuild] <config_path> [--max N]"
  exit 2
fi
shift 1

BIN="./target/release/keyword_candidate_report_all"
SRC="src/bin/keyword_candidate_report_all.rs"

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
  cargo build --release --bin keyword_candidate_report_all
fi

exec "$BIN" "$CFG" "$@"

