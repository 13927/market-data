#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/symbol-availability-report.sh [--rebuild] [config_path]
#
# Reads symbols from config.toml and prints, for each base symbol, which exchanges
# have a supported spot + futures contract (USDT preferred, fallback to USDC).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REBUILD=0
if [[ "${1:-}" == "--rebuild" ]]; then
  REBUILD=1
  shift
fi

CFG="${1:-config.toml}"

BIN="./target/release/symbol_availability_report"

SRC="src/bin/symbol_availability_report.rs"
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
  cargo build --release --bin symbol_availability_report
fi

exec "$BIN" "$CFG"
