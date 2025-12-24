#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs
ts="$(date +%Y%m%d_%H%M%S)"
out="logs/raw-field-probe_${ts}.log"

symbol="${1:-BTCUSDT}"
shift || true

echo "writing to: $out" >&2
echo "running: cargo run --bin raw_field_probe --release -- \"$symbol\" $*" >&2

# Capture both stderr (status/warnings) and stdout (samples) into one file.
cargo run --bin raw_field_probe --release -- "$symbol" "$@" 2>&1 | tee "$out"

