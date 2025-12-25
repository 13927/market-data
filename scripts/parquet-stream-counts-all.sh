#!/usr/bin/env bash
set -euo pipefail

# Batch-run parquet_stream_counts over all Parquet files under a directory.
# - Builds the binary once (release) and reuses it
# - Outputs either human-readable per-file summaries (default) or a CSV
# Usage:
#   bash scripts/parquet-stream-counts-all.sh [DATA_DIR] [OUTPUT_CSV]
# Examples:
#   bash scripts/parquet-stream-counts-all.sh
#   bash scripts/parquet-stream-counts-all.sh data stats_parquet.csv
#
# Env:
#   BUILD (default: 1)       # set BUILD=0 to skip build
#   ROOT (default: repo root)
#

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${1:-${ROOT}/data}"
OUT_CSV="${2:-stream_counts_report.csv}"
BUILD="${BUILD:-1}"

BIN="${ROOT}/target/release/parquet_stream_counts"

if [[ "${BUILD}" == "1" ]]; then
  echo "building: cargo build --release --bin parquet_stream_counts" >&2
  (cd "${ROOT}" && cargo build --release --bin parquet_stream_counts)
fi

if [[ -n "${OUT_CSV}" ]]; then
  # Write CSV header
  echo "path,rows,spot_ticker,swap_ticker,spot_l5,swap_l5" > "${OUT_CSV}"
fi

# Iterate files in stable order, excluding _inprogress files which may be corrupt/incomplete
find "${DATA_DIR}" -type f -name "*.parquet" ! -name "*_inprogress.parquet" -print0 | sort -z | while IFS= read -r -d '' f; do
  echo "Processing: ${f}" >&2
  # Run the counter
  if ! out="$("${BIN}" "${f}" 2>&1)"; then
    echo "ERROR: failed: ${f}" >&2
    echo "${out}" >&2
    continue
  fi

  # Parse output
  # Expected lines:
  # file: /path/to/file.parquet
  # rows: 1234
  # spot_ticker: N
  # swap_ticker: N
  # spot_l5: N
  # swap_l5: N
  path="$(printf '%s\n' "${out}" | awk -F': ' '$1=="file"{print $2}' | tail -n 1)"
  rows="$(printf '%s\n' "${out}" | awk -F': ' '$1=="rows"{print $2}' | tail -n 1)"
  spot_ticker="$(printf '%s\n' "${out}" | awk -F': ' '$1=="spot_ticker"{print $2}' | tail -n 1)"
  swap_ticker="$(printf '%s\n' "${out}" | awk -F': ' '$1=="swap_ticker"{print $2}' | tail -n 1)"
  spot_l5="$(printf '%s\n' "${out}" | awk -F': ' '$1=="spot_l5"{print $2}' | tail -n 1)"
  swap_l5="$(printf '%s\n' "${out}" | awk -F': ' '$1=="swap_l5"{print $2}' | tail -n 1)"

  # Defaults to 0 if missing
  rows="${rows:-0}"
  spot_ticker="${spot_ticker:-0}"
  swap_ticker="${swap_ticker:-0}"
  spot_l5="${spot_l5:-0}"
  swap_l5="${swap_l5:-0}"

  if [[ -n "${OUT_CSV}" ]]; then
    printf '%s,%s,%s,%s,%s,%s\n' "${path}" "${rows}" "${spot_ticker}" "${swap_ticker}" "${spot_l5}" "${swap_l5}" >> "${OUT_CSV}"
  else
    printf '== %s ==\n' "${path}"
    printf 'rows: %s\n' "${rows}"
    printf 'spot_ticker: %s\n' "${spot_ticker}"
    printf 'swap_ticker: %s\n' "${swap_ticker}"
    printf 'spot_l5: %s\n' "${spot_l5}"
    printf 'swap_l5: %s\n' "${swap_l5}"
  fi
done

if [[ -n "${OUT_CSV}" ]]; then
  echo "written: ${OUT_CSV}" >&2
fi

