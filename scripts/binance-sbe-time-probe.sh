#!/usr/bin/env bash
set -euo pipefail

# Probe Binance SBE market-data timestamps for a symbol.
#
# Usage:
#   bash scripts/binance-sbe-time-probe.sh BTCUSDT
#
# It prints one sample each for:
#   - BestBidAskStreamEvent (<symbol>@bestBidAsk): eventTime only
#   - DepthSnapshotStreamEvent (<symbol>@depth20): eventTime only
#   - TradesStreamEvent (<symbol>@trade): eventTime + transactTime
#
# Notes:
# - Requires `[binance_spot_sbe].api_key` in ./config.toml (Ed25519 API key).
# - Does not print the API key.

symbol="${1:-BTCUSDT}"

exec cargo run --bin binance_sbe_time_probe --release -- "${symbol}"

