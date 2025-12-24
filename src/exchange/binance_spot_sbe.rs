use anyhow::Context;

pub fn required_headers(api_key: &str) -> anyhow::Result<Vec<(&'static str, String)>> {
    let api_key = api_key.trim().to_string();
    if api_key.is_empty() {
        anyhow::bail!("binance_spot_sbe.api_key is empty");
    }
    if api_key.contains("PUT_YOUR_") || api_key.contains("ED25519_API_KEY_HERE") {
        anyhow::bail!(
            "binance_spot_sbe.api_key looks like a placeholder; set your real Ed25519 API key"
        );
    }
    Ok(vec![("X-MBX-APIKEY", api_key)])
}

pub fn required_headers_from_config(
    api_key: Option<&str>,
) -> anyhow::Result<Vec<(&'static str, String)>> {
    let api_key = api_key.context("binance_spot_sbe.api_key is required when enabled = true")?;
    required_headers(api_key)
}

pub fn streams_from_symbols(
    symbols: &[String],
    best_bid_ask: bool,
    depth20: bool,
    trade: bool,
) -> Vec<String> {
    let mut streams = Vec::new();
    for symbol in symbols {
        let s = symbol.to_ascii_lowercase();
        if best_bid_ask {
            streams.push(format!("{s}@bestBidAsk"));
        }
        if depth20 {
            streams.push(format!("{s}@depth20"));
        }
        if trade {
            streams.push(format!("{s}@trade"));
        }
    }
    streams
}

pub fn effective_streams(cfg: &crate::config::BinanceSpotSbeConfig) -> Vec<String> {
    // Always promote a stable seed stream to position 0 so the initial `/ws/<stream>`
    // handshake is likely to succeed even when the configured list starts with an
    // illiquid/unsupported symbol.
    if !cfg.streams.is_empty() {
        return promote_seed_stream(cfg.streams.clone());
    }
    let streams = streams_from_symbols(&cfg.symbols, cfg.best_bid_ask, cfg.depth20, cfg.trade);
    promote_seed_stream(streams)
}

fn promote_seed_stream(mut streams: Vec<String>) -> Vec<String> {
    if streams.len() <= 1 {
        return streams;
    }
    // Prefer a liquid, widely supported symbol to avoid handshake failures if the first stream
    // in the list is not available on the SBE endpoint.
    let preferred = [
        "btcusdt@bestBidAsk",
        "ethusdt@bestBidAsk",
        "bnbusdt@bestBidAsk",
        "solusdt@bestBidAsk",
        "xrpusdt@bestBidAsk",
    ];
    for seed in preferred {
        if let Some(idx) = streams.iter().position(|s| s.eq_ignore_ascii_case(seed)) {
            streams.swap(0, idx);
            break;
        }
    }
    streams
}
