use anyhow::Context;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::env;

fn usage() -> anyhow::Result<()> {
    anyhow::bail!(
        "usage: keyword_candidate_report <config_path> <symbol> [--keyword KW] [--max N]\n\
         example: keyword_candidate_report config.toml 1000SATSUSDT\n\
         example: keyword_candidate_report config.toml 1000SATSUSDT --keyword SATS --max 200"
    )
}

fn parse_args() -> anyhow::Result<(String, String, Option<String>, usize)> {
    let mut it = env::args().skip(1);
    let cfg = it.next().ok_or_else(|| usage().unwrap_err())?;
    let symbol = it.next().ok_or_else(|| usage().unwrap_err())?;
    let mut keyword: Option<String> = None;
    let mut max: usize = 200;
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--keyword" => {
                keyword = Some(
                    it.next()
                        .ok_or_else(|| anyhow::anyhow!("--keyword needs value"))?,
                )
            }
            "--max" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--max needs value"))?;
                max = v.parse::<usize>().context("parse --max")?;
            }
            _ => return Err(anyhow::anyhow!("unknown arg: {arg}")),
        }
    }
    Ok((cfg, symbol, keyword, max))
}

fn read_toml(path: &str) -> anyhow::Result<toml::Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    toml::from_str(&raw).context("parse toml")
}

fn toml_get_string(root: &toml::Value, path: &[&str]) -> Option<String> {
    let mut cur = root;
    for p in path {
        cur = cur.get(*p)?;
    }
    cur.as_str().map(|s| s.to_string())
}

fn extract_keyword_from_symbol(symbol: &str) -> Option<String> {
    let s = symbol.trim().to_ascii_uppercase();
    let s = s.strip_suffix("USDT").or_else(|| s.strip_suffix("USDC"))?;
    if s.is_empty() {
        return None;
    }
    // Prefer stripping common multiplier prefixes.
    let mut base = s;
    if let Some(rest) = base.strip_prefix("1000") {
        base = rest;
    }
    if let Some(rest) = base.strip_prefix("1M") {
        base = rest;
    }
    // Generic: strip leading digits.
    if let Some(pos) = base.find(|c: char| !c.is_ascii_digit()) {
        base = &base[pos..];
    }
    if base.is_empty() {
        None
    } else {
        Some(base.to_string())
    }
}

fn contains_kw(hay: &str, kw: &str) -> bool {
    hay.to_ascii_uppercase().contains(&kw.to_ascii_uppercase())
}

async fn fetch_json<T: for<'de> Deserialize<'de>>(url: &str) -> anyhow::Result<T> {
    let client = reqwest::Client::new();
    let resp = client.get(url).send().await.context("request")?;
    let resp = resp.error_for_status().context("http status")?;
    resp.json::<T>().await.context("json")
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbolInfo {
    symbol: String,
    status: String,
}

async fn fetch_binance_symbols(url: &str) -> anyhow::Result<Vec<String>> {
    let info: BinanceExchangeInfo = fetch_json(url).await?;
    Ok(info
        .symbols
        .into_iter()
        .filter(|s| s.status.eq_ignore_ascii_case("TRADING"))
        .map(|s| s.symbol.to_ascii_uppercase())
        .collect())
}

#[derive(Debug, Deserialize)]
struct KucoinResp<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KucoinSpotSymbol {
    symbol: String,
    enable_trading: bool,
}

async fn fetch_kucoin_spot_symbols(rest_endpoint: &str) -> anyhow::Result<Vec<String>> {
    let url = format!("{rest_endpoint}/api/v1/symbols");
    let resp: KucoinResp<Vec<KucoinSpotSymbol>> = fetch_json(&url).await?;
    Ok(resp
        .data
        .into_iter()
        .filter(|s| s.enable_trading)
        .map(|s| s.symbol.to_ascii_uppercase())
        .collect())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KucoinFuturesContract {
    symbol: String,
    #[serde(default)]
    status: Option<String>,
}

async fn fetch_kucoin_futures_contracts(rest_endpoint: &str) -> anyhow::Result<Vec<String>> {
    let url = format!("{rest_endpoint}/api/v1/contracts/active");
    let resp: KucoinResp<Vec<KucoinFuturesContract>> = fetch_json(&url).await?;
    Ok(resp
        .data
        .into_iter()
        .filter(|c| {
            c.status
                .as_deref()
                .unwrap_or("Open")
                .eq_ignore_ascii_case("Open")
        })
        .map(|c| c.symbol.to_ascii_uppercase())
        .collect())
}

#[derive(Debug, Deserialize)]
struct GateSpotPair {
    id: String,
    #[serde(default)]
    trade_status: Option<String>,
}

async fn fetch_gate_spot_pairs() -> anyhow::Result<Vec<String>> {
    let url = "https://api.gateio.ws/api/v4/spot/currency_pairs";
    let pairs: Vec<GateSpotPair> = fetch_json(url).await?;
    Ok(pairs
        .into_iter()
        .filter(|p| {
            p.trade_status
                .as_deref()
                .unwrap_or("tradable")
                .eq_ignore_ascii_case("tradable")
        })
        .map(|p| p.id.to_ascii_uppercase())
        .collect())
}

async fn fetch_gate_futures_contracts(settle: &str) -> anyhow::Result<Vec<String>> {
    let settle = settle.trim().to_ascii_lowercase();
    let url = format!("https://api.gateio.ws/api/v4/futures/{settle}/contracts");
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .send()
        .await
        .context("gate futures request")?;
    if resp.status().as_u16() == 400 && settle == "usdc" {
        eprintln!("WARN: gate futures usdc contracts endpoint returned 400; treating as empty");
        return Ok(vec![]);
    }
    let resp = resp.error_for_status().context("http status")?;
    let v: serde_json::Value = resp.json().await.context("gate futures contracts json")?;
    let Some(arr) = v.as_array() else {
        anyhow::bail!("unexpected gate futures response (not array)");
    };
    let mut out = Vec::new();
    for item in arr {
        let name = item
            .get("name")
            .and_then(|x| x.as_str())
            .or_else(|| item.get("contract").and_then(|x| x.as_str()))
            .or_else(|| item.get("id").and_then(|x| x.as_str()))
            .map(|s| s.to_ascii_uppercase());
        if let Some(name) = name {
            out.push(name);
        }
    }
    Ok(out)
}

fn print_matches(title: &str, all: Vec<String>, keyword: &str, max: usize) {
    let set: BTreeSet<String> = all
        .into_iter()
        .filter(|s| contains_kw(s, keyword))
        .collect();
    let total = set.len();
    println!("== {title} matches={total} ==");
    for s in set.iter().take(max) {
        println!("{s}");
    }
    if total > max {
        println!("... ({} more)", total - max);
    }
    println!();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cfg_path, symbol, keyword_override, max) = parse_args()?;
    let cfg = read_toml(&cfg_path)?;

    let kucoin_rest = toml_get_string(&cfg, &["kucoin", "rest_endpoint"])
        .unwrap_or_else(|| "https://api.kucoin.com".to_string());
    let kucoin_futures_rest = toml_get_string(&cfg, &["kucoin", "futures_rest_endpoint"])
        .unwrap_or_else(|| "https://api-futures.kucoin.com".to_string());

    let keyword = match keyword_override {
        Some(k) => k.to_ascii_uppercase(),
        None => extract_keyword_from_symbol(&symbol)
            .ok_or_else(|| anyhow::anyhow!("cannot derive keyword from symbol={symbol}"))?,
    };

    println!("config: {cfg_path}");
    println!("symbol: {symbol}");
    println!("keyword: {keyword}");
    println!("max: {max}");
    println!();

    let (bin_spot, bin_fut, k_spot, k_fut, g_spot, g_usdt, g_usdc) = tokio::join!(
        fetch_binance_symbols("https://api.binance.com/api/v3/exchangeInfo"),
        fetch_binance_symbols("https://fapi.binance.com/fapi/v1/exchangeInfo"),
        fetch_kucoin_spot_symbols(&kucoin_rest),
        fetch_kucoin_futures_contracts(&kucoin_futures_rest),
        fetch_gate_spot_pairs(),
        fetch_gate_futures_contracts("usdt"),
        fetch_gate_futures_contracts("usdc"),
    );

    print_matches(
        "binance spot (TRADING)",
        bin_spot.context("binance spot exchangeInfo")?,
        &keyword,
        max,
    );
    print_matches(
        "binance futures (TRADING)",
        bin_fut.context("binance futures exchangeInfo")?,
        &keyword,
        max,
    );
    print_matches(
        "kucoin spot (enableTrading)",
        k_spot.context("kucoin spot symbols")?,
        &keyword,
        max,
    );
    print_matches(
        "kucoin futures (Open)",
        k_fut.context("kucoin futures contracts")?,
        &keyword,
        max,
    );
    print_matches(
        "gate spot (tradable)",
        g_spot.context("gate spot pairs")?,
        &keyword,
        max,
    );

    let mut gate_fut = g_usdt.context("gate futures usdt contracts")?;
    gate_fut.extend(g_usdc.context("gate futures usdc contracts")?);
    print_matches("gate futures (usdt+usdc)", gate_fut, &keyword, max);

    Ok(())
}
