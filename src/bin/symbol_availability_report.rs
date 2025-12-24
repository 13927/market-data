use anyhow::Context;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Debug, Clone)]
struct CanonicalSymbol {
    base: String,
    quote: String,
}

fn base_variants_for_alias(base: &str) -> Vec<String> {
    let b = base.to_ascii_uppercase();
    let mut out = Vec::new();
    out.push(b.clone());

    // Common multiplier prefixes used by some venues, e.g. 1000SATS, 1MBABYDOGE.
    if let Some(rest) = b.strip_prefix("1000") {
        if !rest.is_empty() {
            out.push(rest.to_string());
        }
    }
    if let Some(rest) = b.strip_prefix("1M") {
        if !rest.is_empty() {
            out.push(rest.to_string());
        }
    }
    if let Some(rest) = b.strip_prefix("10") {
        if !rest.is_empty() {
            out.push(rest.to_string());
        }
    }
    if let Some(rest) = b.strip_prefix("100") {
        if !rest.is_empty() {
            out.push(rest.to_string());
        }
    }

    // Generic: strip leading digits.
    if let Some(pos) = b.find(|c: char| !c.is_ascii_digit()) {
        if pos > 0 {
            let rest = &b[pos..];
            if !rest.is_empty() {
                out.push(rest.to_string());
            }
        }
    }

    out.sort();
    out.dedup();
    out
}

fn parse_canonical_symbol(s: &str) -> Option<CanonicalSymbol> {
    let u = s.trim().to_ascii_uppercase();
    if u.is_empty() {
        return None;
    }
    // Prefer stablecoin quotes first.
    for q in ["USDT", "USDC", "USD", "BUSD", "FDUSD", "DAI"] {
        if let Some(base) = u.strip_suffix(q) {
            if !base.is_empty() {
                return Some(CanonicalSymbol {
                    base: base.to_string(),
                    quote: q.to_string(),
                });
            }
        }
    }
    None
}

fn preferred_quotes<'a>(quote: &'a str) -> Vec<&'a str> {
    match quote.to_ascii_uppercase().as_str() {
        "USDT" => vec!["USDT", "USDC"],
        "USDC" => vec!["USDC", "USDT"],
        _ => vec![quote],
    }
}

fn pick_binance_symbol(active: &HashSet<String>, base: &str, quote: &str) -> Option<String> {
    for q in preferred_quotes(quote) {
        let cand = format!("{base}{q}");
        if active.contains(&cand) {
            return Some(cand);
        }
    }
    None
}

fn pick_gate_spot_pair(active: &HashSet<String>, base: &str, quote: &str) -> Option<String> {
    for q in preferred_quotes(quote) {
        let cand = format!("{base}_{q}");
        if active.contains(&cand) {
            return Some(cand);
        }
    }
    None
}

fn suggest_gate_spot_pairs(active: &HashSet<String>, base: &str, quote: &str) -> Vec<String> {
    let mut out = BTreeSet::new();
    let variants = base_variants_for_alias(base);
    for q in preferred_quotes(quote) {
        for v in &variants {
            let cand = format!("{v}_{q}");
            if active.contains(&cand) {
                out.insert(cand);
            }
        }
    }
    out.into_iter().take(10).collect()
}

fn kucoin_spot_symbol(base: &str, quote: &str) -> String {
    format!("{base}-{quote}")
}

fn pick_kucoin_spot_symbol(active: &HashSet<String>, base: &str, quote: &str) -> Option<String> {
    for q in preferred_quotes(quote) {
        let cand = kucoin_spot_symbol(base, q);
        if active.contains(&cand) {
            return Some(cand);
        }
    }
    None
}

fn suggest_kucoin_spot_symbols(active: &HashSet<String>, base: &str, quote: &str) -> Vec<String> {
    let mut out = BTreeSet::new();
    let variants = base_variants_for_alias(base);
    for q in preferred_quotes(quote) {
        for v in &variants {
            let cand = kucoin_spot_symbol(v, q);
            if active.contains(&cand) {
                out.insert(cand);
            }
        }
    }
    out.into_iter().take(10).collect()
}

fn kucoin_contract_candidates(base: &str) -> Vec<String> {
    // KuCoin futures uses XBT for BTC.
    let base = if base.eq_ignore_ascii_case("BTC") {
        "XBT"
    } else {
        base
    };
    vec![format!("{base}USDTM"), format!("{base}USDCM")]
}

fn pick_kucoin_futures_contract(active: &HashSet<String>, base: &str) -> Option<String> {
    for cand in kucoin_contract_candidates(base) {
        if active.contains(&cand) {
            return Some(cand);
        }
    }
    None
}

fn suggest_kucoin_futures_contracts(active: &HashSet<String>, base: &str) -> Vec<String> {
    let mut out = BTreeSet::new();
    for v in base_variants_for_alias(base) {
        if let Some(c) = pick_kucoin_futures_contract(active, &v) {
            out.insert(c);
        }
    }
    out.into_iter().take(10).collect()
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

async fn fetch_binance_active_symbols(url: &str) -> anyhow::Result<HashSet<String>> {
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

async fn fetch_kucoin_spot_symbols(rest_endpoint: &str) -> anyhow::Result<HashSet<String>> {
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

async fn fetch_kucoin_futures_contracts(rest_endpoint: &str) -> anyhow::Result<HashSet<String>> {
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

async fn fetch_gate_spot_pairs() -> anyhow::Result<HashSet<String>> {
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

async fn fetch_gate_futures_contracts(settle: &str) -> anyhow::Result<HashSet<String>> {
    let settle = settle.trim().to_ascii_lowercase();
    let url = format!("https://api.gateio.ws/api/v4/futures/{settle}/contracts");
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .send()
        .await
        .context("gate futures request")?;
    if resp.status().as_u16() == 400 && settle == "usdc" {
        // Gate may not support USDC-margined futures on this endpoint. Treat it as "no contracts".
        eprintln!("WARN: gate futures usdc contracts endpoint returned 400; treating as empty");
        return Ok(HashSet::new());
    }
    let resp = resp.error_for_status().context("http status")?;
    let v: serde_json::Value = resp.json().await.context("gate futures contracts json")?;
    let Some(arr) = v.as_array() else {
        anyhow::bail!("unexpected gate futures response (not array)");
    };
    let mut out = HashSet::new();
    for item in arr {
        let name = item
            .get("name")
            .and_then(|x| x.as_str())
            .or_else(|| item.get("contract").and_then(|x| x.as_str()))
            .or_else(|| item.get("id").and_then(|x| x.as_str()))
            .map(|s| s.to_ascii_uppercase());
        if let Some(name) = name {
            out.insert(name);
        }
    }
    Ok(out)
}

fn pick_gate_futures_contract(
    active_usdt: &HashSet<String>,
    active_usdc: &HashSet<String>,
    base: &str,
    quote: &str,
) -> Option<(String, &'static str)> {
    for q in preferred_quotes(quote) {
        let cand = format!("{base}_{q}");
        if q.eq_ignore_ascii_case("USDT") && active_usdt.contains(&cand) {
            return Some((cand, "usdt"));
        }
        if q.eq_ignore_ascii_case("USDC") && active_usdc.contains(&cand) {
            return Some((cand, "usdc"));
        }
    }
    None
}

fn suggest_gate_futures_contracts(
    active_usdt: &HashSet<String>,
    active_usdc: &HashSet<String>,
    base: &str,
    quote: &str,
) -> Vec<(String, &'static str)> {
    let mut out: BTreeSet<(String, &'static str)> = BTreeSet::new();
    for v in base_variants_for_alias(base) {
        if let Some(sel) = pick_gate_futures_contract(active_usdt, active_usdc, &v, quote) {
            out.insert(sel);
        }
    }
    out.into_iter().take(10).collect()
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

fn toml_get_string_array(root: &toml::Value, path: &[&str]) -> Vec<String> {
    let mut cur = root;
    for p in path {
        let Some(next) = cur.get(*p) else {
            return vec![];
        };
        cur = next;
    }
    let Some(arr) = cur.as_array() else {
        return vec![];
    };
    arr.iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect()
}

fn pick_symbol_source(cfg: &toml::Value) -> Vec<String> {
    let gate = toml_get_string_array(cfg, &["gate", "symbols"]);
    if !gate.is_empty() {
        return gate;
    }
    let kucoin = toml_get_string_array(cfg, &["kucoin", "symbols"]);
    if !kucoin.is_empty() {
        return kucoin;
    }
    let bin_fut = toml_get_string_array(cfg, &["binance_futures_json", "symbols"]);
    if !bin_fut.is_empty() {
        return bin_fut;
    }
    let bin_sbe = toml_get_string_array(cfg, &["binance_spot_sbe", "symbols"]);
    if !bin_sbe.is_empty() {
        return bin_sbe;
    }
    vec![]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let cfg = read_toml(&config_path)?;
    let symbols = pick_symbol_source(&cfg);
    if symbols.is_empty() {
        anyhow::bail!("no symbols found in config (expected under [gate]/[kucoin]/[binance_*])");
    }

    let kucoin_rest = toml_get_string(&cfg, &["kucoin", "rest_endpoint"])
        .unwrap_or_else(|| "https://api.kucoin.com".to_string());
    let kucoin_futures_rest = toml_get_string(&cfg, &["kucoin", "futures_rest_endpoint"])
        .unwrap_or_else(|| "https://api-futures.kucoin.com".to_string());

    let mut canonical: Vec<(String, CanonicalSymbol)> = Vec::new();
    let mut parse_fail = Vec::new();
    for s in symbols {
        match parse_canonical_symbol(&s) {
            Some(cs) => canonical.push((s, cs)),
            None => parse_fail.push(s),
        }
    }
    if !parse_fail.is_empty() {
        eprintln!(
            "WARN: cannot parse {} symbols (expected ...USDT/...USDC): {:?}",
            parse_fail.len(),
            parse_fail
        );
    }

    // Fetch all exchange listings in parallel.
    let (bin_spot, bin_fut, k_spot, k_fut, g_spot, g_usdt, g_usdc) = tokio::join!(
        fetch_binance_active_symbols("https://api.binance.com/api/v3/exchangeInfo"),
        fetch_binance_active_symbols("https://fapi.binance.com/fapi/v1/exchangeInfo"),
        fetch_kucoin_spot_symbols(&kucoin_rest),
        fetch_kucoin_futures_contracts(&kucoin_futures_rest),
        fetch_gate_spot_pairs(),
        fetch_gate_futures_contracts("usdt"),
        fetch_gate_futures_contracts("usdc"),
    );

    let bin_spot = bin_spot.context("binance spot exchangeInfo")?;
    let bin_fut = bin_fut.context("binance futures exchangeInfo")?;
    let k_spot = k_spot.context("kucoin spot symbols")?;
    let k_fut = k_fut.context("kucoin futures contracts")?;
    let g_spot = g_spot.context("gate spot pairs")?;
    let g_usdt = g_usdt.context("gate futures usdt contracts")?;
    let g_usdc = g_usdc.context("gate futures usdc contracts")?;

    #[derive(Default)]
    struct Tot {
        ok_spot: usize,
        ok_fut: usize,
        missing_spot: Vec<String>,
        missing_fut: Vec<String>,
        usdc_fallback_spot: usize,
        usdc_fallback_fut: usize,
    }

    let mut totals: BTreeMap<&'static str, Tot> = BTreeMap::new();
    totals.insert("binance", Tot::default());
    totals.insert("kucoin", Tot::default());
    totals.insert("gate", Tot::default());

    #[derive(Default, Clone)]
    struct PerBaseMissing {
        binance_spot: bool,
        binance_fut: bool,
        kucoin_spot: bool,
        kucoin_fut: bool,
        gate_spot: bool,
        gate_fut: bool,
    }
    let mut per_base_missing: BTreeMap<String, PerBaseMissing> = BTreeMap::new();

    // Per-symbol mapping output (kept stable, sorted by original token list order).
    println!("config: {config_path}");
    println!("symbols: {}", canonical.len());
    println!();
    println!(
        "{:12} | {:14} | {:14} | {:16} | {:16} | {:16} | {:20}",
        "BASE", "bin_spot", "bin_fut", "kucoin_spot", "kucoin_fut", "gate_spot", "gate_fut(settle)"
    );
    println!(
        "{}",
        "-".repeat(12 + 3 + 14 + 3 + 14 + 3 + 16 + 3 + 16 + 3 + 16 + 3 + 20)
    );

    for (_raw, cs) in &canonical {
        let base = cs.base.to_string();
        let quote = cs.quote.as_str();

        let b_spot = pick_binance_symbol(&bin_spot, &cs.base, quote);
        let b_fut = pick_binance_symbol(&bin_fut, &cs.base, quote);

        let k_spot_sel = pick_kucoin_spot_symbol(&k_spot, &cs.base, quote);
        let k_fut_sel = pick_kucoin_futures_contract(&k_fut, &cs.base);

        let g_spot_sel = pick_gate_spot_pair(&g_spot, &cs.base, quote);
        let g_fut_sel = pick_gate_futures_contract(&g_usdt, &g_usdc, &cs.base, quote);

        let entry = per_base_missing.entry(cs.base.to_string()).or_default();

        // Totals update
        {
            let t = totals.get_mut("binance").unwrap();
            if let Some(sym) = &b_spot {
                t.ok_spot += 1;
                if sym.ends_with("USDC") && quote != "USDC" {
                    t.usdc_fallback_spot += 1;
                }
            } else {
                t.missing_spot.push(format!("{base}{quote}"));
                entry.binance_spot = true;
            }
            if let Some(sym) = &b_fut {
                t.ok_fut += 1;
                if sym.ends_with("USDC") && quote != "USDC" {
                    t.usdc_fallback_fut += 1;
                }
            } else {
                t.missing_fut.push(format!("{base}{quote}"));
                entry.binance_fut = true;
            }
        }
        {
            let t = totals.get_mut("kucoin").unwrap();
            if let Some(sym) = &k_spot_sel {
                t.ok_spot += 1;
                if sym.ends_with("-USDC") && quote != "USDC" {
                    t.usdc_fallback_spot += 1;
                }
            } else {
                t.missing_spot.push(format!("{base}-{quote}"));
                entry.kucoin_spot = true;
            }
            if let Some(sym) = &k_fut_sel {
                t.ok_fut += 1;
                if sym.ends_with("USDCM") {
                    // We always prefer USDTM then USDCM; if we end up on USDCM it's a fallback.
                    t.usdc_fallback_fut += 1;
                }
            } else {
                t.missing_fut.push(format!("{base}??M"));
                entry.kucoin_fut = true;
            }
        }
        {
            let t = totals.get_mut("gate").unwrap();
            if let Some(sym) = &g_spot_sel {
                t.ok_spot += 1;
                if sym.ends_with("_USDC") && quote != "USDC" {
                    t.usdc_fallback_spot += 1;
                }
            } else {
                t.missing_spot.push(format!("{base}_{quote}"));
                entry.gate_spot = true;
            }
            if let Some((_sym, settle)) = &g_fut_sel {
                t.ok_fut += 1;
                if *settle == "usdc" {
                    t.usdc_fallback_fut += 1;
                }
            } else {
                t.missing_fut.push(format!("{base}_{quote}"));
                entry.gate_fut = true;
            }
        }

        let gate_fut_str = match &g_fut_sel {
            Some((sym, settle)) => format!("{sym}({settle})"),
            None => "-".to_string(),
        };

        println!(
            "{:12} | {:14} | {:14} | {:16} | {:16} | {:16} | {:20}",
            cs.base,
            b_spot.as_deref().unwrap_or("-"),
            b_fut.as_deref().unwrap_or("-"),
            k_spot_sel.as_deref().unwrap_or("-"),
            k_fut_sel.as_deref().unwrap_or("-"),
            g_spot_sel.as_deref().unwrap_or("-"),
            gate_fut_str
        );
    }

    println!();
    println!("==== SUMMARY ====");
    println!("total symbols: {}", canonical.len());
    for (ex, t) in &totals {
        println!(
            "{ex}: spot_ok={} fut_ok={} spot_missing={} fut_missing={} usdc_fallback_spot={} usdc_fallback_fut={}",
            t.ok_spot,
            t.ok_fut,
            t.missing_spot.len(),
            t.missing_fut.len(),
            t.usdc_fallback_spot,
            t.usdc_fallback_fut
        );
    }
    println!();

    for (ex, t) in &totals {
        if !t.missing_spot.is_empty() || !t.missing_fut.is_empty() {
            println!("---- {ex} MISSING ----");
            if !t.missing_spot.is_empty() {
                let uniq: BTreeSet<String> = t.missing_spot.iter().cloned().collect();
                println!("spot_missing({}):", uniq.len());
                for s in uniq.iter().take(200) {
                    println!("  {s}");
                }
            }
            if !t.missing_fut.is_empty() {
                let uniq: BTreeSet<String> = t.missing_fut.iter().cloned().collect();
                println!("fut_missing({}):", uniq.len());
                for s in uniq.iter().take(200) {
                    println!("  {s}");
                }
            }
            println!();
        }
    }

    let mut need_alias: Vec<String> = per_base_missing
        .iter()
        .filter_map(|(base, m)| {
            if m.kucoin_spot
                || m.kucoin_fut
                || m.gate_spot
                || m.gate_fut
                || m.binance_spot
                || m.binance_fut
            {
                Some(base.clone())
            } else {
                None
            }
        })
        .collect();
    need_alias.sort();
    need_alias.dedup();

    println!("---- ALIAS CANDIDATES (heuristic) ----");
    for base in need_alias {
        let m = per_base_missing.get(&base).cloned().unwrap_or_default();

        let mut any = false;
        if m.gate_spot {
            let sug = suggest_gate_spot_pairs(&g_spot, &base, "USDT");
            if !sug.is_empty() {
                println!("gate_spot  {base}: {}", sug.join(", "));
                any = true;
            }
        }
        if m.gate_fut {
            let sug = suggest_gate_futures_contracts(&g_usdt, &g_usdc, &base, "USDT");
            if !sug.is_empty() {
                let rendered: Vec<String> = sug
                    .into_iter()
                    .map(|(s, settle)| format!("{s}({settle})"))
                    .collect();
                println!("gate_fut   {base}: {}", rendered.join(", "));
                any = true;
            }
        }
        if m.kucoin_spot {
            let sug = suggest_kucoin_spot_symbols(&k_spot, &base, "USDT");
            if !sug.is_empty() {
                println!("kucoin_spot {base}: {}", sug.join(", "));
                any = true;
            }
        }
        if m.kucoin_fut {
            let sug = suggest_kucoin_futures_contracts(&k_fut, &base);
            if !sug.is_empty() {
                println!("kucoin_fut  {base}: {}", sug.join(", "));
                any = true;
            }
        }
        if !any {
            // Only print bases that are missing somewhere, but have no obvious alias candidates.
            println!("(no_alias) {base}");
        }
    }

    Ok(())
}
