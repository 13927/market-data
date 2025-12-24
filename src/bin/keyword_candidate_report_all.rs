use anyhow::Context;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::env;

fn usage() -> anyhow::Result<()> {
    anyhow::bail!(
        "usage: keyword_candidate_report_all <config_path> [--max N]\n\
         example: keyword_candidate_report_all config.toml\n\
         example: keyword_candidate_report_all config.toml --max 50"
    )
}

fn parse_args() -> anyhow::Result<(String, usize)> {
    let mut it = env::args().skip(1);
    let cfg = it.next().ok_or_else(|| usage().unwrap_err())?;
    let mut max: usize = 20;
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--max" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--max needs value"))?;
                max = v.parse::<usize>().context("parse --max")?;
            }
            _ => return Err(anyhow::anyhow!("unknown arg: {arg}")),
        }
    }
    Ok((cfg, max))
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

fn ordered_union(lists: &[Vec<String>]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::<String>::new();
    for list in lists {
        for s in list {
            let u = s.trim().to_string();
            if u.is_empty() {
                continue;
            }
            let key = u.to_ascii_uppercase();
            if seen.insert(key) {
                out.push(u);
            }
        }
    }
    out
}

#[derive(Debug, Clone)]
struct CanonicalSymbol {
    base: String,
    quote: String,
}

fn parse_canonical_symbol(s: &str) -> Option<CanonicalSymbol> {
    let u = s.trim().to_ascii_uppercase();
    if u.is_empty() {
        return None;
    }
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

fn extract_keyword_from_symbol(symbol: &str) -> Option<String> {
    let s = symbol.trim().to_ascii_uppercase();
    let s = s.strip_suffix("USDT").or_else(|| s.strip_suffix("USDC"))?;
    if s.is_empty() {
        return None;
    }
    let mut base = s;
    if let Some(rest) = base.strip_prefix("1000") {
        base = rest;
    }
    if let Some(rest) = base.strip_prefix("1M") {
        base = rest;
    }
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

fn preferred_quotes<'a>(quote: &'a str) -> Vec<&'a str> {
    match quote.to_ascii_uppercase().as_str() {
        "USDT" => vec!["USDT", "USDC"],
        "USDC" => vec!["USDC", "USDT"],
        _ => vec![quote],
    }
}

fn kucoin_contract_base(base: &str) -> &str {
    if base.eq_ignore_ascii_case("BTC") {
        "XBT"
    } else {
        base
    }
}

fn exact_exists_binance(active: &HashSet<String>, base: &str, quote: &str) -> bool {
    for q in preferred_quotes(quote) {
        if active.contains(&format!("{base}{q}")) {
            return true;
        }
    }
    false
}

fn exact_exists_kucoin_spot(active: &HashSet<String>, base: &str, quote: &str) -> bool {
    for q in preferred_quotes(quote) {
        if active.contains(&format!("{base}-{q}")) {
            return true;
        }
    }
    false
}

fn exact_exists_kucoin_futures(active: &HashSet<String>, base: &str) -> bool {
    let b = kucoin_contract_base(base);
    active.contains(&format!("{b}USDTM")) || active.contains(&format!("{b}USDCM"))
}

fn exact_exists_gate_spot(active: &HashSet<String>, base: &str, quote: &str) -> bool {
    for q in preferred_quotes(quote) {
        if active.contains(&format!("{base}_{q}")) {
            return true;
        }
    }
    false
}

fn exact_exists_gate_futures(
    active_usdt: &HashSet<String>,
    active_usdc: &HashSet<String>,
    base: &str,
    quote: &str,
) -> bool {
    for q in preferred_quotes(quote) {
        let cand = format!("{base}_{q}");
        if q.eq_ignore_ascii_case("USDT") && active_usdt.contains(&cand) {
            return true;
        }
        if q.eq_ignore_ascii_case("USDC") && active_usdc.contains(&cand) {
            return true;
        }
    }
    false
}

fn candidates_filtered(
    all: &HashSet<String>,
    keyword: &str,
    suffixes: &[String],
    max: usize,
) -> Vec<String> {
    let mut out = BTreeSet::<String>::new();
    for s in all {
        if !contains_kw(s, keyword) {
            continue;
        }
        if !suffixes.is_empty() && !suffixes.iter().any(|suf| s.ends_with(suf)) {
            continue;
        }
        out.insert(s.to_string());
    }
    out.into_iter().take(max).collect()
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

#[derive(Default)]
struct Totals {
    total: usize,
    parse_fail: usize,
    missing_any: usize,
    missing_by_venue: BTreeMap<&'static str, usize>,
    ambiguous_by_venue: BTreeMap<&'static str, usize>,
    no_candidate_by_venue: BTreeMap<&'static str, usize>,
    missing_symbols: BTreeMap<&'static str, Vec<String>>,
}

fn bump(map: &mut BTreeMap<&'static str, usize>, k: &'static str, v: usize) {
    *map.entry(k).or_insert(0) += v;
}

fn push(map: &mut BTreeMap<&'static str, Vec<String>>, k: &'static str, v: String) {
    map.entry(k).or_default().push(v);
}

fn suffixes_for_binance(quote: &str) -> Vec<String> {
    preferred_quotes(quote)
        .into_iter()
        .map(|q| q.to_string())
        .collect()
}

fn suffixes_for_kucoin_spot(quote: &str) -> Vec<String> {
    preferred_quotes(quote)
        .into_iter()
        .map(|q| format!("-{q}"))
        .collect()
}

fn suffixes_for_kucoin_futures() -> Vec<String> {
    vec!["USDTM".to_string(), "USDCM".to_string()]
}

fn suffixes_for_gate(quote: &str) -> Vec<String> {
    preferred_quotes(quote)
        .into_iter()
        .map(|q| format!("_{q}"))
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (config_path, max) = parse_args()?;
    let cfg = read_toml(&config_path)?;

    let kucoin_rest = toml_get_string(&cfg, &["kucoin", "rest_endpoint"])
        .unwrap_or_else(|| "https://api.kucoin.com".to_string());
    let kucoin_futures_rest = toml_get_string(&cfg, &["kucoin", "futures_rest_endpoint"])
        .unwrap_or_else(|| "https://api-futures.kucoin.com".to_string());

    // Collect symbols from all sources; preserve first-seen order.
    let symbols = ordered_union(&[
        toml_get_string_array(&cfg, &["binance_spot_sbe", "symbols"]),
        toml_get_string_array(&cfg, &["binance_spot_json", "symbols"]),
        toml_get_string_array(&cfg, &["binance_futures_json", "symbols"]),
        toml_get_string_array(&cfg, &["kucoin", "symbols"]),
        toml_get_string_array(&cfg, &["kucoin_futures", "symbols"]),
        toml_get_string_array(&cfg, &["gate", "symbols"]),
    ]);

    if symbols.is_empty() {
        anyhow::bail!("no symbols found in config (expected *.*.symbols arrays)");
    }

    // Fetch exchange listings once.
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

    println!("config: {config_path}");
    println!("symbols: {}", symbols.len());
    println!("max_candidates: {max}");
    println!();

    println!(
        "{:14} {:10} | {:13} {:13} {:15} {:15} {:13} {:15}",
        "SYMBOL",
        "KEYWORD",
        "bin_spot",
        "bin_fut",
        "kucoin_spot",
        "kucoin_fut",
        "gate_spot",
        "gate_fut"
    );
    println!(
        "{}",
        "-".repeat(14 + 1 + 10 + 3 + 13 + 1 + 13 + 1 + 15 + 1 + 15 + 1 + 13 + 1 + 15)
    );

    let venues = [
        "bin_spot",
        "bin_fut",
        "kucoin_spot",
        "kucoin_fut",
        "gate_spot",
        "gate_fut",
    ];
    let mut totals = Totals::default();
    totals.total = symbols.len();
    for v in venues {
        totals.missing_by_venue.insert(v, 0);
        totals.ambiguous_by_venue.insert(v, 0);
        totals.no_candidate_by_venue.insert(v, 0);
        totals.missing_symbols.insert(v, Vec::new());
    }

    for raw in &symbols {
        let raw_u = raw.to_ascii_uppercase();
        let cs = match parse_canonical_symbol(&raw_u) {
            Some(cs) => cs,
            None => {
                totals.parse_fail += 1;
                println!(
                    "{:14} {:10} | {:13} {:13} {:15} {:15} {:13} {:15}",
                    raw_u,
                    "-",
                    "PARSE_FAIL",
                    "PARSE_FAIL",
                    "PARSE_FAIL",
                    "PARSE_FAIL",
                    "PARSE_FAIL",
                    "PARSE_FAIL"
                );
                continue;
            }
        };
        let keyword = extract_keyword_from_symbol(&raw_u).unwrap_or_else(|| cs.base.clone());

        let bin_spot_ok = exact_exists_binance(&bin_spot, &cs.base, &cs.quote);
        let bin_fut_ok = exact_exists_binance(&bin_fut, &cs.base, &cs.quote);
        let kucoin_spot_ok = exact_exists_kucoin_spot(&k_spot, &cs.base, &cs.quote);
        let kucoin_fut_ok = exact_exists_kucoin_futures(&k_fut, &cs.base);
        let gate_spot_ok = exact_exists_gate_spot(&g_spot, &cs.base, &cs.quote);
        let gate_fut_ok = exact_exists_gate_futures(&g_usdt, &g_usdc, &cs.base, &cs.quote);

        let mut missing_any = false;
        let mut status: HashMap<&'static str, String> = HashMap::new();

        // Precompute candidate lists only when missing (keeps output small).
        let mut candidate_lists: HashMap<&'static str, Vec<String>> = HashMap::new();

        // binance spot
        if bin_spot_ok {
            status.insert("bin_spot", "OK".to_string());
        } else {
            missing_any = true;
            bump(&mut totals.missing_by_venue, "bin_spot", 1);
            push(&mut totals.missing_symbols, "bin_spot", raw_u.clone());
            let cands =
                candidates_filtered(&bin_spot, &keyword, &suffixes_for_binance(&cs.quote), max);
            if cands.is_empty() {
                bump(&mut totals.no_candidate_by_venue, "bin_spot", 1);
                status.insert("bin_spot", "MISS(0)".to_string());
            } else {
                if cands.len() > 1 {
                    bump(&mut totals.ambiguous_by_venue, "bin_spot", 1);
                }
                status.insert("bin_spot", format!("MISS({})", cands.len()));
                candidate_lists.insert("bin_spot", cands);
            }
        }

        // binance futures
        if bin_fut_ok {
            status.insert("bin_fut", "OK".to_string());
        } else {
            missing_any = true;
            bump(&mut totals.missing_by_venue, "bin_fut", 1);
            push(&mut totals.missing_symbols, "bin_fut", raw_u.clone());
            let cands =
                candidates_filtered(&bin_fut, &keyword, &suffixes_for_binance(&cs.quote), max);
            if cands.is_empty() {
                bump(&mut totals.no_candidate_by_venue, "bin_fut", 1);
                status.insert("bin_fut", "MISS(0)".to_string());
            } else {
                if cands.len() > 1 {
                    bump(&mut totals.ambiguous_by_venue, "bin_fut", 1);
                }
                status.insert("bin_fut", format!("MISS({})", cands.len()));
                candidate_lists.insert("bin_fut", cands);
            }
        }

        // kucoin spot
        if kucoin_spot_ok {
            status.insert("kucoin_spot", "OK".to_string());
        } else {
            missing_any = true;
            bump(&mut totals.missing_by_venue, "kucoin_spot", 1);
            push(&mut totals.missing_symbols, "kucoin_spot", raw_u.clone());
            let cands =
                candidates_filtered(&k_spot, &keyword, &suffixes_for_kucoin_spot(&cs.quote), max);
            if cands.is_empty() {
                bump(&mut totals.no_candidate_by_venue, "kucoin_spot", 1);
                status.insert("kucoin_spot", "MISS(0)".to_string());
            } else {
                if cands.len() > 1 {
                    bump(&mut totals.ambiguous_by_venue, "kucoin_spot", 1);
                }
                status.insert("kucoin_spot", format!("MISS({})", cands.len()));
                candidate_lists.insert("kucoin_spot", cands);
            }
        }

        // kucoin futures
        if kucoin_fut_ok {
            status.insert("kucoin_fut", "OK".to_string());
        } else {
            missing_any = true;
            bump(&mut totals.missing_by_venue, "kucoin_fut", 1);
            push(&mut totals.missing_symbols, "kucoin_fut", raw_u.clone());
            let cands = candidates_filtered(&k_fut, &keyword, &suffixes_for_kucoin_futures(), max);
            if cands.is_empty() {
                bump(&mut totals.no_candidate_by_venue, "kucoin_fut", 1);
                status.insert("kucoin_fut", "MISS(0)".to_string());
            } else {
                if cands.len() > 1 {
                    bump(&mut totals.ambiguous_by_venue, "kucoin_fut", 1);
                }
                status.insert("kucoin_fut", format!("MISS({})", cands.len()));
                candidate_lists.insert("kucoin_fut", cands);
            }
        }

        // gate spot
        if gate_spot_ok {
            status.insert("gate_spot", "OK".to_string());
        } else {
            missing_any = true;
            bump(&mut totals.missing_by_venue, "gate_spot", 1);
            push(&mut totals.missing_symbols, "gate_spot", raw_u.clone());
            let cands = candidates_filtered(&g_spot, &keyword, &suffixes_for_gate(&cs.quote), max);
            if cands.is_empty() {
                bump(&mut totals.no_candidate_by_venue, "gate_spot", 1);
                status.insert("gate_spot", "MISS(0)".to_string());
            } else {
                if cands.len() > 1 {
                    bump(&mut totals.ambiguous_by_venue, "gate_spot", 1);
                }
                status.insert("gate_spot", format!("MISS({})", cands.len()));
                candidate_lists.insert("gate_spot", cands);
            }
        }

        // gate futures (usdt/usdc)
        if gate_fut_ok {
            status.insert("gate_fut", "OK".to_string());
        } else {
            missing_any = true;
            bump(&mut totals.missing_by_venue, "gate_fut", 1);
            push(&mut totals.missing_symbols, "gate_fut", raw_u.clone());

            // Combine USDT + USDC listings, still filtered by suffix.
            let mut combined = g_usdt.clone();
            combined.extend(g_usdc.iter().cloned());
            let cands =
                candidates_filtered(&combined, &keyword, &suffixes_for_gate(&cs.quote), max);
            if cands.is_empty() {
                bump(&mut totals.no_candidate_by_venue, "gate_fut", 1);
                status.insert("gate_fut", "MISS(0)".to_string());
            } else {
                if cands.len() > 1 {
                    bump(&mut totals.ambiguous_by_venue, "gate_fut", 1);
                }
                status.insert("gate_fut", format!("MISS({})", cands.len()));
                candidate_lists.insert("gate_fut", cands);
            }
        }

        if missing_any {
            totals.missing_any += 1;
        }

        println!(
            "{:14} {:10} | {:13} {:13} {:15} {:15} {:13} {:15}",
            raw_u,
            keyword,
            status.get("bin_spot").unwrap(),
            status.get("bin_fut").unwrap(),
            status.get("kucoin_spot").unwrap(),
            status.get("kucoin_fut").unwrap(),
            status.get("gate_spot").unwrap(),
            status.get("gate_fut").unwrap(),
        );

        // Print candidate lists for missing venues.
        let order = [
            ("bin_spot", "binance spot candidates"),
            ("bin_fut", "binance futures candidates"),
            ("kucoin_spot", "kucoin spot candidates"),
            ("kucoin_fut", "kucoin futures candidates"),
            ("gate_spot", "gate spot candidates"),
            ("gate_fut", "gate futures candidates"),
        ];
        for (k, label) in order {
            let Some(list) = candidate_lists.get(k) else {
                continue;
            };
            println!("  - {label} (showing {}): {}", list.len(), list.join(", "));
        }
    }

    println!();
    println!("==== SUMMARY ====");
    println!("total: {}", totals.total);
    println!("parse_fail: {}", totals.parse_fail);
    println!("missing_any: {}", totals.missing_any);
    println!();

    println!("missing_by_venue:");
    for (k, v) in &totals.missing_by_venue {
        println!("- {k}: {v}");
    }
    println!("ambiguous_missing_by_venue (missing with >1 candidate):");
    for (k, v) in &totals.ambiguous_by_venue {
        println!("- {k}: {v}");
    }
    println!("no_candidate_by_venue (missing with 0 candidates):");
    for (k, v) in &totals.no_candidate_by_venue {
        println!("- {k}: {v}");
    }
    println!();

    for v in venues {
        let miss = totals.missing_symbols.get(v).unwrap();
        if miss.is_empty() {
            continue;
        }
        println!("---- {v} MISSING ({}):", miss.len());
        for s in miss {
            println!("{s}");
        }
        println!();
    }

    Ok(())
}
