use std::time::Duration;
use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
use tracing::{info, warn};

use crate::config::GateConfig;

use crate::schema::event::{MarketEvent, Stream};
use crate::util::time::{format_time_str_ms, now_ms};

struct LocalOrderBook {
    // Key is f64::to_bits() for sorting. Prices are positive so this preserves order.
    bids: BTreeMap<u64, f64>,
    asks: BTreeMap<u64, f64>,
    last_id: Option<u64>,
}

impl LocalOrderBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_id: None,
        }
    }
}

const GATE_ACTIVE_PING_EVERY_SECS: u64 = 15;
const GATE_IDLE_DEAD_SECS: u64 = 60;
const GATE_REST_BASE: &str = "https://api.gateio.ws/api/v4";
const GATE_SUBSCRIBE_DELAY_MS: u64 = 300;
const GATE_CONN_STAGGER_MS: u64 = 500;

#[derive(Debug, Deserialize)]
struct GateSpotPair {
    id: String,
}

#[derive(Debug, Deserialize)]
struct GateFuturesContract {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    contract: Option<String>,
    #[serde(default)]
    id: Option<String>,
}

fn jittered_sleep_secs(base_secs: u64) -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    if base_secs == 0 {
        return Duration::from_millis(0);
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .subsec_nanos() as u64;
    let jitter_ms = if base_secs <= 1 {
        nanos % 250
    } else {
        nanos % (base_secs * 200)
    };
    Duration::from_secs(base_secs) + Duration::from_millis(jitter_ms)
}

fn next_backoff_secs(prev: u64, success: bool) -> u64 {
    if success {
        1
    } else {
        (prev.saturating_mul(2)).clamp(1, 30)
    }
}

pub fn normalize_symbol(input: &str) -> (String, String) {
    // Gate spot pairs typically look like BTC_USDT.
    // We store symbol without separators (BTCUSDT) so spot+swap align.
    let upper = input.trim().to_ascii_uppercase();
    if upper.contains('_') {
        let stored = upper.replace('_', "");
        return (upper, stored);
    }
    // Heuristic conversion for common quotes.
    let quotes = ["USDT", "USDC", "BTC", "ETH", "BNB", "DAI", "FDUSD", "USD"];
    let quote = quotes.iter().find(|q| upper.ends_with(*q));
    if let Some(q) = quote {
        let base = &upper[..upper.len() - q.len()];
        (format!("{base}_{q}"), format!("{base}{q}"))
    } else {
        (upper.clone(), upper)
    }
}

fn split_gate_pair(pair: &str) -> Option<(String, String)> {
    let p = pair.trim().to_ascii_uppercase();
    if let Some((base, quote)) = p.rsplit_once('_') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
        return None;
    }

    let quotes = ["USDT", "USDC", "USD", "BTC", "ETH", "BNB", "DAI", "FDUSD"];
    let (base, quote) = quotes
        .iter()
        .find_map(|q| p.strip_suffix(q).map(|b| (b.to_string(), q.to_string())))?;
    if base.is_empty() {
        return None;
    }
    Some((base, quote))
}

fn stored_symbol_from_gate_pair(pair: &str) -> String {
    pair.trim().to_ascii_uppercase().replace('_', "")
}

fn preferred_quotes<'a>(quote: &'a str) -> Vec<&'a str> {
    match quote.to_ascii_uppercase().as_str() {
        "USDC" => vec!["USDC", "USDT"],
        "USDT" => vec!["USDT", "USDC"],
        _ => vec![quote],
    }
}

fn select_gate_spot_pair(
    active: &std::collections::HashSet<String>,
    desired: &str,
) -> Option<String> {
    let desired = desired.trim().to_ascii_uppercase();
    if active.contains(&desired) {
        return Some(desired);
    }

    let (base, quote) = split_gate_pair(&desired)?;
    for q in preferred_quotes(&quote) {
        let cand = format!("{base}_{q}");
        if active.contains(&cand) {
            return Some(cand);
        }
    }
    None
}

fn futures_endpoint_for_settle(endpoint: &str, settle: &str) -> String {
    let settle = settle.trim().to_ascii_lowercase();
    let base = endpoint.trim_end_matches('/');
    match base.rsplit_once('/') {
        Some((prefix, _last)) => format!("{prefix}/{settle}"),
        None => format!("{base}/{settle}"),
    }
}

async fn fetch_gate_spot_pairs() -> anyhow::Result<std::collections::HashSet<String>> {
    let url = format!("{}/spot/currency_pairs", GATE_REST_BASE);
    let client = reqwest::Client::new();
    let resp = client
        .get(url)
        .send()
        .await
        .context("gate spot pairs request")?
        .error_for_status()
        .context("gate spot pairs status")?;
    let pairs: Vec<GateSpotPair> = resp.json().await.context("gate spot pairs json")?;
    let mut out = std::collections::HashSet::new();
    for p in pairs {
        out.insert(p.id.to_ascii_uppercase());
    }
    Ok(out)
}

async fn fetch_gate_futures_contracts(
    settle: &str,
) -> anyhow::Result<std::collections::HashSet<String>> {
    let settle = settle.trim().to_ascii_lowercase();
    let url = format!("{}/futures/{}/contracts", GATE_REST_BASE, settle);
    let client = reqwest::Client::new();
    let resp = client
        .get(url)
        .send()
        .await
        .context("gate futures contracts request")?;
    if resp.status().as_u16() == 400 && settle == "usdc" {
        // Some Gate deployments don't expose USDC-settled contracts on this endpoint.
        return Ok(std::collections::HashSet::new());
    }
    let resp = resp
        .error_for_status()
        .context("gate futures contracts status")?;
    let contracts: Vec<GateFuturesContract> =
        resp.json().await.context("gate futures contracts json")?;
    let mut out = std::collections::HashSet::new();
    for c in contracts {
        if let Some(name) = c.name.or(c.contract).or(c.id) {
            out.insert(name.to_ascii_uppercase());
        }
    }
    Ok(out)
}

pub async fn spawn_gate_ws(
    cfg: GateConfig,
    sender: crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    if cfg.symbols.is_empty() {
        anyhow::bail!("gate.enabled=true but symbols is empty");
    }
    let mut handles = Vec::new();
    if cfg.spot_ticker || cfg.spot_l5 || cfg.spot_trade {
        let mut selected: Vec<(String, String)> = Vec::new();
        match fetch_gate_spot_pairs().await {
            Ok(active) => {
                let mut skipped = 0usize;
                let mut fallback_usdc = 0usize;
                for s in &cfg.symbols {
                    let (desired, _stored) = normalize_symbol(s);
                    match select_gate_spot_pair(&active, &desired) {
                        Some(chosen) => {
                            if chosen.ends_with("_USDC") && !desired.ends_with("_USDC") {
                                fallback_usdc += 1;
                            }
                            selected.push((chosen.clone(), stored_symbol_from_gate_pair(&chosen)));
                        }
                        None => skipped += 1,
                    }
                }
                if skipped > 0 {
                    warn!("gate spot: skip {skipped} unsupported symbols");
                }
                if fallback_usdc > 0 {
                    warn!("gate spot: fallback to USDC for {fallback_usdc} symbols");
                }
            }
            Err(err) => {
                warn!("gate spot: fetch symbols failed: {err:#} (using configured list)");
                selected = cfg.symbols.iter().map(|s| normalize_symbol(s)).collect();
            }
        }
        if !selected.is_empty() {
            handles.extend(spawn_gate_spot_ws(
                cfg.spot_endpoint.clone(),
                selected,
                cfg.spot_ticker,
                cfg.spot_l5,
                cfg.spot_trade,
                sender.clone(),
            )?);
        }
    }

    if cfg.swap_ticker || cfg.swap_l5 || cfg.swap_trade {
        let (usdt_res, usdc_res) = tokio::join!(
            fetch_gate_futures_contracts("usdt"),
            fetch_gate_futures_contracts("usdc")
        );
        let active_usdt = usdt_res.unwrap_or_else(|err| {
            warn!("gate futures: fetch usdt contracts failed: {err:#}");
            std::collections::HashSet::new()
        });
        let active_usdc = usdc_res.unwrap_or_else(|err| {
            warn!("gate futures: fetch usdc contracts failed: {err:#}");
            std::collections::HashSet::new()
        });

        let mut usdt_symbols: Vec<(String, String)> = Vec::new();
        let mut usdc_symbols: Vec<(String, String)> = Vec::new();
        let mut skipped = 0usize;
        let mut fallback_usdc = 0usize;
        for s in &cfg.symbols {
            let (desired, _stored) = normalize_symbol(s);
            let Some((base, quote)) = split_gate_pair(&desired) else {
                skipped += 1;
                continue;
            };
            let mut chosen: Option<(&'static str, String)> = None;
            for q in preferred_quotes(&quote) {
                let cand = format!("{base}_{q}");
                if q.eq_ignore_ascii_case("USDT") && active_usdt.contains(&cand) {
                    chosen = Some(("usdt", cand));
                    break;
                }
                if q.eq_ignore_ascii_case("USDC") && active_usdc.contains(&cand) {
                    chosen = Some(("usdc", cand));
                    break;
                }
            }
            match chosen {
                Some((settle, contract)) => {
                    if settle == "usdc" {
                        fallback_usdc += 1;
                    }
                    let stored = stored_symbol_from_gate_pair(&contract);
                    if settle == "usdt" {
                        usdt_symbols.push((contract, stored));
                    } else {
                        usdc_symbols.push((contract, stored));
                    }
                }
                None => skipped += 1,
            }
        }
        if skipped > 0 {
            warn!("gate futures: skip {skipped} unsupported symbols (usdt/usdc)");
        }
        if fallback_usdc > 0 {
            warn!("gate futures: fallback to USDC for {fallback_usdc} symbols");
        }

        if !usdt_symbols.is_empty() {
            handles.extend(spawn_gate_futures_ws(
                futures_endpoint_for_settle(&cfg.futures_endpoint, "usdt"),
                usdt_symbols,
                cfg.swap_ticker,
                cfg.swap_l5,
                cfg.swap_trade,
                sender.clone(),
            )?);
        }
        if !usdc_symbols.is_empty() {
            handles.extend(spawn_gate_futures_ws(
                futures_endpoint_for_settle(&cfg.futures_endpoint, "usdc"),
                usdc_symbols,
                cfg.swap_ticker,
                cfg.swap_l5,
                cfg.swap_trade,
                sender.clone(),
            )?);
        }
    }

    Ok(handles)
}

fn spawn_gate_spot_ws(
    endpoint: String,
    symbols: Vec<(String, String)>,
    ticker: bool,
    l5: bool,
    trade: bool,
    sender: crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    const SYMBOLS_PER_CONN: usize = 50;
    let mut handles = Vec::new();
    for (idx, chunk) in symbols.chunks(SYMBOLS_PER_CONN).enumerate() {
        let endpoint = endpoint.clone();
        let sender = sender.clone();
        let chunk: Vec<(String, String)> = chunk.to_vec();
        let start_delay = Duration::from_millis(GATE_CONN_STAGGER_MS * idx as u64);
        handles.push(tokio::spawn(async move {
            if start_delay.as_millis() > 0 {
                tokio::time::sleep(start_delay).await;
            }
            let mut backoff_secs = 1u64;
            loop {
                let res = run_gate_spot_once(&endpoint, &chunk, ticker, l5, trade, &sender).await;
                if let Err(err) = &res {
                    warn!("gate spot ws ended: {err:#} backoff_secs={backoff_secs}");
                }
                tokio::time::sleep(jittered_sleep_secs(backoff_secs)).await;
                let ok = res
                    .as_ref()
                    .is_ok_and(|(frames, dur)| *frames > 0 && *dur >= Duration::from_secs(10));
                backoff_secs = next_backoff_secs(backoff_secs, ok);
            }
        }));
    }
    Ok(handles)
}

fn spawn_gate_futures_ws(
    endpoint: String,
    symbols: Vec<(String, String)>,
    ticker: bool,
    l5: bool,
    trade: bool,
    sender: crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    const SYMBOLS_PER_CONN: usize = 30;
    let mut handles = Vec::new();
    for (idx, chunk) in symbols.chunks(SYMBOLS_PER_CONN).enumerate() {
        let endpoint = endpoint.clone();
        let sender = sender.clone();
        let chunk: Vec<(String, String)> = chunk.to_vec();
        let start_delay = Duration::from_millis(GATE_CONN_STAGGER_MS * idx as u64);
        handles.push(tokio::spawn(async move {
            if start_delay.as_millis() > 0 {
                tokio::time::sleep(start_delay).await;
            }
            let mut backoff_secs = 1u64;
            loop {
                let res = run_gate_futures_once(&endpoint, &chunk, ticker, l5, trade, &sender).await;
                if let Err(err) = &res {
                    warn!("gate futures ws ended: {err:#} backoff_secs={backoff_secs}");
                }
                tokio::time::sleep(jittered_sleep_secs(backoff_secs)).await;
                let ok = res
                    .as_ref()
                    .is_ok_and(|(frames, dur)| *frames > 0 && *dur >= Duration::from_secs(10));
                backoff_secs = next_backoff_secs(backoff_secs, ok);
            }
        }));
    }
    Ok(handles)
}

fn handle_gate_snapshot(
    exchange: &str,
    is_futures: bool,
    sym: &str,
    val: serde_json::Value,
    symbols: &[(String, String)],
    sender: &crossbeam_channel::Sender<MarketEvent>,
    books: &mut HashMap<String, LocalOrderBook>,
) -> anyhow::Result<()> {
    let stored_sym = symbols
        .iter()
        .find(|(s, _)| s == sym)
        .map(|(_, stored)| stored.clone())
        .unwrap_or_else(|| sym.to_string());

    let id = val.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
    if id == 0 {
        return Ok(());
    }

    let book = books.entry(sym.to_string()).or_insert_with(LocalOrderBook::new);
    // Reset book with snapshot
    book.bids.clear();
    book.asks.clear();
    book.last_id = Some(id);

    let bids = val.get("bids");
    for (p, q) in parse_book_entries(bids) {
        book.bids.insert(p.to_bits(), q);
    }
    let asks = val.get("asks");
    for (p, q) in parse_book_entries(asks) {
        book.asks.insert(p.to_bits(), q);
    }

    let mut ev = MarketEvent::new(
        exchange,
        if is_futures {
            Stream::FutureL5
        } else {
            Stream::SpotL5
        },
        stored_sym,
    );
    ev.local_ts = now_ms();
    ev.time_str = format_time_str_ms(ev.local_ts);
    ev.update_id = Some(id);

    // Bids
    let mut bids_iter = book.bids.iter().rev();
    if let Some((&p, &q)) = bids_iter.next() { ev.bid_px = Some(f64::from_bits(p)); ev.bid_qty = Some(q); }
    if let Some((&p, &q)) = bids_iter.next() { ev.bid1_px = Some(f64::from_bits(p)); ev.bid1_qty = Some(q); }
    if let Some((&p, &q)) = bids_iter.next() { ev.bid2_px = Some(f64::from_bits(p)); ev.bid2_qty = Some(q); }
    if let Some((&p, &q)) = bids_iter.next() { ev.bid3_px = Some(f64::from_bits(p)); ev.bid3_qty = Some(q); }
    if let Some((&p, &q)) = bids_iter.next() { ev.bid4_px = Some(f64::from_bits(p)); ev.bid4_qty = Some(q); }
    if let Some((&p, &q)) = bids_iter.next() { ev.bid5_px = Some(f64::from_bits(p)); ev.bid5_qty = Some(q); }

    // Asks
    let mut asks_iter = book.asks.iter();
    if let Some((&p, &q)) = asks_iter.next() { ev.ask_px = Some(f64::from_bits(p)); ev.ask_qty = Some(q); }
    if let Some((&p, &q)) = asks_iter.next() { ev.ask1_px = Some(f64::from_bits(p)); ev.ask1_qty = Some(q); }
    if let Some((&p, &q)) = asks_iter.next() { ev.ask2_px = Some(f64::from_bits(p)); ev.ask2_qty = Some(q); }
    if let Some((&p, &q)) = asks_iter.next() { ev.ask3_px = Some(f64::from_bits(p)); ev.ask3_qty = Some(q); }
    if let Some((&p, &q)) = asks_iter.next() { ev.ask4_px = Some(f64::from_bits(p)); ev.ask4_qty = Some(q); }
    if let Some((&p, &q)) = asks_iter.next() { ev.ask5_px = Some(f64::from_bits(p)); ev.ask5_qty = Some(q); }

    let _ = sender.try_send(ev);
    Ok(())
}

async fn run_gate_spot_once(
    endpoint: &str,
    symbols: &[(String, String)],
    ticker: bool,
    l5: bool,
    trade: bool,
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<(u64, Duration)> {
    let url = normalize_gate_spot_endpoint(endpoint);
    info!("connect gate spot: symbols={} url={}", symbols.len(), url);
    let mut req = (&url).into_client_request()?;
    req.headers_mut()
        .insert("X-Gate-Size-Decimal", http::HeaderValue::from_static("1"));
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(req),
    )
    .await
    .context("gate spot connect: timeout")?
    .context("gate spot connect")?;
    let (mut write, mut read) = ws.split();

    // subscribe requests
    if ticker {
        for (sym, _) in symbols {
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "spot.book_ticker",
                "event": "subscribe",
                "payload": [sym]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate spot subscribe ticker")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
        }
    }
    if l5 {
        for (sym, _) in symbols {
            // Gate Spot order_book_update payload: [symbol, interval]
            // No limit parameter for spot.
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "spot.order_book_update",
                "event": "subscribe",
                "payload": [sym, "100ms"]
            });

            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate spot subscribe order_book")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
        }
    }
    if trade {
        for (sym, _) in symbols {
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": [sym]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate spot subscribe trades")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
        }
    }

    let mut ping = tokio::time::interval(Duration::from_secs(GATE_ACTIVE_PING_EVERY_SECS));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watchdog = tokio::time::interval(Duration::from_secs(1));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Active Snapshot Pull (10s interval)
    let (snapshot_tx, mut snapshot_rx) = tokio::sync::mpsc::channel(100);
    let snapshot_symbols: Vec<String> = symbols.iter().map(|(s, _)| s.clone()).collect();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        // Initial snapshot immediately
        let mut first = true;
        loop {
            if !first {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            first = false;

            for sym in &snapshot_symbols {
                let url = format!("{}/spot/order_book?currency_pair={}&limit=10", GATE_REST_BASE, sym);
                match client.get(&url).send().await {
                    Ok(resp) => {
                        if let Ok(val) = resp.json::<serde_json::Value>().await {
                            if snapshot_tx.send((sym.clone(), val)).await.is_err() {
                                return; // Receiver closed
                            }
                        }
                    }
                    Err(e) => {
                        warn!("gate spot snapshot failed for {}: {}", sym, e);
                    }
                }
                // Throttle to avoid hitting rate limits too hard (200/s allowed, we do ~20/s)
                tokio::time::sleep(Duration::from_millis(50)).await; 
            }
        }
    });

    let started = std::time::Instant::now();
    let mut frames = 0u64;
    let mut last_msg_ts = std::time::Instant::now();
    let mut books = HashMap::new();
    loop {
        tokio::select! {
            Some((sym, val)) = snapshot_rx.recv() => {
                handle_gate_snapshot("gate", false, &sym, val, symbols, sender, &mut books)?;
            }
            _ = ping.tick() => {
                write.send(Message::Ping(Vec::new())).await.context("gate spot active ping")?;
            }
            _ = watchdog.tick() => {
                if last_msg_ts.elapsed() > Duration::from_secs(GATE_IDLE_DEAD_SECS) {
                    let _ = write.send(Message::Close(None)).await;
                    anyhow::bail!("gate spot watchdog: idle for >{GATE_IDLE_DEAD_SECS}s");
                }
            }
            next = read.next() => {
                let Some(msg) = next else { break };
                let msg = msg.context("gate spot read msg")?;
                frames += 1;
                last_msg_ts = std::time::Instant::now();
                match msg {
                    Message::Text(text) => {
                        handle_gate_text("gate", false, &text, symbols, sender, &mut books)?;
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.context("gate spot pong")?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }
    Ok((frames, started.elapsed()))
}

async fn run_gate_futures_once(
    endpoint: &str,
    symbols: &[(String, String)],
    ticker: bool,
    l5: bool,
    trade: bool,
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<(u64, Duration)> {
    let url = normalize_gate_futures_endpoint(endpoint);
    info!(
        "connect gate futures: symbols={} url={}",
        symbols.len(),
        url
    );
    let mut req = (&url).into_client_request()?;
    req.headers_mut()
        .insert("X-Gate-Size-Decimal", http::HeaderValue::from_static("1"));
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(req),
    )
    .await
    .context("gate futures connect: timeout")?
    .context("gate futures connect")?;
    let (mut write, mut read) = ws.split();

    // Active Snapshot Pull (10s interval)
    let (snapshot_tx, mut snapshot_rx) = tokio::sync::mpsc::channel(100);
    let snapshot_symbols: Vec<String> = symbols.iter().map(|(s, _)| s.clone()).collect();
    let settle = if endpoint.contains("usdc") { "usdc" } else { "usdt" };
    
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut first = true;
        loop {
            if !first {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            first = false;

            for sym in &snapshot_symbols {
                let url = format!("{}/futures/{}/order_book?contract={}&limit=10", GATE_REST_BASE, settle, sym);
                match client.get(&url).send().await {
                    Ok(resp) => {
                        if let Ok(val) = resp.json::<serde_json::Value>().await {
                            if snapshot_tx.send((sym.clone(), val)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("gate futures snapshot failed for {}: {}", sym, e);
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    if ticker {
        for (sym, _) in symbols {
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": [sym]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate futures subscribe ticker")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
        }
    }
    if l5 {
        for (sym, _) in symbols {
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "futures.order_book_update",
                "event": "subscribe",
                // Gate futures payload: [contract, frequency, level]
                // frequency: "100ms", level: "5"
                "payload": [sym, "100ms", "5"]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate futures subscribe order_book")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
        }
    }
    if trade {
        for (sym, _) in symbols {
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "futures.trades",
                "event": "subscribe",
                "payload": [sym]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate futures subscribe trades")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
        }
    }

    let mut ping = tokio::time::interval(Duration::from_secs(GATE_ACTIVE_PING_EVERY_SECS));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watchdog = tokio::time::interval(Duration::from_secs(1));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let started = std::time::Instant::now();
    let mut frames = 0u64;
    let mut last_msg_ts = std::time::Instant::now();
    let mut books = HashMap::new();
    loop {
        tokio::select! {
            Some((sym, val)) = snapshot_rx.recv() => {
                handle_gate_snapshot("gate", true, &sym, val, symbols, sender, &mut books)?;
            }
            _ = ping.tick() => {
                write
                    .send(Message::Ping(Vec::new())).await.context("gate futures active ping")?;
            }
            _ = watchdog.tick() => {
                if last_msg_ts.elapsed() > Duration::from_secs(GATE_IDLE_DEAD_SECS) {
                    let _ = write.send(Message::Close(None)).await;
                    anyhow::bail!("gate futures watchdog: idle for >{GATE_IDLE_DEAD_SECS}s");
                }
            }
            next = read.next() => {
                let Some(msg) = next else { break };
                let msg = msg.context("gate futures read msg")?;
                frames += 1;
                last_msg_ts = std::time::Instant::now();
                match msg {
                    Message::Text(text) => {
                        handle_gate_text("gate", true, &text, symbols, sender, &mut books)?;
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.context("gate futures pong")?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }
    Ok((frames, started.elapsed()))
}

fn handle_gate_text(
    exchange: &str,
    is_futures: bool,
    text: &str,
    symbols: &[(String, String)],
    sender: &crossbeam_channel::Sender<MarketEvent>,
    books: &mut HashMap<String, LocalOrderBook>,
) -> anyhow::Result<()> {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let event = v.get("event").and_then(|x| x.as_str()).unwrap_or("");
    // Gate order book snapshots use `event=all`, while other streams often use `event=update`.
    if event != "update" && event != "all" {
        // Gate reports many failures as `event=subscribe` with `result.status=fail` and an `error` object.
        let status = v
            .get("result")
            .and_then(|r| r.get("status"))
            .and_then(|s| s.as_str());
        if event == "error" || status == Some("fail") || v.get("error").is_some() {
            warn!("gate ws non-update: {}", truncate_for_log(text, 512));
        }
        return Ok(());
    }
    let channel = v.get("channel").and_then(|x| x.as_str()).unwrap_or("");
    let result = v.get("result").unwrap_or(&serde_json::Value::Null);

    let local_ts = now_ms();
    let time_str = format_time_str_ms(local_ts);

    if channel.ends_with("book_ticker") {
        let sym = get_str(result, &["s", "currency_pair", "contract"]).or_else(|| {
            v.get("payload")
                .and_then(|p| p.get(0))
                .and_then(|x| x.as_str())
        });
        let Some(sym) = sym else {
            return Ok(());
        };
        let stored = symbols
            .iter()
            .find(|(s, _)| s.eq_ignore_ascii_case(sym))
            .map(|(_, stored)| stored.as_str())
            .unwrap_or_else(|| sym);
        let mut ev = MarketEvent::new(
            exchange,
            if is_futures {
                Stream::FutureBook
            } else {
                Stream::SpotBook
            },
            stored.to_string(),
        );
        ev.local_ts = local_ts;
        ev.time_str = time_str;
        ev.event_time = parse_ts_ms(
            result
                .get("t")
                .or_else(|| result.get("time_ms"))
                .or_else(|| v.get("time")),
        );
        // Gate book_ticker provides a monotonically increasing update id in `u` (spot+futures).
        ev.update_id = get_u64(result, &["u", "U", "id", "lastUpdateId"]);

        ev.bid_px = get_f64(result, &["b", "bid", "best_bid", "bestBid"]);
        ev.bid_qty = get_f64(result, &["B", "bid_size", "best_bid_size", "bestBidSize"]);
        ev.ask_px = get_f64(result, &["a", "ask", "best_ask", "bestAsk"]);
        ev.ask_qty = get_f64(result, &["A", "ask_size", "best_ask_size", "bestAskSize"]);
        if sender.try_send(ev).is_err() {
            crate::util::metrics::inc_dropped_events(1);
        }
        return Ok(());
    }

    if channel.ends_with("order_book") || channel.ends_with("order_book_update") {
        info!("gate order_book msg: channel={} result_keys={:?}", channel, result.as_object().map(|m| m.keys().collect::<Vec<_>>()));
        let sym = get_str(result, &["s", "currency_pair", "contract"]).or_else(|| {
            v.get("payload")
                .and_then(|p| p.get(0))
                .and_then(|x| x.as_str())
        });
        let Some(sym) = sym else {
            return Ok(());
        };
        let stored = symbols
            .iter()
            .find(|(s, _)| s.eq_ignore_ascii_case(sym))
            .map(|(_, stored)| stored.as_str())
            .unwrap_or_else(|| sym);

        let full = result.get("full").and_then(|x| x.as_bool()).unwrap_or(true);
        let book = books.entry(stored.to_string()).or_insert_with(LocalOrderBook::new);

        if full {
            book.bids.clear();
            book.asks.clear();
        }

        let bids = result.get("bids").or_else(|| result.get("b"));
        let asks = result.get("asks").or_else(|| result.get("a"));

        for (p, q) in parse_book_entries(bids) {
            if q == 0.0 {
                book.bids.remove(&p.to_bits());
            } else {
                book.bids.insert(p.to_bits(), q);
            }
        }
        for (p, q) in parse_book_entries(asks) {
            if q == 0.0 {
                book.asks.remove(&p.to_bits());
            } else {
                book.asks.insert(p.to_bits(), q);
            }
        }

        let mut ev = MarketEvent::new(
            exchange,
            if is_futures {
                Stream::FutureL5
            } else {
                Stream::SpotL5
            },
            stored.to_string(),
        );
        ev.local_ts = local_ts;
        ev.time_str = time_str;
        ev.event_time = parse_ts_ms(
            result
                .get("t")
                .or_else(|| result.get("time_ms"))
                .or_else(|| v.get("time")),
        );
        ev.update_id = if is_futures {
            get_u64(result, &["id", "u", "U", "lastUpdateId"])
        } else {
            get_u64(result, &["lastUpdateId", "u", "U", "id"])
        };

        // Bids: highest price first
        let mut bids_iter = book.bids.iter().rev();
        if let Some((&p, &q)) = bids_iter.next() { ev.bid_px = Some(f64::from_bits(p)); ev.bid_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid1_px = Some(f64::from_bits(p)); ev.bid1_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid2_px = Some(f64::from_bits(p)); ev.bid2_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid3_px = Some(f64::from_bits(p)); ev.bid3_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid4_px = Some(f64::from_bits(p)); ev.bid4_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid5_px = Some(f64::from_bits(p)); ev.bid5_qty = Some(q); }

        // Asks: lowest price first
        let mut asks_iter = book.asks.iter();
        if let Some((&p, &q)) = asks_iter.next() { ev.ask_px = Some(f64::from_bits(p)); ev.ask_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask1_px = Some(f64::from_bits(p)); ev.ask1_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask2_px = Some(f64::from_bits(p)); ev.ask2_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask3_px = Some(f64::from_bits(p)); ev.ask3_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask4_px = Some(f64::from_bits(p)); ev.ask4_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask5_px = Some(f64::from_bits(p)); ev.ask5_qty = Some(q); }

        if sender.try_send(ev).is_err() {
            crate::util::metrics::inc_dropped_events(1);
        }
        return Ok(());
    }

    if channel.ends_with("trades") {
        if let Some(arr) = result.as_array() {
            for item in arr {
                emit_gate_trade(exchange, is_futures, item, &v, symbols, sender, local_ts, &time_str)?;
            }
            return Ok(());
        } else {
            emit_gate_trade(exchange, is_futures, result, &v, symbols, sender, local_ts, &time_str)?;
            return Ok(());
        }
    }

    Ok(())
}

fn emit_gate_trade(
    exchange: &str,
    is_futures: bool,
    item: &serde_json::Value,
    envelope: &serde_json::Value,
    symbols: &[(String, String)],
    sender: &crossbeam_channel::Sender<MarketEvent>,
    local_ts: i64,
    time_str: &str,
) -> anyhow::Result<()> {
    let sym = get_str(item, &["currency_pair", "contract"]).or_else(|| {
        envelope
            .get("payload")
            .and_then(|p| p.get(0))
            .and_then(|x| x.as_str())
    });
    let Some(sym) = sym else {
        return Ok(());
    };
    let stored = symbols
        .iter()
        .find(|(s, _)| s.eq_ignore_ascii_case(sym))
        .map(|(_, stored)| stored.as_str())
        .unwrap_or_else(|| sym);

    let mut ev = MarketEvent::new(
        exchange,
        if is_futures {
            Stream::FutureTrade
        } else {
            Stream::SpotTrade
        },
        stored.to_string(),
    );
    ev.local_ts = local_ts;
    ev.time_str = time_str.to_string();
    ev.update_id = get_u64(item, &["id", "trade_id", "match_id"]);
    ev.event_time = parse_ts_ms(
        item.get("create_time_ms")
            .or_else(|| item.get("t"))
            .or_else(|| item.get("time_ms"))
            .or_else(|| item.get("time")),
    );
    let side = get_str(item, &["side", "S"]).unwrap_or("");
    let px = get_f64(item, &["price", "p"]);
    let qty = get_f64(item, &["amount", "q", "size"]);
    if side.eq_ignore_ascii_case("sell") {
        ev.bid_px = px;
        ev.bid_qty = qty;
    } else if side.eq_ignore_ascii_case("buy") {
        ev.ask_px = px;
        ev.ask_qty = qty;
    }
    if sender.try_send(ev).is_err() {
        crate::util::metrics::inc_dropped_events(1);
    }
    Ok(())
}
fn truncate_for_log(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut out = s[..max].to_string();
    out.push_str("…");
    out
}

fn get_str<'a>(obj: &'a serde_json::Value, keys: &[&str]) -> Option<&'a str> {
    for k in keys {
        if let Some(v) = obj.get(*k) {
            if let Some(s) = v.as_str() {
                return Some(s);
            }
        }
    }
    None
}

fn get_f64(obj: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(v) = obj.get(*k) {
            if let Some(x) = v.as_f64() {
                return Some(x);
            }
            if let Some(s) = v.as_str() {
                if let Ok(x) = s.parse::<f64>() {
                    return Some(x);
                }
            }
        }
    }
    None
}

fn get_u64(obj: &serde_json::Value, keys: &[&str]) -> Option<u64> {
    for k in keys {
        if let Some(v) = obj.get(*k) {
            if let Some(x) = parse_u64_value(v) {
                return Some(x);
            }
        }
    }
    None
}

fn parse_u64_value(v: &serde_json::Value) -> Option<u64> {
    if let Some(x) = v.as_u64() {
        return Some(x);
    }
    if let Some(x) = v.as_i64() {
        return u64::try_from(x).ok();
    }
    v.as_str().and_then(|s| s.parse::<u64>().ok())
}

fn parse_ts_ms(v: Option<&serde_json::Value>) -> Option<i64> {
    let Some(v) = v else {
        return None;
    };
    let n = if let Some(x) = v.as_i64() {
        x
    } else if let Some(s) = v.as_str() {
        s.parse::<i64>().ok()?
    } else {
        return None;
    };
    // heuristic: seconds vs ms
    if n > 0 && n < 100_000_000_000 {
        Some(n * 1000)
    } else {
        Some(n)
    }
}

fn parse_book_entries(levels: Option<&serde_json::Value>) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    let Some(levels) = levels else {
        return out;
    };
    if let Some(arr) = levels.as_array() {
        for item in arr {
            if let Some(pair_arr) = item.as_array() {
                let px = pair_arr.get(0).and_then(parse_f64_value);
                let qty = pair_arr.get(1).and_then(parse_f64_value);
                if let (Some(p), Some(q)) = (px, qty) {
                    out.push((p, q));
                }
            } else if let Some(obj) = item.as_object() {
                let px = obj
                    .get("p")
                    .or_else(|| obj.get("price"))
                    .and_then(parse_f64_value);
                let qty = obj
                    .get("s")
                    .or_else(|| obj.get("size"))
                    .or_else(|| obj.get("q"))
                    .or_else(|| obj.get("qty"))
                    .and_then(parse_f64_value);
                if let (Some(p), Some(q)) = (px, qty) {
                    out.push((p, q));
                }
            }
        }
    }
    out
}

fn parse_f64_value(v: &serde_json::Value) -> Option<f64> {
    if let Some(x) = v.as_f64() {
        return Some(x);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

fn normalize_gate_spot_endpoint(endpoint: &str) -> String {
    // Gate spot WS v4 endpoint is typically `wss://api.gateio.ws/ws/v4/` (note the trailing slash).
    // Some servers return 404 if the trailing slash is missing.
    let e = endpoint.trim();
    if e.ends_with("/ws/v4/") {
        e.to_string()
    } else if e.ends_with("/ws/v4") {
        format!("{e}/")
    } else {
        e.to_string()
    }
}

fn normalize_gate_futures_endpoint(endpoint: &str) -> String {
    // Gate futures WS endpoints are typically `wss://fx-ws.gateio.ws/v4/ws/usdt` (no trailing slash needed).
    endpoint.trim().to_string()
}
