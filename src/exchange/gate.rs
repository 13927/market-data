use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::config::GateConfig;

use crate::schema::event::{MarketEvent, Stream};
use crate::util::time::{format_time_str_ms, now_ms};

const GATE_ACTIVE_PING_EVERY_SECS: u64 = 15;
const GATE_IDLE_DEAD_SECS: u64 = 60;
const GATE_REST_BASE: &str = "https://api.gateio.ws/api/v4";
const GATE_SUBSCRIBE_DELAY_MS: u64 = 100;
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
        if let Some(name) = c
            .name
            .or(c.contract)
            .or(c.id)
        {
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
    if cfg.spot_ticker || cfg.spot_l5 {
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
                sender.clone(),
            )?);
        }
    }

    if cfg.swap_ticker || cfg.swap_l5 {
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
                sender.clone(),
            )?);
        }
        if !usdc_symbols.is_empty() {
            handles.extend(spawn_gate_futures_ws(
                futures_endpoint_for_settle(&cfg.futures_endpoint, "usdc"),
                usdc_symbols,
                cfg.swap_ticker,
                cfg.swap_l5,
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
    sender: crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    const SYMBOLS_PER_CONN: usize = 100;
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
                let res = run_gate_spot_once(&endpoint, &chunk, ticker, l5, &sender).await;
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
    sender: crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    const SYMBOLS_PER_CONN: usize = 100;
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
                let res = run_gate_futures_once(&endpoint, &chunk, ticker, l5, &sender).await;
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

async fn run_gate_spot_once(
    endpoint: &str,
    symbols: &[(String, String)],
    ticker: bool,
    l5: bool,
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<(u64, Duration)> {
    let url = normalize_gate_spot_endpoint(endpoint);
    info!("connect gate spot: symbols={} url={}", symbols.len(), url);
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&url),
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
            // Many Gate deployments use payload: [symbol, limit, interval]
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "spot.order_book",
                "event": "subscribe",
                "payload": [sym, "5", "100ms"]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate spot subscribe order_book")?;
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
    loop {
        tokio::select! {
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
                        handle_gate_text("gate", false, &text, symbols, sender)?;
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
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<(u64, Duration)> {
    let url = normalize_gate_futures_endpoint(endpoint);
    info!(
        "connect gate futures: symbols={} url={}",
        symbols.len(),
        url
    );
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .context("gate futures connect: timeout")?
    .context("gate futures connect")?;
    let (mut write, mut read) = ws.split();

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
                "channel": "futures.order_book",
                "event": "subscribe",
                // Gate futures uses accuracy in seconds (string), e.g. "0.1" for 100ms.
                "payload": [sym, "5", "0.1"]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate futures subscribe order_book")?;
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
    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Ping(Vec::new())).await.context("gate futures active ping")?;
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
                        handle_gate_text("gate", true, &text, symbols, sender)?;
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
        ev.event_time = parse_ts_ms(result.get("t").or_else(|| v.get("time")));
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

    if channel.ends_with("order_book") {
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
                Stream::FutureL5
            } else {
                Stream::SpotL5
            },
            stored.to_string(),
        );
        ev.local_ts = local_ts;
        ev.time_str = time_str;
        ev.event_time = parse_ts_ms(result.get("t").or_else(|| v.get("time")));
        // Gate order_book:
        // - spot: `lastUpdateId`
        // - futures: `id`
        ev.update_id = if is_futures {
            get_u64(result, &["id", "u", "U", "lastUpdateId"])
        } else {
            get_u64(result, &["lastUpdateId", "u", "U", "id"])
        };

        let bids = result.get("bids");
        let asks = result.get("asks");
        fill_l5(&mut ev, bids, asks);
        if sender.try_send(ev).is_err() {
            crate::util::metrics::inc_dropped_events(1);
        }
        return Ok(());
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

fn fill_l5(
    ev: &mut MarketEvent,
    bids: Option<&serde_json::Value>,
    asks: Option<&serde_json::Value>,
) {
    let (b0p, b0q) = parse_level(bids, 0);
    let (a0p, a0q) = parse_level(asks, 0);
    ev.bid_px = b0p;
    ev.bid_qty = b0q;
    ev.ask_px = a0p;
    ev.ask_qty = a0q;

    let (p, q) = parse_level(bids, 0);
    ev.bid1_px = p;
    ev.bid1_qty = q;
    let (p, q) = parse_level(bids, 1);
    ev.bid2_px = p;
    ev.bid2_qty = q;
    let (p, q) = parse_level(bids, 2);
    ev.bid3_px = p;
    ev.bid3_qty = q;
    let (p, q) = parse_level(bids, 3);
    ev.bid4_px = p;
    ev.bid4_qty = q;
    let (p, q) = parse_level(bids, 4);
    ev.bid5_px = p;
    ev.bid5_qty = q;
    let (p, q) = parse_level(asks, 0);
    ev.ask1_px = p;
    ev.ask1_qty = q;
    let (p, q) = parse_level(asks, 1);
    ev.ask2_px = p;
    ev.ask2_qty = q;
    let (p, q) = parse_level(asks, 2);
    ev.ask3_px = p;
    ev.ask3_qty = q;
    let (p, q) = parse_level(asks, 3);
    ev.ask4_px = p;
    ev.ask4_qty = q;
    let (p, q) = parse_level(asks, 4);
    ev.ask5_px = p;
    ev.ask5_qty = q;
}

fn parse_level(levels: Option<&serde_json::Value>, idx: usize) -> (Option<f64>, Option<f64>) {
    let Some(levels) = levels else {
        return (None, None);
    };
    let Some(arr) = levels.as_array() else {
        return (None, None);
    };
    let Some(pair) = arr.get(idx) else {
        return (None, None);
    };
    if let Some(pair_arr) = pair.as_array() {
        let px = pair_arr.get(0).and_then(parse_f64_value);
        let qty = pair_arr.get(1).and_then(parse_f64_value);
        return (px, qty);
    }
    if let Some(obj) = pair.as_object() {
        // Gate futures order_book uses objects like {"p":"87070.3","s":8757}
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
        return (px, qty);
    }
    (None, None)
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
