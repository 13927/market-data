use std::time::Duration;
use std::collections::{BTreeMap, HashMap, HashSet};

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
            let mut ended_warn_count = 0u64;
            let mut last_backoff_logged = 0u64;
            loop {
                let res = run_gate_spot_once(&endpoint, &chunk, ticker, l5, trade, &sender).await;
                if let Err(err) = &res {
                    ended_warn_count += 1;
                    if ended_warn_count == 1
                        || backoff_secs != last_backoff_logged
                        || ended_warn_count % 10 == 0
                    {
                        warn!(
                            "gate spot ws ended: {err:#} backoff_secs={} restarts={}",
                            backoff_secs, ended_warn_count
                        );
                        last_backoff_logged = backoff_secs;
                    }
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
            let mut ended_warn_count = 0u64;
            let mut last_backoff_logged = 0u64;
            loop {
                let res = run_gate_futures_once(&endpoint, &chunk, ticker, l5, trade, &sender).await;
                if let Err(err) = &res {
                    ended_warn_count += 1;
                    if ended_warn_count == 1
                        || backoff_secs != last_backoff_logged
                        || ended_warn_count % 10 == 0
                    {
                        warn!(
                            "gate futures ws ended: {err:#} backoff_secs={} restarts={}",
                            backoff_secs, ended_warn_count
                        );
                        last_backoff_logged = backoff_secs;
                    }
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

// REST 快照处理已移除

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
    let mut subscribed_obu: HashSet<String> = HashSet::new();
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
            let stream = format!("ob.{sym}.50");
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "spot.obu",
                "event": "subscribe",
                "payload": [stream]
            });

            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate spot subscribe order_book_v2 (obu)")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
            subscribed_obu.insert(stream);
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

    // REST 快照已取消

    let started = std::time::Instant::now();
    let mut frames = 0u64;
    let mut last_msg_ts = std::time::Instant::now();
    let mut books = HashMap::new();
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
                        if let Some(resub) = handle_gate_text("gate", false, &text, symbols, sender, &mut books)? {
                            if subscribed_obu.contains(&resub) {
                                let un = serde_json::json!({
                                    "time": now_ms() / 1000,
                                    "channel": "spot.obu",
                                    "event": "unsubscribe",
                                    "payload": [resub]
                                });
                                write.send(Message::Text(un.to_string())).await.context("gate spot unsubscribe obu")?;
                                tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
                                subscribed_obu.remove(&resub);
                            }
                            let sub = serde_json::json!({
                                "time": now_ms() / 1000,
                                "channel": "spot.obu",
                                "event": "subscribe",
                                "payload": [resub]
                            });
                            write.send(Message::Text(sub.to_string())).await.context("gate spot resubscribe obu")?;
                            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
                            subscribed_obu.insert(resub);
                        }
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
    let mut subscribed_contracts: HashSet<String> = HashSet::new();

    // REST 快照已取消

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
            let stream = format!("ob.{sym}.50");
            let msg = serde_json::json!({
                "time": now_ms() / 1000,
                "channel": "futures.obu",
                "event": "subscribe",
                "payload": [stream]
            });
            write
                .send(Message::Text(msg.to_string()))
                .await
                .context("gate futures subscribe order_book")?;
            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
            subscribed_contracts.insert(stream);
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
                        if let Some(resub) = handle_gate_text("gate", true, &text, symbols, sender, &mut books)? {
                            if subscribed_contracts.contains(&resub) {
                                let un = serde_json::json!({
                                    "time": now_ms() / 1000,
                                    "channel": "futures.obu",
                                    "event": "unsubscribe",
                                    "payload": [resub]
                                });
                                write.send(Message::Text(un.to_string())).await.context("gate futures unsubscribe obu")?;
                                tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
                                subscribed_contracts.remove(&resub);
                            }
                            let sub = serde_json::json!({
                                "time": now_ms() / 1000,
                                "channel": "futures.obu",
                                "event": "subscribe",
                                "payload": [resub]
                            });
                            write.send(Message::Text(sub.to_string())).await.context("gate futures resubscribe obu")?;
                            tokio::time::sleep(Duration::from_millis(GATE_SUBSCRIBE_DELAY_MS)).await;
                            subscribed_contracts.insert(resub);
                        }
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
) -> anyhow::Result<Option<String>> {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(None),
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
        return Ok(None);
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
            return Ok(None);
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
        return Ok(None);
    }

    if channel.ends_with("order_book") || channel.ends_with("order_book_update") || channel.ends_with("obu") {
        let sym_opt = get_str(result, &["s", "currency_pair", "contract"]).or_else(|| {
            v.get("payload")
                .and_then(|p| p.get(0))
                .and_then(|x| x.as_str())
        });
        let Some(raw_sym) = sym_opt else {
            return Ok(None);
        };
        let sym_owned: String = if raw_sym.starts_with("ob.") {
            let s = &raw_sym[3..];
            match s.rsplit_once('.') {
                Some((pair, _level)) => pair.to_string(),
                None => s.to_string(),
            }
        } else {
            raw_sym.to_string()
        };
        let stored = symbols
            .iter()
            .find(|(s, _)| s.eq_ignore_ascii_case(&sym_owned))
            .map(|(_, stored)| stored.as_str())
            .unwrap_or_else(|| sym_owned.as_str());

        let full = result.get("full").and_then(|x| x.as_bool()).unwrap_or(false);
        let book = books.entry(stored.to_string()).or_insert_with(LocalOrderBook::new);
        let start_id = get_u64(result, &["U"]);
        let end_id = if is_futures {
            get_u64(result, &["u", "id", "lastUpdateId", "current"])
        } else {
            get_u64(result, &["u", "lastUpdateId", "id", "current"])
        };

        if full {
            book.bids.clear();
            book.asks.clear();
            if let Some(eid) = end_id {
                info!("gate obu full: symbol={} end_id={}", stored, eid);
            } else {
                info!("gate obu full: symbol={} end_id=(none)", stored);
            }
        } else {
            match (book.last_id, start_id) {
                (Some(prev), Some(start)) if start == prev + 1 => {
                    info!(
                        "gate obu inc: symbol={} U={} u={} last_id={} ok",
                        stored,
                        start,
                        end_id.unwrap_or(0),
                        prev
                    );
                }
                _ => {
                    book.bids.clear();
                    book.asks.clear();
                    book.last_id = None;
                    let resub_payload = format!("ob.{sym_owned}.50");
                    warn!(
                        "gate obu break: symbol={} U={:?} last_id={:?} -> resub {:?}",
                        stored, start_id, book.last_id, resub_payload
                    );
                    return Ok(Some(resub_payload));
                }
            }
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
        if let Some(id) = end_id {
            book.last_id = Some(id);
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
            get_u64(result, &["id", "u", "U", "lastUpdateId", "current"])
        } else {
            get_u64(result, &["lastUpdateId", "u", "U", "id", "current"])
        };

        let mut bids_iter = book.bids.iter().rev();
        if let Some((&p, &q)) = bids_iter.next() {
            let px = f64::from_bits(p);
            ev.bid_px = Some(px);
            ev.bid_qty = Some(q);
            ev.bid1_px = Some(px);
            ev.bid1_qty = Some(q);
        }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid2_px = Some(f64::from_bits(p)); ev.bid2_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid3_px = Some(f64::from_bits(p)); ev.bid3_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid4_px = Some(f64::from_bits(p)); ev.bid4_qty = Some(q); }
        if let Some((&p, &q)) = bids_iter.next() { ev.bid5_px = Some(f64::from_bits(p)); ev.bid5_qty = Some(q); }

        let mut asks_iter = book.asks.iter();
        if let Some((&p, &q)) = asks_iter.next() {
            let px = f64::from_bits(p);
            ev.ask_px = Some(px);
            ev.ask_qty = Some(q);
            ev.ask1_px = Some(px);
            ev.ask1_qty = Some(q);
        }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask2_px = Some(f64::from_bits(p)); ev.ask2_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask3_px = Some(f64::from_bits(p)); ev.ask3_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask4_px = Some(f64::from_bits(p)); ev.ask4_qty = Some(q); }
        if let Some((&p, &q)) = asks_iter.next() { ev.ask5_px = Some(f64::from_bits(p)); ev.ask5_qty = Some(q); }

        if sender.try_send(ev).is_err() {
            crate::util::metrics::inc_dropped_events(1);
        }
        return Ok(None);
    }

    if channel.ends_with("trades") {
        if let Some(arr) = result.as_array() {
            for item in arr {
                emit_gate_trade(exchange, is_futures, item, &v, symbols, sender, local_ts, &time_str)?;
            }
            return Ok(None);
        } else {
            emit_gate_trade(exchange, is_futures, result, &v, symbols, sender, local_ts, &time_str)?;
            return Ok(None);
        }
    }

    Ok(None)
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
    let txn_ts = parse_ts_ms(
        item.get("create_time_ms")
            .or_else(|| item.get("t"))
            .or_else(|| item.get("time_ms"))
            .or_else(|| item.get("time")),
    );
    let event_ts = parse_ts_ms(envelope.get("time"));
    ev.trade_time = txn_ts;
    ev.event_time = event_ts.or(txn_ts);
    let mut side = get_str(item, &["side", "S"])
        .map(|s| s.to_string())
        .unwrap_or_default();
    let px = get_f64(item, &["price", "p"]);
    let mut qty = get_f64(item, &["amount", "q", "size"]);
    if is_futures && side.is_empty() {
        if let Some(v) = item.get("size") {
            let parsed = if let Some(x) = v.as_f64() {
                Some(x)
            } else {
                v.as_str().and_then(|s| s.parse::<f64>().ok())
            };
            if let Some(raw) = parsed {
                let abs = raw.abs();
                if abs > 0.0 {
                    qty = Some(abs);
                    if raw > 0.0 {
                        side = "buy".to_string();
                    } else if raw < 0.0 {
                        side = "sell".to_string();
                    }
                }
            }
        }
    }
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
        if let Ok(x) = s.parse::<i64>() {
            x
        } else if let Ok(f) = s.parse::<f64>() {
            f.round() as i64
        } else {
            return None;
        }
    } else if let Some(f) = v.as_f64() {
        f.round() as i64
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

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    #[test]
    fn parse_spot_trade_ts_ok() {
        let symbols = vec![("BTC_USDT".to_string(), "BTCUSDT".to_string())];
        let text = r#"{
            "time":1700000000400,
            "channel":"spot.trades",
            "event":"update",
            "result":[
                {
                    "currency_pair":"BTC_USDT",
                    "side":"sell",
                    "price":"100.2",
                    "amount":"2.00",
                    "t":1700000000300,
                    "id":"789"
                }
            ]
        }"#;
        let (tx, rx) = unbounded();
        let mut books = HashMap::new();
        handle_gate_text("gate", false, text, &symbols, &tx, &mut books).unwrap();
        let ev = rx.recv().unwrap();
        assert_eq!(ev.exchange, "gate");
        assert!(matches!(ev.stream, Stream::SpotTrade));
        assert_eq!(ev.symbol, "BTCUSDT");
        assert_eq!(ev.update_id, Some(789));
        assert_eq!(ev.event_time, Some(1700000000400));
        assert_eq!(ev.trade_time, Some(1700000000300));
        assert_eq!(ev.bid_px, Some(100.2));
        assert_eq!(ev.bid_qty, Some(2.0));
    }

    #[test]
    fn parse_futures_trade_from_size_sign_ok() {
        let symbols = vec![("BTC_USDT".to_string(), "BTCUSDT".to_string())];
        let text = r#"{
            "time":1700000000500,
            "channel":"futures.trades",
            "event":"update",
            "result":[
                {
                    "contract":"BTC_USDT",
                    "create_time":1700000000,
                    "create_time_ms":1700000000400,
                    "id":640336948,
                    "price":"88800.8",
                    "size":"10"
                }
            ]
        }"#;
        let (tx, rx) = unbounded();
        let mut books = HashMap::new();
        handle_gate_text("gate", true, text, &symbols, &tx, &mut books).unwrap();
        let ev = rx.recv().unwrap();
        assert_eq!(ev.exchange, "gate");
        assert!(matches!(ev.stream, Stream::FutureTrade));
        assert_eq!(ev.symbol, "BTCUSDT");
        assert_eq!(ev.update_id, Some(640336948));
        assert_eq!(ev.event_time, Some(1700000000500));
        assert_eq!(ev.trade_time, Some(1700000000400));
        assert_eq!(ev.ask_px, Some(88800.8));
        assert_eq!(ev.ask_qty, Some(10.0));
    }

    #[test]
    fn spot_obu_incremental_continuous_ok() {
        let symbols = vec![("BTC_USDT".to_string(), "BTCUSDT".to_string())];
        let (tx, rx) = unbounded();
        let mut books = HashMap::new();
        let full = r#"{
            "time":1700000000000,
            "channel":"spot.obu",
            "event":"update",
            "result":{
                "s":"ob.BTC_USDT.50",
                "full":true,
                "u":100,
                "bids":[["100","1.0"]],
                "asks":[["101","2.0"]]
            }
        }"#;
        let resub = handle_gate_text("gate", false, full, &symbols, &tx, &mut books).unwrap();
        assert!(resub.is_none());
        let ev_full = rx.recv().unwrap();
        assert!(matches!(ev_full.stream, Stream::SpotL5));
        assert_eq!(ev_full.symbol, "BTCUSDT");
        assert_eq!(ev_full.update_id, Some(100));
        assert_eq!(ev_full.bid_px, Some(100.0));
        assert_eq!(ev_full.ask_px, Some(101.0));
        let inc = r#"{
            "time":1700000000100,
            "channel":"spot.obu",
            "event":"update",
            "result":{
                "s":"ob.BTC_USDT.50",
                "U":101,
                "u":102,
                "b":[["100","0"],["99","1.5"]],
                "a":[["101","3.0"]]
            }
        }"#;
        let resub2 = handle_gate_text("gate", false, inc, &symbols, &tx, &mut books).unwrap();
        assert!(resub2.is_none());
        let ev_inc = rx.recv().unwrap();
        assert!(matches!(ev_inc.stream, Stream::SpotL5));
        assert_eq!(ev_inc.symbol, "BTCUSDT");
        assert_eq!(ev_inc.update_id, Some(102));
        assert_eq!(ev_inc.bid_px, Some(99.0));
        assert_eq!(ev_inc.bid_qty, Some(1.5));
        assert_eq!(ev_inc.ask_px, Some(101.0));
        assert_eq!(ev_inc.ask_qty, Some(3.0));
    }

    #[test]
    fn spot_obu_incremental_break_triggers_resub() {
        let symbols = vec![("BTC_USDT".to_string(), "BTCUSDT".to_string())];
        let (tx, _rx) = unbounded();
        let mut books = HashMap::new();
        let full = r#"{
            "time":1700000000000,
            "channel":"spot.obu",
            "event":"update",
            "result":{
                "s":"ob.BTC_USDT.50",
                "full":true,
                "u":100,
                "bids":[["100","1.0"]],
                "asks":[["101","2.0"]]
            }
        }"#;
        let resub = handle_gate_text("gate", false, full, &symbols, &tx, &mut books).unwrap();
        assert!(resub.is_none());
        let break_inc = r#"{
            "time":1700000000200,
            "channel":"spot.obu",
            "event":"update",
            "result":{
                "s":"ob.BTC_USDT.50",
                "U":150,
                "u":151,
                "b":[["100","0"]],
                "a":[["101","1.0"]]
            }
        }"#;
        let resub2 = handle_gate_text("gate", false, break_inc, &symbols, &tx, &mut books).unwrap();
        assert_eq!(resub2, Some("ob.BTC_USDT.50".to_string()));
    }
}
