use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::config::KucoinConfig;
use crate::util::pacer::Pacer;

use crate::schema::event::{MarketEvent, Stream};
use crate::util::time::{format_time_str_ms, normalize_epoch_to_ms, now_ms};

// KuCoin WS has strict control-message rate limits (subscribe + ping/pong text frames).
// We support both per-connection pacing (`subscribe_delay_ms`) and an optional *global* pacer shared
// across all KuCoin connections (spot + futures) to avoid 509 throttles when subscribing many topics.

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

#[derive(Debug, Clone)]
pub struct KucoinWsServer {
    pub endpoint: String,
    pub token: String,
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
struct BulletResp<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct BulletData {
    #[serde(rename = "instanceServers")]
    instance_servers: Vec<InstanceServer>,
    token: String,
    #[serde(rename = "expiresIn")]
    #[serde(default)]
    expires_in: Option<i64>, // token TTL for establishing a connection; not the WS session TTL
}

#[derive(Debug, Deserialize)]
struct InstanceServer {
    endpoint: String,
    #[serde(rename = "pingInterval")]
    ping_interval: u64,
    #[serde(rename = "pingTimeout")]
    #[serde(default)]
    ping_timeout: Option<u64>,
}

pub async fn fetch_public_ws_server(rest_endpoint: &str) -> anyhow::Result<KucoinWsServer> {
    let base = rest_endpoint.trim_end_matches('/');
    let url = format!("{base}/api/v1/bullet-public");
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .send()
        .await
        .context("kucoin bullet-public request")?
        .error_for_status()
        .context("kucoin bullet-public status")?;
    let body: BulletResp<BulletData> = resp.json().await.context("kucoin bullet-public json")?;
    let server = body
        .data
        .instance_servers
        .into_iter()
        .next()
        .context("kucoin bullet-public: no instanceServers")?;
    // Note: `expiresIn` is the token TTL for establishing a WS connection. Once connected,
    // the session can remain active without reconnecting when the token expires.
    let _ = body.data.expires_in;
    let ping_timeout_ms = server.ping_timeout.unwrap_or(10_000);
    Ok(KucoinWsServer {
        endpoint: server.endpoint,
        token: body.data.token,
        ping_interval_ms: server.ping_interval,
        ping_timeout_ms,
    })
}

pub fn normalize_symbol(input: &str) -> (String, String) {
    // KuCoin topic uses BTC-USDT, but we store symbol as BTCUSDT for file key consistency.
    let topic_symbol = if input.contains('-') {
        input.to_ascii_uppercase()
    } else if input.len() > 3 {
        // naive conversion: BTCUSDT -> BTC-USDT by splitting last 4 as quote if ends with USDT/USDC/etc
        // keep conservative: only convert USDT/USDC/BTC/ETH/BNB/DAI/FDUSD
        let upper = input.to_ascii_uppercase();
        let quotes = ["USDT", "USDC", "BTC", "ETH", "BNB", "DAI", "FDUSD"];
        let quote = quotes.iter().find(|q| upper.ends_with(*q));
        if let Some(q) = quote {
            let base = &upper[..upper.len() - q.len()];
            format!("{base}-{q}")
        } else {
            upper
        }
    } else {
        input.to_ascii_uppercase()
    };
    let stored_symbol = topic_symbol.replace('-', "");
    (topic_symbol, stored_symbol)
}

#[derive(Debug, Deserialize)]
struct KucoinResp<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct KucoinSpotSymbolInfo {
    symbol: String,
    #[serde(rename = "enableTrading")]
    #[serde(default)]
    enable_trading: bool,
}

async fn fetch_spot_symbol_set(
    rest_endpoint: &str,
) -> anyhow::Result<std::collections::HashSet<String>> {
    let base = rest_endpoint.trim_end_matches('/');
    let url = format!("{base}/api/v1/symbols");
    let client = reqwest::Client::new();
    let resp = client
        .get(url)
        .send()
        .await
        .context("kucoin symbols request")?
        .error_for_status()
        .context("kucoin symbols status")?;
    let body: KucoinResp<Vec<KucoinSpotSymbolInfo>> =
        resp.json().await.context("kucoin symbols json")?;
    let mut out = std::collections::HashSet::new();
    for s in body.data {
        if s.enable_trading {
            out.insert(s.symbol.to_ascii_uppercase());
        }
    }
    Ok(out)
}

fn split_topic_symbol(sym: &str) -> Option<(String, String)> {
    let s = sym.trim().to_ascii_uppercase();
    if let Some((base, quote)) = s.rsplit_once('-') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
        return None;
    }
    let quotes = ["USDT", "USDC", "USD", "BTC", "ETH", "BNB", "DAI", "FDUSD"];
    let (base, quote) = quotes
        .iter()
        .find_map(|q| s.strip_suffix(q).map(|b| (b.to_string(), q.to_string())))?;
    if base.is_empty() {
        return None;
    }
    Some((base, quote))
}

fn preferred_quotes<'a>(quote: &'a str) -> Vec<&'a str> {
    match quote.to_ascii_uppercase().as_str() {
        "USDC" => vec!["USDC", "USDT"],
        "USDT" => vec!["USDT", "USDC"],
        _ => vec![quote],
    }
}

fn select_kucoin_spot_symbol(
    active: &std::collections::HashSet<String>,
    desired: &str,
) -> Option<String> {
    let (topic_sym, _stored) = normalize_symbol(desired);
    if active.contains(&topic_sym) {
        return Some(topic_sym);
    }
    let (base, quote) = split_topic_symbol(&topic_sym)?;
    for q in preferred_quotes(&quote) {
        let cand = format!("{base}-{q}");
        if active.contains(&cand) {
            return Some(cand);
        }
    }
    None
}

fn build_topics_from_symbols(
    symbols: &[String],
    ticker: bool,
    l5: bool,
    trade: bool,
) -> Vec<(String, Stream, String)> {
    let mut out = Vec::new();
    for topic_symbol in symbols {
        let topic_symbol = topic_symbol.to_ascii_uppercase();
        let stored_symbol = topic_symbol.replace('-', "");
        if ticker {
            out.push((
                format!("/market/ticker:{topic_symbol}"),
                Stream::SpotBook,
                stored_symbol.clone(),
            ));
        }
        if l5 {
            out.push((
                // KuCoin spot L5 topic prefix is `/spotMarket/…` (the older `/market/…` prefix returns 404).
                format!("/spotMarket/level2Depth5:{topic_symbol}"),
                Stream::SpotL5,
                stored_symbol.clone(),
            ));
        }
        if trade {
            out.push((
                format!("/market/match:{topic_symbol}"),
                Stream::SpotTrade,
                stored_symbol.clone(),
            ));
        }
    }
    out
}

#[derive(Debug, Deserialize)]
struct KucoinMsg<T> {
    #[serde(rename = "type")]
    #[allow(dead_code)]
    ty: String,
    #[allow(dead_code)]
    topic: Option<String>,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct TickerData {
    #[serde(rename = "bestBid")]
    best_bid: Option<String>,
    #[serde(rename = "bestBidSize")]
    best_bid_size: Option<String>,
    #[serde(rename = "bestAsk")]
    best_ask: Option<String>,
    #[serde(rename = "bestAskSize")]
    best_ask_size: Option<String>,
    // milliseconds
    time: Option<i64>,
}

fn parse_f64(s: &Option<String>) -> Option<f64> {
    s.as_deref().and_then(|v| v.parse::<f64>().ok())
}

fn parse_level_from_value(
    levels: Option<&serde_json::Value>,
    idx: usize,
) -> (Option<f64>, Option<f64>) {
    let Some(levels) = levels else {
        return (None, None);
    };
    let Some(arr) = levels.as_array() else {
        return (None, None);
    };
    let Some(pair) = arr.get(idx) else {
        return (None, None);
    };
    let Some(pair_arr) = pair.as_array() else {
        return (None, None);
    };
    let px = pair_arr.get(0).and_then(parse_f64_value);
    let qty = pair_arr.get(1).and_then(parse_f64_value);
    (px, qty)
}

fn parse_f64_value(v: &serde_json::Value) -> Option<f64> {
    if let Some(x) = v.as_f64() {
        return Some(x);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
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

pub async fn spawn_kucoin_public_ws(
    cfg: KucoinConfig,
    pacer: Option<Arc<Pacer>>,
    sender: crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    let mut chosen_symbols: Vec<String> = Vec::new();
    match fetch_spot_symbol_set(&cfg.rest_endpoint).await {
        Ok(active) => {
            let mut skipped = 0usize;
            let mut fallback_usdc = 0usize;
            for s in &cfg.symbols {
                let desired = normalize_symbol(s).0;
                match select_kucoin_spot_symbol(&active, s) {
                    Some(chosen) => {
                        if chosen.ends_with("-USDC") && !desired.ends_with("-USDC") {
                            fallback_usdc += 1;
                        }
                        chosen_symbols.push(chosen);
                    }
                    None => skipped += 1,
                }
            }
            if skipped > 0 {
                warn!("kucoin spot: skip {skipped} unsupported symbols");
            }
            if fallback_usdc > 0 {
                warn!("kucoin spot: fallback to USDC for {fallback_usdc} symbols");
            }
        }
        Err(err) => {
            warn!("kucoin spot: fetch symbols failed: {err:#} (using configured list)");
            for s in &cfg.symbols {
                chosen_symbols.push(normalize_symbol(s).0);
            }
        }
    }

    let topics = build_topics_from_symbols(&chosen_symbols, cfg.ticker, cfg.l5, cfg.trade);
    if topics.is_empty() {
        anyhow::bail!("kucoin.enabled=true but no topics enabled / symbols empty");
    }

    // KuCoin has per-connection subscription limits; keep conservative.
    const TOPICS_PER_CONN: usize = 100;
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for (idx, chunk) in topics.chunks(TOPICS_PER_CONN).enumerate() {
        let cfg = cfg.clone();
        let pacer = pacer.clone();
        let sender = sender.clone();
        let chunk: Vec<(String, Stream, String)> = chunk.to_vec();
        let start_delay = Duration::from_millis(cfg.conn_stagger_ms.saturating_mul(idx as u64));
        handles.push(tokio::spawn(async move {
            if start_delay.as_millis() > 0 {
                tokio::time::sleep(start_delay).await;
            }
            let mut backoff_secs = 1u64;
            let mut ended_warn_count = 0u64;
            let mut last_backoff_logged = 0u64;
            loop {
                let res = run_kucoin_public_ws_once(&cfg, pacer.as_deref(), &chunk, &sender).await;
                if let Err(err) = &res {
                    ended_warn_count += 1;
                    if ended_warn_count == 1
                        || backoff_secs != last_backoff_logged
                        || ended_warn_count % 10 == 0
                    {
                        warn!(
                            "kucoin ws ended: {err:#} backoff_secs={} restarts={}",
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

async fn run_kucoin_public_ws_once(
    cfg: &KucoinConfig,
    pacer: Option<&Pacer>,
    topics: &[(String, Stream, String)],
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<(u64, Duration)> {
    let server = fetch_public_ws_server(&cfg.rest_endpoint).await?;
    let connect_id = format!("md{}", now_ms());
    let endpoint = normalize_ws_endpoint(&server.endpoint);
    let url = format!("{endpoint}?token={}&connectId={}", server.token, connect_id);

    info!(
        "connect kucoin: topics={} endpoint={}",
        topics.len(),
        server.endpoint
    );

    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .context("kucoin connect: timeout")?
    .context("kucoin connect")?;

    let (mut write, mut read) = ws.split();

    // subscribe
    for (i, (topic, _stream, _symbol)) in topics.iter().enumerate() {
        let id = format!("sub-{i}-{connect_id}");
        let msg = serde_json::json!({
            "id": id,
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true
        });
        if let Some(p) = pacer {
            p.wait().await;
        }
        write
            .send(Message::Text(msg.to_string()))
            .await
            .context("kucoin send subscribe")?;
        // Avoid bursts that can trigger server-side throttles. When a global pacer is used, this
        // per-connection delay is redundant and intentionally skipped.
        if pacer.is_none() && cfg.subscribe_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(cfg.subscribe_delay_ms)).await;
        }
    }

    // heartbeat (KuCoin expects text ping/pong, not just WS ping frames)
    let ping_every_ms = (server.ping_interval_ms / 2).max(1000);
    let ping_timeout_ms = server.ping_timeout_ms.max(1000);
    let mut ping = tokio::time::interval(Duration::from_millis(ping_every_ms));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watchdog = tokio::time::interval(Duration::from_millis(1000));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let started = std::time::Instant::now();
    let mut frames = 0u64;
    let mut last_msg_ts = now_ms();
    let mut last_pong_ts = now_ms();

    loop {
        tokio::select! {
            _ = ping.tick() => {
                let id = format!("ping-{}", now_ms());
                let msg = serde_json::json!({ "id": id, "type": "ping" });
                write.send(Message::Text(msg.to_string())).await.context("kucoin send ping")?;
            }
            _ = watchdog.tick() => {
                let now = now_ms();
                if now.saturating_sub(last_pong_ts) > ping_timeout_ms as i64 {
                    anyhow::bail!("kucoin pong timeout: now-last_pong_ms={}", now.saturating_sub(last_pong_ts));
                }
                if now.saturating_sub(last_msg_ts) > (2 * ping_every_ms) as i64 {
                    anyhow::bail!("kucoin idle timeout: now-last_msg_ms={}", now.saturating_sub(last_msg_ts));
                }
            }
            next = read.next() => {
                let Some(msg) = next else { break };
                let msg = msg.context("kucoin read msg")?;
                frames += 1;
                last_msg_ts = now_ms();
                match msg {
                    Message::Text(text) => {
                        if text.contains(r#""type":"pong""#) {
                            last_pong_ts = now_ms();
                        } else {
                            handle_kucoin_text(&text, topics, sender)?;
                        }
                    }
                    Message::Binary(_) => {}
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.context("kucoin send pong")?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    Message::Frame(_) => {}
                }
            }
        }
    }

    Ok((frames, started.elapsed()))
}

pub fn normalize_ws_endpoint(raw: &str) -> String {
    // KuCoin docs use ".../endpoint?token=...&connectId=..."
    // Some bullet responses return endpoint with or without the "/endpoint" suffix.
    let trimmed = raw.trim_end_matches('/');
    if trimmed.ends_with("/endpoint") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/endpoint")
    }
}

fn handle_kucoin_text(
    text: &str,
    topics: &[(String, Stream, String)],
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    // Fast-path ignore acks and pongs.
    if text.contains(r#""type":"pong""#)
        || text.contains(r#""type":"ack""#)
        || text.contains(r#""type":"welcome""#)
    {
        return Ok(());
    }

    // Determine topic, then parse into the right payload type.
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
    if ty == "error" {
        warn!("kucoin ws error: {}", truncate_for_log(text, 512));
        return Ok(());
    }
    if ty != "message" {
        return Ok(());
    }
    let topic = match v.get("topic").and_then(|x| x.as_str()) {
        Some(t) => t.to_string(),
        None => return Ok(()),
    };

    let (stream, stored_symbol) = match topics.iter().find_map(|(t, s, sym)| {
        if t.eq_ignore_ascii_case(&topic) {
            Some((s.clone(), sym.as_str()))
        } else {
            None
        }
    }) {
        Some(x) => x,
        None => return Ok(()),
    };

    match stream {
        Stream::SpotBook => {
            let update_id = v
                .get("data")
                .and_then(|d| d.get("sequence"))
                .and_then(parse_u64_value);
            let msg: KucoinMsg<TickerData> = match serde_json::from_value(v) {
                Ok(m) => m,
                Err(err) => {
                    warn!(
                        "kucoin parse ticker failed: {err:#} topic={} text={}",
                        topic,
                        truncate_for_log(text, 512)
                    );
                    return Ok(());
                }
            };
            let Some(data) = msg.data else {
                return Ok(());
            };
            let local_ts = now_ms();
            let mut ev = MarketEvent::new("kucoin", Stream::SpotBook, stored_symbol.to_string());
            ev.local_ts = local_ts;
            ev.time_str = format_time_str_ms(local_ts);
            ev.update_id = update_id;
            ev.event_time = data.time.map(normalize_epoch_to_ms);
            ev.bid_px = parse_f64(&data.best_bid);
            ev.bid_qty = parse_f64(&data.best_bid_size);
            ev.ask_px = parse_f64(&data.best_ask);
            ev.ask_qty = parse_f64(&data.best_ask_size);
            if sender.try_send(ev).is_err() {
                crate::util::metrics::inc_dropped_events(1);
            }
        }
        Stream::SpotL5 => {
            let data = match v.get("data") {
                Some(d) => d,
                None => return Ok(()),
            };
            let local_ts = now_ms();
            let mut ev = MarketEvent::new("kucoin", Stream::SpotL5, stored_symbol.to_string());
            ev.local_ts = local_ts;
            ev.time_str = format_time_str_ms(local_ts);
            // KuCoin spot L5 messages often don't carry a sequence/update id; don't fake it with timestamp.
            ev.update_id = data.get("sequence").and_then(parse_u64_value);
            ev.event_time = data
                .get("timestamp")
                .and_then(|x| x.as_i64())
                .or_else(|| data.get("time").and_then(|x| x.as_i64()))
                .or_else(|| data.get("ts").and_then(|x| x.as_i64()))
                .map(normalize_epoch_to_ms);

            let bids = data.get("bids");
            let asks = data.get("asks");

            let (b0p, b0q) = parse_level_from_value(bids, 0);
            let (a0p, a0q) = parse_level_from_value(asks, 0);
            ev.bid_px = b0p;
            ev.bid_qty = b0q;
            ev.ask_px = a0p;
            ev.ask_qty = a0q;

            let (p, q) = parse_level_from_value(bids, 0);
            ev.bid1_px = p;
            ev.bid1_qty = q;
            let (p, q) = parse_level_from_value(bids, 1);
            ev.bid2_px = p;
            ev.bid2_qty = q;
            let (p, q) = parse_level_from_value(bids, 2);
            ev.bid3_px = p;
            ev.bid3_qty = q;
            let (p, q) = parse_level_from_value(bids, 3);
            ev.bid4_px = p;
            ev.bid4_qty = q;
            let (p, q) = parse_level_from_value(bids, 4);
            ev.bid5_px = p;
            ev.bid5_qty = q;
            let (p, q) = parse_level_from_value(asks, 0);
            ev.ask1_px = p;
            ev.ask1_qty = q;
            let (p, q) = parse_level_from_value(asks, 1);
            ev.ask2_px = p;
            ev.ask2_qty = q;
            let (p, q) = parse_level_from_value(asks, 2);
            ev.ask3_px = p;
            ev.ask3_qty = q;
            let (p, q) = parse_level_from_value(asks, 3);
            ev.ask4_px = p;
            ev.ask4_qty = q;
            let (p, q) = parse_level_from_value(asks, 4);
            ev.ask5_px = p;
            ev.ask5_qty = q;

            if sender.try_send(ev).is_err() {
                crate::util::metrics::inc_dropped_events(1);
            }
        }
        Stream::SpotTrade => {
            let data = match v.get("data") {
                Some(d) => d,
                None => return Ok(()),
            };
            let local_ts = now_ms();
            let mut ev = MarketEvent::new("kucoin", Stream::SpotTrade, stored_symbol.to_string());
            ev.local_ts = local_ts;
            ev.time_str = format_time_str_ms(local_ts);
            ev.update_id = data.get("sequence").and_then(parse_u64_value);
            let trade_ts = data
                .get("ts")
                .and_then(parse_u64_value)
                .or_else(|| data.get("time").and_then(parse_u64_value))
                .map(|v| normalize_epoch_to_ms(v as i64));
            ev.event_time = trade_ts;
            ev.trade_time = trade_ts;
            let side = data.get("side").and_then(|x| x.as_str()).unwrap_or("");
            let px = data
                .get("price")
                .and_then(|x| x.as_str().and_then(|s| s.parse::<f64>().ok()))
                .or_else(|| data.get("p").and_then(|x| x.as_str().and_then(|s| s.parse::<f64>().ok())));
            let qty = data
                .get("size")
                .and_then(|x| x.as_str().and_then(|s| s.parse::<f64>().ok()))
                .or_else(|| data.get("amount").and_then(|x| x.as_str().and_then(|s| s.parse::<f64>().ok())))
                .or_else(|| data.get("q").and_then(|x| x.as_str().and_then(|s| s.parse::<f64>().ok())));
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
        }
        _ => {}
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

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    #[test]
    fn parse_ticker_ok() {
        let topics = vec![(
            "/market/ticker:BTC-USDT".to_string(),
            Stream::SpotBook,
            "BTCUSDT".to_string(),
        )];
        let text = r#"{
            "type":"message",
            "topic":"/market/ticker:BTC-USDT",
            "data":{
                "sequence":"123",
                "bestBid":"100.1",
                "bestBidSize":"2.5",
                "bestAsk":"100.2",
                "bestAskSize":"3.0",
                "time":1700000000000
            }
        }"#;

        let (tx, rx) = unbounded();
        handle_kucoin_text(text, &topics, &tx).unwrap();
        let ev = rx.recv().unwrap();
        assert_eq!(ev.exchange, "kucoin");
        assert!(matches!(ev.stream, Stream::SpotBook));
        assert_eq!(ev.symbol, "BTCUSDT");
        assert_eq!(ev.update_id, Some(123));
        assert_eq!(ev.event_time, Some(1700000000000));
        assert_eq!(ev.bid_px, Some(100.1));
        assert_eq!(ev.bid_qty, Some(2.5));
        assert_eq!(ev.ask_px, Some(100.2));
        assert_eq!(ev.ask_qty, Some(3.0));
        assert!(ev.local_ts > 0);
        assert!(!ev.time_str.is_empty());
    }

    #[test]
    fn parse_depth5_ok() {
        let topics = vec![(
            "/spotMarket/level2Depth5:BTC-USDT".to_string(),
            Stream::SpotL5,
            "BTCUSDT".to_string(),
        )];
        let text = r#"{
            "type":"message",
            "topic":"/spotMarket/level2Depth5:BTC-USDT",
            "data":{
                "bids":[["100.1","2.5"],["100.0","1.0"],["99.9","0.5"],["99.8","0.1"],["99.7","0.01"]],
                "asks":[["100.2","3.0"],["100.3","1.2"],["100.4","0.4"],["100.5","0.3"],["100.6","0.2"]],
                "timestamp":1700000000100
            }
        }"#;

        let (tx, rx) = unbounded();
        handle_kucoin_text(text, &topics, &tx).unwrap();
        let ev = rx.recv().unwrap();
        assert_eq!(ev.exchange, "kucoin");
        assert!(matches!(ev.stream, Stream::SpotL5));
        assert_eq!(ev.symbol, "BTCUSDT");
        assert_eq!(ev.update_id, None);
        assert_eq!(ev.event_time, Some(1700000000100));
        assert_eq!(ev.bid_px, Some(100.1));
        assert_eq!(ev.ask_px, Some(100.2));
        assert_eq!(ev.bid1_px, Some(100.1));
        assert_eq!(ev.bid1_qty, Some(2.5));
        assert_eq!(ev.bid5_px, Some(99.7));
        assert_eq!(ev.ask5_px, Some(100.6));
    }

    #[test]
    fn parse_depth5_numeric_ok() {
        let topics = vec![(
            "/spotMarket/level2Depth5:BTC-USDT".to_string(),
            Stream::SpotL5,
            "BTCUSDT".to_string(),
        )];
        let text = r#"{
            "type":"message",
            "topic":"/spotMarket/level2Depth5:BTC-USDT",
            "data":{
                "bids":[[100.1,2.5],[100.0,1.0],[99.9,0.5],[99.8,0.1],[99.7,0.01]],
                "asks":[[100.2,3.0],[100.3,1.2],[100.4,0.4],[100.5,0.3],[100.6,0.2]],
                "timestamp":1700000000100
            }
        }"#;

        let (tx, rx) = unbounded();
        handle_kucoin_text(text, &topics, &tx).unwrap();
        let ev = rx.recv().unwrap();
        assert!(matches!(ev.stream, Stream::SpotL5));
        assert_eq!(ev.bid1_px, Some(100.1));
        assert_eq!(ev.ask1_qty, Some(3.0));
    }

    #[test]
    fn parse_trade_ts_ok() {
        let topics = vec![(
            "/market/match:BTC-USDT".to_string(),
            Stream::SpotTrade,
            "BTCUSDT".to_string(),
        )];
        let text = r#"{
            "type":"message",
            "topic":"/market/match:BTC-USDT",
            "data":{
                "sequence":"456",
                "side":"buy",
                "price":"100.5",
                "size":"1.25",
                "ts":1700000000200
            }
        }"#;
        let (tx, rx) = unbounded();
        handle_kucoin_text(text, &topics, &tx).unwrap();
        let ev = rx.recv().unwrap();
        assert_eq!(ev.exchange, "kucoin");
        assert!(matches!(ev.stream, Stream::SpotTrade));
        assert_eq!(ev.symbol, "BTCUSDT");
        assert_eq!(ev.update_id, Some(456));
        assert_eq!(ev.event_time, Some(1700000000200));
        assert_eq!(ev.trade_time, Some(1700000000200));
        assert_eq!(ev.ask_px, Some(100.5));
        assert_eq!(ev.ask_qty, Some(1.25));
    }
}
