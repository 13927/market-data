use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::config::KucoinFuturesConfig;
use crate::util::pacer::Pacer;

use crate::schema::event::{MarketEvent, Stream};
use crate::util::time::{format_time_str_ms, normalize_epoch_to_ms, now_ms};

use super::kucoin::{fetch_public_ws_server, normalize_ws_endpoint};

// KuCoin futures WS has the same strict control-message rate limits as spot. We support an
// optional global pacer shared with spot to avoid 509 throttles when subscribing many topics.

pub async fn resolve_futures_contract_symbols(
    rest_endpoint: &str,
    symbols: &[String],
) -> anyhow::Result<Vec<String>> {
    // Prefer verifying via active contracts endpoint so we don't subscribe to non-existent topics.
    // KuCoin futures contract list: GET /api/v1/contracts/active
    #[derive(serde::Deserialize)]
    struct Resp<T> {
        data: T,
    }
    #[derive(serde::Deserialize)]
    struct Contract {
        symbol: String,
    }

    let base = rest_endpoint.trim_end_matches('/');
    let url = format!("{base}/api/v1/contracts/active");
    let client = reqwest::Client::new();
    let resp = client
        .get(url)
        .send()
        .await
        .context("kucoin_futures active contracts request")?
        .error_for_status()
        .context("kucoin_futures active contracts status")?;
    let body: Resp<Vec<Contract>> = resp
        .json()
        .await
        .context("kucoin_futures active contracts json")?;

    let mut active = std::collections::HashSet::new();
    for c in body.data {
        active.insert(c.symbol.to_ascii_uppercase());
    }

    let mut out = Vec::new();
    for s in symbols {
        let Some(candidates) = canonical_to_contract_candidates(s) else {
            warn!("kucoin_futures: cannot derive contract for symbol={}", s);
            continue;
        };
        let mut chosen: Option<String> = None;
        for cand in &candidates {
            if active.contains(cand) {
                chosen = Some(cand.clone());
                break;
            }
        }
        match chosen {
            Some(contract) => out.push(contract),
            None => warn!(
                "kucoin_futures: no active contract for symbol={} (tried={})",
                s,
                candidates.join(",")
            ),
        }
    }

    out.sort();
    out.dedup();
    Ok(out)
}

fn canonical_to_contract_candidates(symbol: &str) -> Option<Vec<String>> {
    // Canonical stored symbols are like BTCUSDT (no dash). We also accept BTC-USDT.
    let upper = symbol.trim().to_ascii_uppercase().replace('-', "");
    let quotes = ["USDT", "USDC", "USD"];
    let (base, quote) = quotes.iter().find_map(|q| {
        upper
            .strip_suffix(q)
            .map(|b| (b.to_string(), q.to_string()))
    })?;
    if base.is_empty() {
        return None;
    }
    let base = match base.as_str() {
        "BTC" => "XBT".to_string(),
        _ => base,
    };

    let quote_order: Vec<String> = match quote.as_str() {
        "USDC" => vec!["USDC".to_string(), "USDT".to_string()],
        "USDT" => vec!["USDT".to_string(), "USDC".to_string()],
        _ => vec![quote],
    };

    Some(
        quote_order
            .into_iter()
            .map(|q| format!("{base}{q}M"))
            .collect(),
    )
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

fn build_topics(cfg: &KucoinFuturesConfig) -> Vec<(String, Stream, String)> {
    let mut out = Vec::new();
    for s in &cfg.symbols {
        let topic_symbol = s.to_ascii_uppercase();
        let stored_symbol = canonical_stored_symbol_from_contract(&topic_symbol);
        if cfg.ticker {
            // KuCoin futures ticker V2
            out.push((
                format!("/contractMarket/tickerV2:{topic_symbol}"),
                Stream::FutureBook,
                stored_symbol.clone(),
            ));
        }
        if cfg.l5 {
            // KuCoin futures depth5
            out.push((
                format!("/contractMarket/level2Depth5:{topic_symbol}"),
                Stream::FutureL5,
                stored_symbol.clone(),
            ));
        }
        if cfg.trade {
            out.push((
                format!("/contractMarket/execution:{topic_symbol}"),
                Stream::FutureTrade,
                stored_symbol.clone(),
            ));
        }
    }
    out
}

fn canonical_stored_symbol_from_contract(contract: &str) -> String {
    // Examples (KuCoin futures):
    // - XBTUSDTM (BTC perpetual) -> BTCUSDT
    // - ETHUSDTM -> ETHUSDT
    // Heuristic:
    // - If ends with 'M', drop it.
    // - If ends with known quote (USDT/USDC/BTC/ETH), keep quote.
    // - Apply base aliases (XBT -> BTC).
    let mut s = contract.trim().to_ascii_uppercase();
    if let Some(stripped) = s.strip_suffix('M') {
        s = stripped.to_string();
    }

    let quotes = ["USDT", "USDC", "BTC", "ETH", "EUR", "USD"];
    let (base, quote) = quotes
        .iter()
        .find_map(|q| s.strip_suffix(q).map(|b| (b.to_string(), q.to_string())))
        .unwrap_or_else(|| (s.clone(), String::new()));

    let base = match base.as_str() {
        "XBT" => "BTC".to_string(),
        _ => base,
    };

    if quote.is_empty() {
        base
    } else {
        format!("{base}{quote}")
    }
}

pub fn spawn_kucoin_futures_public_ws(
    cfg: KucoinFuturesConfig,
    pacer: Option<Arc<Pacer>>,
    sender: crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    let topics = build_topics(&cfg);
    if topics.is_empty() {
        anyhow::bail!("kucoin_futures.enabled=true but no topics enabled / symbols empty");
    }

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
                let res = run_kucoin_futures_ws_once(&cfg, pacer.as_deref(), &chunk, &sender).await;
                if let Err(err) = &res {
                    ended_warn_count += 1;
                    if ended_warn_count == 1
                        || backoff_secs != last_backoff_logged
                        || ended_warn_count % 10 == 0
                    {
                        warn!(
                            "kucoin_futures ws ended: {err:#} backoff_secs={} restarts={}",
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

async fn run_kucoin_futures_ws_once(
    cfg: &KucoinFuturesConfig,
    pacer: Option<&Pacer>,
    topics: &[(String, Stream, String)],
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<(u64, Duration)> {
    let server = fetch_public_ws_server(&cfg.rest_endpoint).await?;
    let connect_id = format!("md{}", now_ms());
    let endpoint = normalize_ws_endpoint(&server.endpoint);
    let url = format!("{endpoint}?token={}&connectId={}", server.token, connect_id);

    info!(
        "connect kucoin_futures: topics={} endpoint={}",
        topics.len(),
        server.endpoint
    );

    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .context("kucoin_futures connect: timeout")?
    .context("kucoin_futures connect")?;
    let (mut write, mut read) = ws.split();

    // subscribe: send one subscribe frame per topic, mirroring the spot implementation.
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
            .context("kucoin_futures send subscribe")?;
        // Avoid bursts that can trigger server-side throttles. When a global pacer is used, this
        // per-connection delay is redundant and intentionally skipped.
        if pacer.is_none() && cfg.subscribe_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(cfg.subscribe_delay_ms)).await;
        }
    }

    let ping_every_ms = server.ping_interval_ms.saturating_sub(500).max(1000);
    let ping_timeout_ms = server.ping_timeout_ms.max(1000);

    let mut ping = tokio::time::interval(Duration::from_millis(ping_every_ms));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watchdog = tokio::time::interval(Duration::from_millis(1000));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_millis(0))
            .subsec_nanos() as u64;
        let max = (ping_every_ms / 4).max(1);
        tokio::time::sleep(Duration::from_millis(nanos % max)).await;
    }

    let started = std::time::Instant::now();
    let mut frames = 0u64;
    let mut last_msg_ts = now_ms();
    let mut last_pong_ts = now_ms();

    loop {
        tokio::select! {
            _ = ping.tick() => {
                let id = format!("ping-{}", now_ms());
                let msg = serde_json::json!({ "id": id, "type": "ping" });
                write.send(Message::Text(msg.to_string())).await.context("kucoin_futures send ping")?;
            }
            _ = watchdog.tick() => {
                let now = now_ms();
                if now.saturating_sub(last_pong_ts) > ping_timeout_ms as i64 {
                    anyhow::bail!("kucoin_futures pong timeout: now-last_pong_ms={}", now.saturating_sub(last_pong_ts));
                }
                if now.saturating_sub(last_msg_ts) > (2 * ping_every_ms) as i64 {
                    anyhow::bail!("kucoin_futures idle timeout: now-last_msg_ms={}", now.saturating_sub(last_msg_ts));
                }
            }
            next = read.next() => {
                let Some(msg) = next else { break };
                let msg = msg.context("kucoin_futures read msg")?;
                frames += 1;
                last_msg_ts = now_ms();
                match msg {
                    Message::Text(text) => {
                        if text.contains(r#""type":"pong""#) {
                            last_pong_ts = now_ms();
                        } else {
                            handle_text(&text, topics, sender)?;
                        }
                    }
                    Message::Binary(_) => {}
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.context("kucoin_futures send pong")?;
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

fn handle_text(
    text: &str,
    topics: &[(String, Stream, String)],
    sender: &crossbeam_channel::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    if text.contains(r#""type":"pong""#)
        || text.contains(r#""type":"ack""#)
        || text.contains(r#""type":"welcome""#)
    {
        return Ok(());
    }

    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
    if ty == "error" {
        warn!("kucoin_futures ws error: {}", truncate_for_log(text, 512));
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

    let data = match v.get("data") {
        Some(d) => d,
        None => return Ok(()),
    };

    match stream {
        Stream::FutureBook => {
            let local_ts = now_ms();
            let mut ev = MarketEvent::new("kucoin", Stream::FutureBook, stored_symbol.to_string());
            ev.local_ts = local_ts;
            ev.time_str = format_time_str_ms(local_ts);
            ev.update_id = data
                .get("sequence")
                .and_then(parse_u64_value)
                .or_else(|| data.get("id").and_then(parse_u64_value));
            // KuCoin futures `ts` can be nanoseconds (tickerV2) or milliseconds (depth).
            ev.event_time = data
                .get("ts")
                .and_then(|x| x.as_i64())
                .or_else(|| data.get("time").and_then(|x| x.as_i64()))
                .map(normalize_epoch_to_ms);

            // best bid/ask fields can vary; try common names
            ev.bid_px = get_f64(data, &["bestBidPrice", "bestBid"]);
            ev.bid_qty = get_f64(data, &["bestBidSize", "bestBidSizeValue", "bestBidQty"]);
            ev.ask_px = get_f64(data, &["bestAskPrice", "bestAsk"]);
            ev.ask_qty = get_f64(data, &["bestAskSize", "bestAskSizeValue", "bestAskQty"]);

            if sender.try_send(ev).is_err() {
                crate::util::metrics::inc_dropped_events(1);
            }
        }
        Stream::FutureL5 => {
            let local_ts = now_ms();
            let mut ev = MarketEvent::new("kucoin", Stream::FutureL5, stored_symbol.to_string());
            ev.local_ts = local_ts;
            ev.time_str = format_time_str_ms(local_ts);
            ev.update_id = data
                .get("sequence")
                .and_then(parse_u64_value)
                .or_else(|| data.get("id").and_then(parse_u64_value));
            ev.event_time = data
                .get("timestamp")
                .and_then(|x| x.as_i64())
                .or_else(|| data.get("ts").and_then(|x| x.as_i64()))
                .map(normalize_epoch_to_ms);

            let bids = data.get("bids");
            let asks = data.get("asks");

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

            if sender.try_send(ev).is_err() {
                crate::util::metrics::inc_dropped_events(1);
            }
        }
        Stream::FutureTrade => {
            let local_ts = now_ms();
            let mut ev = MarketEvent::new("kucoin", Stream::FutureTrade, stored_symbol.to_string());
            ev.local_ts = local_ts;
            ev.time_str = format_time_str_ms(local_ts);
            ev.update_id = data
                .get("sequence")
                .and_then(parse_u64_value)
                .or_else(|| data.get("sn").and_then(parse_u64_value))
                .or_else(|| data.get("tradeId").and_then(parse_u64_value));
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
                .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                .or_else(|| data.get("p").and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok())));
            let qty = data
                .get("size")
                .and_then(|v| v.as_f64())
                .or_else(|| data.get("size").and_then(|v| v.as_i64().map(|x| x as f64)))
                .or_else(|| data.get("q").and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok())));
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

fn parse_u64_value(v: &serde_json::Value) -> Option<u64> {
    if let Some(x) = v.as_u64() {
        return Some(x);
    }
    if let Some(x) = v.as_i64() {
        return u64::try_from(x).ok();
    }
    v.as_str().and_then(|s| s.parse::<u64>().ok())
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
    let Some(pair_arr) = pair.as_array() else {
        return (None, None);
    };
    let px = pair_arr
        .get(0)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .or_else(|| pair_arr.get(0).and_then(|v| v.as_f64()));
    let qty = pair_arr
        .get(1)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .or_else(|| pair_arr.get(1).and_then(|v| v.as_f64()));
    (px, qty)
}

#[cfg(test)]
mod tests {
    use super::canonical_stored_symbol_from_contract;

    #[test]
    fn canonical_symbol_maps_xbtusdtm_to_btcusdt() {
        assert_eq!(canonical_stored_symbol_from_contract("XBTUSDTM"), "BTCUSDT");
        assert_eq!(canonical_stored_symbol_from_contract("xbtusdtm"), "BTCUSDT");
    }

    #[test]
    fn canonical_symbol_drops_m_suffix() {
        assert_eq!(canonical_stored_symbol_from_contract("ETHUSDTM"), "ETHUSDT");
        assert_eq!(canonical_stored_symbol_from_contract("SOLUSDCM"), "SOLUSDC");
    }
}
