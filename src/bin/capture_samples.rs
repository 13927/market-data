use std::collections::HashMap;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use std::sync::Arc;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::HeaderName;
use tokio_tungstenite::tungstenite::Message;

struct Store {
    base: PathBuf,
    max_per_cat: usize,
    buf: HashMap<String, Vec<Value>>,
}

impl Store {
    fn new(base: impl Into<PathBuf>, max_per_cat: usize) -> anyhow::Result<Self> {
        let base = base.into();
        create_dir_all(&base).with_context(|| format!("create {}", base.display()))?;
        Ok(Self {
            base,
            max_per_cat,
            buf: HashMap::new(),
        })
    }
    fn push(&mut self, cat: &str, v: Value) {
        let e = self.buf.entry(cat.to_string()).or_default();
        if e.len() < self.max_per_cat {
            e.push(v);
        }
    }
    fn done(&self) -> bool {
        self.buf.values().all(|v| v.len() >= self.max_per_cat)
    }
    fn write_all(&self) -> anyhow::Result<()> {
        for (cat, vals) in &self.buf {
            let mut path = self.base.clone();
            let parts: Vec<&str> = cat.split('/').collect();
            if parts.len() > 1 {
                for i in 0..parts.len() - 1 {
                    path.push(parts[i]);
                }
            }
            create_dir_all(&path)?;
            let filename = parts.last().unwrap_or(&"out");
            let file = path.join(format!("{filename}.json"));
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&file)
                .with_context(|| format!("open {}", file.display()))?;
            let mut first = true;
            for v in vals {
                if !first {
                    f.write_all(b"\n\n")?;
                }
                first = false;
                let s = serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string());
                f.write_all(s.as_bytes())?;
            }
        }
        Ok(())
    }
}

fn lower(s: &str) -> String {
    s.trim().to_ascii_lowercase()
}

fn binance_json_combined_url(symbol: &str) -> String {
    let s = lower(symbol);
    let ep = "wss://stream.binance.com:9443/stream";
    let streams = format!("{s}@bookTicker/{s}@depth5@100ms/{s}@trade");
    format!("{ep}?streams={streams}")
}

fn binance_futures_combined_url(symbol: &str) -> String {
    let s = lower(symbol);
    let ep = "wss://fstream.binance.com/stream";
    let streams = format!("{s}@bookTicker/{s}@depth5@100ms/{s}@trade");
    format!("{ep}?streams={streams}")
}

async fn capture_binance_spot_json(symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let url = binance_json_combined_url(&symbol);
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .context("binance json connect")?;
    let (_write, mut read) = ws.split();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = timeout(Duration::from_secs(2), read.next()).await.ok().flatten() else { continue };
        let msg = msg.context("binance json read")?;
        let Message::Text(text) = msg else { continue };
        let v: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
        let data = v.get("data").cloned().unwrap_or(Value::Null);
        if stream.ends_with("bookTicker") {
            let mut s = store.lock().await;
            s.push("binance_spot_json/ticker", data);
        } else if stream.contains("depth5") {
            let mut s = store.lock().await;
            s.push("binance_spot_json/l5", data);
        } else if stream.ends_with("trade") {
            let mut s = store.lock().await;
            s.push("binance_spot_json/trade", data);
        }
        
    }
    Ok(())
}

async fn capture_binance_futures_json(symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let url = binance_futures_combined_url(&symbol);
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .context("binance futures combined connect")?;
    let (_write, mut read) = ws.split();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = timeout(Duration::from_secs(2), read.next()).await.ok().flatten() else { continue };
        let msg = msg.context("binance futures combined read")?;
        let Message::Text(text) = msg else { continue };
        let v: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
        let data = v.get("data").cloned().unwrap_or(Value::Null);
        if stream.ends_with("bookTicker") {
            let mut s = store.lock().await;
            s.push("binance_futures_json/ticker", data);
        } else if stream.contains("depth5") {
            let mut s = store.lock().await;
            s.push("binance_futures_json/l5", data);
        } else if stream.ends_with("trade") {
            let mut s = store.lock().await;
            s.push("binance_futures_json/trade", data);
        }
    }
    Ok(())
}

fn sbe_endpoint_candidates(endpoint: &str) -> Vec<String> {
    let mut out = Vec::new();
    let ep = endpoint.trim().trim_end_matches('/').to_string();
    if !ep.is_empty() {
        out.push(ep.clone());
    }
    if ep.contains(":9443") {
        out.push(ep.replace(":9443", ""));
    }
    out.dedup();
    out
}

fn sbe_make_url(endpoint: &str, stream: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    format!("{endpoint}/ws/{stream}")
}

async fn capture_binance_spot_sbe(symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let (endpoint, api_key) = {
        let cfg = std::fs::read_to_string("config.toml")
            .ok()
            .and_then(|raw| toml::from_str::<toml::Value>(&raw).ok());
        let endpoint = cfg
            .as_ref()
            .and_then(|v| v.get("binance_spot_sbe"))
            .and_then(|x| x.get("endpoint"))
            .and_then(|x| x.as_str())
            .unwrap_or("wss://stream-sbe.binance.com:9443")
            .to_string();
        let api_key = cfg
            .as_ref()
            .and_then(|v| v.get("binance_spot_sbe"))
            .and_then(|x| x.get("api_key"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
            .or_else(|| std::env::var("BINANCE_SBE_API_KEY").ok())
            .unwrap_or_default();
        (endpoint, api_key)
    };
    if api_key.trim().is_empty() {
        anyhow::bail!("missing binance sbe api key (set in config.toml or BINANCE_SBE_API_KEY)");
    }
    let streams = vec![
        format!("{}@bestBidAsk", lower(&symbol)),
        format!("{}@depth20", lower(&symbol)),
        format!("{}@trade", lower(&symbol)),
    ];
    let extra = streams.iter().skip(1).cloned().collect::<Vec<_>>();
    let mut ws_opt = None;
    for ep in sbe_endpoint_candidates(&endpoint) {
        let url = sbe_make_url(&ep, &streams[0]);
        let mut req = url.into_client_request().context("build ws request")?;
        let header_name = HeaderName::from_static("x-mbx-apikey");
        req.headers_mut().insert(header_name, api_key.trim().parse().context("parse X-MBX-APIKEY")?);
        match tokio_tungstenite::connect_async(req).await {
            Ok((ws, _)) => { ws_opt = Some(ws); break; }
            Err(e) => {
                let err = anyhow::Error::new(e).context("connect ws");
                continue;
            }
        }
    }
    let ws = ws_opt.context("connect sbe ws")?;
    let (mut write, mut read) = ws.split();
    if !extra.is_empty() {
        let sub = serde_json::json!({
            "id": 1,
            "method": "SUBSCRIBE",
            "params": extra
        });
        write.send(Message::Text(sub.to_string())).await.ok();
    }
    let mut bba_count = 0usize;
    let mut depth_count = 0usize;
    let mut depth_diff_count = 0usize;
    let mut trade_count = 0usize;
    let deadline = Duration::from_secs(15);
    let _ = timeout(deadline, async {
        while let Some(msg) = read.next().await {
            let msg = msg.context("sbe read msg")?;
            match msg {
                Message::Binary(b) => {
                    if b.len() < 12 { continue; }
                    let tid = u16::from_le_bytes([b[2], b[3]]);
                    if tid == 10001 {
                        if bba_count < 3 {
                            bba_count += 1;
                            let et = i64::from_le_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]);
                            let bu = i64::from_le_bytes([b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23]]);
                            let j = serde_json::json!({"templateId": tid, "eventTimeUs": et, "bookUpdateId": bu, "rawLen": b.len()});
                            let mut s = store.lock().await;
                            s.push("binance_spot_sbe/ticker", j);
                        }
                    } else if tid == 10002 {
                        if depth_count < 3 {
                            depth_count += 1;
                            let et = i64::from_le_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]);
                            let bu = i64::from_le_bytes([b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23]]);
                            let j = serde_json::json!({"templateId": tid, "eventTimeUs": et, "bookUpdateId": bu, "rawLen": b.len()});
                            let mut s = store.lock().await;
                            s.push("binance_spot_sbe/l5", j);
                        }
                    } else if tid == 10003 {
                        if depth_diff_count < 3 {
                            depth_diff_count += 1;
                            let j = serde_json::json!({"templateId": tid, "rawLen": b.len()});
                            let mut s = store.lock().await;
                            s.push("binance_spot_sbe/l5_delta", j);
                        }
                    } else if tid == 10000 {
                        if trade_count < 3 {
                            trade_count += 1;
                            let et = i64::from_le_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]);
                            let tt = i64::from_le_bytes([b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23]]);
                            let j = serde_json::json!({"templateId": tid, "eventTimeUs": et, "transactTimeUs": tt, "rawLen": b.len()});
                            let mut s = store.lock().await;
                            s.push("binance_spot_sbe/trade", j);
                        }
                    }
                }
                Message::Ping(p) => { write.send(Message::Pong(p)).await.ok(); }
                Message::Text(_) => {}
                Message::Close(_) => break,
                _ => {}
            }
            sleep(Duration::from_millis(5)).await;
        }
        anyhow::Ok(())
    })
    .await;
    Ok(())
}

fn gate_spot_endpoint() -> String {
    "wss://api.gateio.ws/ws/v4/".to_string()
}

fn gate_futures_endpoint_usdt() -> String {
    "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string()
}

async fn capture_gate(ws_url: String, is_futures: bool, symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let sym = {
        let upper = symbol.trim().to_ascii_uppercase().replace('-', "").replace('_', "");
        let quotes = ["USDT", "USDC", "USD", "BTC", "ETH", "BNB", "DAI", "FDUSD"];
        if let Some(q) = quotes.iter().find(|q| upper.ends_with(*q)) {
            let base = &upper[..upper.len() - q.len()];
            format!("{base}_{q}")
        } else { upper }
    };
    let settle_hint = if ws_url.to_ascii_lowercase().contains("/usdc") { "usdc" } else { "usdt" };
    let mut req = ws_url.into_client_request()?;
    req.headers_mut().insert("X-Gate-Size-Decimal", http::HeaderValue::from_static("1"));
    let (ws, _) = tokio_tungstenite::connect_async(req).await.context("gate connect")?;
    let (mut write, mut read) = ws.split();
    let now_sec = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_else(|_| Duration::from_secs(0))).as_secs();
    let (ticker_channel, l5_channel, trades_channel) = if is_futures {
        ("futures.book_ticker", "futures.order_book_update", "futures.trades")
    } else {
        ("spot.book_ticker", "spot.order_book_update", "spot.trades")
    };
    let l5_payload = if is_futures {
        serde_json::json!([sym, "100ms", "5"])
    } else {
        serde_json::json!([sym, "100ms"])
    };
    let sub_ticker = serde_json::json!({"time": now_sec, "channel": ticker_channel, "event":"subscribe", "payload":[sym.clone()]});
    let sub_l5 = serde_json::json!({"time": now_sec, "channel": l5_channel, "event":"subscribe", "payload": l5_payload});
    let sub_trades = serde_json::json!({"time": now_sec, "channel": trades_channel, "event":"subscribe", "payload":[sym.clone()]});
    write.send(Message::Text(sub_ticker.to_string())).await.context("gate subscribe ticker")?;
    sleep(Duration::from_millis(120)).await;
    write.send(Message::Text(sub_l5.to_string())).await.context("gate subscribe l5")?;
    sleep(Duration::from_millis(120)).await;
    if is_futures {
        let url = format!("https://api.gateio.ws/api/v4/futures/{}/order_book?contract={}&limit=10", settle_hint, sym);
        if let Ok(resp) = reqwest::Client::new().get(&url).send().await {
            if let Ok(val) = resp.json::<serde_json::Value>().await {
                let mut s = store.lock().await;
                s.push("gate_swap/l5", val);
            }
        }
    } else {
        let url = format!("https://api.gateio.ws/api/v4/spot/order_book?currency_pair={}&limit=10", sym);
        if let Ok(resp) = reqwest::Client::new().get(&url).send().await {
            if let Ok(val) = resp.json::<serde_json::Value>().await {
                let mut s = store.lock().await;
                s.push("gate_spot/l5", val);
            }
        }
    }
    write.send(Message::Text(sub_trades.to_string())).await.context("gate subscribe trades")?;
    let mut t_count = 0usize;
    let mut l_count = 0usize;
    let mut tr_count = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let Some(next) = timeout(Duration::from_millis(800), read.next()).await.ok().flatten() else { continue };
        let next = next.context("gate read")?;
        match next {
            Message::Ping(p) => { write.send(Message::Pong(p)).await.ok(); }
            Message::Text(text) => {
                let v: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
                let event = v.get("event").and_then(|x| x.as_str()).unwrap_or("");
                if event != "update" && event != "all" { continue; }
                let channel = v.get("channel").and_then(|x| x.as_str()).unwrap_or("");
                let result = v.get("result").cloned().unwrap_or(Value::Null);
                if channel == ticker_channel && t_count < 3 {
                    t_count += 1;
                    let mut s = store.lock().await;
                    s.push(if is_futures { "gate_swap/ticker" } else { "gate_spot/ticker" }, result);
                } else if channel == l5_channel && l_count < 3 {
                    l_count += 1;
                    let mut s = store.lock().await;
                    if is_futures {
                        if event == "all" {
                            s.push("gate_swap/l5", result);
                        } else {
                            s.push("gate_swap/l5_delta", result);
                        }
                    } else {
                        if event == "all" {
                            s.push("gate_spot/l5", result);
                        } else {
                            s.push("gate_spot/l5_delta", result);
                        }
                    }
                } else if channel == trades_channel && tr_count < 3 {
                    tr_count += 1;
                    let mut s = store.lock().await;
                    s.push(if is_futures { "gate_swap/trade" } else { "gate_spot/trade" }, result);
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

fn kucoin_spot_symbol(symbol: &str) -> String {
    let s = symbol.trim().to_ascii_uppercase();
    if s.contains('-') { return s; }
    let quotes = ["USDT", "USDC", "USD", "BTC", "ETH", "BNB", "DAI", "FDUSD"];
    if let Some(q) = quotes.iter().find(|q| s.ends_with(*q)) {
        let b = &s[..s.len()-q.len()];
        format!("{b}-{q}")
    } else { s }
}

async fn kucoin_fetch_public_ws(rest_base: &str) -> anyhow::Result<(String, String)> {
    let base = rest_base.trim_end_matches('/');
    let url = format!("{base}/api/v1/bullet-public");
    let client = reqwest::Client::new();
    let resp = client.post(url).send().await.context("kucoin bullet")?.error_for_status().context("kucoin bullet status")?;
    let v: Value = resp.json().await.context("kucoin bullet json")?;
    let token = v.get("data").and_then(|d| d.get("token")).and_then(|x| x.as_str()).context("kucoin token")?.to_string();
    let endpoint = v.get("data").and_then(|d| d.get("instanceServers")).and_then(|s| s.get(0)).and_then(|s| s.get("endpoint")).and_then(|x| x.as_str()).context("kucoin endpoint")?.to_string();
    Ok((endpoint, token))
}

fn kucoin_ws_url(endpoint: &str, token: &str) -> String {
    let base = endpoint.trim().trim_end_matches('/');
    if base.ends_with("/endpoint") {
        format!("{base}?token={token}&connectId={}", now_id())
    } else {
        format!("{base}/?token={token}&connectId={}", now_id())
    }
}

fn now_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_else(|_| Duration::from_millis(0)).as_nanos();
    format!("{nanos:x}")
}

async fn capture_kucoin_spot(rest_base: String, symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let (endpoint, token) = kucoin_fetch_public_ws(rest_base.as_str()).await?;
    let topic_symbol = kucoin_spot_symbol(&symbol);
    let topics = vec![
        format!("/market/ticker:{topic_symbol}"),
        format!("/spotMarket/level2Depth5:{topic_symbol}"),
        format!("/market/match:{topic_symbol}"),
    ];
    let url = kucoin_ws_url(&endpoint, &token);
    let (ws, _) = tokio_tungstenite::connect_async(url).await.context("kucoin spot connect")?;
    let (mut write, mut read) = ws.split();
    for (i, t) in topics.iter().enumerate() {
        let msg = serde_json::json!({"id": format!("sub-{i}"), "type":"subscribe", "topic": t, "privateChannel": false, "response": true});
        write.send(Message::Text(msg.to_string())).await.context("kucoin subscribe")?;
        sleep(Duration::from_millis(120)).await;
    }
    let mut tk = 0usize;
    let mut l5 = 0usize;
    let mut tr = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = timeout(Duration::from_millis(800), read.next()).await.ok().flatten() else { continue };
        let msg = msg.context("kucoin read")?;
        let Message::Text(text) = msg else { continue };
        let root: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        if root.get("type").and_then(|x| x.as_str()) != Some("message") { continue; }
        let topic = root.get("topic").and_then(|x| x.as_str()).unwrap_or("");
        let data = root.get("data").cloned().unwrap_or(Value::Null);
        if topic.starts_with("/market/ticker") && tk < 3 {
            tk += 1;
            let mut s = store.lock().await;
            s.push("kucoin_spot/ticker", data);
        } else if topic.starts_with("/spotMarket/level2Depth5") && l5 < 3 {
            l5 += 1;
            let mut s = store.lock().await;
            s.push("kucoin_spot/l5", data);
        } else if topic.starts_with("/market/match") && tr < 3 {
            tr += 1;
            let mut s = store.lock().await;
            s.push("kucoin_spot/trade", data);
        }
    }
    Ok(())
}

async fn capture_kucoin_spot_delta(rest_base: String, symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let (endpoint, token) = kucoin_fetch_public_ws(rest_base.as_str()).await?;
    let topic_symbol = kucoin_spot_symbol(&symbol);
    let topic = format!("/spotMarket/level2:{topic_symbol}");
    let url = kucoin_ws_url(&endpoint, &token);
    let (ws, _) = tokio_tungstenite::connect_async(url).await.context("kucoin spot delta connect")?;
    let (mut write, mut read) = ws.split();
    let msg = serde_json::json!({"id":"sub-l2","type":"subscribe","topic":topic,"privateChannel":false,"response":true});
    write.send(Message::Text(msg.to_string())).await.context("kucoin spot delta subscribe")?;
    let mut d = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = timeout(Duration::from_millis(800), read.next()).await.ok().flatten() else { continue };
        let msg = msg.context("kucoin spot delta read")?;
        let Message::Text(text) = msg else { continue };
        let root: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        if root.get("type").and_then(|x| x.as_str()) != Some("message") { continue; }
        let topic = root.get("topic").and_then(|x| x.as_str()).unwrap_or("");
        if !topic.starts_with("/spotMarket/level2") { continue; }
        let data = root.get("data").cloned().unwrap_or(Value::Null);
        if d < 3 {
            d += 1;
            let mut s = store.lock().await;
            s.push("kucoin_spot/l2_delta", data);
        }
    }
    Ok(())
}

fn canonical_to_kucoin_futures_contract(symbol: &str) -> Option<String> {
    let upper = symbol.trim().to_ascii_uppercase().replace('-', "");
    let quotes = ["USDT", "USDC", "USD"];
    let (base, quote) = quotes.iter().find_map(|q| upper.strip_suffix(q).map(|b| (b.to_string(), q.to_string())))?;
    let base = match base.as_str() {
        "BTC" => "XBT".to_string(),
        _ => base,
    };
    Some(format!("{base}{quote}M"))
}

async fn capture_kucoin_futures(rest_base: String, symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let contract = canonical_to_kucoin_futures_contract(&symbol).context("cannot derive kucoin futures contract from symbol")?;
    let (endpoint, token) = kucoin_fetch_public_ws(rest_base.as_str()).await?;
    let topics = vec![
        format!("/contractMarket/tickerV2:{contract}"),
        format!("/contractMarket/level2Depth5:{contract}"),
        format!("/contractMarket/execution:{contract}"),
    ];
    let url = kucoin_ws_url(&endpoint, &token);
    let (ws, _) = tokio_tungstenite::connect_async(url).await.context("kucoin futures connect")?;
    let (mut write, mut read) = ws.split();
    for (i, t) in topics.iter().enumerate() {
        let msg = serde_json::json!({"id": format!("sub-{i}"), "type":"subscribe", "topic": t, "privateChannel": false, "response": true});
        write.send(Message::Text(msg.to_string())).await.context("kucoin futures subscribe")?;
        sleep(Duration::from_millis(120)).await;
    }
    let mut tk = 0usize;
    let mut l5 = 0usize;
    let mut tr = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = timeout(Duration::from_millis(800), read.next()).await.ok().flatten() else { continue };
        let msg = msg.context("kucoin futures read")?;
        let Message::Text(text) = msg else { continue };
        let root: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        if root.get("type").and_then(|x| x.as_str()) != Some("message") { continue; }
        let topic = root.get("topic").and_then(|x| x.as_str()).unwrap_or("");
        let data = root.get("data").cloned().unwrap_or(Value::Null);
        if topic.starts_with("/contractMarket/tickerV2") && tk < 3 {
            tk += 1;
            let mut s = store.lock().await;
            s.push("kucoin_swap/ticker", data);
        } else if topic.starts_with("/contractMarket/level2Depth5") && l5 < 3 {
            l5 += 1;
            let mut s = store.lock().await;
            s.push("kucoin_swap/l5", data);
        } else if topic.starts_with("/contractMarket/execution") && tr < 3 {
            tr += 1;
            let mut s = store.lock().await;
            s.push("kucoin_swap/trade", data);
        }
    }
    Ok(())
}

async fn capture_kucoin_futures_delta(rest_base: String, symbol: String, store: Arc<tokio::sync::Mutex<Store>>) -> anyhow::Result<()> {
    let contract = canonical_to_kucoin_futures_contract(&symbol).context("cannot derive kucoin futures contract from symbol")?;
    let (endpoint, token) = kucoin_fetch_public_ws(rest_base.as_str()).await?;
    let topic = format!("/contractMarket/level2:{contract}");
    let url = kucoin_ws_url(&endpoint, &token);
    let (ws, _) = tokio_tungstenite::connect_async(url).await.context("kucoin futures delta connect")?;
    let (mut write, mut read) = ws.split();
    let msg = serde_json::json!({"id":"sub-l2","type":"subscribe","topic":topic,"privateChannel":false,"response":true});
    write.send(Message::Text(msg.to_string())).await.context("kucoin futures delta subscribe")?;
    let mut d = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = timeout(Duration::from_millis(800), read.next()).await.ok().flatten() else { continue };
        let msg = msg.context("kucoin futures delta read")?;
        let Message::Text(text) = msg else { continue };
        let root: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        if root.get("type").and_then(|x| x.as_str()) != Some("message") { continue; }
        let topic = root.get("topic").and_then(|x| x.as_str()).unwrap_or("");
        if !topic.starts_with("/contractMarket/level2") { continue; }
        let data = root.get("data").cloned().unwrap_or(Value::Null);
        if d < 3 {
            d += 1;
            let mut s = store.lock().await;
            s.push("kucoin_swap/l2_delta", data);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut symbol = "BTCUSDT".to_string();
    let mut out_dir = "samples".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--symbol" => symbol = args.next().unwrap_or(symbol),
            "--out" => out_dir = args.next().unwrap_or(out_dir),
            "--help" | "-h" => {
                eprintln!("usage: capture_samples [--symbol BTCUSDT] [--out DIR]");
                std::process::exit(2);
            }
            other => {
                if !other.starts_with('-') {
                    symbol = other.to_string();
                }
            }
        }
    }
    let store = Arc::new(tokio::sync::Mutex::new(Store::new(&out_dir, 3)?));
    let kucoin_rest = "https://api.kucoin.com".to_string();
    let kucoin_futures_rest = "https://api-futures.kucoin.com".to_string();
    let gate_spot = gate_spot_endpoint();
    let gate_fut_usdt = gate_futures_endpoint_usdt();
    let mut tasks = Vec::new();
    tasks.push(tokio::spawn(capture_binance_spot_json(symbol.clone(), store.clone())));
    tasks.push(tokio::spawn(capture_binance_futures_json(symbol.clone(), store.clone())));
    tasks.push(tokio::spawn(capture_binance_spot_sbe(symbol.clone(), store.clone())));
    tasks.push(tokio::spawn(capture_gate(gate_spot.clone(), false, symbol.clone(), store.clone())));
    tasks.push(tokio::spawn(capture_gate(gate_fut_usdt.clone(), true, symbol.clone(), store.clone())));
    tasks.push(tokio::spawn(capture_kucoin_spot(kucoin_rest.clone(), symbol.clone(), store.clone())));
    tasks.push(tokio::spawn(capture_kucoin_futures(kucoin_futures_rest.clone(), symbol.clone(), store.clone())));
    for t in tasks {
        let _ = t.await;
    }
    store.lock().await.write_all()?;
    eprintln!("done: output dir={}", out_dir);
    Ok(())
}
