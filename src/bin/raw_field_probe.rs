use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[derive(Debug, Clone)]
struct Sample {
    name: &'static str,
    meta: BTreeMap<&'static str, String>,
    raw: Value,
    candidate_fields: Vec<(&'static str, Value)>,
}

fn val_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn pick_fields(obj: &Value, keys: &[&'static str]) -> Vec<(&'static str, Value)> {
    let mut out = Vec::new();
    for &k in keys {
        if let Some(v) = obj.get(k) {
            out.push((k, v.clone()));
        }
    }
    out
}

fn canonical_to_kucoin_spot_symbol(symbol: &str) -> String {
    if symbol.contains('-') {
        return symbol.trim().to_ascii_uppercase();
    }
    let upper = symbol.trim().to_ascii_uppercase();
    let quotes = ["USDT", "USDC", "BTC", "ETH", "BNB", "DAI", "FDUSD", "USD"];
    if let Some(q) = quotes.iter().find(|q| upper.ends_with(*q)) {
        let base = &upper[..upper.len() - q.len()];
        format!("{base}-{q}")
    } else {
        upper
    }
}

fn canonical_to_kucoin_futures_contract(symbol: &str) -> Option<String> {
    // KuCoin futures contracts like XBTUSDTM (BTC perpetual).
    let upper = symbol.trim().to_ascii_uppercase().replace('-', "");
    let quotes = ["USDT", "USDC", "USD"];
    let (base, quote) = quotes.iter().find_map(|q| {
        upper
            .strip_suffix(q)
            .map(|b| (b.to_string(), q.to_string()))
    })?;
    let base = match base.as_str() {
        "BTC" => "XBT".to_string(),
        _ => base,
    };
    Some(format!("{base}{quote}M"))
}

fn canonical_to_gate_symbol(symbol: &str) -> String {
    let upper = symbol
        .trim()
        .to_ascii_uppercase()
        .replace('-', "")
        .replace('_', "");
    let quotes = ["USDT", "USDC", "BTC", "ETH", "BNB", "DAI", "FDUSD", "USD"];
    if let Some(q) = quotes.iter().find(|q| upper.ends_with(*q)) {
        let base = &upper[..upper.len() - q.len()];
        format!("{base}_{q}")
    } else {
        upper
    }
}

fn normalize_kucoin_ws_endpoint(endpoint: &str) -> String {
    // KuCoin bullet-public usually returns a host root like:
    // - wss://ws-api-spot.kucoin.com/
    // - wss://ws-api-futures.kucoin.com/
    // Some deployments return an explicit `/endpoint` path.
    //
    // We keep the endpoint as returned (minus trailing '/'); URL construction happens in `kucoin_ws_url`.
    endpoint.trim().trim_end_matches('/').to_string()
}

fn connect_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    format!("{nanos:x}")
}

fn kucoin_ws_url(endpoint: &str, token: &str) -> String {
    let base = normalize_kucoin_ws_endpoint(endpoint);
    if base.ends_with("/endpoint") {
        format!("{base}?token={token}&connectId={}", connect_id())
    } else {
        format!("{base}/?token={token}&connectId={}", connect_id())
    }
}

async fn kucoin_fetch_public_ws(rest_base: &str) -> anyhow::Result<(String, String)> {
    // returns (endpoint, token)
    let base = rest_base.trim_end_matches('/');
    let url = format!("{base}/api/v1/bullet-public");
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .send()
        .await
        .context("kucoin bullet-public request")?
        .error_for_status()
        .context("kucoin bullet-public status")?;
    let v: Value = resp.json().await.context("kucoin bullet-public json")?;
    let token = v
        .get("data")
        .and_then(|d| d.get("token"))
        .and_then(|x| x.as_str())
        .context("kucoin bullet-public missing data.token")?
        .to_string();
    let endpoint = v
        .get("data")
        .and_then(|d| d.get("instanceServers"))
        .and_then(|s| s.get(0))
        .and_then(|s| s.get("endpoint"))
        .and_then(|x| x.as_str())
        .context("kucoin bullet-public missing data.instanceServers[0].endpoint")?
        .to_string();
    Ok((endpoint, token))
}

async fn probe_kucoin_spot(rest_base: &str, canonical_symbol: &str) -> anyhow::Result<Vec<Sample>> {
    let (endpoint, token) = kucoin_fetch_public_ws(rest_base).await?;
    let topic_symbol = canonical_to_kucoin_spot_symbol(canonical_symbol);

    let want_ticker = format!("/market/ticker:{topic_symbol}");
    let want_l5 = format!("/spotMarket/level2Depth5:{topic_symbol}");

    let url = kucoin_ws_url(&endpoint, &token);
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .context("kucoin spot connect")?;
    let (mut write, mut read) = ws.split();

    for (i, topic) in [want_ticker.as_str(), want_l5.as_str()].iter().enumerate() {
        let msg = serde_json::json!({
            "id": format!("sub-{i}"),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true
        });
        write
            .send(Message::Text(msg.to_string()))
            .await
            .context("kucoin spot send subscribe")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut got_ticker: Option<Sample> = None;
    let mut got_l5: Option<Sample> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = tokio::time::timeout(Duration::from_millis(500), read.next())
            .await
            .ok()
            .flatten()
        else {
            continue;
        };
        let msg = msg.context("kucoin spot ws read")?;
        let Message::Text(text) = msg else { continue };
        let v: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v.get("type").and_then(|x| x.as_str()) != Some("message") {
            continue;
        }
        let topic = v.get("topic").and_then(|x| x.as_str()).unwrap_or("");
        let data = v.get("data").cloned().unwrap_or(Value::Null);
        let fields = [
            "sequence",
            "sequenceStart",
            "sequenceEnd",
            "ts",
            "time",
            "timestamp",
            "u",
            "id",
        ];
        if topic.eq_ignore_ascii_case(&want_ticker) && got_ticker.is_none() {
            let mut meta = BTreeMap::new();
            meta.insert("topic", topic.to_string());
            got_ticker = Some(Sample {
                name: "kucoin_spot_ticker",
                meta,
                candidate_fields: pick_fields(&data, &fields),
                raw: data,
            });
        } else if topic.eq_ignore_ascii_case(&want_l5) && got_l5.is_none() {
            let mut meta = BTreeMap::new();
            meta.insert("topic", topic.to_string());
            got_l5 = Some(Sample {
                name: "kucoin_spot_l5",
                meta,
                candidate_fields: pick_fields(&data, &fields),
                raw: data,
            });
        }
        if got_ticker.is_some() && got_l5.is_some() {
            break;
        }
    }

    let mut out = Vec::new();
    if let Some(s) = got_ticker {
        out.push(s);
    }
    if let Some(s) = got_l5 {
        out.push(s);
    }
    Ok(out)
}

async fn probe_kucoin_futures(
    rest_base: &str,
    canonical_symbol: &str,
) -> anyhow::Result<Vec<Sample>> {
    let contract = canonical_to_kucoin_futures_contract(canonical_symbol)
        .context("cannot derive kucoin futures contract from symbol")?;
    let (endpoint, token) = kucoin_fetch_public_ws(rest_base).await?;

    let want_ticker = format!("/contractMarket/tickerV2:{contract}");
    let want_l5 = format!("/contractMarket/level2Depth5:{contract}");
    let url = kucoin_ws_url(&endpoint, &token);
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .context("kucoin futures connect")?;
    let (mut write, mut read) = ws.split();

    for (i, topic) in [want_ticker.as_str(), want_l5.as_str()].iter().enumerate() {
        let msg = serde_json::json!({
            "id": format!("sub-{i}"),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true
        });
        write
            .send(Message::Text(msg.to_string()))
            .await
            .context("kucoin futures send subscribe")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut got_ticker: Option<Sample> = None;
    let mut got_l5: Option<Sample> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let Some(msg) = tokio::time::timeout(Duration::from_millis(500), read.next())
            .await
            .ok()
            .flatten()
        else {
            continue;
        };
        let msg = msg.context("kucoin futures ws read")?;
        let Message::Text(text) = msg else { continue };
        let v: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v.get("type").and_then(|x| x.as_str()) != Some("message") {
            continue;
        }
        let topic = v.get("topic").and_then(|x| x.as_str()).unwrap_or("");
        let data = v.get("data").cloned().unwrap_or(Value::Null);
        let fields = [
            "sequence",
            "sequenceStart",
            "sequenceEnd",
            "ts",
            "time",
            "timestamp",
            "u",
            "id",
        ];
        if topic.eq_ignore_ascii_case(&want_ticker) && got_ticker.is_none() {
            let mut meta = BTreeMap::new();
            meta.insert("topic", topic.to_string());
            got_ticker = Some(Sample {
                name: "kucoin_swap_ticker",
                meta,
                candidate_fields: pick_fields(&data, &fields),
                raw: data,
            });
        } else if topic.eq_ignore_ascii_case(&want_l5) && got_l5.is_none() {
            let mut meta = BTreeMap::new();
            meta.insert("topic", topic.to_string());
            got_l5 = Some(Sample {
                name: "kucoin_swap_l5",
                meta,
                candidate_fields: pick_fields(&data, &fields),
                raw: data,
            });
        }
        if got_ticker.is_some() && got_l5.is_some() {
            break;
        }
    }

    let mut out = Vec::new();
    if let Some(s) = got_ticker {
        out.push(s);
    }
    if let Some(s) = got_l5 {
        out.push(s);
    }
    Ok(out)
}

async fn probe_gate(
    ws_url: &str,
    is_futures: bool,
    canonical_symbol: &str,
) -> anyhow::Result<Vec<Sample>> {
    let sym = canonical_to_gate_symbol(canonical_symbol);
    let mut req = ws_url.into_client_request()?;
    req.headers_mut()
        .insert("X-Gate-Size-Decimal", http::HeaderValue::from_static("1"));
    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .context("gate connect")?;
    let (write, mut read) = ws.split();
    let write = std::sync::Arc::new(tokio::sync::Mutex::new(write));

    let now_sec = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0)))
    .as_secs();

    let (ticker_channel, l5_channel) = if is_futures {
        ("futures.book_ticker", "futures.order_book_update")
    } else {
        ("spot.book_ticker", "spot.order_book_update")
    };
    let (l5_interval, l5_intv_label) = if is_futures {
        ("0.1", "0.1")
    } else {
        ("100ms", "100ms")
    };

    let sub_ticker = serde_json::json!({
        "time": now_sec,
        "channel": ticker_channel,
        "event": "subscribe",
        "payload": [sym],
    });
    let sub_l5 = serde_json::json!({
        "time": now_sec,
        "channel": l5_channel,
        "event": "subscribe",
        "payload": [sym, "5", l5_interval],
    });

    {
        let mut w = write.lock().await;
        w.send(Message::Text(sub_ticker.to_string()))
            .await
            .context("gate send subscribe ticker")?;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let mut w = write.lock().await;
        w.send(Message::Text(sub_l5.to_string()))
            .await
            .context("gate send subscribe l5")?;
    }

    let mut got_ticker: Option<Sample> = None;
    let mut got_l5: Option<Sample> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let Some(next) = tokio::time::timeout(Duration::from_millis(500), read.next())
            .await
            .ok()
            .flatten()
        else {
            continue;
        };
        let msg = next.context("gate ws read")?;
        match msg {
            Message::Ping(payload) => {
                let mut w = write.lock().await;
                let _ = w.send(Message::Pong(payload)).await;
                continue;
            }
            Message::Text(text) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let event = v.get("event").and_then(|x| x.as_str()).unwrap_or("");
                if event != "update" && event != "all" {
                    continue;
                }
                let channel = v.get("channel").and_then(|x| x.as_str()).unwrap_or("");
                let result = v.get("result").cloned().unwrap_or(Value::Null);
                let fields = [
                    "u",
                    "U",
                    "id",
                    "lastUpdateId",
                    "t",
                    "time",
                    "time_ms",
                    "s",
                    "currency_pair",
                    "contract",
                ];

                if channel == ticker_channel && got_ticker.is_none() {
                    let mut meta = BTreeMap::new();
                    meta.insert("channel", channel.to_string());
                    meta.insert("l5_interval", l5_intv_label.to_string());
                    got_ticker = Some(Sample {
                        name: if is_futures {
                            "gate_swap_ticker"
                        } else {
                            "gate_spot_ticker"
                        },
                        meta,
                        candidate_fields: pick_fields(&result, &fields),
                        raw: result,
                    });
                } else if channel == l5_channel && got_l5.is_none() {
                    if let Some(full) = result.get("full").and_then(|x| x.as_bool()) {
                        if !full {
                            continue;
                        }
                    }
                    let mut meta = BTreeMap::new();
                    meta.insert("channel", channel.to_string());
                    meta.insert("l5_interval", l5_intv_label.to_string());
                    got_l5 = Some(Sample {
                        name: if is_futures {
                            "gate_swap_l5"
                        } else {
                            "gate_spot_l5"
                        },
                        meta,
                        candidate_fields: pick_fields(&result, &fields),
                        raw: result,
                    });
                }
                if got_ticker.is_some() && got_l5.is_some() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    let mut out = Vec::new();
    if let Some(s) = got_ticker {
        out.push(s);
    }
    if let Some(s) = got_l5 {
        out.push(s);
    }
    Ok(out)
}

fn print_sample(sample: &Sample) {
    println!("==== {} ====", sample.name);
    for (k, v) in &sample.meta {
        println!("{k}: {v}");
    }
    println!("raw:");
    println!(
        "{}",
        serde_json::to_string_pretty(&sample.raw).unwrap_or_else(|_| sample.raw.to_string())
    );
    println!("candidate fields:");
    if sample.candidate_fields.is_empty() {
        println!("- (none)");
    } else {
        for (k, v) in &sample.candidate_fields {
            println!("- {k}: {} ({})", v, val_type(v));
        }
    }
    println!();
}

fn parse_args() -> (String, String, String, String, String) {
    // Returns: (symbol, kucoin_rest, kucoin_futures_rest, gate_spot_ws, gate_futures_ws)
    let mut symbol = "BTCUSDT".to_string();
    let mut kucoin_rest = "https://api.kucoin.com".to_string();
    let mut kucoin_futures_rest = "https://api-futures.kucoin.com".to_string();
    let mut gate_spot_ws = "wss://api.gateio.ws/ws/v4/".to_string();
    let mut gate_futures_ws = "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--symbol" => symbol = args.next().unwrap_or(symbol),
            "--kucoin-rest" => kucoin_rest = args.next().unwrap_or(kucoin_rest),
            "--kucoin-futures-rest" => {
                kucoin_futures_rest = args.next().unwrap_or(kucoin_futures_rest)
            }
            "--gate-spot-ws" => gate_spot_ws = args.next().unwrap_or(gate_spot_ws),
            "--gate-futures-ws" => gate_futures_ws = args.next().unwrap_or(gate_futures_ws),
            "--help" | "-h" => {
                eprintln!(
                    "usage: raw_field_probe [--symbol BTCUSDT] [--kucoin-rest URL] [--kucoin-futures-rest URL] [--gate-spot-ws URL] [--gate-futures-ws URL]"
                );
                std::process::exit(2);
            }
            other => {
                // allow positional symbol
                if !other.starts_with('-') {
                    symbol = other.to_string();
                }
            }
        }
    }

    (
        symbol,
        kucoin_rest,
        kucoin_futures_rest,
        gate_spot_ws,
        gate_futures_ws,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (symbol, kucoin_rest, kucoin_futures_rest, gate_spot_ws, gate_futures_ws) = parse_args();
    eprintln!("symbol: {symbol}");

    let mut all: Vec<Sample> = Vec::new();

    // Stagger connection starts to avoid provider-side permit spikes.
    tokio::time::sleep(Duration::from_millis(0)).await;
    match probe_kucoin_spot(&kucoin_rest, &symbol).await {
        Ok(mut samples) => {
            if samples.is_empty() {
                eprintln!("WARN: kucoin_spot: no sample received within timeout");
            }
            all.append(&mut samples);
        }
        Err(err) => eprintln!("WARN: kucoin_spot: {err:#}"),
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    match probe_kucoin_futures(&kucoin_futures_rest, &symbol).await {
        Ok(mut samples) => {
            if samples.is_empty() {
                eprintln!("WARN: kucoin_futures: no sample received within timeout");
            }
            all.append(&mut samples);
        }
        Err(err) => eprintln!("WARN: kucoin_futures: {err:#}"),
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    match probe_gate(&gate_spot_ws, false, &symbol).await {
        Ok(mut samples) => {
            if samples.is_empty() {
                eprintln!("WARN: gate_spot: no sample received within timeout");
            }
            all.append(&mut samples);
        }
        Err(err) => eprintln!("WARN: gate_spot: {err:#}"),
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    match probe_gate(&gate_futures_ws, true, &symbol).await {
        Ok(mut samples) => {
            if samples.is_empty() {
                eprintln!("WARN: gate_futures: no sample received within timeout");
            }
            all.append(&mut samples);
        }
        Err(err) => eprintln!("WARN: gate_futures: {err:#}"),
    }

    if all.is_empty() {
        anyhow::bail!("no samples received");
    }

    for s in &all {
        print_sample(s);
    }

    Ok(())
}
