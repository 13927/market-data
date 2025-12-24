use anyhow::Context;
use serde_json::Value;

use crate::schema::event::{MarketEvent, Stream};
use crate::util::time;

#[derive(Debug, Clone, Copy)]
pub enum BinanceJsonKind {
    SpotBook,
    SpotL5,
    SpotTrade,
    FutureBook,
    FutureL5,
    FutureTrade,
}

impl BinanceJsonKind {
    pub fn stream(&self) -> Stream {
        match self {
            BinanceJsonKind::SpotBook => Stream::SpotBook,
            BinanceJsonKind::SpotL5 => Stream::SpotL5,
            BinanceJsonKind::SpotTrade => Stream::SpotTrade,
            BinanceJsonKind::FutureBook => Stream::FutureBook,
            BinanceJsonKind::FutureL5 => Stream::FutureL5,
            BinanceJsonKind::FutureTrade => Stream::FutureTrade,
        }
    }

    pub fn exchange_name(&self) -> &'static str {
        // Keep `exchange` stable as "binance"; use `stream` to distinguish spot vs futures.
        "binance"
    }

    pub fn ws_endpoint(&self, base: &str, symbol: &str) -> String {
        let base = base.trim_end_matches('/');
        let symbol_lower = symbol.to_ascii_lowercase();
        let stream = match self {
            BinanceJsonKind::SpotBook => format!("{symbol_lower}@bookTicker"),
            BinanceJsonKind::SpotL5 => format!("{symbol_lower}@depth5@100ms"),
            BinanceJsonKind::SpotTrade => format!("{symbol_lower}@trade"),
            BinanceJsonKind::FutureBook => format!("{symbol_lower}@bookTicker"),
            BinanceJsonKind::FutureL5 => format!("{symbol_lower}@depth5@100ms"),
            BinanceJsonKind::FutureTrade => format!("{symbol_lower}@trade"),
        };

        // Spot uses `wss://stream.binance.com:9443/ws/<stream>`
        // Futures uses `wss://fstream.binance.com/ws/<stream>`
        format!("{base}/ws/{stream}")
    }
}

pub fn parse_event(
    kind: BinanceJsonKind,
    symbol_hint: &str,
    text: &str,
) -> anyhow::Result<Option<MarketEvent>> {
    let v: Value = serde_json::from_str(text).context("parse json")?;
    parse_event_value(kind, symbol_hint, &v)
}

pub fn parse_event_value(
    kind: BinanceJsonKind,
    symbol_hint: &str,
    v: &Value,
) -> anyhow::Result<Option<MarketEvent>> {
    let data = unwrap_combined_stream(v);

    // Subscription responses / errors are JSON text frames too; ignore them.
    if data.get("result").is_some() || data.get("id").is_some() && data.get("result").is_some() {
        return Ok(None);
    }
    if data.get("code").is_some() && data.get("msg").is_some() {
        return Ok(None);
    }

    let local_ts = time::now_ms();
    let time_str = time::now_local_time_str();

    let symbol = match kind {
        BinanceJsonKind::SpotL5 => {
            // Spot depth snapshots don't include symbol in `data`; infer from stream name if present.
            let inferred = v
                .get("stream")
                .and_then(|s| s.as_str())
                .and_then(symbol_from_stream_name)
                .unwrap_or_else(|| symbol_hint.to_ascii_uppercase());
            inferred
        }
        _ => get_str(data, "s")
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_else(|| symbol_hint.to_ascii_uppercase()),
    };

    let mut ev = MarketEvent::new(kind.exchange_name(), kind.stream(), symbol);
    ev.local_ts = local_ts;
    ev.time_str = time_str;

    match kind {
        BinanceJsonKind::SpotBook => {
            ev.update_id = get_u64(data, "u");
            ev.bid_px = get_f64(data, "b");
            ev.bid_qty = get_f64(data, "B");
            ev.ask_px = get_f64(data, "a");
            ev.ask_qty = get_f64(data, "A");
        }
        BinanceJsonKind::FutureBook => {
            ev.update_id = get_u64(data, "u");
            ev.event_time = get_i64(data, "E");
            ev.trade_time = get_i64(data, "T");
            ev.bid_px = get_f64(data, "b");
            ev.bid_qty = get_f64(data, "B");
            ev.ask_px = get_f64(data, "a");
            ev.ask_qty = get_f64(data, "A");
        }
        BinanceJsonKind::SpotL5 => {
            ev.update_id = get_u64(data, "lastUpdateId");
            fill_l5(&mut ev, data);
        }
        BinanceJsonKind::FutureL5 => {
            ev.update_id = get_u64(data, "u");
            ev.event_time = get_i64(data, "E");
            ev.trade_time = get_i64(data, "T");
            fill_l5(&mut ev, data);
        }
        BinanceJsonKind::SpotTrade => {
            ev.update_id = get_u64(data, "t");
            ev.event_time = get_i64(data, "E");
            ev.trade_time = get_i64(data, "T");
            fill_trade_side(&mut ev, data)?;
        }
        BinanceJsonKind::FutureTrade => {
            ev.update_id = get_u64(data, "t");
            ev.event_time = get_i64(data, "E");
            ev.trade_time = get_i64(data, "T");
            fill_trade_side(&mut ev, data)?;
        }
    }

    Ok(Some(ev))
}

fn unwrap_combined_stream(v: &Value) -> &Value {
    // Combined streams: { "stream": "...", "data": { ... } }
    v.get("data").unwrap_or(v)
}

pub fn kind_from_stream_name(is_futures: bool, stream_name: &str) -> Option<BinanceJsonKind> {
    if stream_name.contains("@bookTicker") {
        return Some(if is_futures {
            BinanceJsonKind::FutureBook
        } else {
            BinanceJsonKind::SpotBook
        });
    }
    if stream_name.contains("@depth5") {
        return Some(if is_futures {
            BinanceJsonKind::FutureL5
        } else {
            BinanceJsonKind::SpotL5
        });
    }
    if stream_name.contains("@trade") {
        return Some(if is_futures {
            BinanceJsonKind::FutureTrade
        } else {
            BinanceJsonKind::SpotTrade
        });
    }
    None
}

pub fn symbol_from_stream_name(stream_name: &str) -> Option<String> {
    // e.g. "btcusdt@depth5@100ms" / "ethusdt@bookTicker"
    let (sym, _) = stream_name.split_once('@')?;
    if sym.is_empty() {
        return None;
    }
    Some(sym.to_ascii_uppercase())
}

fn fill_trade_side(ev: &mut MarketEvent, data: &Value) -> anyhow::Result<()> {
    let is_bid = data
        .get("m")
        .and_then(|v| v.as_bool())
        .context("trade missing boolean field m")?;
    let px = get_f64(data, "p").context("trade missing p")?;
    let qty = get_f64(data, "q").context("trade missing q")?;

    if is_bid {
        ev.bid_px = Some(px);
        ev.bid_qty = Some(qty);
    } else {
        ev.ask_px = Some(px);
        ev.ask_qty = Some(qty);
    }
    Ok(())
}

fn fill_l5(ev: &mut MarketEvent, data: &Value) {
    let bids = data
        .get("bids")
        .or_else(|| data.get("b"))
        .and_then(|v| v.as_array())
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    let asks = data
        .get("asks")
        .or_else(|| data.get("a"))
        .and_then(|v| v.as_array())
        .map(|v| v.as_slice())
        .unwrap_or(&[]);

    let (bid_px, bid_qty) = l5_level(bids, 0);
    let (ask_px, ask_qty) = l5_level(asks, 0);
    ev.bid_px = bid_px;
    ev.bid_qty = bid_qty;
    ev.ask_px = ask_px;
    ev.ask_qty = ask_qty;

    (ev.bid1_px, ev.bid1_qty) = l5_level(bids, 0);
    (ev.bid2_px, ev.bid2_qty) = l5_level(bids, 1);
    (ev.bid3_px, ev.bid3_qty) = l5_level(bids, 2);
    (ev.bid4_px, ev.bid4_qty) = l5_level(bids, 3);
    (ev.bid5_px, ev.bid5_qty) = l5_level(bids, 4);

    (ev.ask1_px, ev.ask1_qty) = l5_level(asks, 0);
    (ev.ask2_px, ev.ask2_qty) = l5_level(asks, 1);
    (ev.ask3_px, ev.ask3_qty) = l5_level(asks, 2);
    (ev.ask4_px, ev.ask4_qty) = l5_level(asks, 3);
    (ev.ask5_px, ev.ask5_qty) = l5_level(asks, 4);
}

fn l5_level(levels: &[Value], idx: usize) -> (Option<f64>, Option<f64>) {
    let level = match levels.get(idx) {
        Some(v) => v,
        None => return (None, None),
    };
    let arr = match level.as_array() {
        Some(a) => a,
        None => return (None, None),
    };
    let px = arr.first().and_then(value_as_f64);
    let qty = arr.get(1).and_then(value_as_f64);
    (px, qty)
}

fn get_str<'a>(obj: &'a Value, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(|v| v.as_str())
}

fn get_u64(obj: &Value, key: &str) -> Option<u64> {
    obj.get(key).and_then(|v| match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    })
}

fn get_i64(obj: &Value, key: &str) -> Option<i64> {
    obj.get(key).and_then(|v| match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    })
}

fn get_f64(obj: &Value, key: &str) -> Option<f64> {
    obj.get(key).and_then(value_as_f64)
}

fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}
