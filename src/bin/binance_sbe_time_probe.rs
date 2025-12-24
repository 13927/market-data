use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::HeaderName;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateId {
    Trades = 10000,
    BestBidAsk = 10001,
    DepthSnapshot = 10002,
    DepthDiff = 10003,
}

impl TemplateId {
    fn from_u16(v: u16) -> Option<Self> {
        match v {
            10000 => Some(Self::Trades),
            10001 => Some(Self::BestBidAsk),
            10002 => Some(Self::DepthSnapshot),
            10003 => Some(Self::DepthDiff),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MessageHeader {
    template_id: u16,
    schema_id: u16,
    version: u16,
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn read_u16(&mut self) -> anyhow::Result<u16> {
        if self.remaining() < 2 {
            anyhow::bail!("unexpected EOF reading u16");
        }
        let b = &self.bytes[self.pos..self.pos + 2];
        self.pos += 2;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn read_i64(&mut self) -> anyhow::Result<i64> {
        if self.remaining() < 8 {
            anyhow::bail!("unexpected EOF reading i64");
        }
        let b = &self.bytes[self.pos..self.pos + 8];
        self.pos += 8;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn read_message_header(&mut self) -> anyhow::Result<MessageHeader> {
        Ok(MessageHeader {
            // blockLength
            template_id: {
                let _ = self.read_u16().context("blockLength")?;
                self.read_u16().context("templateId")?
            },
            schema_id: self.read_u16().context("schemaId")?,
            version: self.read_u16().context("version")?,
        })
    }
}

fn load_api_key_from_config() -> anyhow::Result<(String, String)> {
    let raw = std::fs::read_to_string("config.toml").context("read config.toml")?;
    let v: toml::Value = toml::from_str(&raw).context("parse config.toml")?;
    let endpoint = v
        .get("binance_spot_sbe")
        .and_then(|x| x.get("endpoint"))
        .and_then(|x| x.as_str())
        .unwrap_or("wss://stream-sbe.binance.com:9443")
        .to_string();
    let api_key = v
        .get("binance_spot_sbe")
        .and_then(|x| x.get("api_key"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    if api_key.trim().is_empty() {
        anyhow::bail!("config.toml: missing [binance_spot_sbe].api_key");
    }
    Ok((endpoint, api_key))
}

fn make_url(endpoint: &str, stream: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    format!("{endpoint}/ws/{stream}")
}

fn endpoint_candidates(endpoint: &str) -> Vec<String> {
    let mut out = Vec::new();
    let ep = endpoint.trim().trim_end_matches('/').to_string();
    if !ep.is_empty() {
        out.push(ep.clone());
    }
    // Many environments block/inspect non-443 ports; try 443 if config uses 9443.
    if ep.contains(":9443") {
        out.push(ep.replace(":9443", ""));
    }
    out.dedup();
    out
}

fn is_http_400(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|e| e.to_string().contains("HTTP error: 400"))
}

fn subscribe_msg(extra_streams: &[String]) -> Option<String> {
    if extra_streams.is_empty() {
        return None;
    }
    Some(
        serde_json::json!({
            "id": 1,
            "method": "SUBSCRIBE",
            "params": extra_streams
        })
        .to_string(),
    )
}

fn hex_preview(bytes: &[u8], max_bytes: usize) -> String {
    let take = bytes.len().min(max_bytes);
    let mut out = String::with_capacity(take * 3 + 32);
    for (i, b) in bytes[..take].iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&format!("{:02x}", b));
    }
    if bytes.len() > take {
        out.push_str(&format!(" …(+{} bytes)", bytes.len() - take));
    }
    out
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string())
        .trim()
        .to_ascii_lowercase();
    let (endpoint, api_key) = load_api_key_from_config()?;

    let streams = vec![
        format!("{symbol}@bestBidAsk"),
        format!("{symbol}@depth20"),
        format!("{symbol}@trade"),
    ];
    let extra = streams.iter().skip(1).cloned().collect::<Vec<_>>();

    eprintln!("endpoint: {endpoint}");
    eprintln!("streams:  {}", streams.join(", "));
    eprintln!("api_key:  <redacted>");

    let mut ws_opt = None;
    let mut last_err: Option<anyhow::Error> = None;
    for ep in endpoint_candidates(&endpoint) {
        let url = make_url(&ep, &streams[0]);
        eprintln!("connect:  {url}");
        let mut req = url.into_client_request().context("build ws request")?;
        let header_name = HeaderName::from_static("x-mbx-apikey");
        req.headers_mut().insert(
            header_name,
            api_key
                .trim()
                .parse()
                .context("parse X-MBX-APIKEY header value")?,
        );
        match tokio_tungstenite::connect_async(req).await {
            Ok((ws, _)) => {
                ws_opt = Some(ws);
                break;
            }
            Err(e) => {
                let err = anyhow::Error::new(e).context("connect ws");
                if is_http_400(&err) {
                    last_err = Some(err);
                    continue;
                }
                return Err(err);
            }
        }
    }
    let ws = ws_opt.ok_or_else(|| {
        last_err.unwrap_or_else(|| anyhow::anyhow!("connect ws failed (no endpoints tried)"))
    })?;
    let (mut write, mut read) = ws.split();

    if let Some(sub) = subscribe_msg(&extra) {
        write.send(Message::Text(sub)).await.ok();
    }

    let mut got_bba = false;
    let mut got_depth = false;
    let mut got_trade = false;
    let mut printed_bba_raw: u8 = 0;
    let mut printed_depth_raw: u8 = 0;

    let deadline = Duration::from_secs(10);
    let _ = timeout(deadline, async {
        while let Some(msg) = read.next().await {
            let msg = msg.context("read ws message")?;
            match msg {
                Message::Text(t) => {
                    // control responses are text frames
                    eprintln!("control: {t}");
                }
                Message::Ping(p) => {
                    write.send(Message::Pong(p)).await.ok();
                }
                Message::Binary(b) => {
                    let mut c = Cursor::new(&b);
                    let hdr = c.read_message_header().context("read SBE messageHeader")?;
                    let tid = hdr.template_id;
                    let template = TemplateId::from_u16(tid);

                    match template {
                        Some(TemplateId::BestBidAsk) => {
                            if printed_bba_raw < 2 {
                                printed_bba_raw += 1;
                                eprintln!(
                                    "BestBidAskStreamEvent raw: len={} hex={}",
                                    b.len(),
                                    hex_preview(&b, 256)
                                );
                            }
                            let event_time_us = c.read_i64().context("eventTime")?;
                            let book_update_id = c.read_i64().context("bookUpdateId")?;
                            eprintln!(
                                "BestBidAskStreamEvent: templateId={tid} schemaId={} v={} event_time_us={event_time_us} (ms={}) book_update_id={book_update_id}",
                                hdr.schema_id,
                                hdr.version,
                                event_time_us / 1_000
                            );
                            got_bba = true;
                        }
                        Some(TemplateId::DepthSnapshot) => {
                            if printed_depth_raw < 2 {
                                printed_depth_raw += 1;
                                eprintln!(
                                    "DepthSnapshotStreamEvent raw: len={} hex={}",
                                    b.len(),
                                    hex_preview(&b, 512)
                                );
                            }
                            let event_time_us = c.read_i64().context("eventTime")?;
                            let book_update_id = c.read_i64().context("bookUpdateId")?;
                            eprintln!(
                                "DepthSnapshotStreamEvent: templateId={tid} schemaId={} v={} event_time_us={event_time_us} (ms={}) book_update_id={book_update_id}",
                                hdr.schema_id,
                                hdr.version,
                                event_time_us / 1_000
                            );
                            got_depth = true;
                        }
                        Some(TemplateId::Trades) => {
                            let event_time_us = c.read_i64().context("eventTime")?;
                            let transact_time_us = c.read_i64().context("transactTime")?;
                            eprintln!(
                                "TradesStreamEvent: templateId={tid} schemaId={} v={} event_time_us={event_time_us} (ms={}) transact_time_us={transact_time_us} (ms={})",
                                hdr.schema_id,
                                hdr.version,
                                event_time_us / 1_000,
                                transact_time_us / 1_000
                            );
                            got_trade = true;
                        }
                        Some(TemplateId::DepthDiff) => {
                            // Not used in our collector, but can exist on-wire.
                            eprintln!(
                                "DepthDiffStreamEvent: templateId={tid} schemaId={} v={} (not parsed)",
                                hdr.schema_id, hdr.version
                            );
                        }
                        None => {
                            eprintln!(
                                "unknown SBE message: templateId={tid} schemaId={} v={} bytes={}",
                                hdr.schema_id,
                                hdr.version,
                                b.len()
                            );
                        }
                    }

                    if got_bba && got_depth && got_trade {
                        break;
                    }
                }
                Message::Close(c) => {
                    eprintln!("close: {c:?}");
                    break;
                }
                _ => {}
            }

            // be polite: don't spin if server is quiet
            if !(got_bba && got_depth && got_trade) {
                sleep(Duration::from_millis(10)).await;
            }
        }
        anyhow::Ok(())
    })
    .await;

    eprintln!("done: got_best_bid_ask={got_bba} got_depth20={got_depth} got_trade={got_trade}");
    Ok(())
}
