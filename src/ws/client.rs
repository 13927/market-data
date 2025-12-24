use anyhow::Context;
use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tungstenite::client::IntoClientRequest;
use tungstenite::http::{HeaderName, HeaderValue};
use tungstenite::Message;
use url::Url;

use super::WsFrame;

const WS_CONNECT_TIMEOUT_SECS: u64 = 15;
const WS_ACTIVE_PING_EVERY_SECS: u64 = 15;
const WS_IDLE_DEAD_SECS: u64 = 90;
const WS_WATCHDOG_TICK_SECS: u64 = 1;

#[derive(Debug, Clone, Copy)]
pub struct WsSessionStats {
    pub frames: u64,
    pub duration: std::time::Duration,
}

pub async fn run_ws_with_handler<F>(
    url: &str,
    subscribe_msg: Option<String>,
    headers: &[(&str, &str)],
    mut on_frame: F,
) -> anyhow::Result<WsSessionStats>
where
    F: FnMut(WsFrame) -> anyhow::Result<()> + Send,
{
    let started = std::time::Instant::now();
    let mut request = Url::parse(url)
        .context("parse ws url")?
        .into_client_request()
        .context("build ws request")?;
    for (name, value) in headers {
        request.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes()).context("parse header name")?,
            HeaderValue::from_str(value).context("parse header value")?,
        );
    }

    let (ws, _) = tokio::time::timeout(
        std::time::Duration::from_secs(WS_CONNECT_TIMEOUT_SECS),
        connect_async(request),
    )
    .await
    .context("connect ws: timeout")?
    .context("connect ws")?;
    let (mut write, mut read) = ws.split();

    if let Some(subscribe_msg) = subscribe_msg {
        write
            .send(Message::Text(subscribe_msg))
            .await
            .context("send subscribe msg")?;
    }

    let mut frames = 0u64;
    let mut last_msg_ts = std::time::Instant::now();
    let mut ping = tokio::time::interval(std::time::Duration::from_secs(WS_ACTIVE_PING_EVERY_SECS));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watchdog = tokio::time::interval(std::time::Duration::from_secs(WS_WATCHDOG_TICK_SECS));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Ping(Vec::new())).await.context("send active ping")?;
            }
            _ = watchdog.tick() => {
                if last_msg_ts.elapsed() > std::time::Duration::from_secs(WS_IDLE_DEAD_SECS) {
                    let _ = write.send(Message::Close(None)).await;
                    anyhow::bail!("ws watchdog: idle for >{WS_IDLE_DEAD_SECS}s");
                }
            }
            next = read.next() => {
                let Some(msg) = next else { break };
                let msg = msg.context("read ws msg")?;
                frames += 1;
                last_msg_ts = std::time::Instant::now();
                match msg {
                    Message::Text(text) => on_frame(WsFrame::Text(text))?,
                    Message::Binary(bytes) => on_frame(WsFrame::Binary(bytes))?,
                    Message::Ping(payload) => {
                        write
                            .send(Message::Pong(payload))
                            .await
                            .context("send pong")?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(c) => {
                        if let Some(cf) = c {
                            tracing::warn!("ws closed by server: code={} reason={}", cf.code, cf.reason);
                        } else {
                            tracing::warn!("ws closed by server: (no info)");
                        }
                        break;
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }

    Ok(WsSessionStats {
        frames,
        duration: started.elapsed(),
    })
}

#[allow(dead_code)]
pub async fn run_ws(
    url: &str,
    subscribe_msg: Option<String>,
    headers: &[(&str, &str)],
    sender: Sender<WsFrame>,
) -> anyhow::Result<()> {
    run_ws_with_handler(url, subscribe_msg, headers, move |frame| {
        let _ = sender.send(frame);
        Ok(())
    })
    .await
    .map(|_| ())
}
