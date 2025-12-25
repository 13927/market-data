use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use market_data::config::{GateConfig, KucoinConfig, KucoinFuturesConfig};
use market_data::exchange;
use market_data::schema::event::{MarketEvent, Stream};

fn expect_set() -> HashSet<(String, String)> {
    let mut s = HashSet::new();
    s.insert(("kucoin".to_string(), format!("{:?}", Stream::SpotBook)));
    s.insert(("kucoin".to_string(), format!("{:?}", Stream::SpotL5)));
    s.insert(("kucoin".to_string(), format!("{:?}", Stream::SpotTrade)));
    s.insert(("kucoin".to_string(), format!("{:?}", Stream::FutureBook)));
    s.insert(("kucoin".to_string(), format!("{:?}", Stream::FutureL5)));
    s.insert(("kucoin".to_string(), format!("{:?}", Stream::FutureTrade)));
    s.insert(("gate".to_string(), format!("{:?}", Stream::SpotBook)));
    s.insert(("gate".to_string(), format!("{:?}", Stream::SpotL5)));
    s.insert(("gate".to_string(), format!("{:?}", Stream::SpotTrade)));
    s.insert(("gate".to_string(), format!("{:?}", Stream::FutureBook)));
    s.insert(("gate".to_string(), format!("{:?}", Stream::FutureL5)));
    s.insert(("gate".to_string(), format!("{:?}", Stream::FutureTrade)));
    s.insert(("binance".to_string(), format!("{:?}", Stream::SpotBook)));
    s.insert(("binance".to_string(), format!("{:?}", Stream::SpotL5)));
    s.insert(("binance".to_string(), format!("{:?}", Stream::SpotTrade)));
    s
}

fn key(exchange: &str, stream: &Stream) -> (String, String) {
    (exchange.to_string(), format!("{:?}", stream))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn exchanges_spot_futures_streams() {
    let (tx, rx) = crossbeam_channel::unbounded::<MarketEvent>();

    let mut kucoin_cfg = KucoinConfig::default();
    kucoin_cfg.enabled = true;
    kucoin_cfg.symbols = vec!["BTC-USDT".to_string()];
    kucoin_cfg.ticker = true;
    kucoin_cfg.l5 = true;
    kucoin_cfg.trade = true;
    let kucoin_handles = exchange::kucoin::spawn_kucoin_public_ws(kucoin_cfg.clone(), None, tx.clone())
        .await
        .expect("spawn kucoin spot ws");

    let mut kf_cfg = KucoinFuturesConfig::default();
    kf_cfg.enabled = true;
    kf_cfg.rest_endpoint = kucoin_cfg.futures_rest_endpoint.clone();
    kf_cfg.symbols = vec!["XBTUSDTM".to_string()];
    kf_cfg.ticker = true;
    kf_cfg.l5 = true;
    kf_cfg.trade = true;
    let kucoin_fut_handles =
        exchange::kucoin_futures::spawn_kucoin_futures_public_ws(kf_cfg, None, tx.clone())
            .expect("spawn kucoin futures ws");

    let mut gate_cfg = GateConfig::default();
    gate_cfg.enabled = true;
    gate_cfg.symbols = vec!["BTC_USDT".to_string()];
    gate_cfg.spot_ticker = true;
    gate_cfg.spot_l5 = true;
    gate_cfg.spot_trade = true;
    gate_cfg.swap_ticker = true;
    gate_cfg.swap_l5 = true;
    gate_cfg.swap_trade = true;
    let gate_handles = exchange::gate::spawn_gate_ws(gate_cfg, tx.clone())
        .await
        .expect("spawn gate ws");

    let sbe_key = if let Ok(cfg) = market_data::config::Config::from_path("config.toml") {
        cfg.binance_spot_sbe.api_key.or_else(|| std::env::var("BINANCE_SBE_API_KEY").ok())
    } else {
        std::env::var("BINANCE_SBE_API_KEY").ok()
    }
    .expect("set binance_spot_sbe.api_key in config.toml or BINANCE_SBE_API_KEY env var for integration test");
    let sbe_handle = spawn_binance_sbe_task(&sbe_key, tx.clone());

    let mut pending = expect_set();
    let mut seen: HashMap<(String, String), usize> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !pending.is_empty() && Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(ev) => {
                let k = key(&ev.exchange, &ev.stream);
                *seen.entry(k.clone()).or_default() += 1;
                pending.remove(&k);
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(_) => break,
        }
    }

    for h in kucoin_handles {
        h.abort();
    }
    for h in kucoin_fut_handles {
        h.abort();
    }
    for h in gate_handles {
        h.abort();
    }
    sbe_handle.abort();

    if !pending.is_empty() {
        panic!(
            "missing streams: {:?} seen={:?}",
            pending
                .iter()
                .map(|(ex, st)| format!("{}::{:?}", ex, st))
                .collect::<Vec<_>>(),
            seen
        );
    }
}

fn spawn_binance_sbe_task(api_key: &str, sender: crossbeam_channel::Sender<MarketEvent>) -> tokio::task::JoinHandle<()> {
    let api_key = api_key.to_string();
    tokio::spawn(async move {
        let streams =
            exchange::binance_spot_sbe::streams_from_symbols(&vec!["BTCUSDT".to_string()], true, true, true);
        let seed = "btcusdt@bestBidAsk";
        let subscribe_msg = {
            let mut params: Vec<&str> = Vec::new();
            for s in &streams {
                if !s.eq_ignore_ascii_case(seed) {
                    params.push(s.as_str());
                }
            }
            if params.is_empty() {
                None
            } else {
                Some(
                    serde_json::json!({
                        "id": 1,
                        "method": "SUBSCRIBE",
                        "params": params
                    })
                    .to_string(),
                )
            }
        };
        let endpoint = market_data::config::BinanceSpotSbeConfig::default().endpoint;
        let url = format!("{}/ws/{}", endpoint.trim_end_matches('/'), seed);
        let headers = exchange::binance_spot_sbe::required_headers(&api_key).expect("sbe headers");
        let header_refs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let allowed: HashSet<String> = HashSet::from_iter(vec!["BTCUSDT".to_string()]);

        let _ = market_data::ws::client::run_ws_with_handler(&url, subscribe_msg, &header_refs, |frame| {
            match frame {
                market_data::ws::WsFrame::Text(_) => {}
                market_data::ws::WsFrame::Binary(bytes) => {
                    match exchange::binance_spot_sbe_decode::decode_frame(&bytes) {
                        Ok(evs) => {
                            for ev in evs {
                                if !allowed.contains(&ev.symbol) {
                                    continue;
                                }
                                let _ = sender.try_send(ev);
                            }
                        }
                        Err(_) => {}
                    }
                }
            }
            Ok(())
        })
        .await;
    })
}
