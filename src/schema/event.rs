use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Stream {
    #[serde(rename = "spot_book")]
    SpotBook,
    #[serde(rename = "spot_l5")]
    SpotL5,
    #[serde(rename = "spot_trade")]
    SpotTrade,
    #[serde(rename = "future_book")]
    FutureBook,
    #[serde(rename = "future_l5")]
    FutureL5,
    #[serde(rename = "future_trade")]
    FutureTrade,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub exchange: String, // "binance"
    pub stream: Stream,

    pub symbol: String,   // BTCUSDT (upper)
    pub local_ts: i64,    // ms
    pub time_str: String, // local time string

    pub update_id: Option<u64>,
    pub event_time: Option<i64>,
    pub trade_time: Option<i64>,

    pub bid_px: Option<f64>,
    pub bid_qty: Option<f64>,
    pub ask_px: Option<f64>,
    pub ask_qty: Option<f64>,

    pub bid1_px: Option<f64>,
    pub bid1_qty: Option<f64>,
    pub bid2_px: Option<f64>,
    pub bid2_qty: Option<f64>,
    pub bid3_px: Option<f64>,
    pub bid3_qty: Option<f64>,
    pub bid4_px: Option<f64>,
    pub bid4_qty: Option<f64>,
    pub bid5_px: Option<f64>,
    pub bid5_qty: Option<f64>,

    pub ask1_px: Option<f64>,
    pub ask1_qty: Option<f64>,
    pub ask2_px: Option<f64>,
    pub ask2_qty: Option<f64>,
    pub ask3_px: Option<f64>,
    pub ask3_qty: Option<f64>,
    pub ask4_px: Option<f64>,
    pub ask4_qty: Option<f64>,
    pub ask5_px: Option<f64>,
    pub ask5_qty: Option<f64>,
}

impl MarketEvent {
    pub fn new(exchange: impl Into<String>, stream: Stream, symbol: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            stream,
            symbol: symbol.into(),
            local_ts: 0,
            time_str: String::new(),
            update_id: None,
            event_time: None,
            trade_time: None,
            bid_px: None,
            bid_qty: None,
            ask_px: None,
            ask_qty: None,
            bid1_px: None,
            bid1_qty: None,
            bid2_px: None,
            bid2_qty: None,
            bid3_px: None,
            bid3_qty: None,
            bid4_px: None,
            bid4_qty: None,
            bid5_px: None,
            bid5_qty: None,
            ask1_px: None,
            ask1_qty: None,
            ask2_px: None,
            ask2_qty: None,
            ask3_px: None,
            ask3_qty: None,
            ask4_px: None,
            ask4_qty: None,
            ask5_px: None,
            ask5_qty: None,
        }
    }
}
