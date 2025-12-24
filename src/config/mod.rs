use anyhow::Context;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub binance_spot_sbe: BinanceSpotSbeConfig,

    #[serde(default)]
    pub binance_spot_json: BinanceSpotJsonConfig,

    #[serde(default)]
    pub binance_futures_json: BinanceFuturesJsonConfig,

    #[serde(default)]
    pub kucoin: KucoinConfig,

    #[serde(default)]
    pub kucoin_futures: KucoinFuturesConfig,

    #[serde(default)]
    pub gate: GateConfig,

    #[serde(default)]
    pub output: OutputConfig,
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        toml::from_str(&raw).context("parse config.toml")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceSpotSbeConfig {
    #[serde(default)]
    pub enabled: bool,

    // Required when enabled = true
    pub api_key: Option<String>,

    // Optional convenience: if `streams` is empty, streams will be generated from `symbols` + flags.
    #[serde(default)]
    pub symbols: Vec<String>,

    #[serde(default = "default_binance_spot_sbe_best_bid_ask")]
    pub best_bid_ask: bool,

    #[serde(default = "default_binance_spot_sbe_depth20")]
    pub depth20: bool,

    #[serde(default)]
    pub trade: bool,

    // Example: ["btcusdt@bestBidAsk", "ethusdt@bestBidAsk"]
    #[serde(default = "default_binance_spot_sbe_streams")]
    pub streams: Vec<String>,

    #[serde(default = "default_binance_spot_sbe_endpoint")]
    pub endpoint: String,
}

impl Default for BinanceSpotSbeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key: None,
            symbols: vec![],
            best_bid_ask: default_binance_spot_sbe_best_bid_ask(),
            depth20: default_binance_spot_sbe_depth20(),
            trade: false,
            streams: default_binance_spot_sbe_streams(),
            endpoint: default_binance_spot_sbe_endpoint(),
        }
    }
}

fn default_binance_spot_sbe_streams() -> Vec<String> {
    vec![]
}

fn default_binance_spot_sbe_endpoint() -> String {
    "wss://stream-sbe.binance.com:9443".to_string()
}

fn default_binance_spot_sbe_best_bid_ask() -> bool {
    true
}

fn default_binance_spot_sbe_depth20() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceSpotJsonConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_binance_spot_json_endpoint")]
    pub endpoint: String,

    #[serde(default)]
    pub symbols: Vec<String>,

    #[serde(default = "default_binance_json_combined")]
    pub combined: bool,

    #[serde(default)]
    pub book: bool,

    #[serde(default)]
    pub l5: bool,

    #[serde(default)]
    pub trade: bool,
}

impl Default for BinanceSpotJsonConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_binance_spot_json_endpoint(),
            symbols: vec![],
            combined: default_binance_json_combined(),
            book: true,
            l5: true,
            trade: false,
        }
    }
}

fn default_binance_spot_json_endpoint() -> String {
    "wss://stream.binance.com:9443".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceFuturesJsonConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_binance_futures_json_endpoint")]
    pub endpoint: String,

    #[serde(default)]
    pub symbols: Vec<String>,

    #[serde(default = "default_binance_json_combined")]
    pub combined: bool,

    #[serde(default)]
    pub book: bool,

    #[serde(default)]
    pub l5: bool,

    #[serde(default)]
    pub trade: bool,
}

impl Default for BinanceFuturesJsonConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_binance_futures_json_endpoint(),
            symbols: vec![],
            combined: default_binance_json_combined(),
            book: true,
            l5: true,
            trade: false,
        }
    }
}

fn default_binance_futures_json_endpoint() -> String {
    "wss://fstream.binance.com".to_string()
}

fn default_binance_json_combined() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct KucoinConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_kucoin_rest_endpoint")]
    pub rest_endpoint: String,

    /// Per-topic subscribe delay within a single connection (ms). Used only when
    /// `global_subscribe_delay_ms == 0`.
    #[serde(default = "default_kucoin_subscribe_delay_ms")]
    pub subscribe_delay_ms: u64,

    /// Stagger startup of each KuCoin WS connection (ms) to avoid bursty handshakes/subscribes.
    #[serde(default = "default_kucoin_conn_stagger_ms")]
    pub conn_stagger_ms: u64,

    /// Global pacing for all KuCoin subscribe messages (ms). When > 0, all subscribe messages
    /// across all KuCoin connections (spot + futures) are serialized at this interval.
    #[serde(default = "default_kucoin_global_subscribe_delay_ms")]
    pub global_subscribe_delay_ms: u64,

    // Whether to also subscribe KuCoin futures (swap) streams using the same `symbols` list.
    // When true, we derive futures contract symbols (e.g. BTCUSDT -> XBTUSDTM) automatically.
    #[serde(default = "default_true")]
    pub swap: bool,

    #[serde(default = "default_kucoin_futures_rest_endpoint")]
    pub futures_rest_endpoint: String,

    // KuCoin public WS topics use hyphenated symbols like BTC-USDT, but we also accept BTCUSDT.
    #[serde(default)]
    pub symbols: Vec<String>,

    #[serde(default)]
    pub ticker: bool,

    #[serde(default)]
    pub l5: bool,

    #[serde(default)]
    pub trade: bool,
}

impl Default for KucoinConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rest_endpoint: default_kucoin_rest_endpoint(),
            subscribe_delay_ms: default_kucoin_subscribe_delay_ms(),
            conn_stagger_ms: default_kucoin_conn_stagger_ms(),
            global_subscribe_delay_ms: default_kucoin_global_subscribe_delay_ms(),
            swap: default_true(),
            futures_rest_endpoint: default_kucoin_futures_rest_endpoint(),
            symbols: vec![],
            ticker: true,
            l5: true,
            trade: true,
        }
    }
}

fn default_kucoin_rest_endpoint() -> String {
    "https://api.kucoin.com".to_string()
}

fn default_kucoin_subscribe_delay_ms() -> u64 {
    100
}

fn default_kucoin_conn_stagger_ms() -> u64 {
    500
}

fn default_kucoin_global_subscribe_delay_ms() -> u64 {
    // Protect against server-side throttles when many connections subscribe in parallel.
    100
}

#[derive(Debug, Clone, Deserialize)]
pub struct KucoinFuturesConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_kucoin_futures_rest_endpoint")]
    pub rest_endpoint: String,

    /// Per-topic subscribe delay within a single connection (ms). Used only when
    /// `global_subscribe_delay_ms == 0`.
    #[serde(default = "default_kucoin_subscribe_delay_ms")]
    pub subscribe_delay_ms: u64,

    /// Stagger startup of each KuCoin futures WS connection (ms).
    #[serde(default = "default_kucoin_conn_stagger_ms")]
    pub conn_stagger_ms: u64,

    /// Global pacing for KuCoin futures subscribe messages (ms). If you enable both
    /// spot + futures, prefer using `kucoin.global_subscribe_delay_ms` and pass a shared pacer.
    #[serde(default)]
    pub global_subscribe_delay_ms: u64,

    // KuCoin futures symbols are typically like XBTUSDTM / ETHUSDTM (no dash).
    #[serde(default)]
    pub symbols: Vec<String>,

    #[serde(default)]
    pub ticker: bool,

    #[serde(default)]
    pub l5: bool,

    #[serde(default)]
    pub trade: bool,
}

impl Default for KucoinFuturesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rest_endpoint: default_kucoin_futures_rest_endpoint(),
            subscribe_delay_ms: default_kucoin_subscribe_delay_ms(),
            conn_stagger_ms: default_kucoin_conn_stagger_ms(),
            global_subscribe_delay_ms: 0,
            symbols: vec![],
            ticker: true,
            l5: true,
            trade: true,
        }
    }
}

fn default_kucoin_futures_rest_endpoint() -> String {
    "https://api-futures.kucoin.com".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_gate_spot_endpoint")]
    pub spot_endpoint: String,

    #[serde(default = "default_gate_futures_endpoint")]
    pub futures_endpoint: String,

    // Gate symbols are usually like BTC_USDT; we also accept BTCUSDT and will attempt to normalize.
    #[serde(default)]
    pub symbols: Vec<String>,

    #[serde(default = "default_true")]
    pub spot_ticker: bool,

    #[serde(default = "default_true")]
    pub spot_l5: bool,

    #[serde(default = "default_true")]
    pub spot_trade: bool,

    #[serde(default = "default_true")]
    pub swap_ticker: bool,

    #[serde(default = "default_true")]
    pub swap_l5: bool,

    #[serde(default = "default_true")]
    pub swap_trade: bool,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            spot_endpoint: default_gate_spot_endpoint(),
            futures_endpoint: default_gate_futures_endpoint(),
            symbols: vec![],
            spot_ticker: true,
            spot_l5: true,
            spot_trade: true,
            swap_ticker: true,
            swap_l5: true,
            swap_trade: true,
        }
    }
}

fn default_gate_spot_endpoint() -> String {
    "wss://api.gateio.ws/ws/v4/".to_string()
}

fn default_gate_futures_endpoint() -> String {
    // USDT-margined perpetual futures
    "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_output_dir")]
    pub dir: String,

    #[serde(default = "default_output_format")]
    pub format: OutputFormat,

    #[serde(default = "default_output_bucket_minutes")]
    pub bucket_minutes: i64,

    #[serde(default = "default_output_retention_hours")]
    pub retention_hours: i64,

    #[serde(default = "default_output_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,

    // Crossbeam channel capacity between websocket tasks and the writer thread.
    // 0 means unbounded (not recommended for large symbol counts).
    #[serde(default = "default_output_queue_capacity")]
    pub queue_capacity: usize,

    #[serde(default = "default_output_parquet_batch_size")]
    pub parquet_batch_size: usize,

    #[serde(default = "default_output_parquet_record_batch_size")]
    pub parquet_record_batch_size: usize,

    /// Soft cap for concurrently open parquet files in the writer.
    /// 0 means unlimited.
    ///
    /// Notes:
    /// - With per-exchange/symbol/hour partitioning, open files ~= number of active symbols.
    /// - If this cap is set below the active working set, the writer may exceed the cap to avoid
    ///   pathological open/close thrash (and `_partN` explosion).
    #[serde(default = "default_output_parquet_max_open_files")]
    pub parquet_max_open_files: usize,

    /// Close (finalize) parquet files that haven't received data for this many seconds,
    /// even if still within the current hour bucket. 0 disables.
    #[serde(default = "default_output_parquet_idle_close_secs")]
    pub parquet_idle_close_secs: u64,

    #[serde(default = "default_output_parquet_zstd_level")]
    pub parquet_zstd_level: i32,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            dir: default_output_dir(),
            format: default_output_format(),
            bucket_minutes: default_output_bucket_minutes(),
            retention_hours: default_output_retention_hours(),
            cleanup_interval_secs: default_output_cleanup_interval_secs(),
            queue_capacity: default_output_queue_capacity(),
            parquet_batch_size: default_output_parquet_batch_size(),
            parquet_record_batch_size: default_output_parquet_record_batch_size(),
            parquet_max_open_files: default_output_parquet_max_open_files(),
            parquet_idle_close_secs: default_output_parquet_idle_close_secs(),
            parquet_zstd_level: default_output_parquet_zstd_level(),
        }
    }
}

fn default_output_dir() -> String {
    "data".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    Parquet,
    Jsonl,
    Csv,
}

fn default_output_format() -> OutputFormat {
    OutputFormat::Parquet
}

fn default_output_bucket_minutes() -> i64 {
    60
}

fn default_output_retention_hours() -> i64 {
    24
}

fn default_output_cleanup_interval_secs() -> u64 {
    300
}

fn default_output_queue_capacity() -> usize {
    // Protect the host from unbounded memory growth when IO is saturated or the writer falls behind.
    // When the queue is full, producers will drop events (best-effort logging).
    200_000
}

fn default_output_parquet_batch_size() -> usize {
    50_000
}

fn default_output_parquet_record_batch_size() -> usize {
    // IMPORTANT: the writer keeps one RecordBatch builder per open file.
    // With per-symbol hourly partitioning, open files can easily reach 3 * 200 = 600.
    // A large batch size pre-allocates large column buffers per file and can OOM.
    512
}

fn default_output_parquet_max_open_files() -> usize {
    // The writer keeps one RecordBatch builder and one ArrowWriter per open file.
    // With 3 exchanges * 200 symbols, open files can exceed 600 and explode memory on small boxes.
    128
}

fn default_output_parquet_idle_close_secs() -> u64 {
    // Helps cap memory when `parquet_max_open_files` is 0 (unlimited) by closing inactive files.
    // For very active tickers this won't trigger often; use `parquet_max_open_files` first.
    0
}

fn default_output_parquet_zstd_level() -> i32 {
    3
}
