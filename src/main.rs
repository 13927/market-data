use anyhow::Context;
use crossbeam_channel::{bounded, unbounded};
use tracing::{info, warn};

mod config;
mod exchange;
mod schema;
mod util;
mod writer;
mod ws;

fn err_contains_http_status(err: &anyhow::Error, status: u16) -> bool {
    let needle = format!("HTTP error: {status}");
    err.chain().any(|e| e.to_string().contains(&needle))
}

fn validate_config_for_scale(cfg: &config::Config) -> anyhow::Result<()> {
    use std::collections::{HashMap, HashSet};

    fn norm_symbol(s: &str) -> String {
        s.trim().to_ascii_uppercase()
    }

    fn add_symbols(
        map: &mut HashMap<&'static str, HashSet<String>>,
        ex: &'static str,
        syms: &[String],
    ) {
        if syms.is_empty() {
            return;
        }
        let set = map.entry(ex).or_default();
        for s in syms {
            set.insert(norm_symbol(s));
        }
    }

    fn add_stream_symbols(
        map: &mut HashMap<&'static str, HashSet<String>>,
        ex: &'static str,
        streams: &[String],
    ) {
        if streams.is_empty() {
            return;
        }
        let set = map.entry(ex).or_default();
        for stream in streams {
            let sym = stream.split('@').next().unwrap_or(stream.as_str());
            if !sym.is_empty() {
                set.insert(norm_symbol(sym));
            }
        }
    }

    // Estimate how many files can be open concurrently.
    // We partition by exchange/symbol/hour (bucket), so worst-case open_files ~= sum(symbols_per_exchange).
    let mut per_exchange: HashMap<&'static str, HashSet<String>> = HashMap::new();

    if cfg.binance_spot_sbe.enabled {
        let streams = exchange::binance_spot_sbe::effective_streams(&cfg.binance_spot_sbe);
        if !streams.is_empty() {
            add_stream_symbols(&mut per_exchange, "binance", &streams);
        } else {
            add_symbols(&mut per_exchange, "binance", &cfg.binance_spot_sbe.symbols);
        }
    }
    if cfg.binance_spot_json.enabled {
        add_symbols(&mut per_exchange, "binance", &cfg.binance_spot_json.symbols);
    }
    if cfg.binance_futures_json.enabled {
        add_symbols(
            &mut per_exchange,
            "binance",
            &cfg.binance_futures_json.symbols,
        );
    }
    if cfg.kucoin.enabled || cfg.kucoin_futures.enabled {
        add_symbols(&mut per_exchange, "kucoin", &cfg.kucoin.symbols);
        add_symbols(&mut per_exchange, "kucoin", &cfg.kucoin_futures.symbols);
    }
    if cfg.gate.enabled {
        add_symbols(&mut per_exchange, "gate", &cfg.gate.symbols);
    }

    let open_files_est: usize = per_exchange.values().map(|s| s.len()).sum();

    if matches!(cfg.output.format, config::OutputFormat::Parquet) {
        let batch = cfg.output.parquet_record_batch_size;
        let row_group = cfg.output.parquet_batch_size;
        if open_files_est >= 100 && batch >= 2048 {
            anyhow::bail!(
                "parquet_record_batch_size={} is too large for estimated open_files≈{}; \
set output.parquet_record_batch_size=256..512 to avoid OOM/host stalls",
                batch,
                open_files_est
            );
        }
        if open_files_est >= 200 && batch > 512 {
            warn!(
                "high symbol count detected (open_files≈{}). Consider output.parquet_record_batch_size=256..512 (current={}) to reduce memory.",
                open_files_est, batch
            );
        }
        if open_files_est >= 100 && row_group >= 20_000 {
            warn!(
                "high symbol count detected (open_files≈{}). Consider output.parquet_batch_size=2000..5000 (current={}) to reduce per-file buffering and memory.",
                open_files_est,
                row_group
            );
        }
        if open_files_est >= 200 && row_group > 10_000 {
            warn!(
                "high symbol count detected (open_files≈{}). output.parquet_batch_size={} may be too large for 2c/4g; consider 2000..5000.",
                open_files_est,
                row_group
            );
        }
    }

    // KuCoin: protect users from 509 throttles caused by sending subscribe messages too quickly.
    // KuCoin's control-message budget includes subscribe + ping/pong text frames.
    if cfg.kucoin.enabled {
        let per_symbol = (cfg.kucoin.ticker as usize) + (cfg.kucoin.l5 as usize);
        let topics = cfg.kucoin.symbols.len().saturating_mul(per_symbol);
        if topics > 0
            && cfg.kucoin.global_subscribe_delay_ms == 0
            && cfg.kucoin.subscribe_delay_ms < 50
        {
            warn!(
                "kucoin pacing is aggressive (topics≈{} subscribe_delay_ms={} global_subscribe_delay_ms=0); \
set kucoin.global_subscribe_delay_ms=100..250 or kucoin.subscribe_delay_ms>=100 to avoid code=509 throttles",
                topics,
                cfg.kucoin.subscribe_delay_ms
            );
        }
    }
    if cfg.kucoin_futures.enabled {
        let per_symbol = (cfg.kucoin_futures.ticker as usize) + (cfg.kucoin_futures.l5 as usize);
        let topics = cfg.kucoin_futures.symbols.len().saturating_mul(per_symbol);
        if topics > 0
            && cfg.kucoin_futures.global_subscribe_delay_ms == 0
            && cfg.kucoin_futures.subscribe_delay_ms < 50
        {
            warn!(
                "kucoin_futures pacing is aggressive (topics≈{} subscribe_delay_ms={} global_subscribe_delay_ms=0); \
set kucoin.global_subscribe_delay_ms=100..250 (shared pacer) or kucoin_futures.subscribe_delay_ms>=100 to avoid code=509 throttles",
                topics,
                cfg.kucoin_futures.subscribe_delay_ms
            );
        }
    }

    Ok(())
}

fn jittered_sleep_secs(base_secs: u64) -> std::time::Duration {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let backfill_csv = args.iter().any(|a| a == "--backfill-csv");
    let mut single_csv: Option<String> = None;
    let mut i = 1;
    while i + 1 < args.len() {
        if args[i] == "--convert-csv" {
            single_csv = Some(args[i + 1].clone());
            break;
        }
        i += 1;
    }

    if backfill_csv || single_csv.is_some() {
        use std::fs::OpenOptions;
        use std::sync::Arc;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("csv_error_report.log")
            .context("open csv_error_report.log")?;
        let writer = Arc::new(file);

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_ansi(false)
            .init();
    }

    let cfg = config::Config::from_path("config.toml")?;
    log_config_summary(&cfg);
    validate_config_for_scale(&cfg)?;

    if let Some(path_str) = single_csv {
        let path = std::path::Path::new(&path_str);
        let start = std::time::Instant::now();
        writer::csv::convert_csv_to_parquet(path)?;
        let dur = start.elapsed();
        println!(
            "Converted single CSV {:?} in {:.3}s",
            path,
            dur.as_secs_f64()
        );
        return Ok(());
    }

    if backfill_csv {
        let dir = std::path::Path::new(&cfg.output.dir);
        let bucket_minutes = cfg.output.bucket_minutes;
        writer::csv::scan_and_convert_legacy_files(dir, bucket_minutes);
        return Ok(());
    }

    let (event_tx, event_rx) = if cfg.output.queue_capacity == 0 {
        unbounded::<schema::event::MarketEvent>()
    } else {
        bounded::<schema::event::MarketEvent>(cfg.output.queue_capacity)
    };
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    if cfg.binance_spot_sbe.enabled {
        let streams = exchange::binance_spot_sbe::effective_streams(&cfg.binance_spot_sbe);
        if streams.is_empty() {
            anyhow::bail!("binance_spot_sbe.enabled=true but no streams/symbols configured");
        }

        let headers = exchange::binance_spot_sbe::required_headers_from_config(
            cfg.binance_spot_sbe.api_key.as_deref(),
        )?;

        // Binance SBE allows up to 1024 streams per connection. When we use a stable seed stream
        // in the URL path (`/ws/<seed>`), that stream is implicitly subscribed, so keep
        // params <= 1023 to stay within the limit even if seed isn't part of the configured list.
        const SBE_PARAMS_PER_CONN: usize = 1023;

        for chunk in streams.chunks(SBE_PARAMS_PER_CONN) {
            let chunk_streams: Vec<String> = chunk.iter().cloned().collect();
            let chunk_len = chunk_streams.len();
            let endpoint = cfg.binance_spot_sbe.endpoint.clone();
            let headers = headers.clone();
            let sender = event_tx.clone();
            let handle = tokio::spawn(async move {
                use std::collections::HashSet;

                let allowed_symbols: HashSet<String> = chunk_streams
                    .iter()
                    .filter_map(|s| s.split('@').next())
                    .map(|s| s.to_ascii_uppercase())
                    .collect();

                let header_refs: Vec<(&str, &str)> =
                    headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
                let mut backoff_secs = 1u64;
                let mut ended_warn_count = 0u64;
                let mut last_backoff_logged = 0u64;
                let seed_candidates = [
                    "btcusdt@bestBidAsk",
                    "ethusdt@bestBidAsk",
                    "bnbusdt@bestBidAsk",
                    "solusdt@bestBidAsk",
                    "xrpusdt@bestBidAsk",
                ];
                let mut seed_idx = 0usize;
                loop {
                    let seed = seed_candidates
                        .get(seed_idx)
                        .copied()
                        .unwrap_or(seed_candidates[0]);
                    let endpoint = endpoint.trim_end_matches('/');
                    let url = format!("{endpoint}/ws/{seed}");
                    let subscribe_msg = {
                        let mut params: Vec<&str> = Vec::with_capacity(chunk_streams.len());
                        for s in &chunk_streams {
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

                    info!(
                        "connect binance_spot_sbe: streams={} url={}",
                        chunk_len, url
                    );
                    if let Some(msg) = &subscribe_msg {
                        info!(
                            "binance_spot_sbe subscribe (extra streams) len={}",
                            msg.len()
                        );
                    }

                    let mut binary_frames = 0u64;
                    let mut logged_first_binary = false;
                    let mut logged_first_text = false;
                    let res = ws::client::run_ws_with_handler(
                        &url,
                        subscribe_msg.clone(),
                        &header_refs,
                        |frame| {
                            match frame {
                                ws::WsFrame::Text(text) => {
                                    if !logged_first_text {
                                        logged_first_text = true;
                                        // subscription responses are JSON text frames
                                        if let Ok(v) =
                                            serde_json::from_str::<serde_json::Value>(&text)
                                        {
                                            if let Some(err) = v.get("error") {
                                                warn!("binance_spot_sbe control error: {}", err);
                                            } else {
                                                info!("binance_spot_sbe control: {}", text);
                                            }
                                        } else {
                                            info!("binance_spot_sbe text: {}", text);
                                        }
                                    }
                                }
                                ws::WsFrame::Binary(bytes) => {
                                    binary_frames += 1;
                                    if !logged_first_binary {
                                        logged_first_binary = true;
                                        info!(
                                            "first event binance_spot_sbe: {} bytes",
                                            bytes.len()
                                        );
                                    }
                                    match exchange::binance_spot_sbe_decode::decode_frame(&bytes) {
                                        Ok(evs) => {
                                            for ev in evs {
                                                if !allowed_symbols.is_empty()
                                                    && !allowed_symbols.contains(&ev.symbol)
                                                {
                                                    continue;
                                                }
                                                if sender.try_send(ev).is_err() {
                                                    crate::util::metrics::inc_dropped_events(1);
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            warn!("decode sbe frame failed: {err:#}");
                                        }
                                    }
                                }
                            }
                            Ok(())
                        },
                    )
                    .await;
                    let ok = res
                        .as_ref()
                        .is_ok_and(|s| s.duration >= std::time::Duration::from_secs(10));
                    match res {
                        Ok(stats) => {
                            ended_warn_count += 1;
                            if ended_warn_count == 1
                                || backoff_secs != last_backoff_logged
                                || ended_warn_count % 10 == 0
                            {
                                warn!(
                                    "binance_spot_sbe ws ended ({url}): frames={} binary_frames={} duration_ms={} backoff_secs={} restarts={}",
                                    stats.frames,
                                    binary_frames,
                                    stats.duration.as_millis(),
                                    backoff_secs,
                                    ended_warn_count
                                );
                                last_backoff_logged = backoff_secs;
                            }
                        }
                        Err(err) => {
                            ended_warn_count += 1;
                            if ended_warn_count == 1
                                || backoff_secs != last_backoff_logged
                                || ended_warn_count % 10 == 0
                            {
                                warn!(
                                    "binance_spot_sbe ws ended ({url}): {err:#} backoff_secs={} restarts={}",
                                    backoff_secs, ended_warn_count
                                );
                                last_backoff_logged = backoff_secs;
                            }
                            if err_contains_http_status(&err, 400) {
                                seed_idx = (seed_idx + 1) % seed_candidates.len();
                                warn!(
                                    "binance_spot_sbe: HTTP 400 on seed stream; trying next seed (seed_idx={}/{})",
                                    seed_idx + 1,
                                    seed_candidates.len()
                                );
                                tokio::time::sleep(jittered_sleep_secs(1)).await;
                                continue;
                            }
                        }
                    }
                    tokio::time::sleep(jittered_sleep_secs(backoff_secs)).await;
                    backoff_secs = next_backoff_secs(backoff_secs, ok);
                }
            });
            tasks.push(handle);
        }
    }

    if cfg.binance_spot_json.enabled {
        if cfg.binance_spot_json.combined {
            tasks.extend(spawn_binance_json_combined(
                false,
                cfg.binance_spot_json.endpoint.clone(),
                cfg.binance_spot_json.symbols.clone(),
                cfg.binance_spot_json.book,
                cfg.binance_spot_json.l5,
                cfg.binance_spot_json.trade,
                event_tx.clone(),
            ));
        } else {
            for symbol in &cfg.binance_spot_json.symbols {
                let symbol = symbol.to_string();
                if cfg.binance_spot_json.book {
                    tasks.push(spawn_binance_json(
                        exchange::binance_json::BinanceJsonKind::SpotBook,
                        cfg.binance_spot_json.endpoint.clone(),
                        symbol.clone(),
                        event_tx.clone(),
                    ));
                }
                if cfg.binance_spot_json.l5 {
                    tasks.push(spawn_binance_json(
                        exchange::binance_json::BinanceJsonKind::SpotL5,
                        cfg.binance_spot_json.endpoint.clone(),
                        symbol.clone(),
                        event_tx.clone(),
                    ));
                }
                if cfg.binance_spot_json.trade {
                    tasks.push(spawn_binance_json(
                        exchange::binance_json::BinanceJsonKind::SpotTrade,
                        cfg.binance_spot_json.endpoint.clone(),
                        symbol.clone(),
                        event_tx.clone(),
                    ));
                }
            }
        }
    }

    if cfg.binance_futures_json.enabled {
        if cfg.binance_futures_json.combined {
            tasks.extend(spawn_binance_json_combined(
                true,
                cfg.binance_futures_json.endpoint.clone(),
                cfg.binance_futures_json.symbols.clone(),
                cfg.binance_futures_json.book,
                cfg.binance_futures_json.l5,
                cfg.binance_futures_json.trade,
                event_tx.clone(),
            ));
        } else {
            for symbol in &cfg.binance_futures_json.symbols {
                let symbol = symbol.to_string();
                if cfg.binance_futures_json.book {
                    tasks.push(spawn_binance_json(
                        exchange::binance_json::BinanceJsonKind::FutureBook,
                        cfg.binance_futures_json.endpoint.clone(),
                        symbol.clone(),
                        event_tx.clone(),
                    ));
                }
                if cfg.binance_futures_json.l5 {
                    tasks.push(spawn_binance_json(
                        exchange::binance_json::BinanceJsonKind::FutureL5,
                        cfg.binance_futures_json.endpoint.clone(),
                        symbol.clone(),
                        event_tx.clone(),
                    ));
                }
                if cfg.binance_futures_json.trade {
                    tasks.push(spawn_binance_json(
                        exchange::binance_json::BinanceJsonKind::FutureTrade,
                        cfg.binance_futures_json.endpoint.clone(),
                        symbol.clone(),
                        event_tx.clone(),
                    ));
                }
            }
        }
    }

    // KuCoin has strict control-message rate limits (subscribe/unsubscribe). When both spot and
    // futures are enabled, we must pace subscriptions *globally* across all KuCoin connections to
    // avoid `code=509 exceed max permits per second`.
    let kucoin_global_ms = cfg
        .kucoin
        .global_subscribe_delay_ms
        .max(cfg.kucoin_futures.global_subscribe_delay_ms);
    let kucoin_pacer = kucoin_global_ms
        .checked_add(0)
        .filter(|ms| *ms > 0)
        .map(|ms| {
            std::sync::Arc::new(util::pacer::Pacer::new(std::time::Duration::from_millis(
                ms,
            )))
        });

    if cfg.kucoin.enabled {
        match exchange::kucoin::spawn_kucoin_public_ws(
            cfg.kucoin.clone(),
            kucoin_pacer.clone(),
            event_tx.clone(),
        )
        .await
        {
            Ok(hs) => tasks.extend(hs),
            Err(err) => warn!("kucoin spawn failed: {err:#}"),
        }
    }

    // KuCoin swap: prefer the unified `[kucoin]` config (swap=true) so users don't maintain
    // a separate futures symbol list. The legacy `[kucoin_futures]` block can still override.
    let want_kucoin_swap = cfg.kucoin_futures.enabled || (cfg.kucoin.enabled && cfg.kucoin.swap);
    if want_kucoin_swap {
        let mut fut_cfg = if cfg.kucoin_futures.enabled {
            cfg.kucoin_futures.clone()
        } else {
            config::KucoinFuturesConfig {
                enabled: true,
                rest_endpoint: cfg.kucoin.futures_rest_endpoint.clone(),
                subscribe_delay_ms: cfg.kucoin.subscribe_delay_ms,
                conn_stagger_ms: cfg.kucoin.conn_stagger_ms,
                global_subscribe_delay_ms: 0,
                symbols: vec![],
                ticker: cfg.kucoin.ticker,
                l5: cfg.kucoin.l5,
                trade: cfg.kucoin.trade,
            }
        };

        if fut_cfg.symbols.is_empty() {
            match exchange::kucoin_futures::resolve_futures_contract_symbols(
                &fut_cfg.rest_endpoint,
                &cfg.kucoin.symbols,
            )
            .await
            {
                Ok(syms) => fut_cfg.symbols = syms,
                Err(err) => warn!("kucoin_futures resolve symbols failed: {err:#}"),
            }
        }

        if fut_cfg.symbols.is_empty() {
            warn!("kucoin_futures enabled but no futures symbols resolved; skipping");
        } else {
            match exchange::kucoin_futures::spawn_kucoin_futures_public_ws(
                fut_cfg,
                kucoin_pacer.clone(),
                event_tx.clone(),
            ) {
                Ok(hs) => tasks.extend(hs),
                Err(err) => warn!("kucoin_futures spawn failed: {err:#}"),
            }
        }
    }

    if cfg.gate.enabled {
        match exchange::gate::spawn_gate_ws(cfg.gate.clone(), event_tx.clone()).await {
            Ok(hs) => tasks.extend(hs),
            Err(err) => warn!("gate spawn failed: {err:#}"),
        }
    }

    if !cfg.binance_spot_sbe.enabled
        && !cfg.binance_spot_json.enabled
        && !cfg.binance_futures_json.enabled
        && !cfg.kucoin.enabled
        && !cfg.kucoin_futures.enabled
        && !cfg.gate.enabled
    {
        warn!("no sources enabled in config.toml");
    }

    let output_dir = cfg.output.dir.clone();
    let bucket_minutes = cfg.output.bucket_minutes;
    let retention_hours = cfg.output.retention_hours;
    let cleanup_interval_secs = cfg.output.cleanup_interval_secs;
    let disk_soft_limit_gb = cfg.output.disk_soft_limit_gb;
    let format = cfg.output.format.clone();
    let parquet_batch_size = cfg.output.parquet_batch_size;
    let parquet_record_batch_size = cfg.output.parquet_record_batch_size;
    let parquet_max_open_files = cfg.output.parquet_max_open_files;
    let parquet_idle_close_secs = cfg.output.parquet_idle_close_secs;
    let parquet_zstd_level = cfg.output.parquet_zstd_level;
    {
        match writer::rollover::cleanup_with_disk_limit(
            std::path::Path::new(&output_dir),
            bucket_minutes,
            retention_hours,
            disk_soft_limit_gb,
        ) {
            Ok(deleted) => {
                if deleted > 0 {
                    info!(
                        "startup cleanup: dir={} deleted_files={}",
                        output_dir, deleted
                    );
                }
            }
            Err(err) => {
                warn!("startup cleanup failed: {err:#}");
            }
        }
    }
    let writer_handle = std::thread::spawn(move || match format {
        config::OutputFormat::Parquet => {
            let mut w = writer::parquet::ParquetFileWriter::new(
                output_dir,
                bucket_minutes,
                parquet_batch_size,
                parquet_record_batch_size,
                parquet_max_open_files,
                parquet_idle_close_secs,
                parquet_zstd_level,
                retention_hours,
                cleanup_interval_secs,
                disk_soft_limit_gb,
            )
            .expect("init parquet writer");
            let tick = crossbeam_channel::tick(std::time::Duration::from_secs(5));
            loop {
                crossbeam_channel::select! {
                    recv(event_rx) -> msg => match msg {
                        Ok(ev) => {
                            if let Err(err) = w.write_event(&ev) {
                                warn!("write_event failed: {err:#}");
                            }
                        }
                        Err(_) => break,
                    },
                    recv(tick) -> _ => {
                        let queue_len = event_rx.len();
                        if let Err(err) = w.maintenance_tick(queue_len) {
                            warn!("writer maintenance failed: {err:#}");
                        }
                    }
                }
            }
            if let Err(err) = w.close_all() {
                warn!("final close failed: {err:#}");
            }
            info!("writer exited; open_files={}", w.open_file_count());
        }
        config::OutputFormat::Jsonl => {
            let mut w = writer::jsonl::JsonlFileWriter::new(
                output_dir,
                bucket_minutes,
                retention_hours,
                cleanup_interval_secs,
                disk_soft_limit_gb,
            )
            .expect("init jsonl writer");
            let tick = crossbeam_channel::tick(std::time::Duration::from_secs(5));
            loop {
                crossbeam_channel::select! {
                    recv(event_rx) -> msg => match msg {
                        Ok(ev) => {
                            if let Err(err) = w.write_event(&ev) {
                                warn!("write_event failed: {err:#}");
                            }
                        }
                        Err(_) => break,
                    },
                    recv(tick) -> _ => {
                        let queue_len = event_rx.len();
                        if let Err(err) = w.maintenance_tick(queue_len) {
                            warn!("writer maintenance failed: {err:#}");
                        }
                    }
                }
            }
            if let Err(err) = w.close_all() {
                warn!("final close failed: {err:#}");
            }
            info!("writer exited; open_files={}", w.open_file_count());
        }
        config::OutputFormat::Csv => {
            let mut w = writer::csv::CsvFileWriter::new(
                output_dir,
                bucket_minutes,
                retention_hours,
                cleanup_interval_secs,
                disk_soft_limit_gb,
            )
            .expect("init csv writer");
            let tick = crossbeam_channel::tick(std::time::Duration::from_secs(5));
            loop {
                crossbeam_channel::select! {
                    recv(event_rx) -> msg => match msg {
                        Ok(ev) => {
                            if let Err(err) = w.write_event(&ev) {
                                warn!("write_event failed: {err:#}");
                            }
                        }
                        Err(_) => break,
                    },
                    recv(tick) -> _ => {
                        let queue_len = event_rx.len();
                        if let Err(err) = w.maintenance_tick(queue_len) {
                            warn!("writer maintenance failed: {err:#}");
                        }
                    }
                }
            }
            if let Err(err) = w.close_all() {
                warn!("final close failed: {err:#}");
            }
            info!("writer exited; open_files={}", w.open_file_count());
        }
    });

    wait_for_shutdown().await?;
    info!("shutdown: stopping tasks");
    for t in &tasks {
        t.abort();
    }
    // Wait for all tasks to finish (releasing tx)
    futures_util::future::join_all(tasks).await;

    drop(event_tx);
    let _ = writer_handle.join();

    Ok(())
}

fn spawn_binance_json(
    kind: exchange::binance_json::BinanceJsonKind,
    endpoint: String,
    symbol: String,
    sender: crossbeam_channel::Sender<schema::event::MarketEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let url = kind.ws_endpoint(&endpoint, &symbol);
        let mut backoff_secs = 1u64;
        let mut ended_warn_count = 0u64;
        let mut last_backoff_logged = 0u64;
        loop {
            let res = ws::client::run_ws_with_handler(&url, None, &[], |frame| {
                let text = match frame {
                    ws::WsFrame::Text(t) => t,
                    ws::WsFrame::Binary(_) => return Ok(()),
                };
                match exchange::binance_json::parse_event(kind, &symbol, &text) {
                    Ok(Some(ev)) => {
                        if sender.try_send(ev).is_err() {
                            crate::util::metrics::inc_dropped_events(1);
                        }
                    }
                    Ok(None) => {}
                    Err(err) => warn!("parse binance json failed ({url}): {err:#}"),
                }
                Ok(())
            })
            .await;
            let ok = res
                .as_ref()
                .is_ok_and(|s| s.frames > 0 && s.duration >= std::time::Duration::from_secs(10));
            match res {
                Ok(stats) => {
                    ended_warn_count += 1;
                    if ended_warn_count == 1
                        || backoff_secs != last_backoff_logged
                        || ended_warn_count % 10 == 0
                    {
                        warn!(
                            "ws ended ({url}): frames={} duration_ms={} backoff_secs={} restarts={}",
                            stats.frames,
                            stats.duration.as_millis(),
                            backoff_secs,
                            ended_warn_count
                        );
                        last_backoff_logged = backoff_secs;
                    }
                }
                Err(err) => {
                    ended_warn_count += 1;
                    if ended_warn_count == 1
                        || backoff_secs != last_backoff_logged
                        || ended_warn_count % 10 == 0
                    {
                        warn!(
                            "ws ended ({url}): {err:#} backoff_secs={} restarts={}",
                            backoff_secs, ended_warn_count
                        );
                        last_backoff_logged = backoff_secs;
                    }
                }
            }
            tokio::time::sleep(jittered_sleep_secs(backoff_secs)).await;
            backoff_secs = next_backoff_secs(backoff_secs, ok);
        }
    })
}

fn spawn_binance_json_combined(
    is_futures: bool,
    endpoint: String,
    symbols: Vec<String>,
    book: bool,
    l5: bool,
    trade: bool,
    sender: crossbeam_channel::Sender<schema::event::MarketEvent>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let base = endpoint.trim_end_matches('/').to_string();
    let mut streams: Vec<String> = Vec::new();
    for symbol in &symbols {
        let s = symbol.to_ascii_lowercase();
        if book {
            streams.push(format!("{s}@bookTicker"));
        }
        if l5 {
            streams.push(format!("{s}@depth5@100ms"));
        }
        if trade {
            streams.push(format!("{s}@trade"));
        }
    }

    if streams.is_empty() {
        warn!("combined streams empty; skipping (is_futures={is_futures})");
        return vec![];
    }

    let mut handles = Vec::new();
    for chunk in streams.chunks(1024) {
        let stream_count = chunk.len();
        let url = format!("{base}/stream?streams={}", chunk.join("/"));
        let sender = sender.clone();
        handles.push(tokio::spawn(async move {
            info!(
                "connect binance{} combined: streams={}",
                if is_futures { "_futures" } else { "" },
                stream_count
            );

            let mut backoff_secs = 1u64;
            let mut ended_warn_count = 0u64;
            let mut last_backoff_logged = 0u64;
            loop {
                let mut logged_first_event = false;
                let res = ws::client::run_ws_with_handler(&url, None, &[], |frame| {
                    let text = match frame {
                        ws::WsFrame::Text(t) => t,
                        ws::WsFrame::Binary(_) => return Ok(()),
                    };

                    let v: serde_json::Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(err) => {
                            warn!("parse combined json failed: {err:#}");
                            return Ok(());
                        }
                    };

                    let stream_name = match v.get("stream").and_then(|s| s.as_str()) {
                        Some(s) => s,
                        None => return Ok(()),
                    };
                    if !logged_first_event {
                        logged_first_event = true;
                        info!(
                            "first event binance{} combined: {}",
                            if is_futures { "_futures" } else { "" },
                            stream_name
                        );
                    }
                    let kind = match exchange::binance_json::kind_from_stream_name(
                        is_futures,
                        stream_name,
                    ) {
                        Some(k) => k,
                        None => return Ok(()),
                    };
                    let symbol_hint = exchange::binance_json::symbol_from_stream_name(stream_name)
                        .unwrap_or_else(|| "UNKNOWN".to_string());

                    match exchange::binance_json::parse_event_value(kind, &symbol_hint, &v) {
                        Ok(Some(ev)) => {
                            if sender.try_send(ev).is_err() {
                                crate::util::metrics::inc_dropped_events(1);
                            }
                        }
                        Ok(None) => {}
                        Err(err) => warn!("parse combined event failed ({url}): {err:#}"),
                    }

                    Ok(())
                })
                .await;
                let ok = res.as_ref().is_ok_and(|s| {
                    s.frames > 0 && s.duration >= std::time::Duration::from_secs(10)
                });
                match res {
                    Ok(stats) => {
                        ended_warn_count += 1;
                        if ended_warn_count == 1
                            || backoff_secs != last_backoff_logged
                            || ended_warn_count % 10 == 0
                        {
                            warn!(
                                "combined ws ended ({url}): frames={} duration_ms={} backoff_secs={} restarts={}",
                                stats.frames,
                                stats.duration.as_millis(),
                                backoff_secs,
                                ended_warn_count
                            );
                            last_backoff_logged = backoff_secs;
                        }
                    }
                    Err(err) => {
                        ended_warn_count += 1;
                        if ended_warn_count == 1
                            || backoff_secs != last_backoff_logged
                            || ended_warn_count % 10 == 0
                        {
                            warn!(
                                "combined ws ended ({url}): {err:#} backoff_secs={} restarts={}",
                                backoff_secs, ended_warn_count
                            );
                            last_backoff_logged = backoff_secs;
                        }
                    }
                }
                tokio::time::sleep(jittered_sleep_secs(backoff_secs)).await;
                backoff_secs = next_backoff_secs(backoff_secs, ok);
            }
        }));
    }

    handles
}

#[cfg(unix)]
async fn wait_for_shutdown() -> anyhow::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigint = signal(SignalKind::interrupt()).context("register SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("register SIGTERM handler")?;
    tokio::select! {
        _ = sigint.recv() => {},
        _ = sigterm.recv() => {},
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown() -> anyhow::Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn log_config_summary(cfg: &config::Config) {
    info!(
        "output: dir={} format={:?} bucket_minutes={} retention_hours={} cleanup_interval_secs={} queue_capacity={} parquet_row_group_size={} parquet_record_batch_size={} parquet_zstd_level={}",
        cfg.output.dir,
        cfg.output.format,
        cfg.output.bucket_minutes,
        cfg.output.retention_hours,
        cfg.output.cleanup_interval_secs,
        cfg.output.queue_capacity,
        cfg.output.parquet_batch_size,
        cfg.output.parquet_record_batch_size,
        cfg.output.parquet_zstd_level
    );

    if cfg.binance_spot_json.enabled {
        info!(
            "source: binance_spot_json enabled endpoint={} symbols={} combined={} book={} l5={} trade={}",
            cfg.binance_spot_json.endpoint,
            cfg.binance_spot_json.symbols.len(),
            cfg.binance_spot_json.combined,
            cfg.binance_spot_json.book,
            cfg.binance_spot_json.l5,
            cfg.binance_spot_json.trade
        );
    }
    if cfg.binance_futures_json.enabled {
        info!(
            "source: binance_futures_json enabled endpoint={} symbols={} combined={} book={} l5={} trade={}",
            cfg.binance_futures_json.endpoint,
            cfg.binance_futures_json.symbols.len(),
            cfg.binance_futures_json.combined,
            cfg.binance_futures_json.book,
            cfg.binance_futures_json.l5,
            cfg.binance_futures_json.trade
        );
    }
    if cfg.binance_spot_sbe.enabled {
        info!(
            "source: binance_spot_sbe enabled endpoint={} symbols={} best_bid_ask={} depth20={} trade={} api_key={}",
            cfg.binance_spot_sbe.endpoint,
            cfg.binance_spot_sbe.symbols.len(),
            cfg.binance_spot_sbe.best_bid_ask,
            cfg.binance_spot_sbe.depth20,
            cfg.binance_spot_sbe.trade,
            match cfg.binance_spot_sbe.api_key.as_deref().unwrap_or("").trim() {
                "" => "<missing>",
                v if v.contains("PUT_YOUR_") || v.contains("ED25519_API_KEY_HERE") => "<placeholder>",
                _ => "<redacted>",
            }
        );
    }

    if cfg.kucoin.enabled {
        info!(
            "source: kucoin enabled rest_endpoint={} futures_rest_endpoint={} symbols={} ticker={} l5={} swap={} subscribe_delay_ms={} conn_stagger_ms={} global_subscribe_delay_ms={}",
            cfg.kucoin.rest_endpoint,
            cfg.kucoin.futures_rest_endpoint,
            cfg.kucoin.symbols.len(),
            cfg.kucoin.ticker,
            cfg.kucoin.l5,
            cfg.kucoin.swap,
            cfg.kucoin.subscribe_delay_ms,
            cfg.kucoin.conn_stagger_ms,
            cfg.kucoin.global_subscribe_delay_ms
        );
    }

    if cfg.kucoin_futures.enabled {
        info!(
            "source: kucoin_futures enabled rest_endpoint={} symbols={} ticker={} l5={} subscribe_delay_ms={} conn_stagger_ms={} global_subscribe_delay_ms={}",
            cfg.kucoin_futures.rest_endpoint,
            cfg.kucoin_futures.symbols.len(),
            cfg.kucoin_futures.ticker,
            cfg.kucoin_futures.l5,
            cfg.kucoin_futures.subscribe_delay_ms,
            cfg.kucoin_futures.conn_stagger_ms,
            cfg.kucoin_futures.global_subscribe_delay_ms
        );
    }

    if cfg.gate.enabled {
        info!(
            "source: gate enabled spot_endpoint={} futures_endpoint={} symbols={} spot_ticker={} spot_l5={} swap_ticker={} swap_l5={}",
            cfg.gate.spot_endpoint,
            cfg.gate.futures_endpoint,
            cfg.gate.symbols.len(),
            cfg.gate.spot_ticker,
            cfg.gate.spot_l5,
            cfg.gate.swap_ticker,
            cfg.gate.swap_l5
        );
    }
}
