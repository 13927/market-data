use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context;
use crossbeam_channel::{unbounded, Sender};
use tracing::{debug, error, info, warn};

use super::{bucket_id, bucket_time_str, exchange_symbol_dir, time_now_ms};
use crate::schema::event::MarketEvent;

struct WriterEntry {
    writer: csv::Writer<BufWriter<fs::File>>,
    file_path: PathBuf,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct FileKey {
    exchange: String,
    symbol: String,
    bucket_id: i64,
}

pub struct CsvFileWriter {
    dir: PathBuf,
    bucket_minutes: i64,
    retention_hours: i64,
    cleanup_interval: Duration,
    last_cleanup: Instant,

    last_queue_len: usize,
    queue_warn_len: usize,
    report_interval: Duration,
    last_report: Instant,

    writers: HashMap<FileKey, WriterEntry>,
    write_count: u64,

    conversion_tx: Option<Sender<PathBuf>>,
    conversion_thread: Option<JoinHandle<()>>,
}

impl CsvFileWriter {
    pub fn new(
        dir: impl AsRef<Path>,
        bucket_minutes: i64,
        retention_hours: i64,
        cleanup_interval_secs: u64,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = unbounded::<PathBuf>();

        let dir_clone = dir.as_ref().to_path_buf();
        let bucket_minutes_clone = bucket_minutes;

        let handle = thread::spawn(move || {
            info!("CSV to Parquet conversion thread started");

            // Scan for legacy CSV files on startup
            scan_and_convert_legacy_files(&dir_clone, bucket_minutes_clone);

            while let Ok(path) = rx.recv() {
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                debug!("Dequeued for conversion: path={:?} size={}", path, size);
                if let Err(e) = convert_csv_to_parquet(&path) {
                    error!("Failed to convert {:?} to parquet: {:?}", path, e);
                }
            }
            info!("CSV to Parquet conversion thread exited");
        });

        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            bucket_minutes,
            retention_hours,
            cleanup_interval: Duration::from_secs(cleanup_interval_secs),
            last_cleanup: Instant::now(),
            last_queue_len: 0,
            queue_warn_len: 10_000,
            report_interval: Duration::from_secs(10),
            last_report: Instant::now(),
            writers: HashMap::new(),
            write_count: 0,
            conversion_tx: Some(tx),
            conversion_thread: Some(handle),
        })
    }

    pub fn open_file_count(&self) -> usize {
        self.writers.len()
    }

    pub fn write_event(&mut self, ev: &MarketEvent) -> anyhow::Result<()> {
        let bucket_id = bucket_id(ev.local_ts, self.bucket_minutes);
        let key = FileKey {
            exchange: ev.exchange.clone(),
            symbol: ev.symbol.clone(),
            bucket_id,
        };

        let entry = match self.writers.entry(key) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let dir = exchange_symbol_dir(&self.dir, &ev.exchange, &ev.symbol)?;
                let time_part = bucket_time_str(ev.local_ts, self.bucket_minutes);
                let filename = format!("{}_{}_{}.csv", ev.exchange, ev.symbol, time_part);
                let path = dir.join(filename);

                let file_exists = path.exists();
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .with_context(|| format!("open csv {}", path.display()))?;

                let writer = csv::WriterBuilder::new()
                    .has_headers(!file_exists)
                    .from_writer(BufWriter::new(file));

                debug!(
                    "Opened CSV for writing: path={:?} existed={}",
                    path, file_exists
                );
                e.insert(WriterEntry {
                    writer,
                    file_path: path,
                })
            }
        };

        entry.writer.serialize(ev).context("write csv record")?;
        self.write_count += 1;
        Ok(())
    }

    pub fn maintenance_tick(&mut self, queue_len: usize) -> anyhow::Result<()> {
        let now = Instant::now();
        if now.duration_since(self.last_report) > self.report_interval {
            info!(
                "CSV writer stats: open_files={}, write_count={}, queue_len={}",
                self.writers.len(),
                self.write_count,
                queue_len
            );
            self.last_report = now;
            self.write_count = 0;
        }

        if queue_len > self.queue_warn_len && queue_len > self.last_queue_len {
            warn!("Output queue growing: {}", queue_len);
        }
        self.last_queue_len = queue_len;

        // Close stale files (rotation)
        let current_bucket_id = bucket_id(time_now_ms(), self.bucket_minutes);

        // Collect keys to remove to satisfy borrow checker
        let stale: Vec<FileKey> = self
            .writers
            .keys()
            .filter(|k| k.bucket_id < current_bucket_id)
            .cloned()
            .collect();

        for key in stale {
            if let Some(mut entry) = self.writers.remove(&key) {
                // Flush and drop writer
                let _ = entry.writer.flush();
                let path = entry.file_path.clone();
                drop(entry); // Closes file

                // Submit for conversion
                if let Some(tx) = &self.conversion_tx {
                    let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    debug!(
                        "Rotated and queued for conversion: path={:?} size={}",
                        path, size
                    );
                    let _ = tx.send(path);
                }
            }
        }

        // Cleanup old files (retention)
        self.maybe_cleanup()?;

        Ok(())
    }

    pub fn close_all(&mut self) -> anyhow::Result<()> {
        for (_, mut entry) in self.writers.drain() {
            entry.writer.flush()?;
            if let Some(tx) = &self.conversion_tx {
                let _ = tx.send(entry.file_path);
            }
        }
        self.maybe_cleanup()?;

        // Drop sender to signal EOF to conversion thread
        self.conversion_tx = None;

        // Wait for conversion thread to finish
        if let Some(handle) = self.conversion_thread.take() {
            info!("Waiting for CSV conversion thread to finish...");
            if let Err(e) = handle.join() {
                error!("Conversion thread panicked: {:?}", e);
            } else {
                info!("CSV conversion thread finished successfully");
            }
        }

        Ok(())
    }

    fn maybe_cleanup(&mut self) -> anyhow::Result<()> {
        if self.last_cleanup.elapsed() < self.cleanup_interval {
            return Ok(());
        }
        self.last_cleanup = Instant::now();
        let bucket_ms = self.bucket_minutes.max(1) * 60_000;
        let cutoff_ms =
            chrono::Utc::now().timestamp_millis() - self.retention_hours * 3_600_000 - bucket_ms;
        let _ = super::rollover::delete_older_than(&self.dir, cutoff_ms)?;
        Ok(())
    }
}

pub fn scan_and_convert_legacy_files(dir: &Path, bucket_minutes: i64) {
    info!("Scanning for legacy CSV files in {:?}", dir);
    let now = crate::writer::time_now_ms();
    let current_time_str = crate::writer::bucket_time_str(now, bucket_minutes);

    let mut candidates: Vec<PathBuf> = Vec::new();

    for entry in walkdir::WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "csv") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Some(idx) = stem.rfind('_') {
                    let time_str = &stem[idx + 1..];
                    if time_str != current_time_str {
                        candidates.push(path.to_path_buf());
                    }
                }
            }
        }
    }

    let total = candidates.len();
    println!("Found {} legacy CSV files to convert", total);
    if total == 0 {
        info!("No legacy CSV files to convert");
        return;
    }

    let overall_start = Instant::now();
    let processed = Arc::new(AtomicUsize::new(0));
    let worker_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 4);
    println!(
        "Starting legacy CSV conversion with {} workers",
        worker_count
    );

    let (tx, rx) = unbounded::<PathBuf>();
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let rx = rx.clone();
        let processed = Arc::clone(&processed);
        let total = total;
        let handle = thread::spawn(move || {
            while let Ok(path) = rx.recv() {
                let file_start = Instant::now();
                let index = processed.fetch_add(1, Ordering::Relaxed) + 1;
                info!("Converting legacy CSV file {}/{}: {:?}", index, total, path);
                match convert_csv_to_parquet(&path) {
                    Ok(()) => {
                        let dur = file_start.elapsed();
                        println!(
                            "Converted {}/{} in {:.3}s: {:?}",
                            index,
                            total,
                            dur.as_secs_f64(),
                            path
                        );
                        info!(
                            "Converted legacy CSV file {}/{} in {:.3}s: {:?}",
                            index,
                            total,
                            dur.as_secs_f64(),
                            path
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to convert legacy file {:?}: {:?}",
                            path, e
                        );
                    }
                }
            }
        });
        handles.push(handle);
    }

    for path in candidates {
        if tx.send(path).is_err() {
            break;
        }
    }
    drop(tx);

    for handle in handles {
        let _ = handle.join();
    }

    let overall_dur = overall_start.elapsed();
    println!(
        "Legacy CSV conversion complete: {} files processed in {:.3}s",
        total,
        overall_dur.as_secs_f64()
    );
    info!(
        "Legacy scan complete, processed {} files in {:.3}s",
        total,
        overall_dur.as_secs_f64()
    );
}

fn looks_like_timestamp_candidate(s: &str) -> bool {
    if s.len() < 8 {
        return false;
    }
    let has_dash = s.as_bytes().iter().any(|b| *b == b'-');
    let has_colon = s.as_bytes().iter().any(|b| *b == b':');
    let has_space = s.as_bytes().iter().any(|b| *b == b' ');
    has_dash && has_colon && has_space
}

fn is_valid_timestamp_layout(s: &str) -> bool {
    let bytes = s.as_bytes();
    match bytes.len() {
        19 => {
            bytes[4] == b'-'
                && bytes[7] == b'-'
                && bytes[10] == b' '
                && bytes[13] == b':'
                && bytes[16] == b':'
                && bytes
                    .iter()
                    .enumerate()
                    .all(|(i, b)| matches!(i, 4 | 7 | 10 | 13 | 16) || b.is_ascii_digit())
        }
        23 => {
            bytes[4] == b'-'
                && bytes[7] == b'-'
                && bytes[10] == b' '
                && bytes[13] == b':'
                && bytes[16] == b':'
                && bytes[19] == b'.'
                && bytes
                    .iter()
                    .enumerate()
                    .all(|(i, b)| matches!(i, 4 | 7 | 10 | 13 | 16 | 19) || b.is_ascii_digit())
        }
        _ => false,
    }
}

fn sanitize_csv(input_path: &Path, output_path: &Path) -> anyhow::Result<usize> {
    let file = fs::File::open(input_path)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(file);

    let outfile = fs::File::create(output_path)?;
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(outfile);

    let mut skipped = 0;
    let mut iter = rdr.records();
    
    let header = match iter.next() {
        Some(Ok(rec)) => rec,
        Some(Err(e)) => return Err(e.into()),
        None => return Ok(0),
    };
    
    let expected_len = header.len();
    wtr.write_record(&header)?;

    let mut int_indices = Vec::new();
    let mut timestamp_str_indices = Vec::new();
    let mut float_indices = Vec::new();

    for (idx, name) in header.iter().enumerate() {
        match name {
            "local_ts" | "event_time" | "trade_time" | "update_id" => {
                int_indices.push(idx);
            }
            "time_str" => {
                timestamp_str_indices.push(idx);
            }
            _ => {
                if name.ends_with("_px") || name.ends_with("_qty") {
                    float_indices.push(idx);
                }
            }
        }
    }

    let mut rec_index = 0usize;
    for result in iter {
        match result {
            Ok(record) => {
                if record.len() == expected_len {
                    let mut valid = true;

                    for idx in &int_indices {
                        if let Some(v) = record.get(*idx) {
                            if !v.is_empty() && v.parse::<i64>().is_err() {
                                valid = false;
                                if skipped <= 5 {
                                    let sample = record
                                        .iter()
                                        .take(12)
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");
                                    warn!(
                                        "Skipping invalid row {:?} rec_index={} cause=bad-int field_index={} value={}",
                                        input_path,
                                        rec_index,
                                        idx,
                                        v
                                    );
                                    warn!(
                                        "Row sample for bad-int {:?}: {}",
                                        input_path,
                                        sample
                                    );
                                }
                                break;
                            }
                        }
                    }

                    if valid {
                        for idx in &timestamp_str_indices {
                            if let Some(v) = record.get(*idx) {
                                if !v.is_empty() {
                                    let layout_ok = is_valid_timestamp_layout(v);
                                    let parsed = chrono::NaiveDateTime::parse_from_str(
                                        v,
                                        "%Y-%m-%d %H:%M:%S%.3f",
                                    )
                                    .or_else(|_| {
                                        chrono::NaiveDateTime::parse_from_str(
                                            v,
                                            "%Y-%m-%d %H:%M:%S",
                                        )
                                    });
                                    if !layout_ok || parsed.is_err() {
                                        valid = false;
                                        if skipped <= 5 {
                                            let sample = record
                                                .iter()
                                                .take(12)
                                                .map(|s| s.to_string())
                                                .collect::<Vec<_>>()
                                                .join(",");
                                            warn!(
                                                "Skipping invalid row {:?} rec_index={} cause=bad-timestamp field_index={} value={}",
                                                input_path,
                                                rec_index,
                                                idx,
                                                v
                                            );
                                            warn!(
                                                "Row sample for bad-timestamp {:?}: {}",
                                                input_path,
                                                sample
                                            );
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if valid && timestamp_str_indices.is_empty() {
                        for (idx, v) in record.iter().enumerate().skip(3) {
                            if v.is_empty() || !looks_like_timestamp_candidate(v) {
                                continue;
                            }
                            let layout_ok = is_valid_timestamp_layout(v);
                            let parsed = chrono::NaiveDateTime::parse_from_str(
                                v,
                                "%Y-%m-%d %H:%M:%S%.3f",
                            )
                            .or_else(|_| {
                                chrono::NaiveDateTime::parse_from_str(
                                    v,
                                    "%Y-%m-%d %H:%M:%S",
                                )
                            });
                            if !layout_ok || parsed.is_err() {
                                valid = false;
                                if skipped <= 5 {
                                    let sample = record
                                        .iter()
                                        .take(12)
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");
                                    warn!(
                                        "Skipping invalid row {:?} rec_index={} cause=bad-timestamp-heuristic field_index={} value={}",
                                        input_path,
                                        rec_index,
                                        idx,
                                        v
                                    );
                                    warn!(
                                        "Row sample for bad-timestamp-heuristic {:?}: {}",
                                        input_path,
                                        sample
                                    );
                                }
                                break;
                            }
                        }
                    }

                    if valid {
                        for idx in &float_indices {
                            if let Some(v) = record.get(*idx) {
                                if !v.is_empty() && v.parse::<f64>().is_err() {
                                    valid = false;
                                    if skipped <= 5 {
                                        let sample = record
                                            .iter()
                                            .take(12)
                                            .map(|s| s.to_string())
                                            .collect::<Vec<_>>()
                                            .join(",");
                                        warn!(
                                            "Skipping invalid row {:?} rec_index={} cause=bad-float field_index={} value={}",
                                            input_path,
                                            rec_index,
                                            idx,
                                            v
                                        );
                                        warn!(
                                            "Row sample for bad-float {:?}: {}",
                                            input_path,
                                            sample
                                        );
                                    }
                                    break;
                                }
                            }
                        }
                    }

                    if valid {
                        wtr.write_record(&record)?;
                    } else {
                        skipped += 1;
                    }
                } else {
                    skipped += 1;
                    if skipped <= 5 {
                        let cause = if record.len() > expected_len {
                            "concat-likely"
                        } else {
                            "truncation-likely"
                        };
                        let sample = record
                            .iter()
                            .take(12)
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        warn!(
                            "Skipping invalid row {:?} rec_index={} cause={} expected_fields={} got_fields={} sample={}",
                            input_path,
                            rec_index,
                            cause,
                            expected_len,
                            record.len(),
                            sample
                        );
                    }
                }
            }
            Err(e) => {
                skipped += 1;
                warn!("Skipping error row in {:?}: rec_index={} error={:?}", input_path, rec_index, e);
            }
        }
        rec_index += 1;
    }
    wtr.flush()?;
    Ok(skipped)
}

fn try_convert_standard(csv_path: &Path, parquet_path: &Path) -> anyhow::Result<()> {
    // Infer schema
    let schema = arrow::csv::infer_schema_from_files(
        &[csv_path.to_string_lossy().to_string()],
        b',',
        Some(100),
        true,
    )
    .context("infer schema")?;

    let file = fs::File::open(csv_path)?;
    let builder = arrow::csv::ReaderBuilder::new(std::sync::Arc::new(schema))
        .with_header(true)
        .with_batch_size(8192);

    let csv_reader = builder.build(file)?;

    // Write to a temporary file first, then rename to final path on success
    let tmp_path = parquet_path.with_extension("parquet.tmp");
    let out_file = fs::File::create(&tmp_path)?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    let mut writer =
        parquet::arrow::ArrowWriter::try_new(out_file, csv_reader.schema(), Some(props))?;

    for batch in csv_reader {
        let batch = batch?;
        writer.write(&batch)?;
    }

    writer.close()?;
    // Atomically replace final parquet
    fs::rename(&tmp_path, parquet_path)?;
    Ok(())
}

pub fn convert_csv_to_parquet(csv_path: &Path) -> anyhow::Result<()> {
    let parquet_path = csv_path.with_extension("parquet");

    if parquet_path.exists() {
        let meta = fs::metadata(&parquet_path)?;
        // Check for valid parquet file (magic bytes PAR1 at start and end)
        // Or just size check. Empty parquet is 4 bytes. Minimal valid parquet is larger.
        if meta.len() > 100 {
            fs::remove_file(csv_path)?;
            info!("Parquet exists and valid; deleted CSV {:?}", csv_path);
            return Ok(());
        } else {
            warn!(
                "Found invalid parquet file {:?} (size {}), reconverting",
                parquet_path,
                meta.len()
            );
            fs::remove_file(&parquet_path)?;
        }
    }

    info!(
        "Converting {:?} -> {:?} csv_bytes={}",
        csv_path,
        parquet_path,
        fs::metadata(csv_path).map(|m| m.len()).unwrap_or(0)
    );

    if let Err(e) = try_convert_standard(csv_path, &parquet_path) {
        warn!(
            "Standard conversion failed for {:?}: {:?}. Sanitizing...",
            csv_path, e
        );

        let sanitized_path = csv_path.with_extension("csv.sanitized");
        let skipped = sanitize_csv(csv_path, &sanitized_path)?;
        info!("Sanitized {:?}: skipped {} rows", csv_path, skipped);

        if let Err(e2) = try_convert_standard(&sanitized_path, &parquet_path) {
            error!("Sanitized conversion also failed for {:?}: {:?}", csv_path, e2);
            let _ = fs::remove_file(&sanitized_path);
            // Ensure we don't leave behind tmp or invalid parquet artifacts
            let _ = fs::remove_file(parquet_path.with_extension("parquet.tmp"));
            // If a small invalid parquet managed to be created, clean it up
            if let Ok(meta) = fs::metadata(&parquet_path) {
                if meta.len() <= 100 {
                    let _ = fs::remove_file(&parquet_path);
                }
            }
            return Err(e2);
        }
        let _ = fs::remove_file(&sanitized_path);
    }

    // Delete CSV
    fs::remove_file(csv_path)?;
    info!("Converted and deleted {:?}", csv_path);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::event::{MarketEvent, Stream};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_csv_rotation_and_conversion() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut writer = CsvFileWriter::new(
            dir.path(),
            1, // bucket_minutes = 1
            24,
            1,
        )?;

        let now_ms = crate::writer::time_now_ms();
        let minute_ms = 60_000;

        // Event 1: 3 minutes ago
        let mut ev1 = MarketEvent::new("binance", Stream::SpotTrade, "BTCUSDT");
        ev1.local_ts = now_ms - 3 * minute_ms;

        // Event 2: 2 minutes ago
        let mut ev2 = MarketEvent::new("binance", Stream::SpotTrade, "BTCUSDT");
        ev2.local_ts = now_ms - 2 * minute_ms;

        writer.write_event(&ev1)?;
        writer.write_event(&ev2)?;

        // Trigger maintenance.
        writer.maintenance_tick(0)?;

        // Give some time for background conversion
        thread::sleep(Duration::from_secs(3));

        // Check files
        let sym_dir = dir.path().join("binance").join("BTCUSDT");
        assert!(sym_dir.exists(), "Symbol dir should exist");

        let entries: Vec<_> = fs::read_dir(&sym_dir)?.collect::<Result<_, _>>()?;
        let filenames: Vec<String> = entries
            .iter()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        println!("Files found: {:?}", filenames);

        let parquet_count = filenames.iter().filter(|n| n.ends_with(".parquet")).count();
        let csv_count = filenames.iter().filter(|n| n.ends_with(".csv")).count();

        assert_eq!(csv_count, 0, "CSVs should be deleted after conversion");
        assert!(parquet_count >= 1, "Should have parquet files (expected 2)");

        Ok(())
    }
}
