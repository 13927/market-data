use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

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

    conversion_tx: Sender<PathBuf>,
}

impl CsvFileWriter {
    pub fn new(
        dir: impl AsRef<Path>,
        bucket_minutes: i64,
        retention_hours: i64,
        cleanup_interval_secs: u64,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = unbounded::<PathBuf>();

        thread::spawn(move || {
            info!("CSV to Parquet conversion thread started");
            while let Ok(path) = rx.recv() {
                if let Err(e) = convert_csv_to_parquet(&path) {
                    error!("Failed to convert {:?} to parquet: {:?}", path, e);
                }
            }
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
            conversion_tx: tx,
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
                let _ = self.conversion_tx.send(path);
            }
        }

        // Cleanup old files (retention)
        self.maybe_cleanup()?;

        Ok(())
    }

    pub fn close_all(&mut self) -> anyhow::Result<()> {
        for (_, mut entry) in self.writers.drain() {
            entry.writer.flush()?;
            let _ = self.conversion_tx.send(entry.file_path);
        }
        self.maybe_cleanup()?;
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

fn convert_csv_to_parquet(csv_path: &Path) -> anyhow::Result<()> {
    let parquet_path = csv_path.with_extension("parquet");
    if parquet_path.exists() {
        // Already converted?
        return Ok(());
    }

    debug!("Converting {:?} to {:?}", csv_path, parquet_path);

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

    let out_file = fs::File::create(&parquet_path)?;
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

    // Delete CSV
    fs::remove_file(csv_path)?;
    debug!("Converted and deleted {:?}", csv_path);

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
