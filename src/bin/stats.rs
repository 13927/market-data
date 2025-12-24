use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

fn main() -> anyhow::Result<()> {
    let root = "data";
    println!("Scanning {}...", root);

    // Output file
    let output_path = "stats_report.csv";
    let mut wtr = csv::Writer::from_path(output_path)?;
    wtr.write_record(&[
        "path",
        "hour",
        "size_bytes",
        "futures_l5_count",
        "futures_ticker_count",
        "spot_l5_count",
        "spot_ticker_count",
    ])?;

    for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if !["csv", "jsonl", "parquet"].contains(&ext) {
            continue;
        }

        let metadata = fs::metadata(path)?;
        let size_bytes = metadata.len();

        // Extract hour from filename: exchange_symbol_YYYYMMDDHHMM.ext
        // We want YYYYMMDDHH
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");
        let parts: Vec<&str> = stem.split('_').collect();
        let hour_str = if parts.len() >= 3 {
            let time_part = parts.last().unwrap();
            if time_part.len() >= 10 {
                &time_part[0..10]
            } else {
                "unknown"
            }
        } else {
            "unknown"
        };

        // Only process if we can extract a valid hour? No, process all valid extensions.

        let (f_l5, f_ticker, s_l5, s_ticker) = count_records(path, ext).unwrap_or_else(|e| {
            eprintln!("Error reading {}: {}", path.display(), e);
            (0, 0, 0, 0)
        });

        wtr.write_record(&[
            path.to_string_lossy().as_ref(),
            hour_str,
            &size_bytes.to_string(),
            &f_l5.to_string(),
            &f_ticker.to_string(),
            &s_l5.to_string(),
            &s_ticker.to_string(),
        ])?;

        println!(
            "Processed {}: size={} futures_l5={} futures_ticker={} spot_l5={} spot_ticker={}",
            path.display(),
            size_bytes,
            f_l5,
            f_ticker,
            s_l5,
            s_ticker
        );
    }

    wtr.flush()?;
    println!("Report written to {}", output_path);
    Ok(())
}

fn count_records(path: &Path, ext: &str) -> anyhow::Result<(usize, usize, usize, usize)> {
    let mut futures_l5 = 0;
    let mut futures_ticker = 0;
    let mut spot_l5 = 0;
    let mut spot_ticker = 0;

    match ext {
        "csv" => {
            let mut rdr = csv::Reader::from_path(path)?;
            // Assume header "stream" exists
            // We need to handle case where header might be missing or different?
            // Assuming standard format written by our writer.
            let headers = rdr.headers()?.clone();
            let stream_idx = headers.iter().position(|h| h == "stream");

            if let Some(idx) = stream_idx {
                for result in rdr.records() {
                    let record = result?;
                    if let Some(s) = record.get(idx) {
                        match s {
                            "swap_l5" => futures_l5 += 1,
                            "swap_ticker" => futures_ticker += 1,
                            "spot_l5" => spot_l5 += 1,
                            "spot_ticker" => spot_ticker += 1,
                            _ => {}
                        };
                    }
                }
            }
        }
        "jsonl" => {
            let file = fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);
            for line in std::io::BufRead::lines(reader) {
                let line = line?;
                // Skip empty lines
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if let Some(s) = v.get("stream").and_then(|s| s.as_str()) {
                        match s {
                            "swap_l5" => futures_l5 += 1,
                            "swap_ticker" => futures_ticker += 1,
                            "spot_l5" => spot_l5 += 1,
                            "spot_ticker" => spot_ticker += 1,
                            _ => {}
                        };
                    }
                }
            }
        }
        "parquet" => {
            let file = fs::File::open(path)?;
            let reader = SerializedFileReader::new(file)?;
            // Naive iteration using get_row_iter
            // Projection: we only need "stream" column if possible?
            // get_row_iter(projection_schema)
            // But constructing projection schema is complex.
            // Just read all for now, as files are likely not incredibly wide (MarketEvent is ~30 cols).

            for row in reader.get_row_iter(None)? {
                let row = row?;
                // Iterate columns to find "stream"
                for (name, field) in row.get_column_iter() {
                    if name == "stream" {
                        if let parquet::record::Field::Str(s) = field {
                            match s.as_ref() {
                                "swap_l5" => futures_l5 += 1,
                                "swap_ticker" => futures_ticker += 1,
                                "spot_l5" => spot_l5 += 1,
                                "spot_ticker" => spot_ticker += 1,
                                _ => {}
                            };
                        }
                        break;
                    }
                }
            }
        }
        _ => {}
    }

    Ok((futures_l5, futures_ticker, spot_l5, spot_ticker))
}
