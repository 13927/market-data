use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::Context;
use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn write_parquet_as_csv(input: &PathBuf, output: &PathBuf) -> anyhow::Result<()> {
    if input
        .file_stem()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.ends_with("_inprogress"))
    {
        anyhow::bail!(
            "parquet file is still in-progress (no footer yet): {}",
            input.display()
        );
    }
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("open parquet reader")?;
    let schema = builder.schema().clone();
    let mut reader = builder
        .with_batch_size(8192)
        .build()
        .context("build reader")?;
    let out_file =
        File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut wtr = csv::Writer::from_writer(out_file);
    let headers: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    wtr.write_record(&headers)?;
    while let Some(batch) = reader.next().transpose().context("read batch")? {
        let cols = batch.columns();
        let n = batch.num_rows();
        for row in 0..n {
            let mut record = Vec::with_capacity(cols.len());
            for col in cols {
                let dt = col.data_type();
                let v = match dt {
                    DataType::Utf8 => {
                        let a = col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .context("Utf8 downcast")?;
                        if a.is_null(row) {
                            String::new()
                        } else {
                            a.value(row).to_string()
                        }
                    }
                    DataType::Int64 => {
                        let a = col
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .context("Int64 downcast")?;
                        if a.is_null(row) {
                            String::new()
                        } else {
                            a.value(row).to_string()
                        }
                    }
                    DataType::UInt64 => {
                        let a = col
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .context("UInt64 downcast")?;
                        if a.is_null(row) {
                            String::new()
                        } else {
                            a.value(row).to_string()
                        }
                    }
                    DataType::Float64 => {
                        let a = col
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .context("Float64 downcast")?;
                        if a.is_null(row) {
                            String::new()
                        } else {
                            a.value(row).to_string()
                        }
                    }
                    _ => String::new(),
                };
                record.push(v);
            }
            wtr.write_record(&record)?;
        }
    }
    wtr.flush()?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path: PathBuf = args
        .next()
        .context("usage: parquet_head <file.parquet> [rows|--csv <out.csv>]")?
        .into();
    if let Some(arg1) = args.next() {
        if arg1 == "--csv" {
            let out: PathBuf = args
                .next()
                .context("usage: parquet_head <file.parquet> --csv <out.csv>")?
                .into();
            write_parquet_as_csv(&path, &out)?;
            return Ok(());
        }
        let max_rows: usize = arg1
            .parse::<usize>()
            .context("rows must be an integer")?;
        return print_head(&path, max_rows);
    } else {
        return print_head(&path, 20);
    }
}

fn print_head(path: &PathBuf, max_rows: usize) -> anyhow::Result<()> {
    if path
        .file_stem()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.ends_with("_inprogress"))
    {
        anyhow::bail!(
            "parquet file is still in-progress (no footer yet): {}",
            path.display()
        );
    }

    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).context("open parquet reader")?;
    let schema = builder.schema().clone();

    eprintln!("file: {}", path.display());
    eprintln!("schema:\n{schema:#?}");

    let mut rows_left = max_rows;
    let mut reader = builder
        .with_batch_size(1024)
        .build()
        .context("build reader")?;
    while rows_left > 0 {
        let Some(batch) = reader.next().transpose().context("read batch")? else {
            break;
        };
        let take = rows_left.min(batch.num_rows());
        let sliced = batch.slice(0, take);
        let table =
            arrow::util::pretty::pretty_format_batches(&[sliced]).context("format batches")?;
        let mut out = io::stdout().lock();
        if let Err(err) = write!(out, "{table}") {
            if err.kind() == io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(err).context("write stdout");
        }
        rows_left -= take;
    }

    Ok(())
}
