use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::Context;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path: PathBuf = args
        .next()
        .context("usage: parquet_head <file.parquet> [rows]")?
        .into();
    let max_rows: usize = args
        .next()
        .map(|s| s.parse::<usize>())
        .transpose()
        .context("rows must be an integer")?
        .unwrap_or(20);

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
