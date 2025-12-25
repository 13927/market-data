use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use anyhow::Context;
use arrow::array::Array;
use arrow::array::StringArray;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// # 逐个扫描 data/ 下所有 Parquet 文件，并输出每个文件里各 stream 的行数统计
// # - find ... -print0：用 NUL 分隔文件名（避免文件名包含空格导致解析错误）
// # - sort -z：按文件名排序（同样使用 NUL 分隔）
// # - while read -d ''：逐个读取 NUL 分隔的文件路径
// # - parquet_stream_counts：读取 parquet 并按 stream 列计数
/*
find data -type f -name "*.parquet" -print0 | sort -z | while IFS= read -r -d '' f; do
  echo "== $f =="
  cargo run --bin parquet_stream_counts --release -- "$f"
done

*/

/*

bad=$(mktemp)
ok=$(mktemp)

find data -type f -name "*.parquet" -print0 | sort -z | while IFS= read -r -d '' f; do
  echo "== $f =="
  if cargo run --bin parquet_stream_counts --release -- "$f"; then
    echo "$f" >> "$ok"
  else
    code=$?
    echo "$f (exit=$code)" >> "$bad"
  fi
done

echo "==== SUMMARY ===="
echo "ok:  $(wc -l < "$ok" | awk '{print $1}')"
echo "bad: $(wc -l < "$bad" | awk '{print $1}')"
if [ -s "$bad" ]; then
  echo "---- BAD FILES ----"
  cat "$bad"
fi

rm -f "$ok" "$bad"

*/

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path: PathBuf = args
        .next()
        .context("usage: parquet_stream_counts <file.parquet>")?
        .into();

    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).context("open parquet reader")?;
    let schema = builder.schema().clone();

    let stream_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "stream")
        .context("no `stream` column in schema")?;

    let mut reader = builder
        .with_batch_size(8192)
        .build()
        .context("build reader")?;

    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut total: u64 = 0;

    while let Some(batch) = reader.next().transpose().context("read batch")? {
        let col = batch.column(stream_idx);
        let arr = col
            .as_any()
            .downcast_ref::<StringArray>()
            .context("stream column is not Utf8")?;
        for i in 0..arr.len() {
            let v = arr.value(i);
            *counts.entry(v.to_string()).or_insert(0) += 1;
            total += 1;
        }
    }

    // Alert if the set of streams isn't exactly the 6 expected ones.
    // 统一命名为：
    // - spot_book
    // - future_book
    // - spot_l5
    // - future_l5
    // - spot_trade
    // - future_trade

    const EXPECTED: [&str; 6] =
        ["spot_book", "future_book", "spot_l5", "future_l5", "spot_trade", "future_trade"];
    let mut missing: Vec<&str> = EXPECTED
        .iter()
        .copied()
        .filter(|k| !counts.contains_key(*k))
        .collect();
    missing.sort_unstable();

    let mut extra: Vec<String> = counts
        .keys()
        .filter(|k| !EXPECTED.iter().any(|e| *e == k.as_str()))
        .cloned()
        .collect();
    extra.sort_unstable();

    let mut items: Vec<(String, u64)> = counts.into_iter().collect();
    items.sort_by(|a, b| b.1.cmp(&a.1));

    println!("file: {}", path.display());
    println!("rows: {total}");
    for (k, v) in items {
        println!("{k}: {v}");
    }

    if !missing.is_empty() || !extra.is_empty() {
        eprintln!(
            "ALERT: stream set mismatch. missing={:?} extra={:?}",
            missing, extra
        );
        // Do not exit with error code, just warn.
        // std::process::exit(2);
    }

    Ok(())
}
