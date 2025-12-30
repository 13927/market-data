总结 IO 控制 / 内存控制，以及“Mem 仍然缓慢增长”的最可能原因与排查路径。

**1) 数据流与 IO 写入策略（现在怎么写）**

- 采集端（各交易所 WS task）解析出 MarketEvent，写入一个全局事件队列；当 queue_capacity>0 用有界队列，否则无界。[main.rs:L196-L200](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L196-L200)
- 写盘端是单独的 writer 线程从队列消费，按 (exchange, symbol, hour_bucket) 路由到一个文件；同一 symbol 的不同主题靠 stream 列区分。[main.rs:L582-L595](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L582-L595)
- 每个打开的文件包含：
  - ArrowWriter<BufWriter<File>>（Parquet 编码/压缩）与受限缓冲：BufWriter 容量 128KB。[parquet.rs:L21-L26](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L21-L26)
  - EventBatchBuilder（攒到 record_batch_size 条再写 RecordBatch）。[parquet.rs:L65-L71](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L65-L71) · [parquet.rs:L250-L253](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L250-L253)
  - RowGroup 大小由 WriterProperties 设置（影响单文件内部缓冲）。[parquet.rs:L124-L127](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L124-L127)
- 定期维护（每 5 秒）：跨小时关闭、关闭 idle 文件、有限预算地 flush 打开的 BufWriter、运行保留清理、打印统计。[main.rs:L596-L614](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L596-L614) · [parquet.rs:L260-L269](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L260-L269) · [parquet.rs:L275-L291](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L275-L291) · [parquet.rs:L315-L338](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L315-L338) · [parquet.rs:L457-L480](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L457-L480) · [parquet.rs:L482-L501](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L482-L501)
- 桶切换优化：新桶打开时主动 finalize 同一 symbol 的旧桶，避免桶边界的短暂“双开”。[parquet.rs:L183-L190](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L183-L190) · [parquet.rs:L293-L313](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L293-L313)
- 启动恢复：扫描并恢复遗留的 *_inprogress.parquet，防止重启后被占位导致 _partN 爆炸。[parquet.rs:L834-L912](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L834-L912)

**2) 内存控制点（现在靠什么限内存）**
A) **队列（全局 in-flight 事件）**

- 配置 output.queue_capacity 决定队列是 bounded 还是 unbounded。[main.rs:L196-L200](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L196-L200)
- 采集端普遍使用 try_send（队列满会丢并计数 dropped_events_total），属“保活优先”。示例：Binance JSON、KuCoin、Gate。[main.rs:L714-L718](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L714-L718) · [kucoin.rs:L590-L597](file:///Users/bp/Documents/trae_projects/market-data/src/exchange/kucoin.rs#L590-L597) · [gate.rs:L876-L882](file:///Users/bp/Documents/trae_projects/market-data/src/exchange/gate.rs#L876-L882)

B) **每文件 builder 的上限**

- EventBatchBuilder 的容量由 record_batch_size 决定（来自 output.parquet_record_batch_size），每个打开文件常驻一套 builder。[parquet.rs:L229-L237](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L229-L237) · [parquet.rs:L575-L583](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L575-L583)
- 这是关键的“每文件常驻内存”参数之一。

C) **Parquet row group 上限**

- output.parquet_batch_size 设置 WriterProperties::set_max_row_group_size。[parquet.rs:L124-L127](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L124-L127)
- row group 越大压缩更好，但单文件内部缓冲更大；高符号规模（例如 3×150≈450 open files）时建议调小到 2000–5000。[main.rs:L104-L118](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L104-L118)

D) **打开文件数（FD/内存）**

- 现在 partition 是 exchange/symbol/hour，150 symbols × 3 exchanges ≈ 450 个同时打开文件是常见规模。
- max_open_files 是“软限制”：仅在存在足够 idle 的 writer 时才驱逐，否则宁可超限以避免 thrash 与 *_partN 爆炸。[parquet.rs:L340-L353](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L340-L353) · [parquet.rs:L377-L393](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L377-L393)

E) **内存可观测性**

- writer stats 当前打印事件数、open_files、queue_len、record_batch_size、dropped_total、evicted 等。[parquet.rs:L507-L525](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L507-L525)
- dropped 计数由 util::metrics 维护。[metrics.rs:L1-L14](file:///Users/bp/Documents/trae_projects/market-data/src/util/metrics.rs#L1-L14)
- 系统内存（RSS/VMS）需通过 OS 观测（如 ps/top/外部监控），代码未内置该指标。

**3) 为什么 Mem 还会缓慢增长（最常见的 3 种情况）**

1. **“冷门币”填满 row group 的缓慢爬坡**

- 低频 symbol 的 row group 很久才满、内部缓冲会慢慢涨到 row_group_size 后才稳定；当 open_files 较多且 row_group_size 偏大时，此类爬坡更明显。

1. **Rust/系统 allocator 不把内存还给 OS（看起来像增长）**

- 有些分配会复用但 RSS 不会立刻下降；需结合 OS 观测来判断是否“持续无上限增长”。

1. **队列曾经积压（即使 dropped=0，也会推高内存）**

- 看 writer stats 的 queue_len 是否经常 >0 或持续上升；若上升，说明写盘吞吐 < 事件吞吐，可能导致内存与延迟持续上扬。

**4) 你“不能丢数据”的要求：现在代码并不保证**

- 因为采集端用了 try_send，队列满会丢（多处 WS 处理使用 try_send）。[main.rs:L714-L718](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L714-L718) · [kucoin.rs:L590-L597](file:///Users/bp/Documents/trae_projects/market-data/src/exchange/kucoin.rs#L590-L597) · [gate.rs:L876-L882](file:///Users/bp/Documents/trae_projects/market-data/src/exchange/gate.rs#L876-L882)
- 如果你真的要“绝不丢”，工程上只有三条路：
  1. **改成阻塞回压**（send/send_timeout）：队列满就让采集端卡住；代价是可能导致 WS 读不动被交易所断开。
  2. **队列满就触发“自杀式退出”**（fail-fast）：宁可程序退出让你监控报警，也不 silent drop。
  3. **上 WAL/本地磁盘缓冲队列**：把突发写到本地 append log，writer 慢慢落 parquet；这是最稳但工程量最大。

**5) 现在你应该怎么判断“是否真的泄漏/不可控增长”**

- 直接看日志里的这行是否持续抬升：writer stats ... events=... open_files=... queue_len=... record_batch_size=... dropped_total=... evicted_files_total=... 等。[parquet.rs:L507-L525](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L507-L525)
- 你现在的目标是：
  - queue_len 长期接近 0
  - dropped_total 永远为 0
  - open_files 稳定在 ~450
  - 系统 RSS 先爬坡后平台；如果长期线性增长，需要进一步收紧配置或减少分区维度

如果你愿意我下一步就直接改代码：把 try_send 改成“队列满就阻塞/超时退出并报警”，让“不允许丢数据”在机制上成立；同时把相关参数（row_group / record_batch / flush）按 2c4g 的上限再收紧一档，确保 RSS 能平台。









writer 指的是程序里专门负责“把事件批量写成 Parquet 文件”的写盘组件。[parquet.rs](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs)
它占用多少内存，核心看两件事：

1. **同时打开多少个文件**
   open_files≈3 交易所 × 150 symbols × 1 小时桶（每个 symbol/hour 一个 writer 常驻）。这决定了内存下限。
2. **每个 open file 的“写入缓存”大小**
   每个打开的 Parquet writer 通常会持有：

- Arrow builders：record_batch_size 条的各列临时缓存（字符串列包含 data buffer / offsets / validity bitmap）。[parquet.rs:L575-L606](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L575-L606)
- Parquet 写入侧缓冲：row group/page 缓冲 + zstd 压缩临时缓冲 + 字典/元数据结构。[parquet.rs:L124-L127](file:///Users/bp/Documents/trae_projects/market-data/src/writer/parquet.rs#L124-L127)

工程经验（随实现/allocator 变化，仅供量级参考）：

- **每个 open file 常驻 1–10MB** 是常见范围（即便 record_batch_size 很小，Parquet 写入侧仍有固定开销）。
- 所以 open_files≈450 时，仅 writer 常驻就可能在 **450MB–4.5GB** 之间。

结论：内存压力主要来自 **“每 symbol/hour 常开一个 Parquet writer”** 的分区方式。要显著降低内存，需减少分区维度（如改为 exchange/hour 或 exchange/market/hour，将 symbol 作为列），或进一步收紧 row_group/record_batch/idle_close/max_open_files 等参数以平台化内存。
