# 市场数据平台：性能与内存优化总结

**项目概览**
- 多交易所市场数据采集与落地（Binance 现货 SBE/JSON、合约 JSON；KuCoin；Gate）。
- 数据按 exchange/symbol/hour 分区落地，支持 Parquet/JSONL/CSV，可配置批量与并发参数。
- 面向 2c/4g 等小内存机器的稳定运行与资源控制。

**优化目标**
- 降低峰值内存占用，避免 OOM、宿主机卡顿。
- 提升高并发符号订阅时的稳定性（连接、订阅、写入）。
- 控制日志体量，减少无效 IO 占用与 CPU 干扰。

**关键成果**
- 写入侧内存显式控制：并发打开文件软上限、空闲文件自动关闭、RecordBatch 大小与 RowGroup 批量可控。
- 背压与丢弃策略：当写入队列达到上限时进行丢弃保护，防止内存失控。
- 连接/订阅节流：对 KuCoin 的订阅进行分级节流，避免 509 throttle 与短时峰值。
- 日志降噪：默认日志级别降为 warn，并对重连类告警做限速，显著降低日志体量与资源开销。

**技术措施**
- 写入侧内存管理
  - 并发打开文件上限：output.parquet_max_open_files（默认 128），避免同时打开数百文件导致常驻内存暴涨。[default_output_parquet_max_open_files](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L495-L499)
  - 空闲文件关闭：output.parquet_idle_close_secs（默认 0，可按需设 300），对低活跃符号及时释放写入资源。[default_output_parquet_idle_close_secs](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L501-L505)
  - RecordBatch 大小：output.parquet_record_batch_size（默认 512），降低每个打开文件的列缓冲占用。[default_output_parquet_record_batch_size](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L488-L493)
  - RowGroup 批量：output.parquet_batch_size（默认 50000），在高符号规模场景建议调小至 2000–5000，降低单文件缓冲。[default_output_parquet_batch_size](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L484-L486) · [validate_config_for_scale](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L87-L118)
  - 压缩级别：output.parquet_zstd_level（默认 3），在 CPU/IO 与压缩率间取得平衡。[default_output_parquet_zstd_level](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L507-L509)
- 并发与背压
  - 事件队列容量：output.queue_capacity（默认 200000），满载时丢弃事件（best-effort），避免不可控内存膨胀。[default_output_queue_capacity](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L478-L482)
  - 队列类型：当容量为 0 使用无界队列，>0 使用有界队列。[main.rs:L196-L200](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L196-L200)
- 订阅与连接节流
  - KuCoin 节流配置：subscribe_delay_ms、conn_stagger_ms、global_subscribe_delay_ms，减少短时控制消息峰值与 509 错误。[mod.rs:KucoinConfig 默认值](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L230-L246) · [main.rs:L120-L151](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L120-L151)
  - Binance SBE 稳定握手：始终将稳定种子流提升至首位，降低初始握手失败率，避免反复重连造成资源抖动。[binance_spot_sbe.rs:L56-L76](file:///Users/bp/Documents/trae_projects/market-data/src/exchange/binance_spot_sbe.rs#L56-L76)
- 日志与观测
  - 默认日志级别：EnvFilter 默认 warn，减少无效日志的 CPU 与 IO。[main.rs:L183-L190](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L183-L190) · [market-data-daemon.sh:L16,L35](file:///Users/bp/Documents/trae_projects/market-data/scripts/market-data-daemon.sh#L16-L35)
  - 配置规模校验：在 Parquet 模式下对批量参数与估算的打开文件数进行预警，指导安全参数区间。[main.rs:L85-L118](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L85-L118)

**配置调优清单**
- 小内存机器（2c/4g）+ 高符号数（≥200）
  - output.format = parquet
  - output.parquet_record_batch_size = 256..512
  - output.parquet_batch_size = 2000..5000
  - output.parquet_max_open_files = 128（或更低）
  - output.parquet_idle_close_secs = 300（低活跃场景）
  - output.queue_capacity = 200000（保命线）
  - kucoin.global_subscribe_delay_ms = 100..250（或提高 subscribe_delay_ms）
- 低符号数（≤50）
  - 可增大批量与关闭 idle close，提升吞吐和压缩效率。

**代码参考**
- 写入参数默认值与说明：[config/mod.rs:L432-L509](file:///Users/bp/Documents/trae_projects/market-data/src/config/mod.rs#L432-L509)
- 规模校验与内存提示：[main.rs:L85-L118](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L85-L118)
- 事件队列构建（有界/无界）：[main.rs:L196-L200](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L196-L200)
- KuCoin 节流提示逻辑：[main.rs:L120-L151](file:///Users/bp/Documents/trae_projects/market-data/src/main.rs#L120-L151)
- Binance SBE 种子流提升：[binance_spot_sbe.rs:L56-L76](file:///Users/bp/Documents/trae_projects/market-data/src/exchange/binance_spot_sbe.rs#L56-L76)
- Daemon 默认日志级别（warn）：[scripts/market-data-daemon.sh:L16-L35](file:///Users/bp/Documents/trae_projects/market-data/scripts/market-data-daemon.sh#L16-L35)

**简历亮点**
- 设计并实现面向多交易所高并发采集的内存控制方案（RecordBatch/RowGroup/文件并发/空闲关闭）。
- 构建队列背压与丢弃策略，在 IO 饱和时稳定守护宿主资源。
- 针对 KuCoin 控制消息的订阅节流策略，避免服务端 throttle 风险与客户端重连风暴。
- 默认日志策略与限速机制，显著降低日志体量与无效资源消耗。
- 在 2c/4g 小内存机器上支撑数百符号并行采集与落地，保证稳定性与可扩展性。

**后续优化方向**
- 在 Parquet 路径上引入动态批量调节（基于系统负载与打开文件数）。
- 针对不同交易所/符号活跃度采用自适应 idle close 与文件并发上限。
- 引入可观测性指标（写入延迟、队列深度、丢弃比率）用于自动调参。
