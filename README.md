# 市场数据平台：执行方法

**环境**
- Rust（stable）
- macOS 或 Linux（systemd 适用于 Linux）

**一次性准备**
- 复制示例配置并填写密钥

```bash
cp config.example.toml config.toml
# Binance Spot SBE 需要 Ed25519 APIKEY，填入 [binance_spot_sbe].api_key
```

- 构建发布版

```bash
cargo build --release
```

**直接运行（终端）**
- 运行二进制（建议设置日志级别为 warn）

```bash
RUST_LOG=warn ./target/release/market-data
```

**守护脚本运行（macOS/Linux）**
- 使用仓库内的守护脚本管理启动/停止/日志轮转
- 脚本位置：scripts/market-data-daemon.sh

```bash
# 启动
bash scripts/market-data-daemon.sh start
# 查看状态
bash scripts/market-data-daemon.sh status
# 停止
bash scripts/market-data-daemon.sh stop
# 重启
bash scripts/market-data-daemon.sh restart
# 立即轮转日志
bash scripts/market-data-daemon.sh rotate
```

- 可选环境变量
  - RUST_LOG（默认：warn）
  - BUILD（默认：1，启动前自动构建）
  - LOG_MAX_MB、LOG_KEEP、LOG_GZIP（日志轮转与保留）

**Linux：systemd 部署（/mnt/market_data）**
- 模板文件：
  - 服务：[market-data.service](file:///Users/bp/Documents/trae_projects/market-data/scripts/systemd/market-data.service)
  - 心跳服务：[feishu-heartbeat.service](file:///Users/bp/Documents/trae_projects/market-data/scripts/systemd/feishu-heartbeat.service)
  - 心跳定时器：[feishu-heartbeat.timer](file:///Users/bp/Documents/trae_projects/market-data/scripts/systemd/feishu-heartbeat.timer)
- 模板已按 /mnt/market_data 配置（WorkingDirectory/ExecStart/EnvironmentFile）
- 安装并启用

```bash
sudo install -D -m 0644 scripts/systemd/market-data.service /etc/systemd/system/market-data.service
sudo install -D -m 0644 scripts/systemd/feishu-heartbeat.service /etc/systemd/system/feishu-heartbeat.service
sudo install -D -m 0644 scripts/systemd/feishu-heartbeat.timer /etc/systemd/system/feishu-heartbeat.timer

sudo systemctl daemon-reload
sudo systemctl enable --now market-data.service
sudo systemctl enable --now feishu-heartbeat.timer
```

**每小时飞书心跳（Linux）**
- 心跳脚本：scripts/feishu_heartbeat.sh
- 本地环境文件：feishu.env（已在 .gitignore 中忽略）
- 文件内容示例（请自行修改为你的 webhook）：

```bash
echo 'FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/......' > feishu.env
```

- 立即测试一次心跳（不经 systemd）

```bash
FEISHU_WEBHOOK="$(grep -E '^FEISHU_WEBHOOK=' feishu.env | cut -d= -f2-)" bash scripts/feishu_heartbeat.sh "项目正常运行"
```

- 通过 systemd 定时发送（每小时）
  - feishu-heartbeat.service 的 EnvironmentFile 指向 /mnt/market_data/feishu.env

```bash
sudo systemctl start feishu-heartbeat.service
sudo systemctl status feishu-heartbeat.timer
sudo journalctl -u feishu-heartbeat.service --since "1 hour ago"
```

**注意**
- 请勿将密钥与 webhook 提交到仓库；使用本地环境文件或环境变量注入
- 对于大规模订阅，建议将 RUST_LOG 保持为 warn，减少日志 IO
