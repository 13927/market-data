#!/usr/bin/env bash
set -euo pipefail
WEBHOOK="${FEISHU_WEBHOOK:-}"
DEFAULT_MSG="项目正常运行"
MSG="${1:-$DEFAULT_MSG}"
if [[ -z "${WEBHOOK}" ]]; then
  echo "missing FEISHU_WEBHOOK" >&2
  exit 2
fi
host="$(hostname 2>/dev/null || echo unknown)"
ts="$(date -Is 2>/dev/null || date)"
service="market-data.service"
active="$(systemctl is-active "${service}" 2>/dev/null || echo unknown)"
main_pid="$(systemctl show -p MainPID --value "${service}" 2>/dev/null || echo "")"
if [[ -z "${main_pid}" || "${main_pid}" == "0" ]]; then
  main_pid="$(pgrep -x market-data 2>/dev/null || true)"
  if [[ -z "${main_pid}" ]]; then
    main_pid="$(pgrep -f '/mnt/market_data/target/release/market-data' 2>/dev/null | head -n1 || true)"
  fi
fi
cpu_pct=""
mem_pct=""
rss_mb=""
vsz_mb=""
fd_cnt=""
nofile_lim=""
if [[ -n "${main_pid}" ]]; then
  ps_out="$(ps -p "${main_pid}" -o %cpu=,%mem=,rss=,vsz= 2>/dev/null || true)"
  if [[ -n "${ps_out}" ]]; then
    cpu_pct="$(echo "${ps_out}" | awk '{print $1}')"
    mem_pct="$(echo "${ps_out}" | awk '{print $2}')"
    rss_kb="$(echo "${ps_out}" | awk '{print $3}')"
    vsz_kb="$(echo "${ps_out}" | awk '{print $4}')"
    rss_mb="$(awk -v v="${rss_kb:-0}" 'BEGIN{printf "%.1f", v/1024}')"
    vsz_mb="$(awk -v v="${vsz_kb:-0}" 'BEGIN{printf "%.1f", v/1024}')"
  fi
  if [[ -d "/proc/${main_pid}/fd" ]]; then
    fd_cnt="$(ls -U "/proc/${main_pid}/fd" 2>/dev/null | wc -l | tr -d ' ' || echo "?")"
  fi
  if [[ -f "/proc/${main_pid}/limits" ]]; then
    nofile_lim="$(awk '/Max open files/ {print $4}' "/proc/${main_pid}/limits" 2>/dev/null | tail -n1)"
  fi
fi
loadavg="$(cat /proc/loadavg 2>/dev/null | awk '{print $1" "$2" "$3}')"
mem_total=""
mem_used=""
mem_avail=""
if command -v free >/dev/null 2>&1; then
  read -r mem_total mem_used mem_avail < <(free -m | awk '/^Mem:/ {print $2" "$3" "$7}')
else
  mt="$(awk '/MemTotal/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
  ma="$(awk '/MemAvailable/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
  mu=$(( mt - ma ))
  mem_total="$(awk -v v="${mt}" 'BEGIN{printf "%.0f", v/1024}')"
  mem_used="$(awk -v v="${mu}" 'BEGIN{printf "%.0f", v/1024}')"
  mem_avail="$(awk -v v="${ma}" 'BEGIN{printf "%.0f", v/1024}')"
fi
disk_size=""
disk_used=""
disk_avail=""
disk_usep=""
read -r _ disk_size disk_used disk_avail disk_usep _ < <(df -h /mnt/market_data 2>/dev/null | awk 'NR==2{print $1" "$2" "$3" "$4" "$5" "$6}')

retention_hours=""
disk_soft_limit_gb=""
if [[ -r "/mnt/market_data/config.toml" ]]; then
  retention_hours="$(awk -F'=' '/^[[:space:]]*retention_hours[[:space:]]*=/ {gsub(/[ #]/,"",$2); print $2; exit}' /mnt/market_data/config.toml || true)"
  disk_soft_limit_gb="$(awk -F'=' '/^[[:space:]]*disk_soft_limit_gb[[:space:]]*=/ {gsub(/[ #]/,"",$2); print $2; exit}' /mnt/market_data/config.toml || true)"
fi

data_window="unknown"
if [[ -d "/mnt/market_data/data" ]]; then
  mapfile -t ts_list < <(
    find "/mnt/market_data/data" -type f \( -name '*.parquet' -o -name '*.jsonl' -o -name '*.csv' \) -printf '%f\n' 2>/dev/null |
      grep -oE '[0-9]{12}' | sort
  )
  count=${#ts_list[@]}
  if (( count > 0 )); then
    data_start_raw="${ts_list[0]}"
    data_end_raw="${ts_list[count-1]}"
  fi
  if [[ -n "${data_start_raw:-}" && -n "${data_end_raw:-}" ]]; then
    start_fmt="$(date -u -d "${data_start_raw:0:8} ${data_start_raw:8:2}:${data_start_raw:10:2}" '+%Y-%m-%d %H:%M' 2>/dev/null || true)"
    end_fmt="$(date -u -d "${data_end_raw:0:8} ${data_end_raw:8:2}:${data_end_raw:10:2}" '+%Y-%m-%d %H:%M' 2>/dev/null || true)"
    if [[ -n "${start_fmt}" && -n "${end_fmt}" ]]; then
      data_window="${start_fmt} ~ ${end_fmt}"
    fi
  fi
fi
if [[ "${data_window}" == "unknown" && -n "${retention_hours}" ]]; then
  data_window="≥${retention_hours}h"
fi

disk_limit_desc="no soft limit"
if [[ -n "${disk_soft_limit_gb}" && "${disk_soft_limit_gb}" != "0" ]]; then
  disk_limit_desc="${disk_soft_limit_gb}GiB soft limit"
fi

if [[ "$MSG" == "$DEFAULT_MSG" ]]; then
  case "$active" in
    active)
      MSG="项目正常运行"
      ;;
    unknown)
      MSG="项目已停止"
      ;;
    *)
      MSG="项目未正常运行"
      ;;
  esac
fi

# Prepare variables for JSON
pid="${main_pid:-unknown}"
cpu="${cpu_pct:-?}"
mem="${mem_pct:-?}"
rss="${rss_mb:-?}"
vsz="${vsz_mb:-?}"
fd="${fd_cnt:-?}"
load="${loadavg:-?}"
mem_stats="${mem_used:-?}/${mem_total:-?}MB (Avail: ${mem_avail:-?}MB)"
disk_stats="${disk_used:-?}/${disk_size:-?} (${disk_usep:-?}) (Avail: ${disk_avail:-?})"
window_stats="${data_window}"
if [[ -n "${disk_limit_desc}" ]]; then
  window_stats="${window_stats} (${disk_limit_desc})"
fi

# Determine color based on service status or message content
HEADER_COLOR="blue"
if [[ "$active" != "active" ]]; then
    HEADER_COLOR="red"
elif [[ "$MSG" =~ "Error" ]] || [[ "$MSG" =~ "Failed" ]] || [[ "$MSG" =~ "异常" ]]; then
    HEADER_COLOR="red"
fi

# Send interactive card
jq -n \
  --arg msg "$MSG" \
  --arg host "$host" \
  --arg ts "$ts" \
  --arg active "$active" \
  --arg pid "$pid" \
  --arg cpu "$cpu" \
  --arg mem "$mem" \
  --arg rss "$rss" \
  --arg fd "$fd" \
  --arg load "$load" \
  --arg mem_stats "$mem_stats" \
  --arg disk_stats "$disk_stats" \
  --arg window_stats "$window_stats" \
  --arg color "$HEADER_COLOR" \
  '{
    msg_type: "interactive",
    card: {
      header: {
        template: $color,
        title: {
          content: $msg,
          tag: "plain_text"
        }
      },
      elements: [
        {
          tag: "div",
          fields: [
            { is_short: true, text: { tag: "lark_md", content: ("**Host:**\n" + $host) } },
            { is_short: true, text: { tag: "lark_md", content: ("**Service:**\n" + $active) } },
            { is_short: true, text: { tag: "lark_md", content: ("**PID:**\n" + $pid) } },
            { is_short: true, text: { tag: "lark_md", content: ("**Time:**\n" + $ts) } }
          ]
        },
        { tag: "hr" },
        {
          tag: "div",
          fields: [
            { is_short: true, text: { tag: "lark_md", content: ("**CPU:** " + $cpu + "%") } },
            { is_short: true, text: { tag: "lark_md", content: ("**Mem:** " + $mem + "% (" + $rss + "MB)") } },
            { is_short: true, text: { tag: "lark_md", content: ("**Load:** " + $load) } },
            { is_short: true, text: { tag: "lark_md", content: ("**FD:** " + $fd) } }
          ]
        },
        {
          tag: "div",
          text: {
             tag: "lark_md",
             content: ("**System Mem:** " + $mem_stats + "\n**Disk:** " + $disk_stats + "\n**Data Window (UTC):** " + $window_stats)
          }
        }
      ]
    }
  }' | curl -sS -X POST "${WEBHOOK}" -H "Content-Type: application/json" -d @-
