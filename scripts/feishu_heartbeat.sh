#!/usr/bin/env bash
set -euo pipefail
WEBHOOK="${FEISHU_WEBHOOK:-}"
MSG="${1:-项目正常运行}"
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
    fd_cnt="$(ls -U "/proc/${main_pid}/fd" 2>/dev/null | wc -l | tr -d ' ')"
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
text="$MSG
host=${host} time=${ts}
service=${service} active=${active}
pid=${main_pid:-unknown}
cpu=${cpu_pct:-?}% mem=${mem_pct:-?}% rss=${rss_mb:-?}MB vsz=${vsz_mb:-?}MB fd=${fd_cnt:-?} nofile=${nofile_lim:-?}
loadavg=${loadavg:-?}
mem_total=${mem_total:-?}MB mem_used=${mem_used:-?}MB mem_avail=${mem_avail:-?}MB
disk(/mnt/market_data) size=${disk_size:-?} used=${disk_used:-?} avail=${disk_avail:-?} use%=${disk_usep:-?}"
json_text="$(printf "%s" "${text}" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e ':a;N;$!ba;s/\n/\\n/g')"
curl -sS -o /dev/null -X POST "${WEBHOOK}" -H "Content-Type: application/json" -d "{\"msg_type\":\"text\",\"content\":{\"text\":\"${json_text}\"}}"
