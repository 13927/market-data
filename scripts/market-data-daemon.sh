#!/usr/bin/env bash
set -euo pipefail

# Simple daemon wrapper for market-data:
# - start/stop/restart/status
# - writes PID to ./.run/market-data.pid
# - writes logs to ./logs/market-data.log
#
# Usage:
#   bash scripts/market-data-daemon.sh start
#   bash scripts/market-data-daemon.sh status
#   bash scripts/market-data-daemon.sh stop
#   bash scripts/market-data-daemon.sh restart
#
# Optional env vars:
#   RUST_LOG (default: info)
#   MARKET_DATA_BIN (default: ./target/release/market-data)
#   MARKET_DATA_LOG (default: ./logs/market-data.log)
#   MARKET_DATA_PID (default: ./.run/market-data.pid)
#   BUILD (default: 1)  # set BUILD=0 to skip auto-build
#   LOG_KEEP (default: 20)          # how many rotated logs to keep
#   LOG_GZIP (default: 1)           # gzip rotated logs (best-effort)
#   LOG_MAX_MB (default: 1024)      # rotate when log exceeds this size (MB)
#   LOG_ROTATE_ON_START (default: 1)# rotate before starting a new daemon
#   KILL_ORPHANS (default: 1)       # stop() also kills other matching processes (best-effort)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="${ROOT_DIR}/.run"
LOG_DIR="${ROOT_DIR}/logs"

MARKET_DATA_BIN="${MARKET_DATA_BIN:-${ROOT_DIR}/target/release/market-data}"
MARKET_DATA_LOG="${MARKET_DATA_LOG:-${LOG_DIR}/market-data.log}"
MARKET_DATA_PID="${MARKET_DATA_PID:-${PID_DIR}/market-data.pid}"
MARKET_DATA_LOCK="${MARKET_DATA_LOCK:-${PID_DIR}/market-data.lock}"
RUST_LOG="${RUST_LOG:-info}"
BUILD="${BUILD:-1}"
LOG_KEEP="${LOG_KEEP:-20}"
LOG_GZIP="${LOG_GZIP:-1}"
LOG_MAX_MB="${LOG_MAX_MB:-1024}"
LOG_ROTATE_ON_START="${LOG_ROTATE_ON_START:-1}"
KILL_ORPHANS="${KILL_ORPHANS:-1}"

mkdir -p "${PID_DIR}" "${LOG_DIR}"

lock_pidfile() {
  echo "${MARKET_DATA_LOCK}/pid"
}

lock_acquire() {
  local tries=0
  while true; do
    if mkdir "${MARKET_DATA_LOCK}" >/dev/null 2>&1; then
      echo "$$" >"$(lock_pidfile)" 2>/dev/null || true
      return 0
    fi

    # Lock exists: if it's stale, remove it and retry.
    local owner=""
    owner="$(cat "$(lock_pidfile)" 2>/dev/null || true)"
    if [[ -n "${owner}" ]] && is_running "${owner}"; then
      echo "already running (lock): pid=${owner}" >&2
      exit 0
    fi

    tries=$((tries + 1))
    if [[ "${tries}" -ge 3 ]]; then
      echo "ERROR: unable to acquire lock at ${MARKET_DATA_LOCK} (remove it if stale)" >&2
      exit 1
    fi
    rm -rf "${MARKET_DATA_LOCK}" >/dev/null 2>&1 || true
    sleep 0.2
  done
}

lock_release() {
  rm -rf "${MARKET_DATA_LOCK}" >/dev/null 2>&1 || true
}

is_running() {
  local pid="$1"
  kill -0 "${pid}" >/dev/null 2>&1
}

ps_works() {
  # Some environments have `ps` on PATH but deny executing /bin/ps (e.g. "Operation not permitted").
  # Probe execution rather than using `command -v ps`.
  command -v ps >/dev/null 2>&1 || return 1
  ps -o pid= -p "$$" >/dev/null 2>&1
}

log_ts() {
  date +"%Y%m%d-%H%M%S"
}

log_size_bytes() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    echo 0
    return 0
  fi
  wc -c <"$f" 2>/dev/null | tr -d ' ' || echo 0
}

prune_rotated_logs() {
  local keep="${LOG_KEEP}"
  if [[ "${keep}" -le 0 ]]; then
    return 0
  fi
  # Keep newest N rotated logs (both .log.* and .log.*.gz).
  local pattern="${LOG_DIR}/market-data.log."*
  # shellcheck disable=SC2086
  ls -1t ${pattern} 2>/dev/null | tail -n "+$((keep + 1))" | while IFS= read -r f; do
    rm -f -- "$f" || true
  done
}

rotate_logs_if_needed() {
  local reason="${1:-manual}"
  local force="${2:-0}"

  local size
  size="$(log_size_bytes "${MARKET_DATA_LOG}")"
  local max_bytes=$((LOG_MAX_MB * 1024 * 1024))
  if [[ "${force}" != "1" ]] && [[ "${size}" -lt "${max_bytes}" ]]; then
    return 0
  fi
  if [[ "${size}" -eq 0 ]]; then
    return 0
  fi

  local ts dst
  ts="$(log_ts)"
  dst="${LOG_DIR}/market-data.log.${ts}"

  local pid pgid mode
  read -r pid pgid mode < <(read_pidfile)
  if [[ -n "${pid}" ]] && is_running "${pid}"; then
    # Safe while running: copy then truncate (process writes with O_APPEND).
    cp -p "${MARKET_DATA_LOG}" "${dst}" || return 0
    : >"${MARKET_DATA_LOG}" || true
    echo "rotated log (copytruncate): reason=${reason} file=${dst}" >&2
  else
    mv "${MARKET_DATA_LOG}" "${dst}" || return 0
    echo "rotated log: reason=${reason} file=${dst}" >&2
    : >"${MARKET_DATA_LOG}" || true
  fi

  if [[ "${LOG_GZIP}" == "1" ]] && command -v gzip >/dev/null 2>&1; then
    gzip -f "${dst}" || true
  fi

  prune_rotated_logs
}

read_pidfile() {
  # Backward compatible:
  # - old format: "<pid>"
  # - new format:
  #     pid=<pid>
  #     pgid=<pgid>
  #     mode=<setsid|nohup>
  local pid=""
  local pgid=""
  local mode=""
  if [[ ! -f "${MARKET_DATA_PID}" ]]; then
    echo "" "" ""
    return 0
  fi
  local raw
  raw="$(cat "${MARKET_DATA_PID}" 2>/dev/null || true)"
  if [[ "${raw}" =~ ^[0-9]+$ ]]; then
    pid="${raw}"
  else
    pid="$(printf '%s\n' "${raw}" | awk -F= '$1=="pid"{print $2}' | tail -n 1)"
    pgid="$(printf '%s\n' "${raw}" | awk -F= '$1=="pgid"{print $2}' | tail -n 1)"
    mode="$(printf '%s\n' "${raw}" | awk -F= '$1=="mode"{print $2}' | tail -n 1)"
  fi
  echo "${pid}" "${pgid}" "${mode}"
}

write_pidfile() {
  local pid="$1"
  local pgid="${2:-}"
  local mode="${3:-}"
  {
    echo "pid=${pid}"
    if [[ -n "${pgid}" ]]; then
      echo "pgid=${pgid}"
    fi
    if [[ -n "${mode}" ]]; then
      echo "mode=${mode}"
    fi
  } >"${MARKET_DATA_PID}"
}

kill_group() {
  local sig="$1"
  local pgid="$2"
  # kill(1) with negative PID targets the whole process group.
  kill -"${sig}" -- "-${pgid}" >/dev/null 2>&1 || true
}

resolve_pgid() {
  local pid="$1"
  local pgid="${2:-}"
  local mode="${3:-}"
  if [[ -n "${pgid}" ]]; then
    echo "${pgid}"
    return 0
  fi
  if [[ "${mode}" == "setsid" ]]; then
    # `setsid` makes the process the leader of a new session and process group; `pid == pgid`.
    echo "${pid}"
    return 0
  fi

  # Prefer Python's os.getpgid because some environments deny /bin/ps (e.g. macOS sandboxing).
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$pid" <<'PY' 2>/dev/null || true
import os, sys
try:
    pid = int(sys.argv[1])
    print(os.getpgid(pid))
except Exception:
    pass
PY
    return 0
  fi

  if ps_works; then
    ps -o pgid= "${pid}" 2>/dev/null | tr -d ' ' || true
  fi
}

kill_children() {
  local sig="$1"
  local pid="$2"
  # Best-effort: kill direct children (helps if pidfile points to a wrapper).
  if command -v pkill >/dev/null 2>&1; then
    pkill -"${sig}" -P "${pid}" >/dev/null 2>&1 || true
  fi
}

list_running_pids_by_bin() {
  # Best-effort: find PIDs whose command line matches the market-data binary (same user).
  # This helps clean up orphans if pidfile is stale or multiple daemons were started.
  local uid
  uid="$(id -u 2>/dev/null || true)"
  local bin_name
  bin_name="$(basename "${MARKET_DATA_BIN}")"

  if command -v pgrep >/dev/null 2>&1; then
    local pat1 pat2
    pat1="${MARKET_DATA_BIN}"
    pat2="${ROOT_DIR}/target/release/$(basename "${MARKET_DATA_BIN}")"
    {
      if [[ -n "${uid}" ]]; then
        # Some environments show only the executable name (no full path) in the process list.
        # Match by exact process name as a fallback so `stop` can kill orphans reliably.
        pgrep -u "${uid}" -x "${bin_name}" 2>/dev/null || true
        pgrep -u "${uid}" -f "${pat1}" 2>/dev/null || true
        pgrep -u "${uid}" -f "${pat2}" 2>/dev/null || true
      else
        pgrep -x "${bin_name}" 2>/dev/null || true
        pgrep -f "${pat1}" 2>/dev/null || true
        pgrep -f "${pat2}" 2>/dev/null || true
      fi
    } | sort -n -u 2>/dev/null || true
    return 0
  fi

  if ps_works; then
    ps -axo pid=,command= 2>/dev/null | awk -v pat="${MARKET_DATA_BIN}" '$0 ~ pat {print $1}' || true
  fi
}

kill_orphans() {
  local sig="$1"
  local pid_keep="${2:-}"
  local p

  if [[ "${KILL_ORPHANS}" != "1" ]]; then
    return 0
  fi

  while IFS= read -r p; do
    [[ -z "${p}" ]] && continue
    [[ -n "${pid_keep}" && "${p}" == "${pid_keep}" ]] && continue
    if is_running "${p}"; then
      echo "stopping orphan: pid=${p}" >&2
      kill -"${sig}" "${p}" >/dev/null 2>&1 || true
    fi
  done < <(list_running_pids_by_bin)
}

require_config() {
  if [[ ! -f "${ROOT_DIR}/config.toml" ]]; then
    echo "config missing: ${ROOT_DIR}/config.toml" >&2
    echo "hint: copy config.example.toml -> config.toml and edit it" >&2
    exit 1
  fi
}

build_if_needed() {
  if [[ "${BUILD}" != "1" ]]; then
    return
  fi
  # Always run an incremental build when BUILD=1 so code changes are picked up automatically.
  echo "building: cargo build --release" >&2
  (cd "${ROOT_DIR}" && cargo build --release)
}

start() {
  require_config
  build_if_needed

  lock_acquire

  local existing_pid existing_pgid existing_mode
  read -r existing_pid existing_pgid existing_mode < <(read_pidfile)
  if [[ -n "${existing_pid}" ]] && is_running "${existing_pid}"; then
    echo "already running: pid=${existing_pid}" >&2
    lock_release
    exit 0
  fi

  echo "starting market-data..." >&2
  echo "  bin: ${MARKET_DATA_BIN}" >&2
  echo "  log: ${MARKET_DATA_LOG}" >&2
  echo "  pid: ${MARKET_DATA_PID}" >&2
  echo "  RUST_LOG=${RUST_LOG}" >&2

  if [[ "${LOG_ROTATE_ON_START}" == "1" ]]; then
    rotate_logs_if_needed "start" 1
  else
    rotate_logs_if_needed "start" 0
  fi

  # With per-exchange/per-symbol/hour partitioning, the process can keep hundreds of files open.
  # Best-effort raise the file descriptor limit to avoid stalls/crashes on small defaults (e.g. 1024).
  if ! ulimit -n 65535 >/dev/null 2>&1; then
    echo "WARN: failed to raise file descriptor limit (ulimit -n). Current: $(ulimit -n 2>/dev/null || echo '?')" >&2
  fi

  # Run the compiled binary so PID refers to the long-lived process (not cargo).
  # Start in its own process group/session when possible so `stop` can reliably terminate
  # all child tasks spawned by the binary (and avoid leaving orphan processes).
  (
    cd "${ROOT_DIR}"
    mode=""
    if command -v setsid >/dev/null 2>&1; then
      # `setsid` makes the process the leader of a new session and process group; `pid == pgid`.
      mode="setsid"
      setsid env RUST_LOG="${RUST_LOG}" "${MARKET_DATA_BIN}" >>"${MARKET_DATA_LOG}" 2>&1 </dev/null &
    else
      # Fallback (still backgrounded, but may share PGID with the parent shell).
      mode="nohup"
      nohup env RUST_LOG="${RUST_LOG}" "${MARKET_DATA_BIN}" >>"${MARKET_DATA_LOG}" 2>&1 </dev/null &
    fi
    pid=$!
    if [[ "${mode}" == "setsid" ]]; then
      pgid="${pid}"
    else
      pgid=""
      pgid="$(resolve_pgid "${pid}" "" "${mode}")"
    fi
    write_pidfile "${pid}" "${pgid}" "${mode}"
  )

  local pid pgid mode
  read -r pid pgid mode < <(read_pidfile)
  if [[ -z "${pid}" ]]; then
    echo "failed to write pidfile" >&2
    lock_release
    exit 1
  fi
  echo "started: pid=${pid}${pgid:+ pgid=${pgid}}${mode:+ mode=${mode}}" >&2
}

status() {
  local pid pgid mode
  read -r pid pgid mode < <(read_pidfile)
  if [[ -z "${pid}" ]]; then
    # Fallback: check for running processes matching the binary even if pidfile is missing.
    local others
    others="$(list_running_pids_by_bin | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
    if [[ -n "${others}" ]]; then
      echo "running (no pidfile): pids=${others}" >&2
      exit 0
    else
      echo "not running (no pidfile): ${MARKET_DATA_PID}" >&2
      exit 1
    fi
  fi
  if is_running "${pid}"; then
    echo "running: pid=${pid}${pgid:+ pgid=${pgid}}${mode:+ mode=${mode}}" >&2
    exit 0
  fi
  echo "not running (stale pidfile): pid=${pid}" >&2
  exit 1
}

stop() {
  local pid pgid mode
  read -r pid pgid mode < <(read_pidfile)
  if [[ -z "${pid}" ]] && [[ -f "$(lock_pidfile)" ]]; then
    pid="$(cat "$(lock_pidfile)" 2>/dev/null || true)"
    pgid=""
    mode=""
  fi
  if [[ -z "${pid}" ]]; then
    echo "not running (no pidfile/lock)" >&2
    lock_release
    exit 0
  fi
  if ! is_running "${pid}"; then
    echo "not running (stale pidfile): pid=${pid}" >&2
    rm -f "${MARKET_DATA_PID}"
    # Best-effort cleanup: kill any remaining matching processes.
    kill_orphans INT ""
    lock_release
    exit 0
  fi

  local stop_grace_secs stop_term_secs stop_kill_secs
  stop_grace_secs="${STOP_GRACE_SECS:-20}"
  stop_term_secs="${STOP_TERM_SECS:-10}"
  stop_kill_secs="${STOP_KILL_SECS:-5}"

  pgid="$(resolve_pgid "${pid}" "${pgid}" "${mode}")"
  echo "stopping: pid=${pid}${pgid:+ pgid=${pgid}}" >&2
  if [[ -n "${pgid}" ]]; then
    kill_group INT "${pgid}"
  fi
  kill_children INT "${pid}"
  kill -INT "${pid}" >/dev/null 2>&1 || true
  kill_orphans INT "${pid}"

  # Wait for graceful shutdown.
  for _ in $(seq 1 "${stop_grace_secs}"); do
    if ! is_running "${pid}"; then
      rm -f "${MARKET_DATA_PID}"
      echo "stopped" >&2
      lock_release
      exit 0
    fi
    sleep 1
  done

  echo "graceful stop timed out; sending SIGTERM" >&2
  if [[ -n "${pgid}" ]]; then
    kill_group TERM "${pgid}"
  fi
  kill_children TERM "${pid}"
  kill -TERM "${pid}" >/dev/null 2>&1 || true
  kill_orphans TERM "${pid}"

  for _ in $(seq 1 "${stop_term_secs}"); do
    if ! is_running "${pid}"; then
      rm -f "${MARKET_DATA_PID}"
      echo "stopped" >&2
      lock_release
      exit 0
    fi
    sleep 1
  done

  if is_running "${pid}"; then
    echo "still running; sending SIGKILL" >&2
    if [[ -n "${pgid}" ]]; then
      kill_group KILL "${pgid}"
    fi
    kill_children KILL "${pid}"
    kill -KILL "${pid}" >/dev/null 2>&1 || true
    kill_orphans KILL "${pid}"
  fi

  for _ in $(seq 1 "${stop_kill_secs}"); do
    if ! is_running "${pid}"; then
      rm -f "${MARKET_DATA_PID}"
      echo "stopped" >&2
      lock_release
      exit 0
    fi
    sleep 1
  done

  echo "ERROR: stop failed; process still running: pid=${pid}${pgid:+ pgid=${pgid}}" >&2
  if [[ "${KILL_ORPHANS}" == "1" ]]; then
    local others
    others="$(list_running_pids_by_bin | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
    if [[ -n "${others}" ]]; then
      echo "still running (matching): ${others}" >&2
    fi
  fi
  echo "NOTE: pidfile left in place to prevent starting a second instance; investigate and kill manually." >&2
  exit 1
}

restart() {
  stop || true
  start
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) restart ;;
  status) status ;;
  rotate) rotate_logs_if_needed "manual" 1 ;;
  *)
    echo "usage: $0 {start|stop|restart|status|rotate}" >&2
    exit 2
    ;;
esac
