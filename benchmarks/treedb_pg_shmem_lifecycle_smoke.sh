#!/usr/bin/env bash
# Bounded lifecycle smoke tests for the opt-in PostgreSQL shared-memory TreeDB transport.
#
# The script starts a fresh temporary PostgreSQL cluster with
# shared_preload_libraries=treedb_pgext and treedb.pg_shmem_enabled=on, then runs
# focused normal/restart/exhaustion checks. It assumes the extension has already
# been built and installed (run `make install` from the repo root first).

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: benchmarks/treedb_pg_shmem_lifecycle_smoke.sh [--help]

Environment:
  TDB_LIFECYCLE_OUT          artifact directory (default: artifacts/tam_lifecycle/<utc-ts>)
  TDB_LIFECYCLE_MODES        modes to run (default: "normal worker_restart slot_exhaustion")
                              supported: normal worker_restart slot_exhaustion
  TDB_LIFECYCLE_KEEP_CLUSTER keep temp cluster after exit (default: 0)
  TDB_LIFECYCLE_HOLDERS      holder sessions for slot_exhaustion (default: 64)
  TDB_LIFECYCLE_HOLD_SECONDS holder sleep seconds (default: 20)
  TDB_LIFECYCLE_PSQL_TIMEOUT per-psql timeout seconds (default: 20)
  PG_CONFIG                  pg_config binary (default: pg_config)

Artifacts:
  environment.txt            versions, repo SHA, temp cluster settings
  postgres.log               server log
  commands.log               exact commands
  normal.log                 normal pg_shmem checkpoint smoke
  worker_restart.log         bgworker restart/recovery smoke
  slot_exhaustion*.log       slot exhaustion and cleanup evidence
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ $# -ne 0 ]]; then
  usage >&2
  exit 2
fi

: "${PG_CONFIG:=pg_config}"
: "${TDB_LIFECYCLE_MODES:=normal worker_restart slot_exhaustion}"
: "${TDB_LIFECYCLE_KEEP_CLUSTER:=0}"
: "${TDB_LIFECYCLE_HOLDERS:=64}"
: "${TDB_LIFECYCLE_HOLD_SECONDS:=20}"
: "${TDB_LIFECYCLE_PSQL_TIMEOUT:=20}"

utc_ts=$(date -u +%Y%m%dT%H%M%SZ)
: "${TDB_LIFECYCLE_OUT:=artifacts/tam_lifecycle/${utc_ts}}"
mkdir -p "$TDB_LIFECYCLE_OUT"
commands_log="$TDB_LIFECYCLE_OUT/commands.log"
: > "$commands_log"

if command -v gtimeout >/dev/null 2>&1; then
  timeout_bin=gtimeout
elif command -v timeout >/dev/null 2>&1; then
  timeout_bin=timeout
else
  timeout_bin=""
fi

run_timeout() {
  local seconds=$1
  shift
  if [[ -n "$timeout_bin" ]]; then
    "$timeout_bin" --kill-after=5s "${seconds}s" "$@"
  else
    "$@"
  fi
}

log_cmd() {
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n'
  } | tee -a "$commands_log"
}

run_logged() {
  local log=$1
  shift
  log_cmd "$@"
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n'
    "$@"
  } >"$log" 2>&1
}

psql_logged() {
  local log=$1
  shift
  run_logged "$log" run_timeout "$TDB_LIFECYCLE_PSQL_TIMEOUT" \
    env PGOPTIONS='-c treedb.transport=pg_shmem' \
    psql -X -v ON_ERROR_STOP=1 "$@"
}

find_free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
PY
}

pg_bindir=$("$PG_CONFIG" --bindir)
export PATH="$pg_bindir:$PATH"

run_root=$(mktemp -d "${TMPDIR:-/tmp}/tdb_pgshmem_lifecycle_XXXXXX")
pgdata="$run_root/pgdata"
sockdir="$run_root/sock"
mkdir -p "$sockdir"
port=$(find_free_port)
db=tdb_lifecycle

declare -a holder_pids=()
cleanup() {
  local status=$?
  for pid in "${holder_pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${holder_pids[@]:-}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  if [[ -d "$pgdata" ]]; then
    pg_ctl -D "$pgdata" -m fast stop >/dev/null 2>&1 || true
  fi
  if [[ "$TDB_LIFECYCLE_KEEP_CLUSTER" == "1" ]]; then
    echo "temporary cluster kept at $run_root" > "$TDB_LIFECYCLE_OUT/cluster.txt"
  else
    rm -rf "$run_root"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

write_environment() {
  {
    echo "utc_ts=$utc_ts"
    echo "git_head=$(git rev-parse HEAD 2>/dev/null || true)"
    echo "git_branch=$(git branch --show-current 2>/dev/null || true)"
    echo "uname=$(uname -a)"
    echo "pg_config=$(command -v "$PG_CONFIG" || true)"
    "$PG_CONFIG" --version 2>/dev/null || true
    echo "pg_bindir=$pg_bindir"
    echo "psql=$(command -v psql || true)"
    psql --version 2>/dev/null || true
    echo "pg_ctl=$(command -v pg_ctl || true)"
    echo "initdb=$(command -v initdb || true)"
    echo "timeout_bin=${timeout_bin:-none}"
    echo "TDB_LIFECYCLE_MODES=$TDB_LIFECYCLE_MODES"
    echo "TDB_LIFECYCLE_HOLDERS=$TDB_LIFECYCLE_HOLDERS"
    echo "TDB_LIFECYCLE_HOLD_SECONDS=$TDB_LIFECYCLE_HOLD_SECONDS"
    echo "TDB_LIFECYCLE_PSQL_TIMEOUT=$TDB_LIFECYCLE_PSQL_TIMEOUT"
    echo "run_root=$run_root"
    echo "pgdata=$pgdata"
    echo "sockdir=$sockdir"
    echo "port=$port"
  } > "$TDB_LIFECYCLE_OUT/environment.txt"
}

start_cluster() {
  run_logged "$TDB_LIFECYCLE_OUT/initdb.log" initdb -D "$pgdata" --no-locale -E UTF8
  cat >> "$pgdata/postgresql.conf" <<EOF
shared_preload_libraries = 'treedb_pgext'
treedb.pg_shmem_enabled = on
listen_addresses = ''
unix_socket_directories = '$sockdir'
port = $port
log_min_messages = info
log_connections = off
log_disconnections = off
EOF
  run_logged "$TDB_LIFECYCLE_OUT/pg_ctl_start.log" pg_ctl -D "$pgdata" -l "$TDB_LIFECYCLE_OUT/postgres.log" -w -t 30 start
  export PGHOST="$sockdir"
  export PGPORT="$port"
  export PGDATABASE=postgres
  run_logged "$TDB_LIFECYCLE_OUT/createdb.log" createdb "$db"
  export PGDATABASE="$db"
  run_logged "$TDB_LIFECYCLE_OUT/create_extension.log" psql -X -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION treedb_pgext;'
  psql_logged "$TDB_LIFECYCLE_OUT/verify_pg_shmem.log" -c 'SHOW treedb.pg_shmem_enabled; SHOW treedb.transport;'
}

bgworker_pid() {
  # The TreeDB bgworker does not establish a database connection, so it is not
  # guaranteed to appear in pg_stat_activity.  Find the postmaster child whose
  # command line contains the registered worker name/type.
  local postmaster_pid
  postmaster_pid=$(head -1 "$pgdata/postmaster.pid" 2>/dev/null || true)
  if [[ -z "$postmaster_pid" ]]; then
    return 0
  fi
  ps -axo pid=,ppid=,command= | \
    awk -v ppid="$postmaster_pid" '$2 == ppid && $0 ~ /treedb/ && pid == "" { pid = $1 } END { if (pid != "") print pid }'
}

wait_for_holder_connections() {
  local holders=$1
  local deadline=$((SECONDS + TDB_LIFECYCLE_PSQL_TIMEOUT))
  local count

  while (( SECONDS < deadline )); do
    count=$(psql -X -At -d postgres -c "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%holder_ready%' AND pid <> pg_backend_pid();" 2>/dev/null || echo 0)
    if [[ "${count:-0}" -ge "$holders" ]]; then
      # Give the already-connected holders a short grace period to complete
      # their one RPC and enter pg_sleep() while keeping their backend slots.
      sleep 0.5
      return 0
    fi
    sleep 0.1
  done
  echo "only ${count:-0} holder connections became visible (expected $holders)" > "$TDB_LIFECYCLE_OUT/slot_exhaustion_holder_wait_failed.log"
  return 1
}

mode_normal() {
  psql_logged "$TDB_LIFECYCLE_OUT/normal.log" -c 'SELECT treedb_checkpoint_all();'
}

mode_worker_restart() {
  local log="$TDB_LIFECYCLE_OUT/worker_restart.log"
  local oldpid newpid deadline

  oldpid=$(bgworker_pid)
  if [[ -z "$oldpid" ]]; then
    echo "no treedb background worker found" > "$log"
    return 1
  fi
  {
    echo "old_worker_pid=$oldpid"
    echo "killing old worker with SIGKILL to force bgworker restart"
  } > "$log"
  kill -KILL "$oldpid"

  deadline=$((SECONDS + 15))
  newpid=""
  while (( SECONDS < deadline )); do
    newpid=$(bgworker_pid || true)
    if [[ -n "$newpid" && "$newpid" != "$oldpid" ]]; then
      break
    fi
    sleep 0.2
  done
  echo "new_worker_pid=${newpid:-}" >> "$log"
  if [[ -z "$newpid" || "$newpid" == "$oldpid" ]]; then
    echo "worker did not restart within 15s" >> "$log"
    return 1
  fi

  psql_logged "$TDB_LIFECYCLE_OUT/worker_restart_checkpoint.log" -c 'SELECT treedb_checkpoint_all();'
}

mode_slot_exhaustion() {
  local holders=$TDB_LIFECYCLE_HOLDERS
  local log_prefix="$TDB_LIFECYCLE_OUT/slot_exhaustion"
  local failed_log="${log_prefix}_expected_error.log"
  local cleanup_log="${log_prefix}_cleanup_success.log"

  for i in $(seq 1 "$holders"); do
    local hlog="${log_prefix}_holder_${i}.log"
    env PGOPTIONS="-c treedb.transport=pg_shmem -c application_name=tdb_slot_holder_${i}" \
      psql -X -v ON_ERROR_STOP=1 -d "$db" \
      -c "SELECT 'holder_ready' AS holder_ready, treedb_checkpoint_all(); SELECT pg_sleep(${TDB_LIFECYCLE_HOLD_SECONDS});" \
      >"$hlog" 2>&1 &
    holder_pids+=("$!")
  done

  wait_for_holder_connections "$holders"

  local deadline=$((SECONDS + TDB_LIFECYCLE_PSQL_TIMEOUT))
  local saw_exhaustion=0
  while (( SECONDS < deadline )); do
    if psql_logged "$failed_log" -c 'SELECT treedb_checkpoint_all();'; then
      echo "extra request succeeded before slots were full; retrying" >> "$failed_log"
      sleep 0.2
      continue
    fi
    if grep -q 'no free pg_shmem RPC slots' "$failed_log"; then
      saw_exhaustion=1
      break
    fi
    echo "slot exhaustion error did not contain expected diagnostic; retrying" >> "$failed_log"
    sleep 0.2
  done
  if [[ "$saw_exhaustion" != "1" ]]; then
    echo "did not observe expected slot exhaustion within ${TDB_LIFECYCLE_PSQL_TIMEOUT}s" >> "$failed_log"
    return 1
  fi

  # Terminating one holder backend should invoke backend shared-memory-exit
  # cleanup and make its slot reusable without waiting for pg_sleep() to expire.
  local victim_backend_pid
  victim_backend_pid=$(psql -X -At -d postgres -c "SELECT pid FROM pg_stat_activity WHERE query LIKE '%holder_ready%' AND pid <> pg_backend_pid() ORDER BY pid LIMIT 1;")
  if [[ -z "$victim_backend_pid" ]]; then
    echo "could not find holder backend to terminate" > "$cleanup_log"
    return 1
  fi
  run_logged "${log_prefix}_terminate_backend.log" \
    psql -X -v ON_ERROR_STOP=1 -d postgres -c "SELECT pg_terminate_backend(${victim_backend_pid});"
  sleep 0.5

  psql_logged "$cleanup_log" -c 'SELECT treedb_checkpoint_all();'
}

write_environment
start_cluster

for mode in $TDB_LIFECYCLE_MODES; do
  case "$mode" in
    normal) mode_normal ;;
    worker_restart) mode_worker_restart ;;
    slot_exhaustion) mode_slot_exhaustion ;;
    *) echo "unknown mode: $mode" >&2; exit 2 ;;
  esac
done

echo "artifacts written to $TDB_LIFECYCLE_OUT"
