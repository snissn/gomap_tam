#!/usr/bin/env bash
# Repeatable pgbench harness for TreeDB TAM transport baseline evidence.
# It compares PostgreSQL heap with the current TreeDB iceoryx singleton-owner path.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: benchmarks/treedb_transport_baseline.sh [--help]

Creates fresh pgbench databases and records heap vs TreeDB iceoryx transport
baseline artifacts. PostgreSQL must already be running; for TreeDB runs the
extension must be installed and listed in shared_preload_libraries.

Environment:
  PGDATABASE              maintenance database for createdb/dropdb (default: postgres)
  PGHOST/PGPORT/PGUSER   standard libpq connection settings
  TDB_BENCH_OUT          artifact directory (default: artifacts/tam_transport/<utc-ts>)
  TDB_BENCH_SCALE        pgbench scale (default: 1)
  TDB_BENCH_TIME         seconds per pgbench run (default: 30)
  TDB_BENCH_CLIENTS      read matrix clients/jobs (default: "1 2 4 8 16")
  TDB_BENCH_TRANSPORTS   transports to run (default: "heap treedb_iceoryx")
  TDB_BENCH_PREFIX       database name prefix (default: tam_<utc-ts>)
  TDB_BENCH_KEEP_DBS     keep generated DBs after success (default: 0)

Artifacts:
  environment.txt        tool versions, git SHA, connection/database settings
  commands.log           exact commands executed
  results.tsv            parsed TPS summary
  sum.sql                SUM scan pgbench script
  *.log                  stdout/stderr for each setup and benchmark command

Direct-CGO probe note: this harness intentionally does not run direct-CGO as a
production transport. If a local prototype branch has such a probe, run only c=1
outside this script and archive it under a separate direct_cgo_probe/ artifact
subdirectory with the prototype branch/SHA and unsafe-c>1 caveat.
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

: "${PGDATABASE:=postgres}"
: "${TDB_BENCH_SCALE:=1}"
: "${TDB_BENCH_TIME:=30}"
: "${TDB_BENCH_CLIENTS:=1 2 4 8 16}"
: "${TDB_BENCH_TRANSPORTS:=heap treedb_iceoryx}"
: "${TDB_BENCH_KEEP_DBS:=0}"

utc_ts=$(date -u +%Y%m%dT%H%M%SZ)
: "${TDB_BENCH_OUT:=artifacts/tam_transport/${utc_ts}}"
: "${TDB_BENCH_PREFIX:=tam_${utc_ts}}"

mkdir -p "$TDB_BENCH_OUT"
commands_log="$TDB_BENCH_OUT/commands.log"
results_tsv="$TDB_BENCH_OUT/results.tsv"
sum_sql="$TDB_BENCH_OUT/sum.sql"
: > "$commands_log"
printf 'transport\tbenchmark\tclients\tjobs\tseconds\tscale\ttps\tlog\n' > "$results_tsv"
printf 'SELECT SUM(abalance) FROM pgbench_accounts;\n' > "$sum_sql"

declare -a created_dbs=()

log_cmd() {
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n'
  } | tee -a "$commands_log"
}

run_logged() {
  local name=$1
  shift
  local log="$TDB_BENCH_OUT/${name}.log"
  log_cmd "$@"
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n'
    "$@"
  } >"$log" 2>&1
}

extract_tps() {
  local log=$1
  awk '/^tps = / { print $3; found=1 } END { if (!found) print "" }' "$log" | tail -1
}

run_pgbench() {
  local transport=$1
  local benchmark=$2
  local clients=$3
  local db=$4
  shift 4
  local jobs=$clients
  local name="${transport}_${benchmark}_c${clients}"
  local log="$TDB_BENCH_OUT/${name}.log"

  run_logged "$name" pgbench -d "$db" -c "$clients" -j "$jobs" -T "$TDB_BENCH_TIME" "$@"
  local tps
  tps=$(extract_tps "$log")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$transport" "$benchmark" "$clients" "$jobs" "$TDB_BENCH_TIME" \
    "$TDB_BENCH_SCALE" "$tps" "$log" >> "$results_tsv"
}

createdb_fresh() {
  local db=$1
  run_logged "drop_${db}" dropdb --if-exists --maintenance-db "$PGDATABASE" "$db"
  run_logged "createdb_${db}" createdb --maintenance-db "$PGDATABASE" "$db"
  created_dbs+=("$db")
}

cleanup() {
  local status=$?
  if [[ "$status" -eq 0 && "$TDB_BENCH_KEEP_DBS" == "0" ]]; then
    for db in "${created_dbs[@]}"; do
      dropdb --if-exists --maintenance-db "$PGDATABASE" "$db" >/dev/null 2>&1 || true
    done
  else
    {
      echo "Generated databases kept for inspection: ${created_dbs[*]:-none}"
      echo "Set TDB_BENCH_KEEP_DBS=0 and rerun, or drop them manually when done."
    } >> "$TDB_BENCH_OUT/cleanup.txt"
  fi
}
trap cleanup EXIT

write_environment() {
  {
    echo "utc_ts=$utc_ts"
    echo "git_head=$(git rev-parse HEAD 2>/dev/null || true)"
    echo "git_branch=$(git branch --show-current 2>/dev/null || true)"
    echo "uname=$(uname -a)"
    echo "pg_config=$(command -v pg_config || true)"
    pg_config --version 2>/dev/null || true
    echo "pgbench=$(command -v pgbench || true)"
    pgbench --version 2>/dev/null || true
    echo "psql=$(command -v psql || true)"
    psql --version 2>/dev/null || true
    echo "go=$(command -v go || true)"
    go version 2>/dev/null || true
    echo "PGDATABASE=$PGDATABASE"
    echo "PGHOST=${PGHOST:-}"
    echo "PGPORT=${PGPORT:-}"
    echo "PGUSER=${PGUSER:-}"
    echo "TDB_BENCH_SCALE=$TDB_BENCH_SCALE"
    echo "TDB_BENCH_TIME=$TDB_BENCH_TIME"
    echo "TDB_BENCH_CLIENTS=$TDB_BENCH_CLIENTS"
    echo "TDB_BENCH_TRANSPORTS=$TDB_BENCH_TRANSPORTS"
    echo "TDB_BENCH_PREFIX=$TDB_BENCH_PREFIX"
    echo "TDB_BENCH_KEEP_DBS=$TDB_BENCH_KEEP_DBS"
    echo "server_version=$(psql -X -At -d "$PGDATABASE" -c 'SHOW server_version;' 2>/dev/null || true)"
    echo "shared_preload_libraries=$(psql -X -At -d "$PGDATABASE" -c 'SHOW shared_preload_libraries;' 2>/dev/null || true)"
    if command -v go >/dev/null 2>&1 && [[ -d go ]]; then
      (
        cd go
        go list -m -f 'gomap_module={{.Path}} version={{.Version}} dir={{.Dir}}{{with .Replace}} replace={{.Path}}=>{{.Dir}}{{end}}' github.com/snissn/gomap 2>/dev/null || true
      )
      local gomap_dir
      gomap_dir=$(cd go && go list -m -f '{{.Dir}}' github.com/snissn/gomap 2>/dev/null || true)
      if [[ -n "$gomap_dir" && -d "$gomap_dir/.git" ]]; then
        echo "gomap_git_head=$(git -C "$gomap_dir" rev-parse HEAD 2>/dev/null || true)"
        echo "gomap_git_branch=$(git -C "$gomap_dir" branch --show-current 2>/dev/null || true)"
      fi
    fi
  } > "$TDB_BENCH_OUT/environment.txt"
}

require_treedb_preload() {
  local preload
  preload=$(psql -X -At -d "$PGDATABASE" -c 'SHOW shared_preload_libraries;')
  if [[ ",${preload// /}," != *",treedb_pgext,"* ]]; then
    cat >&2 <<EOF
error: TDB_BENCH_TRANSPORTS includes treedb_iceoryx, but shared_preload_libraries is "$preload".
Install the extension, set shared_preload_libraries = 'treedb_pgext', restart PostgreSQL,
and rerun this harness. See benchmarks/README.md.
EOF
    exit 2
  fi
}

checkpoint_treedb() {
  local db=$1
  local label=$2
  run_logged "treedb_checkpoint_${label}" \
    psql -X -v ON_ERROR_STOP=1 -d "$db" -c 'SELECT treedb_checkpoint_all();'
}

run_heap() {
  local db="${TDB_BENCH_PREFIX}_heap"
  createdb_fresh "$db"
  run_logged heap_init pgbench -i -s "$TDB_BENCH_SCALE" "$db"

  run_pgbench heap tpcb 1 "$db"
  for c in $TDB_BENCH_CLIENTS; do
    run_pgbench heap select_only "$c" "$db" -S
  done
  for c in $TDB_BENCH_CLIENTS; do
    run_pgbench heap sum_scan "$c" "$db" -f "$sum_sql"
  done
}

run_treedb_iceoryx() {
  local db="${TDB_BENCH_PREFIX}_treedb"
  createdb_fresh "$db"
  run_logged treedb_create_extension psql -X -v ON_ERROR_STOP=1 -d "$db" -c 'CREATE EXTENSION treedb_pgext;'
  run_logged treedb_init env PGOPTIONS='-c default_table_access_method=treedb' \
    pgbench -i -s "$TDB_BENCH_SCALE" "$db"
  run_logged treedb_verify_table_am psql -X -v ON_ERROR_STOP=1 -d "$db" -c \
    "SELECT c.relname, am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam WHERE c.relname LIKE 'pgbench_%' AND c.relkind = 'r' ORDER BY 1;"
  checkpoint_treedb "$db" after_init

  run_pgbench treedb_iceoryx tpcb 1 "$db"
  checkpoint_treedb "$db" before_select_only
  for c in $TDB_BENCH_CLIENTS; do
    run_pgbench treedb_iceoryx select_only "$c" "$db" -S
  done
  checkpoint_treedb "$db" before_sum_scan
  for c in $TDB_BENCH_CLIENTS; do
    run_pgbench treedb_iceoryx sum_scan "$c" "$db" -f "$sum_sql"
  done
}

write_environment

for transport in $TDB_BENCH_TRANSPORTS; do
  case "$transport" in
    heap)
      run_heap
      ;;
    treedb_iceoryx)
      require_treedb_preload
      run_treedb_iceoryx
      ;;
    direct_cgo_probe)
      cat >&2 <<'EOF'
error: direct_cgo_probe is intentionally not a production transport in this harness.
Run any local probe branch manually at c=1 only and archive logs separately with the
unsafe-c>1 TreeDB lock caveat.
EOF
      exit 2
      ;;
    *)
      echo "error: unknown transport '$transport'" >&2
      exit 2
      ;;
  esac
done

printf '\nArtifacts written to %s\n' "$TDB_BENCH_OUT"
printf 'Summary: %s\n' "$results_tsv"
