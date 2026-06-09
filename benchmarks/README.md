# TreeDB TAM transport baseline harness

This directory contains the repeatable benchmark foundation for GitHub issue #2:
heap vs TreeDB Table Access Method transport paths, with checkpointed TreeDB
readers and artifact guidance suitable for transport comparison PRs.

## Scope

- Measures PostgreSQL heap and TreeDB singleton-owner transports:
  - `treedb_iceoryx` (default production path);
  - `treedb_pg_shmem` (opt-in PostgreSQL shared-memory/latch path, when enabled).
- Uses fresh `pgbench` scale=1 databases by default.
- Runs the tracker matrix:
  - TPC-B: `pgbench -c 1 -j 1 -T 30`
  - select-only: `pgbench -S -c 1,2,4,8,16 -j same -T 30`
  - SUM scan: `SELECT SUM(abalance) FROM pgbench_accounts` at c=1,2,4,8,16
- Runs `SELECT treedb_checkpoint_all();` after TreeDB initialization and before
  read matrices so TreeDB reader evidence is checkpointed.

Non-goals for this harness:

- It does **not** remove iceoryx.
- It does **not** make direct-CGO-per-backend a production architecture.

## Prerequisites

Use the same PostgreSQL instance and hardware for all transports in a run.
Record the versions in the artifact directory; the script captures these in
`environment.txt`.

1. Build and install the extension from this repo:

   ```bash
   make install
   ```

2. Configure PostgreSQL and restart it:

   ```conf
   shared_preload_libraries = 'treedb_pgext'
   # Optional for treedb_pg_shmem benchmark runs:
   # treedb.pg_shmem_enabled = on
   ```

3. Confirm required tools are on `PATH`:

   ```bash
   pg_config --version
   pgbench --version
   psql --version
   go version
   ```

## Run the baseline matrix

```bash
OUT="artifacts/tam_transport/$(date -u +%Y%m%dT%H%M%SZ)"
TDB_BENCH_OUT="$OUT" \
TDB_BENCH_SCALE=1 \
TDB_BENCH_TIME=30 \
TDB_BENCH_CLIENTS="1 2 4 8 16" \
TDB_BENCH_TRANSPORTS="heap treedb_iceoryx treedb_pg_shmem" \
benchmarks/treedb_transport_baseline.sh
```

The script creates fresh databases named from `TDB_BENCH_PREFIX` (default:
`tam_<utc-ts>_heap` and `tam_<utc-ts>_treedb`). By default they are dropped after
a successful run; set `TDB_BENCH_KEEP_DBS=1` to keep them for inspection.

To run only one transport while debugging:

```bash
TDB_BENCH_TRANSPORTS="heap" benchmarks/treedb_transport_baseline.sh
TDB_BENCH_TRANSPORTS="treedb_iceoryx" benchmarks/treedb_transport_baseline.sh
TDB_BENCH_TRANSPORTS="treedb_pg_shmem" benchmarks/treedb_transport_baseline.sh
```

## PG-shmem lifecycle smoke

For lifecycle-hardening changes, run the bounded smoke script after
`make install`. It starts a fresh temporary PostgreSQL cluster with
`treedb.pg_shmem_enabled=on` and archives normal, bgworker restart, and slot
exhaustion/cleanup evidence:

```bash
TDB_LIFECYCLE_OUT="artifacts/tam_lifecycle/$(date -u +%Y%m%dT%H%M%SZ)" \
  benchmarks/treedb_pg_shmem_lifecycle_smoke.sh
```

Use `TDB_LIFECYCLE_MODES="normal slot_exhaustion"` for a shorter failure smoke,
or `TDB_LIFECYCLE_KEEP_CLUSTER=1` to keep the temporary cluster for inspection.
The script is bounded with per-psql timeouts when `gtimeout` or `timeout` is
available and writes `environment.txt`, `commands.log`, `postgres.log`, and
mode-specific logs under the artifact directory.

## Artifacts to attach or cite

Each run writes:

| Artifact | Contents |
| --- | --- |
| `environment.txt` | UTC timestamp, repo and gomap module/git identity, OS, tool versions, libpq env, benchmark env, server version, `shared_preload_libraries`, TreeDB transport GUCs |
| `commands.log` | exact commands executed with shell quoting |
| `results.tsv` | parsed TPS summary: transport, benchmark, clients/jobs, seconds, scale, TPS, source log |
| `sum.sql` | the exact SUM scan pgbench script |
| `*.log` | setup, checkpoint, and pgbench stdout/stderr logs |

PRs should include the artifact directory path and either paste `results.tsv` or
summarize it in a markdown table. Do not mix numbers from different hardware,
PostgreSQL versions, scale factors, or TreeDB commits in one comparison table
without saying so explicitly.

## Checkpointed TreeDB reader setup

The TreeDB transport stores rows in the background worker's singleton TreeDB
handles. Before reader benchmarks, run:

```sql
SELECT treedb_checkpoint_all();
```

`treedb_checkpoint_all()` is a benchmark/setup helper that sends one RPC to the
background worker and calls `DB.Checkpoint()` on every currently open relation
TreeDB handle. It returns the number of open relation handles checkpointed.

The harness invokes it:

1. after `pgbench -i` with `default_table_access_method=treedb`;
2. after the c=1 TPC-B write run and before select-only reads;
3. before the SUM scan matrix.

For manual setup, initialize TreeDB pgbench data with the default iceoryx transport:

```bash
createdb tam_manual_treedb
psql -X -v ON_ERROR_STOP=1 -d tam_manual_treedb -c 'CREATE EXTENSION treedb_pgext;'
env PGOPTIONS='-c default_table_access_method=treedb' pgbench -i -s 1 tam_manual_treedb
psql -X -v ON_ERROR_STOP=1 -d tam_manual_treedb -c 'SELECT treedb_checkpoint_all();'
```

For the PG shared-memory transport, start PostgreSQL with
`treedb.pg_shmem_enabled=on`, then pass transport PGOPTIONS to every setup and
benchmark command that touches TreeDB:

```bash
createdb tam_manual_treedb_shmem
psql -X -v ON_ERROR_STOP=1 -d tam_manual_treedb_shmem -c 'CREATE EXTENSION treedb_pgext;'
env PGOPTIONS='-c treedb.transport=pg_shmem -c default_table_access_method=treedb' \
  pgbench -i -s 1 tam_manual_treedb_shmem
env PGOPTIONS='-c treedb.transport=pg_shmem' \
  psql -X -v ON_ERROR_STOP=1 -d tam_manual_treedb_shmem -c 'SELECT treedb_checkpoint_all();'
```

## Direct-CGO probe status

No direct-CGO production path is part of this branch. If a local prototype branch
contains a direct-CGO-per-backend probe, treat it only as a c=1 diagnostic for
transport overhead:

- run it separately from this harness and store artifacts under
  `direct_cgo_probe/` with the prototype branch and head SHA;
- report only c=1 numbers unless the branch explicitly fixes TreeDB
  multi-process ownership;
- document that direct-CGO c>1 is unsafe in the current design because each
  PostgreSQL backend opens its own TreeDB handle and collides on TreeDB file
  locks (for example, `treedb: lock already held: .../dictdb/LOCK`).

That failure mode supports the tracker design: production c>1 work must preserve
a singleton TreeDB owner (#3/#4) and evaluate internal read concurrency only
inside that owner (#5).

## Current local evidence to compare against

Coordinator experiments on PostgreSQL 18.4 Homebrew, Apple M3, pgbench scale=1,
checkpointed TreeDB readers showed:

| Benchmark | heap | TreeDB iceoryx IPC | direct-CGO c=1 probe |
| --- | ---: | ---: | ---: |
| TPC-B c=1 TPS | 6,226 | 4,638 | 5,123 |
| `pgbench -S` c=1 TPS | 44,394 | 33,958 | 39,917 |
| `SUM(abalance)` c=1 TPS | 370.9 | 106.6 | 127.7 |

These are baseline context, not replacement evidence for a PR that changes the
measured path. If a PR claims performance changes, rerun the harness on that PR's
latest head and cite its artifacts.
