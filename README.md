# treedb_pgext

A PostgreSQL [Table Access Method](https://www.postgresql.org/docs/current/tableam.html) backed by [TreeDB](../gomap/TreeDB), a mmap'd B+tree storage engine written in Go.

Tables using this AM store rows in TreeDB instead of PostgreSQL's heap. Point lookups and random-access writes go through the B+tree directly, bypassing heap I/O.

## Architecture

```
PostgreSQL backend (C)
  └─ TAM callbacks (treedb_tam.c)
       └─ RPC transport: iceoryx2 by default, or opt-in PG shared memory/latches
            └─ Background worker (treedb_bgworker.c)
                 └─ CGO → treedb_shim.so (Go)
                      └─ TreeDB B+tree (per-relation mmap'd file)
```

The background worker owns the Go runtime and all TreeDB file handles. By default, each backend uses its own iceoryx2 client and the worker wakes on a kqueue/epoll event service (notifier+WaitSet). If `treedb.pg_shmem_enabled=on` is set at postmaster start, sessions can explicitly select `treedb.transport=pg_shmem`; backends then acquire fixed PostgreSQL shared-memory request/response slots and wake the same singleton worker via latches. Iceoryx remains the default and fallback transport.

## Prerequisites

| Dependency | Version | Notes |
|---|---|---|
| PostgreSQL | 18 | `pg_config` must be on `PATH` |
| Go | 1.22+ | `CGO_ENABLED=1` required |
| Rust + Cargo | stable | for building iceoryx2 |
| cmake | 3.16+ | for iceoryx2 C bindings |

## Build and install

**1. Build iceoryx2 C bindings** (one-time, ~2 min):

```bash
cd ../iceoryx2
export PATH="$HOME/.cargo/bin:$PATH"
cmake -S . -B target/ff/cc/build \
  -DCMAKE_BUILD_TYPE=Release \
  -DIOX2_BUILDTYPE_RELEASE=ON
cmake --build target/ff/cc/build --parallel 8
```

**2. Build and install the extension**:

```bash
cd treedb_pgext
make install
```

This builds `treedb_shim.so` (Go, placed in `$(pg_config --pkglibdir)`) and `treedb_pgext.dylib`/`.so` (C extension).

**3. Configure PostgreSQL** (`postgresql.conf`):

```
shared_preload_libraries = 'treedb_pgext'
# Optional, only if you want to benchmark/use the PG shared-memory transport:
# treedb.pg_shmem_enabled = on
```

Restart PostgreSQL after changing this.

**4. Create the extension** (once per database):

```sql
CREATE EXTENSION treedb_pgext;
```

## Usage

Use `treedb` as the table access method on any table:

```sql
-- Per table:
CREATE TABLE my_table (id int PRIMARY KEY, val text) USING treedb;

-- Or as the default for all new tables in a session:
SET default_table_access_method = 'treedb';
```

Standard SQL works as normal — `INSERT`, `UPDATE`, `DELETE`, `SELECT`, indexes, and `TRUNCATE` all work. `DROP TABLE` cleans up TreeDB data atomically on commit.

To opt in to the PostgreSQL shared-memory/latch RPC path for a session after starting the server with `treedb.pg_shmem_enabled=on`:

```sql
SET treedb.transport = 'pg_shmem';
```

If shared memory was not enabled at postmaster start, selecting `pg_shmem` fails closed on the first TreeDB RPC. The PG-shmem path also fails closed on stale slot generations, backend exit, worker restart/exit, slot exhaustion, and bounded response timeout; errors include slot/worker diagnostics to avoid silent hangs. Row data is stored under `$PGDATA/treedb_data/<relfilenode>/`.

## Benchmarks

Use the repeatable transport baseline harness in
[`benchmarks/`](benchmarks/README.md) for current numbers and artifact capture.
The harness records exact commands, environment, result logs, and checkpointed
TreeDB reader setup for heap vs TreeDB singleton-owner transports:

```bash
TDB_BENCH_OUT="artifacts/tam_transport/$(date -u +%Y%m%dT%H%M%SZ)" \
TDB_BENCH_SCALE=1 \
TDB_BENCH_TIME=30 \
TDB_BENCH_CLIENTS="1 2 4 8 16" \
TDB_BENCH_TRANSPORTS="heap treedb_iceoryx treedb_pg_shmem" \
benchmarks/treedb_transport_baseline.sh
```

Current coordinator context on Apple M3 / PostgreSQL 18.4 / pgbench scale=1
showed heap ahead of the current iceoryx path for c=1 TPC-B, select-only, and
SUM scan benchmarks. Treat direct-CGO numbers as c=1 diagnostic context only;
direct-CGO-per-backend is not a production c>1 architecture because it violates
singleton TreeDB ownership and collides on TreeDB locks.

## Tuning

| Constant | File | Default | Effect |
|---|---|---|---|
| `TDB_SCAN_BATCH_BUF` | `treedb_pgext.h` | 64 KB | Bytes fetched per scan RPC. 32–512 KB all perform similarly; 64 KB minimises memory waste. |
| `TDB_SPIN_ITERS` | `treedb_pgext.h` | 2048 | ARM `yield` spins before `sched_yield` fallback. 2048 ≈ 10 µs; doubling to 4096 decreased throughput. |
| `TDB_PG_SHMEM_SLOT_COUNT` | `treedb_bgworker.c` | 64 | Fixed backend request/response slots for the opt-in PG shared-memory transport. |

Only `treedb_pgext` needs recompiling after changing these constants — the Go shim reads `max_bytes` from the request payload at runtime.

## Profiling

The Go shim starts a pprof HTTP server at `localhost:6060` when the background worker loads:

```bash
# during a pgbench run:
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
```

A 30 s profile during TPC-B showed: 73% C IPC machinery, 21% Go scheduler (CGO crossings), <1% actual TreeDB operations.

## Limitations

- Single background worker — all backends share one Go runtime and one TreeDB handle per relation; no internal concurrent TreeDB execution yet.
- No MVCC — snapshot isolation is not implemented; all reads see the latest committed state.
- No WAL — crash recovery is not implemented.
- Sequential scans are ~10× slower than heap for in-memory datasets.
- `FETCH_AND_UPDATE` cannot be collapsed to one RPC at the TAM layer; PostgreSQL evaluates `SET col = col + delta` before calling `tuple_update`.

See [PLAN.md](../PLAN.md) for the full implementation roadmap.
