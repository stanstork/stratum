# Benchmarks: Stratum

A reproducible, in-repo benchmark of **Stratum** on MySQL <-> PostgreSQL bulk
load. [pgloader](https://github.com/dimitri/pgloader) is available as an optional
comparison on the PostgreSQL-target workloads, measured against **pgloader v4**
(the current Clojure/JVM rewrite; v3.x was Common Lisp on SBCL).

```bash
./benchmarks/run.sh                      # benchmark Stratum (100M-row synthetic table)
BENCH_ROWS=10000000 ./benchmarks/run.sh  # scaled-down run
WITH_PGLOADER=1 ./benchmarks/run.sh      # also compare against pgloader
```

Everything lives in [`benchmarks/`](../benchmarks/): the harness, the SMQL and
pgloader configs, the data generators, and this methodology. Results land in
`benchmarks/results/<timestamp>/` as a self-contained report (raw logs
included).

> **pgloader is opt-in.** By default `run.sh` benchmarks Stratum only. Pass
> `WITH_PGLOADER=1` to add pgloader on the workloads it can express - `sakila`
> and `synthetic`; it's skipped on `synthetic_heavy` (no computed-column
> transforms) and `reverse` (can't target MySQL). Stratum runs from a native
> binary when one exists at `STRATUM_BIN` (default `target/release/stratum`),
> otherwise from `Dockerfile.stratum`.

## What is measured

Eight workloads:

| Workload | Direction | Shape | What it stresses |
|---|---|---|---|
| **sakila** | MySQL -> PostgreSQL | the full [Sakila](https://dev.mysql.com/doc/sakila/en/) sample DB: 15 tables, ~46K rows | many-small-tables overhead: connection setup, schema creation, per-table coordination |
| **synthetic** | MySQL -> PostgreSQL | one `orders` table, **100M rows** by default, ~200 B/row: BIGINT PK, INT, ENUM, DECIMAL, SMALLINT, FLOAT, CHAR, VARCHARs, BOOLEAN, UUID-shaped CHAR(36), TIMESTAMP, DATETIME, DATE, with NULLs sprinkled | sustained single-table throughput and memory behavior |
| **synthetic_heavy** | MySQL -> PostgreSQL | the same `orders` table projected through ~19 mixed columns: a few string functions (`concat`/`upper`/`lower`/`trim`), several arithmetic expressions, date extraction (`year`/`month`/`quarter`), and some copied-through columns | Stratum only - expression-evaluation CPU (compiled expressions, per-batch schema) |
| **synthetic_plugin_rust** | MySQL -> PostgreSQL | the same table with the **native-Rust WASM** `order_net` transform plugin called 3x per row, plus copied-through columns | Stratum only - per-row cost of repeated Rust WASM transform calls |
| **synthetic_plugin_js** | MySQL -> PostgreSQL | the same table, same 3 calls, via a **JavaScript (QuickJS) WASM** transform plugin | Stratum only - the same, for the JS-on-QuickJS runtime (compare against the Rust plugin) |
| **synthetic_filter_rust** | MySQL -> PostgreSQL | a narrower 8-column projection of `orders`, each row validated by 3 calls to the **native-Rust WASM** `order_ok` filter plugin (every row passes) | Stratum only - per-row cost of WASM *filter* calls through the validation stage |
| **synthetic_filter_js** | MySQL -> PostgreSQL | the same 8-column projection, same 3 filter calls, via a **JavaScript (QuickJS) WASM** filter plugin | Stratum only - the same, for the JS-on-QuickJS runtime (compare against the Rust filter) |
| **reverse** | PostgreSQL -> MySQL | the same `orders` table, into MySQL via `LOAD DATA` | Stratum only - the MySQL write path (pgloader loads into PostgreSQL) |

The four plugin workloads run **matched pairs** so the Rust-WASM and
JS-QuickJS-WASM runtimes are compared on identical per-row work: the two
`plugin` cases invoke the `order_net` transform (`a * b`) three times per row plus
copied-through columns; the two `filter` cases invoke the `order_ok` filter
(pass if non-negative) three times per row through the validation stage. All four
are Stratum-only and need native Stratum plus the host toolchain (the
`wasm32-wasip1` target and `npx`); `run.sh` builds the plugins and skips these
workloads with a note if the toolchain is absent.

Stratum scenarios per workload:

| Scenario | Command |
|---|---|
| stratum | `stratum apply` (native binary or Docker image), 1 lane (default) |
| stratum&#8209;lanes | `lanes = 4` - split a single-table copy into 4 parallel key-range lanes, each on its own source + destination connection (config `synthetic_lanes.smql`). Shows how one large table parallelizes. |
| stratum&#8209;integrity | `stratum apply --integrity` - row hashing + Merkle receipts, so the verification overhead is visible instead of hidden (works with lanes) |

> **Lanes need an integer primary key.** Stratum parallelizes a single table by
> range-splitting its integer PK (`min..max`); a table without one transparently
> falls back to a single lane (no error, no speedup). So the `stratum-lanes`
> scenario runs on `synthetic` (BIGINT PK) but is skipped on `sakila`.

Optional comparison (`WITH_PGLOADER=1`), added only to the MySQL -> PostgreSQL
copy workloads - `sakila` and `synthetic`. It is not run on the transform
workloads (`synthetic_heavy`, `synthetic_plugin_rust`, `synthetic_plugin_js` -
Stratum-only) or `reverse` (pgloader loads into PostgreSQL, so there is no
MySQL-destination comparison):

Per run we record **wall time** (GNU `time -v`), **rows/s** (source rows /
wall), and **peak RSS** of the migrating process (`Maximum resident set size`;
for dockerized pgloader it is sampled via `docker stats` and marked
approximate). Sakila scenarios run 3x (median reported); the single-table
workloads run once by default (`SYNTH_RUNS` / `REV_RUNS` to change). After
**every** run the harness
compares row counts table-by-table between source and destination and fails
loudly on any mismatch - a number only counts if the data actually arrived.

## Fairness rules

- **Same databases, same settings**: both tools talk to the same two
  containers ([`benchmarks/compose.yml`](../benchmarks/compose.yml)) run
  back-to-back on the same machine. The destination PostgreSQL keeps
  `fsync`/`synchronous_commit` **on** - loads are measured with real
  durability. The MySQL source relaxes durability (it is read-only during
  measured runs; the tuning only speeds up data generation).
- **Fresh state every run**: the destination database is dropped and
  recreated, and Stratum runs with an isolated `$HOME` so checkpoint/resume
  state can never carry over between runs.
- **Deterministic data**: every synthetic value is a pure function of the row
  number, so two machines generating `BENCH_ROWS` rows produce identical
  tables.
- **Same scope on the synthetic table** (the rigorous comparison): `orders`
  has only a primary key, so both tools do identical work - create the table,
  copy every row. No index-parity ambiguity.
- **Sakila is not a like-for-like comparison** - see the caveat in its section
  below. On this workload pgloader also builds the secondary indexes and foreign
  keys, so it does more work than Stratum's tables + PK copy. Treat Sakila as
  directional only.
- **JVM in UTC**: pgloader v4 must run with `-Duser.timezone=UTC`, or its
  MySQL JDBC driver throws `HOUR_OF_DAY: 3 -> 4` on any timestamp that lands in
  a daylight-saving gap in the JVM's local zone. The data is fine - Stratum
  reads the same rows without issue; it's a Connector/J footgun. Wall time
  includes JVM startup + JIT warmup (~2 s), counted honestly.
- **pgloader runs with default tuning.** The harness passes no pgloader
  performance options - no `workers`/`concurrency`, batch/prefetch sizing, or
  `WITH` performance clauses beyond what's needed to create tables and copy
  rows. pgloader exposes several knobs (`workers`, `concurrency`, `batch rows`,
  `prefetch rows`, plus destination `work_mem`/`maintenance_work_mem`) that can
  raise its throughput, so **these numbers are its out-of-the-box behavior, not
  its ceiling** - a tuned pgloader would likely do better. Stratum is likewise
  near-default here (`batch_size = 50000`); its main lever is the `lanes` setting
  (the `stratum-lanes` scenario).

Tunables are in-repo and deliberately boring. Stratum runs `batch_size = 50000`
and is otherwise default at 1 lane; its parallelism lever is the `lanes` setting,
so the **4-lane row below is a tuned Stratum config, not the default**. pgloader
runs its defaults (one COPY stream per table, no worker/concurrency tuning). Read
the 1-lane Stratum and default pgloader rows as each tool's out-of-the-box point,
and the 4-lane row as a tuned Stratum data point.

## Results

> A real, measured **10M-row** run on a single machine - not the official 100M
> numbers (those will come from a dedicated reference machine), but 10M is large
> enough that fixed startup cost is amortized. Stratum runs from its native
> binary; pgloader is the v4 JAR on OpenJDK, default tuning.

<!-- BENCH_SAMPLE_START -->
```
stratum 0.1.0 (native) · pgloader v4 (Clojure/JVM, default tuning)
10,000,000-row orders table · MySQL 8.0 <-> PostgreSQL 16
single machine · benchmark harness DBs in Docker
```

### Synthetic - single `orders` table, MySQL -> PostgreSQL

Both tools do the same work: create the table, copy every row (PK only, so no
index-parity fuzz). This is the clean head-to-head.

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, 1 lane (default) | 1 | 17.6 | 567k | 0.49 GB |
| stratum, 1 lane `--integrity` | 1 | 19.2 | 521k | 0.33 GB |
| stratum, 4 lanes (tuned) | 4 | 8.3 | 1.20M | 1.49 GB |
| pgloader v4 (default) | 1 | 38.6 | 259k | 0.83 GB |

- **1 lane (default):** 567k rows/s, 0.49 GB (17.6 s) - the identical
  table-create-plus-copy that pgloader ran (its row above: 259k rows/s, 0.83 GB,
  38.6 s).
- **4 lanes (`lanes = 4`, a tuned setting):** 1.20M rows/s, 1.49 GB - roughly 2x
  the single-lane rate (8.3 s vs 17.6 s). Scaling is sublinear: past ~2 lanes the
  shared PostgreSQL ingest ceiling (~1.2M rows/s on this box) bounds the total, so
  lanes trade connections and memory for total throughput (each lane runs
  concurrently with its own in-flight window, hence the ~1.5 GB).
- **`--integrity`:** 521k rows/s - about 9% over the plain 1-lane run - while
  hashing every row and building Merkle receipts.
- **pgloader v4 (default tuning):** 259k rows/s, 0.83 GB.

### Synthetic-heavy - `orders` -> `orders_heavy` with ~19 mixed columns (Stratum only)

Same source table, but each row is projected through ~19 columns: a few string
functions (`concat`/`upper`/`lower`/`trim`), several arithmetic expressions, date
extraction (`year`/`month`/`quarter`), and some copied-through source columns.
This isolates transform CPU from raw data movement with a balanced mix. 
It's a Stratum-only workload, so there's no comparison row.

**Why the rows/s is lower than the plain copy:** transforms run **in-flight** -
each row's computed columns are evaluated inline as it streams through the
pipeline (producer -> transform -> consumer -> COPY), not in a separate pass over
the table. There's no extra I/O, staging, or second read: the drop is purely the
added per-row expression-evaluation CPU layered onto the same single streaming
pass. So this number is the cost of the transform work itself, and it scales with
how much computation each row carries (here ~19 mixed columns).

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, 1 lane | 1 | 32.3 | 310k | 0.77 GB |
| stratum, 1 lane `--integrity` | 1 | 36.6 | 273k | 0.39 GB |

For reference, the plain copy of the same table (no computed columns) ran 567k
rows/s at 1 lane - so evaluating the ~19 computed columns per row roughly halves
the throughput (310k rows/s), the pure per-row expression cost layered onto the
same streaming pass. `--integrity` on top of the transforms lands at 273k rows/s
(+~13% wall, hashing the projected output rows as they pass).

### Plugin transforms - Rust WASM vs JS (QuickJS) WASM (Stratum only)

The `order_net` transform (`a * b`) run through a plugin **three times per row**,
so the two plugin runtimes are compared on identical per-row work (plus several
copied-through columns for a realistic write width).

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, rust plugin | 1 | 20.6 | 486k | 0.80 GB |
| stratum, rust plugin `--integrity` | 1 | 25.5 | 392k | 0.78 GB |
| stratum, js plugin | 1 | 78.7 | 127k | 0.70 GB |
| stratum, js plugin `--integrity` | 1 | 85.4 | 117k | 0.74 GB |

- **Rust plugins are near-native.** At 486k rows/s the native-Rust transform runs
  within ~15-30% of the no-plugin throughput despite three boundary crossings per row.
- **JS plugins are interpreter-bound.** The same three calls through the QuickJS
  runtime run at 127k rows/s - a ~4x gap that is the interpreter itself executing
  the guest code, not the boundary crossing or marshalling.

> **The boundary is batched.** Plugins are invoked **once per batch**, not
> once per row: a whole batch crosses the WASM host<->guest boundary in a single
> call over a columnar binary wire, and the guest iterates the rows internally.
> That is why the native-Rust plugin sits close to the no-plugin rate instead
> of well below it - the remaining Rust cost is the actual `compute` work, and the
> remaining JS gap is the QuickJS interpreter, not per-row boundary overhead.

### Filter plugins - Rust WASM vs JS (QuickJS) WASM (Stratum only)

The mirror of the transform plugins on the **validation** stage: an 8-column
projection of `orders` where each row is checked by three `order_ok` filter calls
(pass if non-negative; every row passes, so the full pipeline runs). Same matched
Rust-vs-JS comparison, exercising the filter path instead of the transform path.

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, rust filter | 1 | 14.2 | 704k | 0.80 GB |
| stratum, rust filter `--integrity` | 1 | 18.2 | 549k | 0.75 GB |
| stratum, js filter | 1 | 83.9 | 119k | 0.64 GB |
| stratum, js filter `--integrity` | 1 | 88.4 | 113k | 0.73 GB |

- **Rust filter: 704k rows/s** - near-native again (the higher rate than the
  transform run reflects the narrower 8-column projection being written).
- **JS filter: 119k rows/s** - essentially the same as the JS *transform* (127k).
  The QuickJS runtime lands at ~120-127k regardless of whether it filters or
  transforms, which is the tell: this is the interpreter's floor, not a cost of
  the stage or the boundary.

### Reverse - `orders`, PostgreSQL -> MySQL (Stratum only)

pgloader loads into PostgreSQL, so this is Stratum's MySQL write path alone (the
`LOAD DATA` fast path into InnoDB):

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, 1 lane | 1 | 37.2 | 269k | 0.59 GB |

A single stream sustains ~269k rows/s into InnoDB end-to-end - about half
Stratum's own PostgreSQL COPY rate, reflecting InnoDB's always-clustered-index
writes (the destination engine's ceiling). Lanes apply here too (`orders` has an
integer PK).

### Sakila (many small tables) - directional only

The full Sakila DB (15 tables, ~46k rows) is the opposite workload: fixed
per-table cost dominates, not throughput. Stratum's `sakila.smql` fans the 15
tables out into independent pipelines run concurrently (`execution { parallel }`).

| scenario | wall (s, median) | rows/s | peak RSS | scope |
|---|---|---|---|---|
| stratum | 0.4 | 119k | 0.14 GB | tables + data (PK only) |
| stratum `--integrity` | 0.4 | 113k | 0.16 GB | + Merkle receipts |
| pgloader v4 | 3.2 | 14.4k | 0.69 GB | tables + data **+ 37 indexes + 18 FKs** |

Stratum finishes in ~0.4 s; pgloader in ~3.2 s. The two do **different work**
here, though, so the numbers aren't directly comparable:
on this run pgloader also built the full schema (37 secondary indexes + 18 FKs)
that Stratum's config skips, and at 46k rows ~2 s of pgloader's wall time is JVM
startup + JIT. Directional only.
<!-- BENCH_SAMPLE_END -->

## Reading the numbers honestly

- **Benchmark at scale, not toy sizes.** Fixed startup (runtime boot, JVM JIT
  warmup, schema introspection) is ~1-2 s for both tools. Below a few million
  rows it dominates the wall clock and distorts the numbers. Use ≥10M rows.
- **Parallelism is a lever, and it's a tuning axis - read it as such.** Both
  write one COPY stream per table by default; Stratum's `lanes = N` is a tuned
  setting (the 4-lane row above), pgloader splits a table via `concurrency`.
  Past ~2 streams both sit on the shared PostgreSQL ingest ceiling, so a single
  stream is the out-of-the-box point and the multi-lane number is a tuned one.
- `--integrity` hashes every row and maintains Merkle receipts; the point of the
  separate row is that you see exactly what verification costs. It has no
  pgloader counterpart, so there's no pgloader row for it.
- Peak RSS measures the migrating process only, not the databases.

## Memory behavior

Both tools stream and are bounded - neither holds the whole table, and neither's
footprint grows with table size. pgloader v4 (JVM) did 10M in ~0.83 GB with no
tuning, bounded by the JVM heap (`-Xmx`) and its `prefetch rows`.

Stratum holds only a bounded in-flight window: **peak RSS is flat with table
size** - the same at 10M as at 100M - but *scales with lane count*, since each
lane has its own window (≈0.49 GB at 1 lane -> ≈1.5 GB at 4 lanes on the sample
box). That is a deliberate, predictable trade: memory for parallelism.

Two things set Stratum's per-lane footprint:

- **Bounded in-flight window.** The producer -> consumer batch channel is
  bounded two ways, whichever binds first:
  - *by batch count* (4 batches deep). For normal-width rows this is the
    dominant bound, since footprint tracks row count; at `batch_size = 25000`
    it caps a lane's live set at ~100k rows regardless of table size.
  - *by data bytes* (128 MiB). A batch of very wide rows draws proportionally
    more of this budget, so a few wide batches can be resident instead of a
    full channel's worth - without it, a 4 KB-row table used ~2x the memory of
    a narrow one; with it, they land within ~15% of each other.
- **Allocator.** The pipeline is allocation-heavy (each row carries its
  column values), and the default glibc allocator spawns many per-thread
  arenas on a high-core machine and retains freed memory in them - which
  inflated peak RSS ~2-3x as a pure artifact. Stratum links
  [mimalloc](https://github.com/microsoft/mimalloc) to keep that in check and
  return memory to the OS; it also modestly improved throughput.

## Reproducing

Prerequisites: Docker (with compose v2) and GNU time (`/usr/bin/time`). Stratum
runs natively from `STRATUM_BIN` (default `target/release/stratum`) when it
exists, otherwise it is built and run from `Dockerfile.stratum` - which compiles
Stratum inside a Rust builder stage, so Docker alone suffices. The host Rust
toolchain is needed only to build the native binary yourself.

```bash
./benchmarks/run.sh                      # benchmark Stratum; ~45 GB free disk at 100M rows
BENCH_ROWS=1000000 RUNS=3 ./benchmarks/run.sh   # smaller, faster
WITH_PGLOADER=1 ./benchmarks/run.sh      # add the pgloader comparison
./benchmarks/run.sh clean                # tear down containers + volumes
```

pgloader is **opt-in** (`WITH_PGLOADER=1`) and only on the PostgreSQL-target
workloads. Set `PGLOADER_BIN` to measure a local pgloader natively; unset, it
runs as Docker **v4** built from `Dockerfile.pgloader`. For a fair wall-clock run
both tools the same way - both native (`STRATUM_BIN` + `PGLOADER_BIN`) or both
Docker; the harness warns when they differ. Key env vars:

| Var | Purpose |
|---|---|
| `WITH_PGLOADER=1` | add pgloader on the PG-target workloads (off by default) |
| `PGLOADER_BIN` | local pgloader binary; unset -> Docker v4 image (`PGLOADER_IMAGE` / `PGLOADER_JAR_URL`) |
| `STRATUM_BIN` | Stratum binary; absent -> build and run from `Dockerfile.stratum` |
| `PG_DEST_DB` / `MYSQL_DEST_DB` / `PG_SRC_DB` | destination / source database names |

The synthetic table is generated once (server-side, deterministic) and cached
in a Docker volume; only the first run at a given `BENCH_ROWS` pays the
generation cost. See [`benchmarks/README.md`](../benchmarks/README.md) for the
full knob list.

**Write encoding.** The PostgreSQL destination uses **binary `COPY`** by default
(`COPY ... WITH (FORMAT binary)`), which skips both the client-side text
formatting and the server-side text parser. A table is encoded in binary only
when every destination column is a type Stratum can encode exactly; anything
else (arrays, network/geometry types, …) transparently falls back to the CSV
text `COPY` path, as does any individual value that can't be encoded. Force the
CSV path for comparison or debugging with `to { postgres { copy_format = "text" } }`.

`run.sh` drives the `stratum-lanes` scenario (4 lanes, `synthetic_lanes.smql`)
directly. Lanes are an SMQL setting, not an env var, so for a different lane
count copy that config and change `lanes = N`, or run it manually against the
harness's databases:

```bash
# Stratum, 4 lanes over the table (isolated $HOME + fresh dest DB per run)
BENCH_SYNTH_MYSQL_URL=mysql://bench:bench@127.0.0.1:33307/bench \
  BENCH_SYNTH_PG_URL=postgres://bench:bench@127.0.0.1:54329/bench_dest \
  /usr/bin/time -v target/release/stratum apply -c benchmarks/stratum/synthetic_lanes.smql
```

The pgloader v4 load files the harness generates use the multi-line `FROM`/`INTO`
form; the Sakila one casts `geometry` to `bytea` since the bench PostgreSQL has
no PostGIS.
