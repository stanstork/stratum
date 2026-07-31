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

Six workloads:

| Workload | Direction | Shape | What it stresses |
|---|---|---|---|
| **sakila** | MySQL -> PostgreSQL | the full [Sakila](https://dev.mysql.com/doc/sakila/en/) sample DB: 15 tables, ~46K rows | many-small-tables overhead: connection setup, schema creation, per-table coordination |
| **synthetic** | MySQL -> PostgreSQL | one `orders` table, **100M rows** by default, ~200 B/row: BIGINT PK, INT, ENUM, DECIMAL, SMALLINT, FLOAT, CHAR, VARCHARs, BOOLEAN, UUID-shaped CHAR(36), TIMESTAMP, DATETIME, DATE, with NULLs sprinkled | sustained single-table throughput and memory behavior |
| **synthetic_heavy** | MySQL -> PostgreSQL | the same `orders` table projected through ~20 computed columns (nested `concat`/`upper`/`lower`/`trim`/`year`/`month` + arithmetic over several source columns) | Stratum only - expression-evaluation CPU (compiled expressions, per-batch schema) |
| **synthetic_plugin_rust** | MySQL -> PostgreSQL | the same table with one column computed by a **native-Rust WASM** transform plugin | Stratum only - per-row cost of a Rust WASM plugin (boundary + marshalling) |
| **synthetic_plugin_js** | MySQL -> PostgreSQL | the same table, same column, via a **JavaScript (QuickJS) WASM** plugin | Stratum only - the same, for the JS-on-QuickJS runtime (compare against the Rust plugin) |
| **reverse** | PostgreSQL -> MySQL | the same `orders` table, into MySQL via `LOAD DATA` | Stratum only - the MySQL write path (pgloader loads into PostgreSQL) |

The two `plugin` workloads run the **identical** transform (`net = amount * quantity`)
so the Rust-WASM and JS-QuickJS-WASM runtimes are compared on the same per-row
work. They are Stratum-only and need native Stratum plus the host toolchain (the
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
approximate). Sakila scenarios run 3× (median reported); the single-table
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
  near-default here (`batch_size = 25000`); its main lever is the `lanes` setting
  (the `stratum-lanes` scenario).

Tunables are in-repo and deliberately boring. Stratum runs `batch_size = 25000`
and is otherwise default at 1 lane; its parallelism lever is the `lanes` setting,
so the **4-lane row below is a tuned Stratum config, not the default**. pgloader
runs its defaults (one COPY stream per table, no worker/concurrency tuning). Read
the 1-lane Stratum and default pgloader rows as each tool's out-of-the-box point,
and the 4-lane row as a tuned Stratum data point.

## Results

> A real, measured **10M-row** run on a developer laptop - not the official 100M
> numbers (those will come from a dedicated reference machine), but 10M is large
> enough that fixed startup cost is amortized. Stratum runs from its native
> binary; pgloader is the v4 JAR on OpenJDK, default tuning.

<!-- BENCH_SAMPLE_START -->
```
stratum 0.1.0 (native) · pgloader v4.0.0 (Clojure/JVM, OpenJDK 25)
AMD Ryzen AI 9 HX 370 · 24 cores · 30.5 GB RAM · Linux 7.1.3
10,000,000-row orders table · MySQL 8.0 <-> PostgreSQL 16
```

### Synthetic - single `orders` table, MySQL -> PostgreSQL

Both tools do the same work: create the table, copy every row (PK only, so no
index-parity fuzz).

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, 1 lane (default) | 1 | 20.4 | 490k | 0.49 GB |
| stratum, 1 lane `--integrity` | 1 | 20.9 | 477k | 0.32 GB |
| stratum, 4 lanes (tuned) | 4 | 10.2 | 978k | 1.49 GB |
| pgloader v4 (default) | 1 | 40.6 | 246k | 0.75 GB |

- **1 lane (default):** 490k rows/s, 0.49 GB.
- **4 lanes (`lanes = 4`, a tuned setting):** 978k rows/s, 1.49 GB. Scaling is
  sublinear - past ~2 lanes the shared PostgreSQL ingest ceiling (~1M rows/s on
  this box) bounds the total and the per-lane rate settles near ~245k. Lanes
  trade connections and memory for total throughput.
- **`--integrity`:** 477k rows/s - about 2.5% over the plain 1-lane run - while
  hashing every row and building Merkle receipts.
- **pgloader v4 (default tuning):** 246k rows/s, 0.75 GB.

### Synthetic-heavy - `orders` -> `orders_heavy` with ~20 computed columns (Stratum only)

Same source table, but each row is projected through ~20 computed columns
(nested `concat`/`upper`/`lower`/`trim`/`year`/`month` + arithmetic). This
isolates expression-evaluation CPU from raw data movement. It's a Stratum-only
workload, so there's no comparison row.

**Why the rows/s is lower than the plain copy:** transforms run **in-flight** -
each row's computed columns are evaluated inline as it streams through the
pipeline (producer -> transform -> consumer -> COPY), not in a separate pass over
the table. There's no extra I/O, staging, or second read: the drop is purely the
added per-row expression-evaluation CPU layered onto the same single streaming
pass. So this number is the cost of the transform work itself, and it scales with
how much computation each row carries (here ~20 nested-function columns).

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, 1 lane | 1 | 43.2 | 231k | 0.46 GB |
| stratum, 1 lane `--integrity` | 1 | 57.1 | 175k | 0.48 GB |

For reference, the plain copy of the same table (no computed columns) ran 490k
rows/s at 1 lane - so evaluating the ~20 computed columns per row roughly doubles
the per-row cost (231k rows/s). Memory is unchanged (~0.46 GB): the transform is
in-flight, so it adds CPU per row, not resident data. `--integrity` on top of the
transforms lands at 175k rows/s (hashing the projected output rows as they pass).

### Plugin transforms - Rust WASM vs JS (QuickJS) WASM (Stratum only)

The same one-column transform (`net = amount * quantity`) run through a plugin,
so the two plugin runtimes are compared on identical per-row work. The transform
is trivial on purpose - the number is dominated by each runtime's per-row
invocation cost (the WASM boundary + value marshalling), which is the point.

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, rust plugin | 1 | 62.0 | 161k | 0.91 GB |
| stratum, js plugin | 1 | 128.0 | 78k | 1.00 GB |

For reference, the plain copy of the same table ran 490k rows/s and the built-in
`synthetic_heavy` transform 231k - so routing one column through a WASM plugin
per row costs most of the throughput (native Rust 161k, JS-on-QuickJS 78k, ~2×
apart) and roughly doubles memory (~0.9–1.0 GB vs 0.49 GB) for the runtime and
marshalling buffers.

> **Why these are lower than a built-in transform, and what's planned.** Plugin
> transforms are currently invoked **once per row**: every row crosses the WASM
> host<->guest boundary and marshals its values (JSON) in and out. That per-row
> boundary cost - not the arithmetic - is what these numbers measure, which is
> why both plugin cases sit well below the plain copy (~490k) and even the
> built-in `synthetic_heavy` transforms (~231k), and why the heavier runtime (JS
> on QuickJS) is ~2× slower than native Rust.
>
> **Planned: batch plugin invocation.** Pass a whole batch to the plugin in one
> call - as sink plugins already do (`__stratum_write_batch`) - so the boundary
> crossing and marshalling happen **once per batch** instead of once per row,
> with the per-row `compute` running inside the guest. This should move plugin
> throughput toward the plain-copy rate; these single-row-call numbers are the
> pre-optimization baseline.

### Reverse - `orders`, PostgreSQL -> MySQL (Stratum only)

pgloader loads into PostgreSQL, so this is Stratum's MySQL write path alone (the
`LOAD DATA` fast path into InnoDB):

| scenario | streams | wall (s) | rows/s | peak RSS |
|---|---|---|---|---|
| stratum, 1 lane | 1 | 40.8 | 245k | 0.59 GB |

A single stream sustains ~245k rows/s into InnoDB - about half Stratum's own
PostgreSQL COPY rate, reflecting InnoDB's always-clustered-index writes (the
destination engine's ceiling). Lanes apply here too (`orders` has an integer PK).

### Sakila (many small tables) - directional only

The full Sakila DB (15 tables, ~46k rows) is the opposite workload: fixed
per-table cost dominates, not throughput. Stratum's `sakila.smql` fans the 15
tables out into independent pipelines run concurrently (`execution { parallel }`).

| scenario | wall (s, median) | rows/s | scope |
|---|---|---|---|
| stratum | 0.6 | 82.6k | tables + data (PK only) |
| stratum `--integrity` | 0.6 | 82.6k | + Merkle receipts |
| pgloader v4 | 3.4 | 13.6k | tables + data **+ 37 indexes + 18 FKs** |

The two do **different work** here, so the numbers aren't directly comparable:
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
footprint grows with table size. pgloader v4 (JVM) did 10M in ~0.75 GB with no
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
    full channel's worth - without it, a 4 KB-row table used ~2× the memory of
    a narrow one; with it, they land within ~15% of each other.
- **Allocator.** The pipeline is allocation-heavy (each row carries its
  column values), and the default glibc allocator spawns many per-thread
  arenas on a high-core machine and retains freed memory in them - which
  inflated peak RSS ~2-3× as a pure artifact. Stratum links
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
