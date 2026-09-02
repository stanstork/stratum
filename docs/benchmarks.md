# Benchmarks: Stratum

A reproducible, in-repo benchmark of Stratum on MySQL <-> PostgreSQL bulk
load. [pgloader](https://github.com/dimitri/pgloader) is available as an optional
comparison on the PostgreSQL-target workloads, measured against pgloader v4
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
| stratum&#8209;lanes&#8209;integrity | `lanes = 4` plus `--integrity` - the "4 lanes +int" column. Measures the verification cost when four lanes each write their own hash stream. |

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
workloads run once by default (`SYNTH_RUNS` / `REV_RUNS` to change). After every run the harness compares
row counts table-by-table between source and destination and fails loudly on
any mismatch - a number only counts if the data actually arrived.

## Fairness rules

- **Same databases, same settings**: both tools talk to the same two
  containers ([`benchmarks/compose.yml`](../benchmarks/compose.yml)). The
  destination PostgreSQL keeps `fsync`/`synchronous_commit` on - loads are
  measured with real durability. The MySQL source relaxes durability (it is
  read-only during measured runs; the tuning only speeds up data generation).
- **Fresh state every run**: the destination database is dropped and
  recreated, and Stratum runs with an isolated `$HOME` so checkpoint/resume
  state can never carry over between runs.
- **Deterministic data**: every synthetic value is a pure function of the row
  number, so two machines generating `BENCH_ROWS` rows produce identical
  tables.
- **Same scope on the synthetic table** (the like-for-like row): `orders`
  has only a primary key, so both tools do identical work - create the table,
  copy every row. No index-parity ambiguity.
- **Sakila is not a like-for-like comparison** - see the caveat in its section
  below. Stratum builds the tables, primary keys, and secondary indexes, but
  not the foreign keys; pgloader also builds the foreign keys, so it still does
  somewhat more work. Treat Sakila as directional only.
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
  raise its throughput, so these numbers are its out-of-the-box behavior; a
  tuned pgloader would likely do better. Stratum is likewise
  near-default here (`batch_size = 50000`); its main lever is the `lanes` setting
  (the `stratum-lanes` scenario).

Tunables are in-repo and deliberately boring. Read the 1-lane Stratum and
default-pgloader rows as each tool's out-of-the-box point, and the 4-lane row
as a tuned Stratum data point.

## Results

The same harness, configs, and generators run twice on different hardware. Read
the two tables together. The **100M separated-hosts** run is the reference, the
realistic networked end-to-end shape. The **10M single-machine** run is the
reproducible-anywhere companion, faster in absolute terms only because co-located
`localhost` databases have no network latency. What each column means is in
[What each configuration measures](#what-each-configuration-measures) below; the
findings are the same on both runs, only the numbers move.

Figures are **rows/s** (source rows ÷ wall). Full wall time and peak RSS per run
are in each run's `benchmarks/results/<ts>/summary.md`; memory is discussed under
[Memory behavior](#memory-behavior). Every run's destination row count was verified.

### 100M rows - separated cloud hosts (the reference)

Three hosts, AWS eu-north-1, one AZ + cluster placement group: engine
`c7i.2xlarge` (Xeon 8488C, 8 vCPU); source and destination each `i4i.2xlarge`
(8 vCPU, 64 GB, local NVMe) running MySQL 8.0 (buffer pool 32 GiB) and
PostgreSQL 16 (`fsync` on), on a private same-AZ network. Stratum 0.1.0 native;
pgloader v4.0.0 native jar, default tuning. Run via the harness's external-DB
mode (`EXTERNAL_DB=1 MYSQL_HOST=… PG_HOST=…`).

| workload | 1 lane | 1 lane +int | 4 lanes | 4 lanes +int | pgloader v4 |
|---|---|---|---|---|---|
| **synthetic** (like-for-like) | **388,818** | 336,428 | **944,109** | 673,991 | 170,317 |
| synthetic_heavy (~19 transforms) | 250,815 | 224,462 | — | — | — |
| plugin_rust (WASM transform) | 331,488 | 269,665 | — | — | — |
| plugin_js (QuickJS transform) | 94,107 | 87,560 | — | — | — |
| filter_rust (WASM filter) | 445,692 | 344,388 | — | — | — |
| filter_js (QuickJS filter) | 85,502 | 81,020 | — | — | — |
| reverse (PG → MySQL) | 157,503 | — | — | — | — |
| sakila (many small tables) | 62,524 | 60,879 | — | — | 13,144 |

**Peak RSS** (the migrating process, not the databases): ~0.5 GB at one lane
(0.46–0.94 GB across workloads, higher under `--integrity` and on wider
projections), ~1.5 GB at four lanes; pgloader v4 ~0.6 GB. It's flat with table
size and scales with lane count - full per-scenario figures are the `peak_rss_mb`
column of `summary.tsv`, and the mechanism is in [Memory behavior](#memory-behavior).

pgloader is included on the `synthetic` and `sakila` rows as a familiar reference
point (a widely-used tool most people have run), so the numbers have something
recognizable to sit next to.

### 10M rows - single machine (reproducible companion)

One machine with the source + destination in Docker alongside Stratum (shared
CPU/disk): AMD Ryzen AI 9 HX 370 (12c/24t, Zen 5), 32 GB, Samsung PM9A1 NVMe,
Fedora 43; MySQL 8.0 / PostgreSQL 16 (`fsync` on). Stratum 0.1.0 native; pgloader
v4 native. Directional and fully reproducible with `./benchmarks/run.sh`. This is not
server hardware, and the co-located `localhost` path is why the absolute rates
run higher than the networked 100M reference above.

| workload | 1 lane | 1 lane +int | 4 lanes | 4 lanes +int | pgloader v4 |
|---|---|---|---|---|---|
| **synthetic** (like-for-like) | **542k** | 504k | **1.18M** | 876k | 256k |
| synthetic_heavy (~19 transforms) | 355k | 298k | — | — | — |
| plugin_rust (WASM transform) | 510k | 389k | — | — | — |
| plugin_js (QuickJS transform) | 128k | 116k | — | — | — |
| filter_rust (WASM filter) | 710k | 515k | — | — | — |
| filter_js (QuickJS filter) | 119k | 112k | — | — | — |
| reverse (PG -> MySQL) | 255k | — | — | — | — |
| sakila (many small tables) | 101k | 91k | — | — | 15.1k |

**Peak RSS:** ~0.5 GB at one lane on the plain copy (up to ~0.78 GB on the
transform/filter workloads, whose wider write rows draw a larger in-flight
window), ~1.5 GB at four lanes; pgloader v4 ~0.79 GB.

## What each configuration measures

The two tables share these configs. The qualitative findings hold on both runs;
the hardware moves only the absolute rates.

### synthetic - the like-for-like row

Both tools do identical work: create the table and copy every row (`orders` has
only a primary key, so there is no index-parity ambiguity). That makes this the
cleanest row to read the two side by side. Lanes scale sublinearly: past ~2
lanes the shared PostgreSQL ingest ceiling bounds the total, so lanes trade connections and memory
for throughput (each lane runs concurrently with its own in-flight window). Read
the 1-lane row as Stratum's out-of-the-box point and the 4-lane row as a tuned one.

### synthetic_heavy - expression-evaluation CPU

The same table projected through ~19 mixed computed columns (a few string
functions, several arithmetic expressions, date extraction, plus copied-through
columns). Transforms run in-flight: each row's computed columns are evaluated
inline as it streams (producer -> transform -> consumer -> COPY), with no extra I/O,
staging, or second read. So the drop from the plain copy is purely the added
per-row expression CPU: evaluating ~19 columns cuts throughput to about
two-thirds (355k vs 542k here; 251k vs 389k on the 100M run). `--integrity` costs
its usual fraction of a microsecond per row on top (about -16% here) - the
expression CPU does not fully hide the hashing.

### Plugins - Rust WASM vs JS (QuickJS)

Matched pairs: the `order_net` transform (`a * b`) called three times per row
through each runtime, on identical work (plus copied-through columns for a
realistic write width). Two findings hold on both runs:

- **Rust plugins are near-native**, within ~5-15% of the no-plugin rate despite
  three plugin calls per row.
- **JS plugins are interpreter-bound**: the ~4-6× gap is the QuickJS
  interpreter executing the guest code.

> **The boundary is batched.** Plugins are invoked once per batch: a whole batch crosses the WASM host<->guest boundary in a single call
> over a columnar binary wire, and the guest iterates the rows internally. That is
> why the native-Rust plugin sits close to the no-plugin rate instead of well
> below it. The remaining Rust cost is the actual `compute` work, and the
> remaining JS gap is the QuickJS interpreter, not per-row boundary overhead.

### Filters - the validation stage

The mirror of the plugin transforms on the validation stage: an 8-column
projection of `orders` where each row is checked by three `order_ok` filter calls
(every row passes, so the full pipeline runs). Same Rust-near-native /
JS-interpreter-floor result. The JS *filter* and JS *transform* land at
essentially the same rate regardless of stage (~120K/s at 10M, ~85K/s networked),
so that rate is the interpreter's floor, not a cost of the stage or the boundary. The
Rust filter posts a higher rate than the Rust transform because the narrower
8-column projection is cheaper to write.

### reverse - PostgreSQL -> MySQL

Stratum's MySQL write path alone (the `LOAD DATA` fast path into InnoDB);
pgloader loads *into* PostgreSQL, so it has no comparison row for a MySQL
destination. A single stream sustains roughly half Stratum's own PostgreSQL
COPY rate. The limit is InnoDB's always-clustered-index writes, the destination
engine's ceiling. Lanes apply here too (`orders` has an integer PK).

### sakila - many small tables (directional only)

The opposite of a throughput workload: 15 tables, ~46K rows, where fixed
per-table cost dominates. Stratum's `sakila.smql` fans the tables out into
independent pipelines run concurrently (`execution { parallel }`). **Not
scope-matched:** Stratum builds tables, primary keys, and secondary indexes, but a
fanned-out `tables = [...]` run does not recreate foreign keys (its independent
per-table pipelines have no cross-table ordering), while pgloader also builds the
18 foreign keys, so it does more work, and ~2 s of its wall is JVM startup + JIT.
Treat as directional; `synthetic` is the clean comparison.

### The cost of `--integrity`

Hashing every row and folding a Merkle receipt adds a small per-row cost - a
fraction of a microsecond, clustering around half a µs/row across these
single-run measurements - so the *percentage* overhead mostly tracks the baseline
speed rather than the workload. Per row, on the 10M run:

| workload | baseline | `--integrity` | delta | added per row |
|---|---|---|---|---|
| synthetic_filter_rust | 710k | 515k | -27% | +0.53 µs |
| synthetic_plugin_rust | 510k | 389k | -24% | +0.61 µs |
| synthetic_heavy | 355k | 298k | -16% | +0.54 µs |
| sakila | 101k | 91k | -10% | +1.08 µs |
| synthetic_plugin_js | 128k | 116k | -9% | +0.77 µs |
| synthetic | 542k | 504k | -7% | +0.14 µs |
| synthetic_filter_js | 119k | 112k | -5% | +0.46 µs |

On the fastest workload (a narrow projection through a Rust filter) that sub-µs
cost is about a quarter of the per-row budget, so it reads as -27%; on the JS
runs, where each row already costs ~8 µs in the interpreter, the same fraction of
a µs disappears into single-digit noise. These are single-run measurements
(`SYNTH_RUNS=1`), so the per-row column carries real spread - the plain
`synthetic` copy landed at -7% and `synthetic_heavy` at -16% this run - read them
as "sub-µs per row, low-to-mid double-digit percent on fast workloads," not exact
deltas. The same physics holds on the networked 100M run, where the disk under the
hash log matters: on a local-NVMe engine `--integrity` costs ~13% single-lane and
~29% at four lanes (four lanes write four hash streams at once).

Two components make up the per-row figure: hashing and keying each row as it
passes (overlapped with the pipeline, so often invisible), and a serial finalize
after the last row (sorting the keyed set and folding the Merkle root) that is
always on the clock. The row hashes are streamed to disk, roughly 51 bytes per
row (~510 MB for a 10M-row integer-PK table), so they cost disk, not memory.
`verify` is the other half of the cost, a separate command run later: re-reading
the destination, hashing every row, and diffing it against the receipt runs at
roughly the migration's own rate (a sequential read plus a hash per row). See
[verification.md](verification.md#storage-footprint).

## Reading the numbers honestly

- **Benchmark at scale.** Fixed startup (runtime boot, JVM JIT
  warmup, schema introspection) is ~1-2 s for both tools. Below a few million
  rows it dominates the wall clock and distorts the numbers. Use ≥10M rows.
- **Parallelism is a tuning axis.** Both write one COPY stream per table by default; Stratum's `lanes = N` is a tuned
  setting (the 4-lane row), pgloader splits a table via `concurrency`. Past ~2
  streams both sit on the shared PostgreSQL ingest ceiling, so a single stream is
  the out-of-the-box point and the multi-lane number is a tuned one.
- `--integrity` hashes every row and maintains Merkle receipts; the point of the
  separate column is that you see exactly what verification costs. It has no
  pgloader counterpart, so there's no pgloader figure for it.
- Peak RSS measures the migrating process only, not the databases.

## Memory behavior

Both tools stream and are bounded - neither holds the whole table, and neither's
footprint grows with table size. pgloader v4 (JVM) did 10M in ~0.79 GB with no
tuning, bounded by the JVM heap (`-Xmx`) and its `prefetch rows`.

Stratum holds only a bounded in-flight window: peak RSS is flat with table
size - the same at 10M as at 100M - but scales with lane count, since each
lane has its own window (≈0.5 GB at 1 lane -> ≈1.5 GB at 4 lanes on the sample
box). That is a deliberate trade of memory for parallelism.

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

**Separated hosts.** To reproduce the 100M reference run with the databases on
their own machines, set `EXTERNAL_DB=1` plus `MYSQL_HOST` / `PG_HOST` - the harness
then skips `compose` and talks to the databases over networked `mysql`/`psql`
clients (at `MYSQL_HOST:33307` / `PG_HOST:54329`).

pgloader is **opt-in** (`WITH_PGLOADER=1`) and only on the PostgreSQL-target
workloads. Set `PGLOADER_BIN` to measure a local pgloader natively (a v4 `.jar`
is run with `java -jar`); unset, it runs as Docker v4 built from
`Dockerfile.pgloader`. For a fair wall-clock run both tools the same way - both
native (`STRATUM_BIN` + `PGLOADER_BIN`) or both Docker; the harness warns when
they differ. Key env vars:

| Var | Purpose |
|---|---|
| `WITH_PGLOADER=1` | add pgloader on the PG-target workloads (off by default) |
| `PGLOADER_BIN` | local pgloader binary or `.jar`; unset -> Docker v4 image (`PGLOADER_IMAGE` / `PGLOADER_JAR_URL`) |
| `STRATUM_BIN` | Stratum binary; absent -> build and run from `Dockerfile.stratum` |
| `EXTERNAL_DB` / `MYSQL_HOST` / `PG_HOST` | target databases on other hosts instead of local compose |
| `PG_DEST_DB` / `MYSQL_DEST_DB` / `PG_SRC_DB` | destination / source database names |

The synthetic table is generated once (server-side, deterministic) and cached
in a Docker volume; only the first run at a given `BENCH_ROWS` pays the
generation cost. See [`benchmarks/README.md`](../benchmarks/README.md) for the
full knob list.

**Write encoding.** The PostgreSQL destination uses binary `COPY` by default
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
