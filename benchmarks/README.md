# benchmarks/

Reproducible Paganel benchmark for MySQL <-> PostgreSQL bulk load. pgloader is
an optional comparison (see below). Methodology and published results:
[docs/benchmarks.md](../docs/benchmarks.md).

```bash
./benchmarks/run.sh                      # benchmark Paganel (100M-row synthetic; ~45 GB disk)
BENCH_ROWS=1000000 ./benchmarks/run.sh   # scaled-down run
WITH_PGLOADER=1 ./benchmarks/run.sh      # also compare against pgloader
./benchmarks/run.sh clean                # tear down bench containers, volumes, image
```

Prerequisites: Docker + compose v2, GNU time (`/usr/bin/time`). Docker alone is
enough - `Dockerfile.paganel` compiles Paganel inside a Rust builder stage. The
host Rust toolchain is needed only for the *native* path (to build
`target/release/pag`).

## Paganel: binary or Docker

If a Paganel binary exists at `PAGANEL_BIN` (default `target/release/pag`) it
is measured natively; otherwise `run.sh` builds and runs it from
`Dockerfile.paganel`. So `cargo build --release -p cli` first for a native run,
or just run with nothing built to benchmark the Docker image.

## Separated hosts (external databases)

By default `run.sh` runs both databases in local Docker (`compose.yml`), sharing
one machine with the engine. To measure the engine over a real network - source
and destination on their own hosts - set `EXTERNAL_DB=1` and point it at them:

```bash
EXTERNAL_DB=1 MYSQL_HOST=10.0.0.11 PG_HOST=10.0.0.12 \
  WITH_PGLOADER=1 PGLOADER_BIN=~/pgloader.jar \
  ./benchmarks/run.sh
```

In this mode the harness skips `compose` and talks to the databases with
networked `mysql`/`psql` clients (at `MYSQL_HOST:33307` / `PG_HOST:54329`), so
those clients must be installed on the engine host. The databases must already be
running with the same names/credentials the local compose uses - bring each up
with `compose.yml` on its own host.

## pgloader comparison (opt-in)

pgloader is off by default - the benchmark measures Paganel. Set
`WITH_PGLOADER=1` to add pgloader on the PostgreSQL-target workloads (`sakila`,
`synthetic`); it never runs on the MySQL-target `reverse` workload, since
pgloader only migrates *into* PostgreSQL.

`PGLOADER_BIN` measures a local pgloader natively - either an executable or a v4
`.jar` (run with `java -jar`, so Java 21+ must be installed); unset, pgloader runs
in Docker as v4 (the JVM rewrite). v4 ships only as a JAR (needs Java 21+) with no
published image, so `run.sh` builds one from `Dockerfile.pgloader` - the JAR
comes from `PGLOADER_JAR_URL` (default: latest `v4-dev`). Point `PGLOADER_IMAGE`
at a prebuilt image (e.g. the old `dimitri/pgloader:latest` Lisp build) to pull
it as-is instead.

Run both tools the same way for a fair wall-clock: both native (set
`PAGANEL_BIN` + `PGLOADER_BIN`) or both Docker (set neither). The harness warns
if they differ but won't force it. Peak RSS for a dockerized tool is sampled from
`docker stats` (~1-2s, approximate); run native for exact GNU-time RSS.

pgloader runs with default tuning (no `workers`/`concurrency`/batch/prefetch
options), so the numbers are its out-of-the-box behavior; a tuned pgloader
would likely do better.

## Sakila scope note

The Sakila `pgloader` row is **not scope-matched** with Paganel. Paganel's
`sakila.ppl` creates the destination tables, primary keys, and secondary indexes
and copies every row; the one thing it does not build on this fanned-out run is
the foreign keys (its independent per-table pipelines have no cross-table
ordering), and pgloader does build those - so pgloader does a little more work.
Read the Sakila `pgloader` number as a full-schema migration, not a like-for-like
data copy. The `synthetic` workload (a single table with only a primary key) is
the like-for-like comparison.

## Reverse benchmark (PG -> MySQL, paganel only)

`RUN_REVERSE=1` (default) also runs a PostgreSQL -> MySQL load, reported in its
own `reverse` rows. pgloader migrates *into* PostgreSQL, so there is nothing to
compare it against for a MySQL destination - this is a paganel-only measurement
of the `LOAD DATA` write path. The PG source table is seeded once from
`synthetic/generate_pg.sql` (deterministic, cached like the MySQL source). Set
`RUN_REVERSE=0` to skip it.

## Plugin workloads (Rust vs JS WASM)

Four Paganel-only workloads, in two matched Rust-vs-JS pairs so the WASM runtimes
are compared on identical per-row work:

- **Transform** (`synthetic_plugin_rust`, `synthetic_plugin_js`) invoke the same
  `order_net` transform (`a * b`) three times per row (plus several
  copied-through source columns), through a WASM transform plugin.
- **Filter** (`synthetic_filter_rust`, `synthetic_filter_js`) validate an 8-column
  projection of `orders` with three `order_ok` filter calls per row through the
  validation stage (pass if non-negative; every row passes).

Each pair is one plugin compiled from native Rust and one from JavaScript
(QuickJS). All four are Paganel-only and need native Paganel plus the host
toolchain: the `wasm32-wasip1` target (`rustup target add wasm32-wasip1`) and
`npx` (Node.js). `run.sh` builds all four plugins into `plugins/build/` before the
run; if native Paganel or the toolchain is missing it logs a note and skips just
these workloads.

## Layout

| Path | Purpose |
|---|---|
| `run.sh` | the harness: builds, seeds, runs every scenario, validates row counts, writes the report |
| `compose.yml` | dedicated bench databases (MySQL 8.0 :33307, PostgreSQL 16 :54329) - isolated from the dev compose |
| `Dockerfile.paganel` | image built to run Paganel when no `PAGANEL_BIN` is present |
| `Dockerfile.pgloader` | image built for docker-mode pgloader v4 (JVM rewrite, from its JAR) |
| `paganel/*.ppl` | Paganel configs (credential-free; URLs injected via env); `synthetic_lanes.ppl` (4-lane), `synthetic_heavy.ppl` (~19 mixed computed/copied columns: some string fns + arithmetic + dates), `synthetic_plugin_{rust,js}.ppl` (WASM transform plugin, 3 calls/row), `synthetic_filter_{rust,js}.ppl` (WASM filter plugin, 3 calls/row), `synthetic_reverse.ppl` (PG->MySQL) |
| `plugins/` | WASM plugins for the plugin workloads: `rust/order_net` + `js/order_net.js` (transform, `a * b`), `rust/order_ok` + `js/order_ok.js` (filter, non-negative check); `run.sh` builds all four into `plugins/build/` |
| `pgloader/*.load.tpl` | pgloader configs (URLs substituted by `run.sh`) |
| `synthetic/` | deterministic generators: `generate_mysql.sql` (MySQL source), `generate_pg.sql` (PG source for the reverse run) |
| `results/<ts>/` | per-run output: `summary.md`, `summary.tsv` (with a **state on disk (MB)** column - the integrity row-hash store's footprint), `env.txt`, raw logs (gitignored) |

## Knobs (environment variables)

| Var | Default | Meaning |
|---|---|---|
| `BENCH_ROWS` | `100000000` | synthetic table size |
| `RUNS` | `3` | repetitions per Sakila scenario (median reported) |
| `SYNTH_RUNS` | `1` | repetitions per synthetic scenario |
| `WORKLOADS` | `sakila synthetic synthetic_heavy synthetic_plugin_rust synthetic_plugin_js synthetic_filter_rust synthetic_filter_js` | forward (MySQL->PG) workloads; `synthetic_heavy`, the `synthetic_plugin_*`, and the `synthetic_filter_*` cases are Paganel-only |
| `TOOLS` | `paganel paganel-integrity paganel-lanes paganel-lanes-integrity` | Paganel scenarios: `paganel-integrity` adds `--integrity`; `paganel-lanes` = 4 PK-range lanes (integer-PK tables only); `paganel-lanes-integrity` = both |
| `WITH_PGLOADER` | `0` | also run pgloader on PG-target workloads (comparison) |
| `PAGANEL_BIN` | `target/release/pag` | Paganel binary; if it is absent, Paganel runs in Docker |
| `PAGANEL_IMAGE` | `paganel-bench:local` | image tag built for docker-mode Paganel |
| `PGLOADER_BIN` | *(unset)* | local pgloader binary or v4 `.jar` (run with `java -jar`); unset -> Docker v4 image |
| `PGLOADER_IMAGE` | `pgloader-bench:v4` | built from `Dockerfile.pgloader`; set to a prebuilt image to pull instead |
| `PGLOADER_JAR_URL` | latest `v4-dev` JAR | pgloader v4 JAR baked into the built image |
| `RUN_REVERSE` | `1` | also run the PG->MySQL reverse benchmark (paganel only) |
| `REV_ROWS` | `$BENCH_ROWS` | row count for the reverse benchmark's PG source |
| `REV_RUNS` | `$SYNTH_RUNS` | repetitions for the reverse benchmark |
| `PG_DEST_DB` | `bench_dest` | PostgreSQL destination db (MySQL->PG workloads) |
| `MYSQL_DEST_DB` | `bench_rev` | MySQL destination db (PG->MySQL reverse) |
| `PG_SRC_DB` | `bench_src` | PostgreSQL source db seeded for the reverse |
| `EXTERNAL_DB` | `0` | databases are external: skip `compose`, use networked `mysql`/`psql` clients (see [Separated hosts](#separated-hosts-external-databases)) |
| `MYSQL_HOST` / `PG_HOST` | `127.0.0.1` | database hosts, used when `EXTERNAL_DB=1` |
| `KEEP_STATE` | `0` | keep each run's `$HOME/.paganel` instead of deleting it, to inspect the integrity row-hash store on disk |

Every run validates row counts source-vs-destination and aborts on mismatch -
a reported number always means the data actually arrived.

## MySQL server prerequisites for high-throughput loads

Loading *into* MySQL is bound by the server's InnoDB settings, not by Paganel.
These are the DBA's / operator's job (my.cnf, or a managed-DB parameter group on
RDS/Aurora/CloudSQL) - Paganel never changes server config, it only warns when a
setting will throttle the load. Provision them before a large migration:

| Setting | Why it matters | Guidance |
|---|---|---|
| `local_infile = 1` | LOAD DATA fast path; without it writes fall back to slow INSERT | required for bulk loads |
| `innodb_redo_log_capacity` | the 100 MB default stalls large loads on constant checkpoint flushing (measured **2-3x** slower) | size to the load, e.g. `4G` |
| `innodb_buffer_pool_size` | holds the working set / index in memory | ≥ the table's hot size |
| `innodb_doublewrite = 0` | removes 2x write amplification | **only** on throwaway/regenerable targets |
| `innodb_flush_log_at_trx_commit = 0/2` | relaxes per-commit fsync | a durability trade-off - the DBA's call |

Paganel's own levers (which it *does* control): the `LOAD DATA` fast path,
session-scoped `unique_checks`/`foreign_key_checks=0` on the write connection
(the standard bulk pattern, same as `mysqldump`), two-phase FK creation, and
`lanes` for parallel key-range writes. With the server settings above plus
`lanes=4`, a MySQL destination approaches Paganel's PostgreSQL binary-COPY rate
on the synthetic table (see [../docs/benchmarks.md](../docs/benchmarks.md) for
measured figures).
