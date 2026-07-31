# benchmarks/

Reproducible **Stratum** benchmark for MySQL <-> PostgreSQL bulk load. pgloader is
an optional comparison (see below). Methodology and published results:
[docs/benchmarks.md](../docs/benchmarks.md).

```bash
./benchmarks/run.sh                      # benchmark Stratum (100M-row synthetic; ~45 GB disk)
BENCH_ROWS=1000000 ./benchmarks/run.sh   # scaled-down run
WITH_PGLOADER=1 ./benchmarks/run.sh      # also compare against pgloader
./benchmarks/run.sh clean                # tear down bench containers, volumes, image
```

Prerequisites: Docker + compose v2, GNU time (`/usr/bin/time`). Docker alone is
enough - `Dockerfile.stratum` compiles Stratum inside a Rust builder stage. The
host Rust toolchain is needed only for the *native* path (to build
`target/release/stratum`).

## Stratum: binary or Docker

If a Stratum binary exists at `STRATUM_BIN` (default `target/release/stratum`) it
is measured natively; otherwise `run.sh` builds and runs it from
`Dockerfile.stratum`. So `cargo build --release -p cli` first for a native run,
or just run with nothing built to benchmark the Docker image.

## pgloader comparison (opt-in)

pgloader is **off by default** - the benchmark measures Stratum. Set
`WITH_PGLOADER=1` to add pgloader on the PostgreSQL-target workloads (`sakila`,
`synthetic`); it never runs on the MySQL-target `reverse` workload, since
pgloader only migrates *into* PostgreSQL.

`PGLOADER_BIN` measures a local pgloader natively; unset, pgloader runs in Docker
as **v4** (the JVM rewrite). v4 ships only as a JAR (needs Java 21+) with no
published image, so `run.sh` builds one from `Dockerfile.pgloader` - the JAR
comes from `PGLOADER_JAR_URL` (default: latest `v4-dev`). Point `PGLOADER_IMAGE`
at a prebuilt image (e.g. the old `dimitri/pgloader:latest` Lisp build) to pull
it as-is instead.

**Run both tools the same way for a fair wall-clock** - both native (set
`STRATUM_BIN` + `PGLOADER_BIN`) or both Docker (set neither). The harness warns
if they differ but won't force it. Peak RSS for a dockerized tool is sampled from
`docker stats` (~1-2s, approximate); run native for exact GNU-time RSS.

pgloader runs with **default tuning** (no `workers`/`concurrency`/batch/prefetch
options) - the numbers are its out-of-the-box behavior, not its ceiling; a tuned
pgloader would likely do better.

## Sakila scope note

The Sakila `pgloader` row is **not scope-matched** with Stratum. Stratum's
`sakila.smql` creates the destination tables + primary keys and copies every
row; on this workload pgloader also builds the secondary indexes and foreign
keys, so it does more work. Read the Sakila `pgloader` number as a full-schema
migration, not a like-for-like data copy. The **synthetic** workload (a single
table with only a primary key) is the like-for-like comparison.

## Reverse benchmark (PG -> MySQL, stratum only)

`RUN_REVERSE=1` (default) also runs a PostgreSQL -> MySQL load, reported in its
own `reverse` rows. pgloader migrates *into* PostgreSQL, so there is nothing to
compare it against for a MySQL destination - this is a stratum-only measurement
of the `LOAD DATA` write path. The PG source table is seeded once from
`synthetic/generate_pg.sql` (deterministic, cached like the MySQL source). Set
`RUN_REVERSE=0` to skip it.

## Plugin workloads (Rust vs JS WASM)

`synthetic_plugin_rust` and `synthetic_plugin_js` run the same one-column
transform (`net = amount * quantity`) through a WASM transform plugin - one
compiled from native Rust, one from JavaScript (QuickJS) - so the two plugin
runtimes are compared on identical per-row work. They are **Stratum-only** and
need **native Stratum** plus the host toolchain: the `wasm32-wasip1` target
(`rustup target add wasm32-wasip1`) and `npx` (Node.js). `run.sh` builds both
plugins into `plugins/build/` before the run; if native Stratum or the toolchain
is missing it logs a note and skips just these two workloads.

## Layout

| Path | Purpose |
|---|---|
| `run.sh` | the harness: builds, seeds, runs every scenario, validates row counts, writes the report |
| `compose.yml` | dedicated bench databases (MySQL 8.0 :33307, PostgreSQL 16 :54329) - isolated from the dev compose |
| `Dockerfile.stratum` | image built to run Stratum when no `STRATUM_BIN` is present |
| `Dockerfile.pgloader` | image built for docker-mode pgloader v4 (JVM rewrite, from its JAR) |
| `stratum/*.smql` | Stratum configs (credential-free; URLs injected via env); `synthetic_lanes.smql` (4-lane), `synthetic_heavy.smql` (~20 computed columns), `synthetic_plugin_{rust,js}.smql` (WASM transform plugin), `synthetic_reverse.smql` (PG->MySQL) |
| `plugins/` | transform plugins for the plugin workloads: `rust/order_net` (native -> wasm32), `js/order_net.js` (JS -> QuickJS wasm); `run.sh` builds both into `plugins/build/` |
| `pgloader/*.load.tpl` | pgloader configs (URLs substituted by `run.sh`) |
| `synthetic/` | deterministic generators: `generate_mysql.sql` (MySQL source), `generate_pg.sql` (PG source for the reverse run) |
| `results/<ts>/` | per-run output: `summary.md`, `summary.tsv`, `env.txt`, raw logs (gitignored) |

## Knobs (environment variables)

| Var | Default | Meaning |
|---|---|---|
| `BENCH_ROWS` | `100000000` | synthetic table size |
| `RUNS` | `3` | repetitions per Sakila scenario (median reported) |
| `SYNTH_RUNS` | `1` | repetitions per synthetic scenario |
| `WORKLOADS` | `sakila synthetic synthetic_heavy synthetic_plugin_rust synthetic_plugin_js` | forward (MySQL->PG) workloads; `synthetic_heavy` and the `synthetic_plugin_*` cases are Stratum-only |
| `TOOLS` | `stratum stratum-integrity stratum-lanes` | Stratum scenarios (`stratum-lanes` = 4 PK-range lanes, integer-PK tables only) |
| `WITH_PGLOADER` | `0` | also run pgloader on PG-target workloads (comparison) |
| `STRATUM_BIN` | `target/release/stratum` | Stratum binary; if it is absent, Stratum runs in Docker |
| `STRATUM_IMAGE` | `stratum-bench:local` | image tag built for docker-mode Stratum |
| `PGLOADER_BIN` | *(unset)* | local pgloader binary; unset -> Docker v4 image |
| `PGLOADER_IMAGE` | `pgloader-bench:v4` | built from `Dockerfile.pgloader`; set to a prebuilt image to pull instead |
| `PGLOADER_JAR_URL` | latest `v4-dev` JAR | pgloader v4 JAR baked into the built image |
| `RUN_REVERSE` | `1` | also run the PG->MySQL reverse benchmark (stratum only) |
| `REV_ROWS` | `$BENCH_ROWS` | row count for the reverse benchmark's PG source |
| `REV_RUNS` | `$SYNTH_RUNS` | repetitions for the reverse benchmark |
| `PG_DEST_DB` | `bench_dest` | PostgreSQL destination db (MySQL->PG workloads) |
| `MYSQL_DEST_DB` | `bench_rev` | MySQL destination db (PG->MySQL reverse) |
| `PG_SRC_DB` | `bench_src` | PostgreSQL source db seeded for the reverse |

Every run validates row counts source-vs-destination and aborts on mismatch -
a reported number always means the data actually arrived.

## MySQL server prerequisites for high-throughput loads

Loading *into* MySQL is bound by the server's InnoDB settings, not by Stratum.
These are the DBA's / operator's job (my.cnf, or a managed-DB parameter group on
RDS/Aurora/CloudSQL) - Stratum never changes server config, it only warns when a
setting will throttle the load. Provision them before a large migration:

| Setting | Why it matters | Guidance |
|---|---|---|
| `local_infile = 1` | LOAD DATA fast path; without it writes fall back to slow INSERT | required for bulk loads |
| `innodb_redo_log_capacity` | the 100 MB default stalls large loads on constant checkpoint flushing (measured **2-3x** slower) | size to the load, e.g. `4G` |
| `innodb_buffer_pool_size` | holds the working set / index in memory | ≥ the table's hot size |
| `innodb_doublewrite = 0` | removes 2x write amplification | **only** on throwaway/regenerable targets |
| `innodb_flush_log_at_trx_commit = 0/2` | relaxes per-commit fsync | a durability trade-off - the DBA's call |

Stratum's own levers (which it *does* control): the `LOAD DATA` fast path,
session-scoped `unique_checks`/`foreign_key_checks=0` on the write connection
(the standard bulk pattern, same as `mysqldump`), two-phase FK creation, and
`lanes` for parallel key-range writes. With the server settings above plus
`lanes=4`, a MySQL destination approaches Stratum's PostgreSQL binary-COPY rate
on the synthetic table (see [../docs/benchmarks.md](../docs/benchmarks.md) for
measured figures).
