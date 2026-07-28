# benchmarks/

Reproducible Stratum vs pgloader benchmark. Methodology and published results:
[docs/benchmarks.md](../docs/benchmarks.md).

```bash
./benchmarks/run.sh                      # full suite (100M-row synthetic; ~45 GB disk)
BENCH_ROWS=1000000 ./benchmarks/run.sh   # scaled-down run
./benchmarks/run.sh clean                # tear down bench containers + volumes
```

Prerequisites: Docker + compose v2, GNU time (`/usr/bin/time`), Rust toolchain
(or `STRATUM_BIN`). pgloader: native binary if on `PATH`, Docker image
otherwise.

## Layout

| Path | Purpose |
|---|---|
| `run.sh` | the harness: builds, seeds, runs every scenario, validates row counts, writes the report |
| `compose.yml` | dedicated bench databases (MySQL 8.0 :33307, PostgreSQL 16 :54329) - isolated from the dev compose |
| `stratum/*.smql` | Stratum configs (credential-free; URLs injected via env) |
| `pgloader/*.load.tpl` | pgloader configs (URLs substituted by `run.sh`) |
| `synthetic/` | deterministic generator for the `orders` table |
| `results/<ts>/` | per-run output: `summary.md`, `summary.tsv`, `env.txt`, raw logs (gitignored) |

## Knobs (environment variables)

| Var | Default | Meaning |
|---|---|---|
| `BENCH_ROWS` | `100000000` | synthetic table size |
| `RUNS` | `3` | repetitions per Sakila scenario (median reported) |
| `SYNTH_RUNS` | `1` | repetitions per synthetic scenario |
| `WORKLOADS` | `sakila synthetic` | subset of workloads |
| `TOOLS` | `stratum stratum-integrity pgloader` | subset of tools |
| `STRATUM_BIN` | `target/release/stratum` | skip the build, use this binary |
| `PGLOADER_IMAGE` | `dimitri/pgloader:latest` | Docker fallback image |

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
`lanes=4`, a MySQL destination reaches parity with PostgreSQL binary COPY
(~400k rows/s on the synthetic table).
