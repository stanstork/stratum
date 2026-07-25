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
