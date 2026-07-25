#!/usr/bin/env bash
# Stratum vs pgloader benchmark harness.
#
#   ./benchmarks/run.sh              # full suite (needs ~45 GB free disk at 100M rows)
#   ./benchmarks/run.sh clean        # tear down bench containers + volumes
#
# Workloads
#   sakila     full Sakila DB, MySQL -> PostgreSQL: schema + data + indexes + FKs
#   synthetic  one deterministic ~200 B/row table (default 100M rows), MySQL -> PostgreSQL
#
# Tools measured per workload
#   stratum            stratum apply (release build)
#   stratum-integrity  stratum apply --integrity (hashing overhead made visible)
#   pgloader           native binary if installed, docker image otherwise
#
# Knobs (env vars)
#   BENCH_ROWS=100000000   synthetic table size (scale down for a smoke run)
#   RUNS=3                 repetitions per sakila scenario (median reported)
#   SYNTH_RUNS=1           repetitions per synthetic scenario
#   WORKLOADS="sakila synthetic"
#   TOOLS="stratum stratum-integrity pgloader"
#   STRATUM_BIN=...        skip the release build, use this binary
#   PGLOADER_IMAGE=dimitri/pgloader:latest   docker fallback image
#
# Results land in benchmarks/results/<timestamp>/ (summary.md, summary.tsv, logs).
# Methodology: ../docs/benchmarks.md
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$SCRIPT_DIR/compose.yml" -p stratum-bench)

MYSQL_CTR=stratum-bench-mysql
PG_CTR=stratum-bench-postgres
MYSQL_PORT=33307
PG_PORT=54329

BENCH_ROWS="${BENCH_ROWS:-100000000}"
RUNS="${RUNS:-3}"
SYNTH_RUNS="${SYNTH_RUNS:-1}"
WORKLOADS="${WORKLOADS:-sakila synthetic}"
TOOLS="${TOOLS:-stratum stratum-integrity pgloader}"
PGLOADER_IMAGE="${PGLOADER_IMAGE:-dimitri/pgloader:latest}"

SAKILA_TABLES=(actor address category city country customer film film_actor
    film_category inventory language payment rental staff store)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
log() { printf '\033[1m[bench]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[31m[bench] error:\033[0m %s\n' "$*" >&2; exit 1; }

mysql_root() { docker exec -i "$MYSQL_CTR" mysql -uroot -pbench --local-infile=1 2>/dev/null; }
mysql_scalar() { docker exec "$MYSQL_CTR" mysql -N -uroot -pbench -e "$1" 2>/dev/null; }
psql_admin() { docker exec "$PG_CTR" psql -U bench -d postgres -v ON_ERROR_STOP=1 -qAt -c "$1"; }
pg_scalar() { docker exec "$PG_CTR" psql -U bench -d "$1" -qAt -c "$2"; }

# "1:23.45" or "1:02:03" -> seconds (from GNU time -v output)
wall_to_seconds() {
    awk -F: '{ if (NF == 3) print $1*3600 + $2*60 + $3; else print $1*60 + $2 }' <<<"$1"
}

# Parse GNU `time -v` output file -> "wall_seconds max_rss_mb"
parse_time_v() {
    local wall rss
    wall=$(grep 'Elapsed (wall clock)' "$1" | awk '{print $NF}')
    rss=$(grep 'Maximum resident set size' "$1" | awk '{print $NF}')
    echo "$(wall_to_seconds "$wall") $(awk "BEGIN{printf \"%.1f\", $rss/1024}")"
}

# ---------------------------------------------------------------------------
# clean subcommand
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "clean" ]]; then
    "${COMPOSE[@]}" down -v
    log "bench containers and volumes removed"
    exit 0
fi

# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------
command -v docker >/dev/null || die "docker is required"
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
[[ -x /usr/bin/time ]] || die "GNU time (/usr/bin/time) is required"

STRATUM_BIN="${STRATUM_BIN:-$REPO_ROOT/target/release/stratum}"
if [[ ! -x "$STRATUM_BIN" ]]; then
    command -v cargo >/dev/null || die "cargo is required to build stratum (or set STRATUM_BIN)"
    log "building stratum (release)..."
    (cd "$REPO_ROOT" && cargo build --release -p cli)
fi
[[ -x "$STRATUM_BIN" ]] || die "stratum binary not found at $STRATUM_BIN"

PGLOADER_MODE=skip
if [[ " $TOOLS " == *" pgloader "* ]]; then
    if command -v pgloader >/dev/null; then
        PGLOADER_MODE=native
        log "pgloader: native ($(pgloader --version 2>/dev/null | head -1))"
    else
        PGLOADER_MODE=docker
        log "pgloader: not installed, using docker image $PGLOADER_IMAGE"
        docker image inspect "$PGLOADER_IMAGE" >/dev/null 2>&1 || docker pull "$PGLOADER_IMAGE"
    fi
fi

# ---------------------------------------------------------------------------
# databases up + seed
# ---------------------------------------------------------------------------
log "starting bench databases (mysql:$MYSQL_PORT, postgres:$PG_PORT)..."
"${COMPOSE[@]}" up -d --wait

# The compose user only gets grants on `sakila`; give it the synthetic db too.
mysql_scalar "CREATE DATABASE IF NOT EXISTS bench;
              GRANT ALL PRIVILEGES ON bench.* TO 'bench'@'%';
              ALTER USER 'bench'@'%' IDENTIFIED WITH mysql_native_password BY 'bench';
              FLUSH PRIVILEGES;" >/dev/null

# Generate (or reuse) the synthetic table. Deterministic, so an existing table
# with the right row count is byte-identical to a fresh one.
current_rows=$(mysql_scalar "SELECT COUNT(*) FROM bench.orders" || echo 0)
if [[ "$current_rows" != "$BENCH_ROWS" ]]; then
    log "generating bench.orders with $BENCH_ROWS rows (have: $current_rows). This is one-time and cached."
    mysql_root <"$SCRIPT_DIR/synthetic/schema.sql"
    mysql_root <"$SCRIPT_DIR/synthetic/generate.sql"
    gen_start=$(date +%s)
    echo "CALL bench.gen_orders($BENCH_ROWS);" | mysql_root
    log "generation took $(( $(date +%s) - gen_start ))s"
    [[ "$(mysql_scalar 'SELECT COUNT(*) FROM bench.orders')" == "$BENCH_ROWS" ]] \
        || die "generation produced wrong row count"
else
    log "reusing existing bench.orders ($current_rows rows)"
fi

sakila_rows=0
for t in "${SAKILA_TABLES[@]}"; do
    sakila_rows=$(( sakila_rows + $(mysql_scalar "SELECT COUNT(*) FROM sakila.$t") ))
done
log "source row counts: sakila=$sakila_rows synthetic=$BENCH_ROWS"

# ---------------------------------------------------------------------------
# results dir + environment capture
# ---------------------------------------------------------------------------
RESULTS="$SCRIPT_DIR/results/$(date -u +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS"
{
    echo "date_utc: $(date -u +%FT%TZ)"
    echo "commit: $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "stratum: $("$STRATUM_BIN" --version 2>/dev/null | head -1 || echo unknown)"
    case $PGLOADER_MODE in
        native) echo "pgloader: $(pgloader --version 2>/dev/null | head -1) (native)" ;;
        docker) echo "pgloader: $PGLOADER_IMAGE (docker)" ;;
        *) echo "pgloader: skipped" ;;
    esac
    echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
    echo "cores: $(nproc)"
    echo "mem_total: $(awk '/MemTotal/{printf "%.1f GB", $2/1048576}' /proc/meminfo)"
    echo "kernel: $(uname -sr)"
    echo "docker: $(docker --version)"
    echo "bench_rows: $BENCH_ROWS"
    echo "runs: sakila=$RUNS synthetic=$SYNTH_RUNS"
} | tee "$RESULTS/env.txt" >&2

TSV="$RESULTS/summary.tsv"
echo -e "workload\ttool\trun\twall_s\trows\trows_per_s\tpeak_rss_mb" >"$TSV"

# ---------------------------------------------------------------------------
# per-run plumbing
# ---------------------------------------------------------------------------
reset_dest_db() { # $1 = pg database name
    psql_admin "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null
    psql_admin "CREATE DATABASE $1 OWNER bench" >/dev/null
}

validate() { # $1 = workload, $2 = pg db
    local bad=0
    if [[ "$1" == sakila ]]; then
        for t in "${SAKILA_TABLES[@]}"; do
            local src dst
            src=$(mysql_scalar "SELECT COUNT(*) FROM sakila.$t")
            dst=$(pg_scalar "$2" "SELECT COUNT(*) FROM $t" || echo MISSING)
            [[ "$src" == "$dst" ]] || { log "  MISMATCH $t: src=$src dst=$dst"; bad=1; }
        done
    else
        local dst
        dst=$(pg_scalar "$2" "SELECT COUNT(*) FROM orders" || echo MISSING)
        [[ "$dst" == "$BENCH_ROWS" ]] || { log "  MISMATCH orders: src=$BENCH_ROWS dst=$dst"; bad=1; }
    fi
    return $bad
}

# Poll docker stats for a container's memory while it runs; track the peak.
# The running max is written to the outfile on every sample so the parent can
# kill this loop at any point without losing the measurement.
# Sampled (~2s resolution) - approximate, and noted as such in the report.
sample_docker_mem() { # $1 = container name, $2 = outfile
    local max=0 cur
    sleep 1 # let the container come up before the first poll
    while [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == true ]]; do
        cur=$(docker stats --no-stream --format '{{.MemUsage}}' "$1" 2>/dev/null) || break
        [[ -n "$cur" ]] || break
        cur=$(awk '{v=$1; if (v ~ /GiB/) m=1024; else if (v ~ /MiB/) m=1; else m=1/1024;
                    gsub(/[A-Za-z]/,"",v); printf "%.1f", v*m}' <<<"${cur%% /*}")
        max=$(awk "BEGIN{print ($cur > $max) ? $cur : $max}")
        echo "$max" >"$2"
        sleep 1
    done
}

run_stratum() { # $1 workload, $2 config, $3 pg db, $4 extra flags, $5 log prefix
    local run_home="$RESULTS/$5.home"
    mkdir -p "$run_home"
    local flags=()
    [[ -n "$4" ]] && flags=($4)
    env HOME="$run_home" \
        BENCH_SAKILA_MYSQL_URL="mysql://bench:bench@127.0.0.1:$MYSQL_PORT/sakila" \
        BENCH_SAKILA_PG_URL="postgres://bench:bench@127.0.0.1:$PG_PORT/$3" \
        BENCH_SYNTH_MYSQL_URL="mysql://bench:bench@127.0.0.1:$MYSQL_PORT/bench" \
        BENCH_SYNTH_PG_URL="postgres://bench:bench@127.0.0.1:$PG_PORT/$3" \
        /usr/bin/time -v -o "$RESULTS/$5.time" \
        "$STRATUM_BIN" apply -c "$2" "${flags[@]}" >"$RESULTS/$5.log" 2>&1 \
        || die "stratum run failed - see $RESULTS/$5.log"
    rm -rf "$run_home"
    parse_time_v "$RESULTS/$5.time"
}

run_pgloader() { # $1 load template, $2 pg db, $3 log prefix
    local loadfile="$RESULTS/$3.load"
    sed -e "s|@@SAKILA_MYSQL_URL@@|mysql://bench:bench@127.0.0.1:$MYSQL_PORT/sakila|" \
        -e "s|@@SAKILA_PG_URL@@|postgresql://bench:bench@127.0.0.1:$PG_PORT/$2|" \
        -e "s|@@SYNTH_MYSQL_URL@@|mysql://bench:bench@127.0.0.1:$MYSQL_PORT/bench|" \
        -e "s|@@SYNTH_PG_URL@@|postgresql://bench:bench@127.0.0.1:$PG_PORT/$2|" \
        "$1" >"$loadfile"

    if [[ $PGLOADER_MODE == native ]]; then
        /usr/bin/time -v -o "$RESULTS/$3.time" \
            pgloader "$loadfile" >"$RESULTS/$3.log" 2>&1 \
            || die "pgloader run failed - see $RESULTS/$3.log"
        parse_time_v "$RESULTS/$3.time"
    else
        local ctr=stratum-bench-pgloader memfile="$RESULTS/$3.mem" start end rc
        docker rm -f "$ctr" >/dev/null 2>&1 || true
        start=$(date +%s.%N)
        docker run --name "$ctr" --network host \
            -v "$RESULTS:/bench:ro,z" "$PGLOADER_IMAGE" \
            pgloader "/bench/$3.load" >"$RESULTS/$3.log" 2>&1 &
        local waiter=$!
        sample_docker_mem "$ctr" "$memfile" &
        local sampler=$!
        rc=0
        wait "$waiter" || rc=$?
        end=$(date +%s.%N)
        # Remove the container first: docker stats keeps answering for an
        # exited-but-present container, which would keep the sampler alive.
        docker rm -f "$ctr" >/dev/null 2>&1 || true
        kill "$sampler" 2>/dev/null || true
        wait "$sampler" 2>/dev/null || true
        [[ $rc -eq 0 ]] || die "pgloader run failed - see $RESULTS/$3.log"
        local mem
        mem=$(cat "$memfile" 2>/dev/null || echo 0)
        # A run shorter than the ~2s docker-stats sampling interval yields no
        # samples; report n/a rather than a misleading zero.
        [[ "$mem" == 0 ]] && mem="n/a"
        echo "$(awk "BEGIN{printf \"%.2f\", $end - $start}") $mem"
    fi
}

# ---------------------------------------------------------------------------
# main loop
# ---------------------------------------------------------------------------
for workload in $WORKLOADS; do
    case $workload in
        sakila) rows=$sakila_rows; dest_db=sakila_bench; n_runs=$RUNS ;;
        synthetic) rows=$BENCH_ROWS; dest_db=bench_dest; n_runs=$SYNTH_RUNS ;;
        *) die "unknown workload '$workload'" ;;
    esac

    for tool in $TOOLS; do
        [[ "$tool" == pgloader && $PGLOADER_MODE == skip ]] && continue
        for run in $(seq 1 "$n_runs"); do
            prefix="$workload-$tool-$run"
            log "[$prefix] resetting destination..."
            reset_dest_db "$dest_db"

            log "[$prefix] running..."
            case $tool in
                stratum)
                    out=$(run_stratum "$workload" "$SCRIPT_DIR/stratum/$workload.smql" "$dest_db" "" "$prefix") ;;
                stratum-integrity)
                    out=$(run_stratum "$workload" "$SCRIPT_DIR/stratum/$workload.smql" "$dest_db" "--integrity" "$prefix") ;;
                pgloader)
                    out=$(run_pgloader "$SCRIPT_DIR/pgloader/$workload.load.tpl" "$dest_db" "$prefix") ;;
                *) die "unknown tool '$tool'" ;;
            esac

            wall=$(cut -d' ' -f1 <<<"$out")
            rss=$(cut -d' ' -f2 <<<"$out")
            rps=$(awk "BEGIN{printf \"%.0f\", ($wall > 0) ? $rows / $wall : 0}")

            validate "$workload" "$dest_db" || die "[$prefix] row-count validation failed"
            log "[$prefix] wall=${wall}s rows/s=$rps peak_rss=${rss}MB - counts verified"
            echo -e "$workload\t$tool\t$run\t$wall\t$rows\t$rps\t$rss" >>"$TSV"
        done
    done
done

# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------
{
    echo "# Stratum vs pgloader - benchmark results"
    echo
    echo '```'
    cat "$RESULTS/env.txt"
    echo '```'
    echo
    echo "## Medians per scenario"
    echo
    echo "| workload | tool | runs | wall (s, median) | rows | rows/s (median) | peak RSS (MB, max) |"
    echo "|---|---|---|---|---|---|---|"
    awk -F'\t' 'NR>1 { key=$1 FS $2; n[key]++; rows[key]=$5;
                       walls[key]=walls[key] FS $4; rpss[key]=rpss[key] FS $6;
                       if ($7 == "n/a") rss[key]="n/a";
                       else if (rss[key] != "n/a" && $7+0 > rss[key]+0) rss[key]=$7 }
        function median(s,  a, m, i) { m=split(substr(s,2), a, FS);
            for (i=1;i<m;i++) for (j=i+1;j<=m;j++) if (a[j]+0<a[i]+0) {t=a[i];a[i]=a[j];a[j]=t}
            return (m%2) ? a[(m+1)/2] : (a[m/2]+a[m/2+1])/2 }
        END { for (k in n) { split(k, p, FS);
            printf "| %s | %s | %d | %.1f | %s | %.0f | %s |\n",
                p[1], p[2], n[k], median(walls[k]), rows[k], median(rpss[k]), rss[k] } }' \
        "$TSV" | sort
    echo
    echo "## All runs"
    echo
    echo "| workload | tool | run | wall (s) | rows | rows/s | peak RSS (MB) |"
    echo "|---|---|---|---|---|---|---|"
    awk -F'\t' 'NR>1 { printf "| %s | %s | %s | %s | %s | %s | %s |\n", $1,$2,$3,$4,$5,$6,$7 }' "$TSV"
    echo
    echo "Peak RSS for dockerized pgloader is sampled via \`docker stats\` (~1-2s"
    echo "resolution) and approximate; install pgloader natively for exact numbers."
} >"$RESULTS/summary.md"

log "done. results in $RESULTS"
echo
cat "$RESULTS/summary.md"
