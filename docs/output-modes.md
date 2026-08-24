# Output Modes

How `stratum apply` and `stratum verify` report progress and results in the
terminal - and how to pick the mode that fits your context (CI, an interactive
terminal, or a live migration you want to watch).

---

## Table of Contents

- [apply: three ways to watch a migration](#apply-three-ways-to-watch-a-migration)
  - [Default (log) mode](#default-log-mode)
  - [`--pretty` mode](#--pretty-mode)
  - [`--tui` mode](#--tui-mode)
- [The TUI dashboard](#the-tui-dashboard)
  - [Estimated vs actual row counts](#estimated-vs-actual-row-counts)
  - [Keyboard controls](#keyboard-controls)
  - [Pause, resume, and cancel](#pause-resume-and-cancel)
  - [Responsive columns](#responsive-columns)
  - [Integrity finalization](#integrity-finalization)
  - [Resuming from a checkpoint](#resuming-from-a-checkpoint)
  - [Already-completed runs](#already-completed-runs)
- [verify output](#verify-output)
  - [Default output](#default-output)
  - [`--pretty` output](#--pretty-output)
- [Colors and `--no-color`](#colors-and---no-color)
- [Choosing a mode](#choosing-a-mode)

---

## apply: three ways to watch a migration

`apply` has three mutually exclusive output modes. Passing both `--tui` and
`--pretty` is an error.

| Mode | Flag | Best for |
|------|------|----------|
| Log (default) | *(none)* | CI, cron, redirected output, `RUST_LOG` debugging |
| Pretty | `--pretty` | An interactive terminal where you want readable, colored progress |
| TUI | `--tui` | Watching a live migration with per-pipeline progress and controls |

### Default (log) mode

With no flag, `apply` emits structured `tracing` logs and nothing else. This is
the mode for CI, cron, and anything that redirects output - it's line-oriented,
greppable, and honors `RUST_LOG` / `--log-level`.

```bash
stratum apply -c migration.smql
```

```
INFO executing migration config=migration.smql
INFO migration completed successfully
```

Turn up the detail with `-v` / `-vv` or `--log-level debug` for the full event
stream (per-batch progress, checkpoints, retries).

### `--pretty` mode

`--pretty` prints a colored, symbol-tagged line per event to stdout, each with an
elapsed-time stamp. It's meant for a human watching an interactive terminal.

```bash
stratum apply -c migration.smql --pretty
```

```
[  0.001s] ▶ Starting migration: run-1
[  0.002s] ◉ Pipeline 'migrate_actor' started (snapshot mode)
[  0.140s] -> migrate_actor 1,000 rows
[  1.320s] -> migrate_actor 500,000 rows, 12 skipped, 3 failed
[  2.500s] ✓ Pipeline 'migrate_actor' completed: 1,000,000 rows, 12 skipped, 3 failed in 2.50s (400,000/s)
[  2.505s] ✓ Migration completed!
   Total:      1,000,000 rows, 12 skipped, 3 failed
   Pipelines:  1
   Duration:   2.51s
   Throughput: 398,406/s
```

The line symbols: `▶` run start, `◉` pipeline start, `->` progress, `✓` success,
`✗` failure, `◆`/`⧗` integrity finalization (see below). Row counts and
throughput are thousands-separated.

In pretty mode the raw log stream is routed to `~/.stratum/pretty.log` instead of
the terminal, so the colored output stays clean. Tail that file if you need the
underlying logs.

### `--tui` mode

`--tui` takes over the terminal with a full-screen dashboard: a per-pipeline
table, an execution-stage map, and live aggregate panels for progress,
throughput, timing, and data volume.

```bash
stratum apply -c migration.smql --tui
```

See [The TUI dashboard](#the-tui-dashboard) for the full walkthrough.

---

## The TUI dashboard

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STRATUM  RUNNING                                                                  View: Overview │
│                                                                                                  │
│Pipeline          Status        Progress                        Rows          Rate        ETA     │
│> migrate_actor   ✔ Done        [████████████████████]  100%    200           --/s        0s      │
│  migrate_custom  ▶ Running     [███████████         ]   58%    8.0K/13.8K    --/s        --      │
│  migrate_orders  ○ Pending     [                    ]    0%    0/127.5K      --/s        --      │
│                                                                                                  │
│┌ Execution Stages ──────────────────────────────────────────────────────────────────────────────┐│
││Stage 0: migrate_actor ✓                                                                        ││
││Stage 1: migrate_customers  migrate_orders ●                                                    ││
│└────────────────────────────────────────────────────────────────────────────────────────────────┘│
│┌ Progress ─────────────┐┌ Throughput ──────────┐┌ Timing ───────────────┐┌ Data Volume ─────────┐│
││█░░░░░░░░░░░░░░░ 5.8%  ││Rate: 0/s             ││Elapsed: 0s            ││Volume: 0 B           ││
││1 / 3 pipelines        ││Peak: 0/s             ││ETA:     --            ││Rate:   0 B/s         ││
││Rows: 8.2K / 141.5K    ││                      ││                       ││Peak:   0 B/s         ││
│└───────────────────────┘└──────────────────────┘└───────────────────────┘└──────────────────────┘│
│                             [Q]uit  [Tab]View  [Space]Pause  [C]ancel                            │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Views** (cycle with `Tab`, or jump with `1`-`4`):

- **Overview** - the pipeline table plus the aggregate dashboard (shown above).
- **Pipeline Detail** - a single pipeline in depth; select it with `↑`/`↓` first.
- **Errors** - failed pipelines and their error messages.
- **Help** - the keyboard reference.

### Estimated vs actual row counts

The totals the dashboard shows progress *against* - the `/N` in the **Rows**
column, the denominator in the **Progress** panel, and every percentage and ETA
derived from them - are **estimates**. They come from the source's row-count
statistics gathered at plan time, not an exact `COUNT(*)`, so they can be off in
either direction (statistics lag; filtered or freshly-written tables are the
usual culprits). The processed-row counts are exact; only the target is an
estimate.

Two consequences to expect:

- A pipeline can reach or pass its estimated total before it actually finishes.
  The aggregate **Progress** bar caps at 100% so it never shows more than fully
  done, and a pipeline is marked **Completed** only when its producer genuinely
  finishes - not when it crosses the estimate.
- The final row count reported at completion is the real number written, which
  may differ from the estimate you watched during the run.

If you need an exact count-verified guarantee that the destination matches the
source, that's what [`stratum verify`](#verify-output) and `--integrity` are for -
the dashboard estimate is for progress feedback, not a correctness check.

### Keyboard controls

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select a pipeline |
| `Tab` | Switch views |
| `1`-`4` | Jump to Overview / Detail / Errors / Help |
| `Space` | Pause the migration (drains the current batch, then checkpoints) |
| `c` | Cancel the migration (asks to confirm) |
| `q` | Quit the app |

### Pause, resume, and cancel

These three are deliberately distinct:

- **Pause** (`Space`) is graceful. The in-flight batch is allowed to drain, a
  checkpoint is written, and the run is marked paused. Re-running the same
  command later continues from that checkpoint. The dashboard freezes its stats
  and shows a notice explaining how to resume.
- **Cancel** (`c`) stops the migration and *stays* in the dashboard so you can
  read the final state. Data already written is checkpointed, so a later re-run
  still resumes. Because it's destructive, it asks for confirmation (`[y]`/`[n]`).
- **Quit** (`q`) leaves the application. If a migration is still running it first
  requests a graceful stop ("Stopping…") before exiting.

There is no live resume or per-pipeline retry inside the TUI - resuming is always
"re-run the same command", which picks up from the last checkpoint.

### Responsive columns

The pipeline table adapts to terminal width. The **Pipeline** name and **Status**
columns are always present; secondary columns drop as space runs out:

| Column | Shown when width ≥ |
|--------|--------------------|
| Progress bar | 74 |
| Rate | 86 |
| ETA | 96 |

At an 80×24 terminal (the classic minimum) you keep the name, status, progress
bar, and rows; rate and ETA are dropped. Pipeline identity is never sacrificed.

The progress bars and row totals are measured against *estimated* source counts -
see [Estimated vs actual row counts](#estimated-vs-actual-row-counts) above.

### Integrity finalization

When you run with `--integrity`, each pipeline seals its keyed Merkle receipts
*after* its data is written. The TUI surfaces this as a modal so you don't mistake
the sealing pause for a hang, and so completion isn't declared prematurely:

```
┌────────────── ⧗ SEALING INTEGRITY RECEIPTS ──────────────┐
│                                                          │
│ Sealing 'orders'…                                        │
│                                                          │
│ Receipts committed:  2                                   │
│                                                          │
│ Committing verification receipts - please wait.          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

The status line reads **FINALIZING** while this runs. When sealing finishes the
modal switches to "✓ INTEGRITY RECEIPTS COMMITTED" and waits for you to press
`Enter` before showing the completion summary - the receipt count is real, not a
spinner, and the run only reports **COMPLETED** once every pipeline's receipts are
committed.

In `--pretty` mode the same phases print inline:

```
[  2.500s] ✓ Pipeline 'migrate_actor' completed: 1,000,000 rows in 2.50s (400,000/s)
[  2.505s] ◆ Finalizing integrity for 'migrate_actor': 2 tables
[  2.505s] ⧗ Sealing 'actor' (sorting & merging row hashes)…
[  3.100s] ✓ Receipt 'actor': 1,000,000 rows, root a3f1b2c4
[  3.101s] ✓ Migration completed!
```

Once receipts are committed, verify the destination against them with
[`stratum verify`](#verify-output). See [verification.md](verification.md) for
the cryptographic model.

### Resuming from a checkpoint

Checkpoints store the *cumulative* rows written across runs. When you re-run a
partially-completed migration, the TUI seeds each pipeline's progress from what
was already written - the bars start where the last run left off rather than
snapping back to zero, and pipelines that were already fully done show as
**Completed** immediately.

### Already-completed runs

If a run has already completed, `apply` doesn't reopen the dashboard (or re-run
the migration). It prints a short notice and exits successfully:

```
Migration for 'migration.smql' already completed.
```

The same guard applies to `--pretty` and default modes; the default mode logs it
as an `info` line. Use `stratum reset -c migration.smql` to clear the state and
run again from scratch.

---

## verify output

`verify` streams one line per table as it checks each pipeline's destination
against its stored receipt. It has two output modes.

### Default output

The default output keeps the `✓` / `✗` / `?` status markers - they're the
documented result glyphs - but adds no color, header, phase lines, or summary.
It's stable, greppable, and identical to what `--output <file>` writes to disk.

```bash
stratum verify -c migration.smql
```

```
✓ migrate_actor/actor - match (200 rows, root abababababababab, 45ms)
? migrate_film - no integrity receipt (run `apply --integrity` first)
```

A mismatch expands to the divergences (missing / changed / extra), capped with a
"… and N more" line:

```
✗ migrate_orders/orders - MISMATCH (0 missing, 1 changed, 0 extra; 127,491 rows expected, 127,491 found; 2,841ms)
  expected root abababababababab
  actual   root cdcdcdcdcdcdcdcd
  order_id=3412 - changed: expected abababababababab actual cdcdcdcdcdcdcdcd
```

**Marker meanings:** `✓` match · `✗` mismatch · `?` inconclusive or no receipt.

**Exit codes:** any mismatch exits non-zero; so does an *inconclusive* result
(the receipt exists but its row-hash log is missing or truncated), rather than
passing silently.

### `--pretty` output

`--pretty` adds a cyan header, per-table progress phases (`⧗ reading /
sorting / comparing`), color-coded result lines, and a final tally. The `✓`/`✗`/`?`
glyphs are the same - pretty mode layers decoration on top, it doesn't change the
result lines.

```bash
stratum verify -c migration.smql --pretty
```

```
◆ Verifying: migration.smql
⧗ migrate_actor/actor
    reading destination…
    sorting row hashes…
    comparing…
✓ migrate_actor/actor - match (200 rows, root abababababababab, 45ms)
? migrate_film - no integrity receipt (run `apply --integrity` first)
✓ 1 matched, 1 without a receipt
```

Like `apply --pretty`, this mode routes raw logs to `~/.stratum/pretty.log` to
keep the terminal output clean.

---

## Colors and `--no-color`

Color is only applied when **all** of these hold: the mode opts into it
(`--pretty`, or the TUI), `--no-color` is not set, and stdout is a TTY. Piping to
a file or another process therefore yields plain text automatically. The `✓`/`✗`/
`?` glyphs and Unicode symbols are not color - they remain in the default,
uncolored output.

---

## Choosing a mode

- **CI, cron, redirected output** -> default log mode (add `--log-level debug` to
  trace). `verify` default output is stable and diffable.
- **Watching an interactive run** -> `--pretty` for a readable scrolling log, or
  `--tui` for a live dashboard with controls.
- **Long or large migrations you want to steer** -> `--tui`, so you can pause and
  checkpoint (`Space`) or cancel (`c`) without losing written data.
- **Scripting `verify`** -> default output, or `--output report.txt` for the same
  text on disk; check the exit code for pass/fail/inconclusive.
