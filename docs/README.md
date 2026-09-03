# Paganel Documentation

## What is Paganel?
Paganel is a declarative data pipeline engine that safely migrates data and schema between databases with:

- Parallel producer-consumer execution with batching
- Checkpointing, retries, and circuit breakers built in
- Declarative PPL pipelines for data movement and schema migration
- Automatic schema inference and type coercion
- Crash-safe resume via sled-backed state tracking

## Supported Connectors

**Sources:**
- MySQL
- PostgreSQL
- CSV files

**Destinations:**
- PostgreSQL (with COPY fast-path)
- MySQL (with LOAD DATA fast-path)

## Core Features

- DAG-based pipeline execution with parallel levels
- Schema migration: CREATE TABLE, indexes, foreign keys, sequences, ENUMs
- Snapshot migrations with cursor-based pagination (pk / numeric / timestamp)
- Field-level transformations and computed columns
- Row-level data validation
- Dead Letter Queue for failed rows
- WASM plugins (transform / filter / source / sink) in native Rust or JavaScript
- Graceful shutdown (SIGINT/SIGTERM)
- Dry-run analysis (`plan` command)
- Automatic resume from checkpoints

## Architecture at a Glance

```
PPL -> ExecutionPlan -> DAG Executor
                           ↓  (level by level, parallel within level)
                  PipelineOrchestrator
                      ↓           ↓
              Schema Ops      Data Pipeline
          (CREATE TABLE,    run_producer() -> MPSC -> run_consumer()
           indexes, FKs)         ↓                       ↓
                            Source DB             Destination DB
                                                  + SledStateStore
                                                    (checkpoints)
```

## Documentation

| Document | Description |
|----------|-------------|
| [plan.md](plan.md) | Reading `pag plan` - the summary layout, flags, sampling, and the magnitude bar |
| [output-modes.md](output-modes.md) | `apply` and `verify` output - default logs, `--pretty`, and the `--tui` dashboard with its controls |
| [architecture.md](architecture.md) | Full crate map, layer breakdown, design decisions |
| [ppl-reference.md](ppl-reference.md) | PPL language reference with examples |
| [verification.md](verification.md) | Cryptographic verification - Merkle trees, proof storage, verify command |
| [plugins/](plugins/README.md) | WASM plugins - roles, runtimes (native Rust / JS-QuickJS), authoring, CLI |