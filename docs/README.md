# Stratum Documentation

## What is Stratum?
Stratum is a declarative data pipeline engine that safely migrates data and schema between databases with:

- **High Performance**: Parallel producer-consumer architecture with batching
- **Reliability**: Built-in checkpointing, retries, and circuit breakers
- **Declarative**: SMQL pipelines for data movement and schema migration
- **Type Safety**: Automatic schema inference and type coercion
- **Resumability**: Crash-safe with sled-backed state tracking

## Supported Connectors

**Sources:**
- MySQL
- PostgreSQL
- CSV files

**Destinations:**
- PostgreSQL (with COPY fast-path)

## Core Features

- DAG-based pipeline execution with parallel levels
- Schema migration: CREATE TABLE, indexes, foreign keys, sequences, ENUMs
- Snapshot migrations with cursor-based pagination (pk / numeric / timestamp)
- Field-level transformations and computed columns
- Row-level data validation
- Dead Letter Queue for failed rows
- Graceful shutdown (SIGINT/SIGTERM)
- Dry-run analysis (`plan` command)
- Automatic resume from checkpoints

## Architecture at a Glance

```
SMQL → ExecutionPlan → DAG Executor
                           ↓  (level by level, parallel within level)
                  PipelineOrchestrator
                      ↓           ↓
              Schema Ops      Data Pipeline
          (CREATE TABLE,    run_producer() → MPSC → run_consumer()
           indexes, FKs)         ↓                       ↓
                            Source DB             Destination DB
                                                  + SledStateStore
                                                    (checkpoints)
```

## Documentation

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Full crate map, layer breakdown, design decisions |
| [smql-reference.md](smql-reference.md) | SMQL v2.1 language reference with examples |
| [verification.md](verification.md) | Cryptographic verification — Merkle trees, proof storage, verify command |