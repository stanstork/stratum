# Stratum Documentation

## What is Stratum?
Stratum is a declarative data migration engine that moves data between databases with:

- **High Performance**: Parallel producer-consumer architecture with batching
- **Reliability**: Built-in checkpointing, retries, and circuit breakers
- **Declarative**: Simple SMQL syntax to define migrations
- **Type Safety**: Automatic schema inference and type coercion
- **Resumability**: Crash-safe with state tracking

## Supported Connectors

**Sources:**
- MySQL
- PostgreSQL  
- CSV files

**Destinations:**
- PostgreSQL (with COPY fast-path)

## Core Features

- Snapshot migrations with pagination
- Field-level transformations
- Computed columns
- Graceful shutdown (SIGINT/SIGTERM)
- Progress tracking
- Dry-run validation
- Automatic resume from checkpoints

## Architecture at a Glance


```
User → SMQL Parser → Migration Plan → Execution Engine
                                       ↓
                          [Producer] → MPSC → [Consumer]
                                 ↓               ↓
                          Source DB        Destination DB
```

See [Architecture Overview](architecture.md) for detailed diagrams.