# Cryptographic Migration Verification

Merkle Tree-Based Post-Migration Integrity Verification

---

## Table of Contents

- [What Verification Proves](#what-verification-proves)
- [What Verification Does Not Prove](#what-verification-does-not-prove)
- [Architecture](#architecture)
- [Core Data Structures](#core-data-structures)
- [Canonical Row Serialization](#canonical-row-serialization)
- [Merkle Tree Construction](#merkle-tree-construction)
- [State Persistence](#state-persistence)
- [Verification Process](#verification-process)
- [CLI Usage](#cli-usage)
- [Cascade Pipelines](#cascade-pipelines)
- [Edge Cases](#edge-cases)
- [Performance](#performance)
- [Future Extensions](#future-extensions)

---

## What Verification Proves

**The destination contains exactly the data that was written during migration.**

Conventional migration verification relies on row count comparison. Matching counts do not guarantee matching data: silent corruption, partial writes, network-level bit flips, and OOM kills mid-batch can all produce a destination with the correct row count but incorrect data.

Stratum's cryptographic verification hashes each post-transform row, organizes hashes into a Merkle tree per batch, and persists the batch roots to sled on pipeline completion. The `verify` command re-reads the destination, recomputes the same structure, and compares. When roots differ, batch-level comparison isolates the failing batch — then row-level comparison (when available) identifies the exact divergent rows.

This detects:

- Rows deleted from the destination after migration
- Rows modified in the destination after migration
- Rows inserted into the destination after migration (via sentinel fetch)
- Rows silently dropped or corrupted during the write phase

## What Verification Does Not Prove

**Transform correctness is a unit and integration testing concern, not a cryptographic verification concern.**

The hash is computed over post-transform output. Whether `lower(trim(email))` does the right thing, whether a `when` expression maps tiers correctly — that is validated by tests. Verification checks that the destination matches what was written, not whether what was written is semantically correct.

Verification also does not prove that the correct rows were selected from the source. If a `where` filter was wrong and selected the wrong rows, the destination hash will still match the stored receipt — because the receipt was computed from whatever was written.

**Source-to-destination comparison** (proving "destination rows match source rows through the transform") is out of scope and deferred as a future extension.

---

## Architecture

### Integration Point in the Pipeline

Hashing runs inside `BatchCoordinator` (`IntegrityState`), after `TransformService` applies all field mappings, computed columns, and type coercions, and before the batch is sent to the consumer channel.

```
Source (SnapshotReader)
  -> TransformService (field mapping, computed columns, coercions)
  -> IntegrityState::hash_batch()        ← hash rows, accumulate batch roots
  -> BatchCoordinator (send batch to consumer)
  ↓ MPSC channel
Sink (BatchWriter)
  -> StateManager (checkpoint)
```

On pipeline completion, `IntegrityState::save_receipts()` builds the final `VerificationReceipt` per table and persists it to sled.

### Crate Responsibilities

| Crate                 | Components                                                                                                                                | Responsibility                                                                           |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `model`             | `HashAlgorithm`, `RowHasher`, `MerkleTree`, `VerificationReceipt`, `VerificationResult`, `IntegrityConfig`, `IntegrityMode` | Core data structures and hashing logic. No I/O.                                          |
| `engine-processing` | `IntegrityState`, `BatchCoordinator`                                                                                                  | Hash rows per batch, accumulate roots, persist receipt at completion.                    |
| `engine-state`      | `MerkleStore` trait, `SledStateStore`                                                                                                 | Persist and load `VerificationReceipt` from sled. Key: `receipt:{pipeline}:{table}`. |
| `engine-verify`     | `verify()`, `read_and_hash_batched()`, `find_divergent_batches()`, `find_divergent_rows()`                                        | Re-read destination, compare against receipt, build `VerificationResult`.              |
| `cli`               | `apply --integrity`, `apply --full-integrity`, `verify`                                                                             | Map CLI flags to `IntegrityMode`, invoke `engine-verify`, print or write results.    |

### Data Flow: Migration (Write Path)

1. Producer reads a page of rows from source.
2. `TransformService` applies transforms (field mapping, computed columns, coercions).
3. `IntegrityState::hash_batch()` groups rows by destination table, hashes each row canonically, and accumulates:
   - **Primary table**: one subtree root per batch (hash of row hashes), plus row count.
   - **Cascade tables**: a deduplicated `HashSet` of row hashes (order-independent).
   - If `--full-integrity`: individual row hashes stored flat for later row-level diff.
4. Batch is sent through the MPSC channel to the consumer.
5. Consumer writes to destination.
6. After all batches: `IntegrityState::save_receipts()` finalizes and persists one `VerificationReceipt` per table.

### Data Flow: Verification (Read-Back Path)

1. Load `VerificationReceipt` from sled for each pipeline/table pair.
2. Re-read destination rows using the same offset strategy as the original migration.
3. For each batch: recompute subtree root; compare against `receipt.batch_roots[i]`.
4. After all batches: sentinel fetch detects rows inserted beyond the receipt boundary.
5. For divergent batches: compare row-by-row if receipt carries `row_hashes`.
6. Return `Vec<VerificationResult>` — one result per table.

### Diagram

```
                          apply --integrity
  ╔══════════════════════════════════════════════════════════════════╗
  ║  SOURCE          TRANSFORM         HASH              WRITE       ║
  ║                                                                  ║
  ║  ┌──────┐   ┌─────────────────┐                   ┌────────────┐ ║
  ║  │MySQL │─▶│ TransformService│                   │ BatchWriter│ ║
  ║  └──────┘   └────────┬────────┘                   └────────────┘ ║
  ║                      │  rows[]                         ▲         ║
  ║             ┌────────▼─────────┐    Batch{}            │         ║
  ║             │  IntegrityState  │─────────────────────▶│         ║
  ║             │  hash_batch()    │                                 ║
  ║             └─────────┬────────┘                                 ║
  ║  Batch 0              │  h0  h1  h2  ... hN                      ║
  ║                       │  └────────────────▶ root0  ────┐        ║
  ║  Batch 1              │  h0  h1  h2  ... hN             │        ║
  ║                       │  └────────────────▶ root1  ────┤        ║
  ║  Batch 2              │  h0  h1  h2  ... hN             │        ║
  ║                       │  └────────────────▶ root2  ────┤        ║
  ║                       │                                 ▼        ║
  ║                       │                    MerkleTree::root      ║
  ║                       │                    ┌──────────────────┐  ║
  ║                       └──────────────────▶│ VerifReceipt     │  ║
  ║                                            │  table_root      │  ║
  ║                                            │  batch_roots[]   │  ║
  ║                                            │  rows_per_batch[]│  ║
  ║                                            │  column_order[]  │  ║
  ║                                            │  row_hashes[]?   │  ║
  ║                                            └────────┬─────────┘  ║
  ║                                                     │            ║
  ╚═════════════════════════════════════════════════════╪════════════╝
                                                        │ sled
  ╔═════════════════════════════════════════════════════╪════════════╗
  ║  verify                                             │            ║
  ║                                                     ▼            ║
  ║  ┌────────────┐   rows[]  ┌────────────────┐  VerifReceipt       ║
  ║  │ Destination│─────────▶│ read_and_hash  │◀───────────────────║
  ║  │  (re-read) │           │   _batched()   │                     ║
  ║  └────────────┘           └───────┬────────┘                     ║
  ║       ▲                           │                              ║
  ║  sentinel                         │ actual_roots[]               ║
  ║  (extra row                       ▼                              ║
  ║   past boundary)    ┌──────────────────────────┐                 ║
  ║                     │  find_divergent_batches()│                 ║
  ║                     │  receipt.batch_roots[i]  │                 ║
  ║                     │  vs actual_roots[i]      │                 ║
  ║                     └────────────┬─────────────┘                 ║
  ║                                  │ divergent batches             ║
  ║                                  ▼                               ║
  ║                     ┌─────────────────────────┐                  ║
  ║                     │  find_divergent_rows()  │◀── row_hashes[] ║
  ║                     │  (--full-integrity only)│    from receipt  ║
  ║                     └────────────┬────────────┘                  ║
  ║                                  │                               ║
  ║                                  ▼                               ║
  ║                          VerificationResult                      ║
  ║                     Match | Mismatch | NoPriorRun                ║
  ╚══════════════════════════════════════════════════════════════════╝
```

---

## Core Data Structures

### `IntegrityMode`

```rust
pub enum IntegrityMode {
    /// No hashing. Zero overhead. No receipt written.
    Off,
    /// Compute Merkle batch roots and write a VerificationReceipt.
    /// Corresponds to `apply --integrity`.
    BatchHashes,
    /// BatchHashes plus individual row hashes stored in the receipt.
    /// Enables row-level divergence reporting. ~32 bytes/row additional storage.
    /// Corresponds to `apply --full-integrity`.
    FullHashes,
}
```

### `VerificationReceipt`

Written to sled at pipeline completion. Key: `receipt:{pipeline_name}:{table_name}` — stable across runs so each new `apply --integrity` overwrites the previous receipt.

```rust
pub struct VerificationReceipt {
    pub run_id: String,
    pub pipeline_name: String,
    pub table_name: String,
    /// Root of the Merkle tree over all batch roots.
    pub table_root: [u8; 32],
    /// One subtree root per batch, in write order.
    pub batch_roots: Vec<[u8; 32]>,
    /// Lexicographically sorted destination column names.
    /// Embedded in the receipt so verify is self-contained.
    pub column_order: Vec<String>,
    /// Rows hashed and written (excludes DLQ skips).
    pub total_rows: u64,
    /// Rows sent to DLQ - not present in destination.
    pub skipped_rows: u64,
    /// Rows written per batch. Used by verify to reproduce exact batch boundaries.
    pub rows_per_batch: Vec<u64>,
    /// When true: row hashes were sorted before building the tree.
    /// Set for cascade tables (rows arrive out of PK order during migration).
    pub sorted_hashes: bool,
    pub algorithm: HashAlgorithm,
    pub created_at: DateTime<Utc>,
    /// Individual row hashes for row-level diff. Present only with `--full-integrity`.
    pub row_hashes: Option<Vec<[u8; 32]>>,
}
```

`column_order` is embedded so that verify can reproduce the canonical encoding without re-introspecting the destination schema.

`rows_per_batch` is critical: verify uses it to re-read the destination in exactly the same batch sizes as the migration used, ensuring batch boundaries align perfectly.

### `VerificationResult`

```rust
pub enum VerificationResult {
    Match {
        receipt: VerificationReceipt,
        duration_ms: u64,
    },
    Mismatch {
        receipt: VerificationReceipt,
        actual_root: [u8; 32],
        divergent_batches: Vec<DivergentBatch>,
        duration_ms: u64,
    },
    /// Receipt not found - pipeline was run without --integrity.
    NoPriorRun { pipeline_name: String },
}

pub struct DivergentBatch {
    pub batch_index: u64,
    pub expected_root: [u8; 32],
    pub actual_root: [u8; 32],
    pub row_start: u64,  // inclusive
    pub row_end: u64,    // exclusive
    /// Populated only when receipt was written with --full-integrity.
    pub divergent_rows: Vec<DivergentRow>,
}

pub struct DivergentRow {
    /// Zero-based index across the whole table (not within the batch).
    pub row_index: u64,
    pub expected_hash: [u8; 32],
    pub actual_hash: [u8; 32],
}
```

### `IntegrityConfig`

Constructed from `IntegrityMode` in the pipeline orchestrator and passed to `BatchCoordinator`.

```rust
pub struct IntegrityConfig {
    pub algorithm: HashAlgorithm,
    /// table_name -> sorted destination column names.
    pub tables: HashMap<String, Vec<String>>,
    /// The primary (root) destination table.
    /// Primary table rows are hashed in insertion order.
    /// All other tables are treated as cascade tables.
    pub primary_table: String,
    /// When true, store individual row hashes in the receipt.
    pub store_row_hashes: bool,
    /// Destination column types for coercion at hash time.
    /// table_name -> column_name -> pg_type_string.
    pub column_types: HashMap<String, HashMap<String, String>>,
}
```

---

## Canonical Row Serialization

The same row must always produce the same byte sequence regardless of whether it is read from the producer transform output or from a destination `SELECT`. `RowHasher` in `model` implements this.

### Encoding Protocol

For a given row, using `column_order` from `VerificationReceipt`:

```
for each column_name in column_order (lexicographic order):
    look up field in record by name
    if field not found: treat as Null

    write 1-byte type tag + value bytes
```

| Type               | Tag      | Encoding                                                |
| ------------------ | -------- | ------------------------------------------------------- |
| `Null` / missing | `0x00` | no body                                                 |
| `Int(i)`         | `0x01` | 8-byte little-endian i64                                |
| `UInt(u)`        | `0x02` | 8-byte little-endian u64                                |
| `Boolean(b)`     | `0x03` | 1 byte:`0x00` = false, `0x01` = true                |
| `String(s)`      | `0x10` | 4-byte LE length + UTF-8 bytes                          |
| `Decimal(d)`     | `0x11` | `"{mantissa}:{scale}"` ASCII, 4-byte LE length prefix |
| `Float(f)`       | `0x12` | 8-byte big-endian IEEE 754 (NaN ->`0x00` Null tag)    |
| `Date(d)`        | `0x20` | 4-byte LE signed days since Unix epoch                  |
| `Timestamp(ts)`  | `0x21` | 8-byte LE microseconds since Unix epoch, UTC            |
| `Uuid(u)`        | `0x30` | 16 bytes big-endian                                     |
| `Binary(b)`      | `0x40` | 4-byte LE length + raw bytes                            |
| `Json(j)`        | `0x50` | canonical JSON (sorted keys), 4-byte LE length prefix   |
| `Array(a)`       | `0x60` | 4-byte LE element count + recursively encoded elements  |
| `Enum { value }` | `0x70` | string value only — no type name                       |

### Column Order

`column_order` is the list of destination column names sorted lexicographically, established at migration start and stored in the receipt. Both the write path and verify path use this exact list. This decouples the hash from result-set ordering, which varies by driver.

### Value Coercions

Some values must be coerced at hash time to match what the COPY writer actually writes. The coercion lives in `IntegrityState::hash_batch()` via `coerce_row_for_hash`:

- **String written to an array column** (`TEXT[]`, `SET`): split on comma into `Value::Array`. This mirrors the `parse_array_string` fallback in the COPY writer.

Column types are passed in `IntegrityConfig.column_types` and used on both the write path (migration) and the read path (verify re-hashes destination rows with the same coercions applied in `TableReader`).

### Special Cases

- **NaN float**: Encoded as `0x00` (Null). NaN has undefined equality semantics.
- **Missing column**: Treated as Null. Handles nullable columns absent from a record.
- **Timestamp timezones**: Normalized to UTC before encoding.
- **Enum type names**: String value only. MySQL `ENUM` -> PostgreSQL `VARCHAR` produces identical bytes.

---

## Merkle Tree Construction

### Batch-Level Tree (Write Path)

For each batch of N post-transform rows, `MerkleTree::root_from_hashes()` computes a subtree root from the row hashes. Only the root is kept — no intermediate nodes are retained. Memory is O(N) for the leaf hashes during root computation, then discarded.

```
Row hashes: h0  h1  h2  h3  h4
              \  /    \  /    ↑ promoted (odd node)
               b0      b1    h4
                 \    /    /
                  c0      c1
                    \    /
                     Root
```

Odd last node is **promoted without hashing** to avoid second-preimage vulnerabilities from duplicate-leaf strategies.

### Table-Level Root

After all batches, `MerkleTree::root_from_hashes()` combines all batch subtree roots (in order) into the table-level root stored in `receipt.table_root`.

### Cascade Tables

Cascade tables (FK-referenced tables pulled in via `with references { data = cascade }`) receive rows from multiple source batches in non-PK order. A `HashSet` deduplicates row hashes across all batches. At finalization, the set is sorted and a single Merkle root is computed. `receipt.sorted_hashes = true` tells verify to apply the same sort before building its tree — making the comparison order-independent.

---

## State Persistence

### Receipt Key

```
receipt:{pipeline_name}:{table_name}
```

Stored in a dedicated sled tree (`MerkleStore`), separate from the main checkpoint tree. Each `apply --integrity` run overwrites the previous receipt for the same pipeline/table pair. The key is stable across run IDs so that `verify` always compares against the most recent migration.

### What Is Stored

One `VerificationReceipt` per destination table per pipeline. For a cascade pipeline touching 5 tables, 5 receipts are written. Cascade table receipts always have `row_hashes: None` (sorted-hash mode; row-level diff is not meaningful for out-of-order rows).

### `MerkleStore` Trait

```rust
pub trait MerkleStore: Send + Sync {
    async fn save_receipt(&self, receipt: &VerificationReceipt) -> Result<(), StateStoreError>;
    async fn load_receipt(&self, pipeline_name: &str, table_name: &str)
        -> Result<Option<VerificationReceipt>, StateStoreError>;
    async fn list_receipts(&self) -> Result<Vec<VerificationReceipt>, StateStoreError>;
}
```

---

## Verification Process

### `verify()` Function

Lives in `engine-verify`. Takes an `ExecutionPlan` and returns `Vec<VerificationResult>` — one result per (pipeline, table) pair. Has no dependency on `engine-runtime`.

```rust
pub async fn verify(
    plan: ExecutionPlan,
    env: Arc<EnvContext>,
) -> Result<Vec<VerificationResult>, VerifyError>
```

### Batch Comparison

For each table, verify reads the destination using the same offset strategy as the original migration. The fetch size for each batch is taken from `receipt.rows_per_batch[i]` so boundaries align exactly.

```
for i in 0..receipt.batch_roots.len():
    rows = reader.next_batch(cursor, receipt.rows_per_batch[i])
    actual_root = MerkleTree::root_from_hashes(row_hashes, receipt.algorithm)
    if actual_root != receipt.batch_roots[i]:
        divergent_batches.push(DivergentBatch { ... })
```

### Sentinel Fetch

After the batch loop, one extra row is fetched past the last receipt cursor position. A non-empty result means rows were inserted into the destination after `apply` completed. The sentinel pushes a dummy `[0xff; 32]` root that causes a batch count mismatch, which is reported as a `Mismatch`.

### Row-Level Diff

When a batch is divergent and the receipt was written with `--full-integrity` (i.e., `receipt.row_hashes` is `Some`), `find_divergent_rows()` compares the stored per-row hashes against the recomputed ones:

```rust
fn find_divergent_rows(
    row_start: u64,
    row_end: u64,
    receipt_row_hashes: &[[u8; 32]],
    actual_row_hashes: &[[u8; 32]],
) -> Vec<DivergentRow>
```

Each `DivergentRow` carries the zero-based row index and both hashes. Without `--full-integrity`, `divergent_rows` is empty and only the batch range (`row_start`–`row_end`) is reported.

### Batch Count Mismatch

If `actual_batch_roots.len() != receipt.batch_roots.len()` (different number of rows than expected), the result is immediately a `Mismatch` with a single `DivergentBatch` covering the whole table. Row-level diff is not attempted.

### Output

Results are printed via `print_result()` / `format_result()` in `engine-verify`. The CLI also supports writing the report to a file via `--output`.

```
✓ migrate_actor/actor - match (2 batches, 200 rows, 45ms)
✗ migrate_payment/payment - MISMATCH (1 divergent batches, 312ms)
  batch 3 (rows 3000-4000): expected a3f1b2c4... actual 9d8c7b6a...
    row 3412: expected a3f1... actual 9d8c...
? migrate_film/film - no integrity receipt (run `apply --integrity` first)
```

---

## CLI Usage

```bash
# Run migration and store batch-level Merkle roots
stratum apply -c migration.smql --integrity

# Run migration and store per-row hashes (enables row-level mismatch reporting)
stratum apply -c migration.smql --full-integrity

# Verify destination matches stored receipt
stratum verify -c migration.smql

# Write verification report to file
stratum verify -c migration.smql --output report.txt
```

`--full-integrity` implies `--integrity`. Without either flag, hashing is disabled and zero overhead is incurred.

---

## Pagination Requirement

**Verification requires a `paginate` block for reliable results.**

Both the write path (`apply --integrity`) and the verify path (`verify`) must read rows in the same order to produce matching batch-level Merkle roots. Without an explicit `paginate` block, the default strategy uses `OFFSET/LIMIT` without `ORDER BY` -- the database is free to return rows in any order, which can change between runs (e.g. after `VACUUM`, concurrent writes, or heap reorganization). This produces false mismatches.

Add a `paginate` block to every pipeline that uses integrity verification:

```smql
paginate {
    using  = "pk"
    column = orders.id
}
```

When `paginate` references source-side column names (e.g. `orders.id`), Stratum automatically resolves them to destination-side names through the pipeline's field mappings before verifying. For example, if the pipeline renames `customer.customer_id` to `id` and the destination table is `users`, the verify path reads from `users` ordered by `id`.

If a pipeline runs with `--integrity` but no `paginate` block, a warning is logged at both `apply` and `verify` time:

```
WARN: Pipeline 'migrate_customer' has no `paginate` block. Verification requires
deterministic row ordering to reproduce batch boundaries. Results may show false
mismatches. Add a `paginate` block for reliable verification.
```

---

## Cascade Pipelines

For pipelines using `with references { data = cascade }`, each FK-referenced table is independently verified:

- Each table gets its own `VerificationReceipt` with `sorted_hashes = true`.
- `IntegrityState` accumulates row hashes in a `HashSet` per cascade table, deduplicating rows referenced by multiple source batches.
- At finalization, hashes are sorted and a single Merkle root is stored.
- Verify reads all rows from each cascade destination table, sorts their hashes, and compares to `receipt.table_root`.
- `row_hashes` is always `None` for cascade receipts — row-level diff is not supported for out-of-order tables.

---

## Edge Cases

| Scenario                        | Behavior                                                                                                                                                                     |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Empty table                     | `root_from_hashes(&[])` returns a fixed sentinel. Verify re-reads zero rows, computes the same sentinel. Match.                                                            |
| Single row                      | Treated as a single-leaf tree. Root equals the row hash directly.                                                                                                            |
| Pipeline with `where` filter  | Receipt covers only the filtered rows. Verify re-reads destination (already contains only filtered rows) and compares — no re-application of the filter on the verify path. |
| `action = skip` validation    | Skipped rows are not written and not hashed. Receipt reflects only written rows.`skipped_rows` counter records the count.                                                  |
| Rows inserted after migration   | Detected by the sentinel fetch. Reported as a batch count mismatch.                                                                                                          |
| `NoPriorRun`                  | Pipeline ran without `--integrity`. Verify returns `NoPriorRun` (not an error).                                                                                          |
| Multiple pipelines (DAG)        | Each pipeline is verified independently. Results are collected in execution order.                                                                                           |
| Many null columns               | Null-heavy rows serialize compactly (`0x00` per null column). No special handling required.                                                                                |
| Cascade table row deduplication | A row referenced by 100 source batches contributes one hash to the set. The stored root reflects unique destination rows only.                                               |
| No `paginate` block           | Row order is non-deterministic. Batch boundaries may not align between write and verify, causing false mismatches. A warning is logged. Add a `paginate` block to fix.      |

---

## Performance

### Write Path Overhead

| Operation                | Cost                   | Notes                                                  |
| ------------------------ | ---------------------- | ------------------------------------------------------ |
| Canonical serialization  | ~200 ns/row            | Buffer reuse via `RowHasher`. No per-row allocation. |
| SHA-256 hash             | ~300 ns/row            | SHA-NI hardware acceleration. ~3 GB/s.                 |
| Subtree root computation | ~50 ns/row (amortized) | Single-pass level reduction, O(N) memory.              |
| Receipt write to sled    | ~10 µs once           | One KV write per table at pipeline completion.         |

Total: ~550 ns/row. At 30K rows/sec (typical MySQL->PostgreSQL), under 2% overhead. Bottleneck remains network I/O.

`--full-integrity` adds one `Vec<[u8; 32]>` element per row (~32 bytes) retained in memory for the duration of the pipeline, plus the same 32 bytes persisted in sled. A 1M row table uses ~32 MB of additional sled storage.

### Verification Path Cost

Full destination re-read at the same speed as the original migration. For a 10M row table: ~5–6 minutes. Batch-level early termination means a single corrupted row is detected after scanning only to the divergent batch — potentially seconds for early batches.

---

## Future Extensions

### Incremental Verification

For CDC pipelines: maintain Merkle trees incrementally as new rows arrive, recomputing only the affected subtree. Extends the model from batch migrations to continuous replication.

### Parallel Verification

For very large tables: read PK ranges concurrently, build batch subtrees in parallel. The batch-level tree structure supports this naturally — each batch is an independent subtree.
