use crate::{error::VerifyError, reader::TableReader};
use connectors::{
    sql::metadata::table::TableMetadata, traits::introspector::SchemaIntrospector,
    traits::reader::DataReader,
};
use engine_core::{
    context::{env::EnvContext, exec::ExecutionContext},
    dispatch_driver,
    drivers::DriverRef,
    plan::{cascade::resolve_cascade_tables, execution::ExecutionPlan},
    schema::{
        graph_expander::GraphExpander,
        type_registry::{Dialect, TypeRegistry},
    },
};
use engine_state::{MerkleStore, sled_store::SledStateStore};
use model::{
    execution::{
        pipeline::{Pagination, Pipeline},
        references::{DataMode, GraphReferences},
    },
    integrity::{
        coerce::coerce_row_for_hash,
        hasher::RowHasher,
        merkle::MerkleTree,
        receipt::VerificationReceipt,
        result::{DivergentBatch, DivergentRow, VerificationResult},
    },
    pagination::cursor::Cursor,
    records::Record,
    transform::mapping::TransformationMetadata,
};
use query_builder::offsets::{OffsetStrategy, OffsetStrategyFactory};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::warn;

pub async fn verify(
    plan: ExecutionPlan,
    env: Arc<EnvContext>,
) -> Result<Vec<VerificationResult>, VerifyError> {
    let state = init_state()?;
    let exec_ctx = ExecutionContext::new(&plan, state.clone(), env).await?;
    let mut results: Vec<VerificationResult> = Vec::new();

    for pipeline in &plan.pipelines {
        verify_pipeline(pipeline, &exec_ctx, &state, &mut results).await?;
    }

    Ok(results)
}

fn init_state() -> Result<Arc<SledStateStore>, VerifyError> {
    let home_dir = dirs::home_dir().ok_or_else(|| {
        VerifyError::InitializationError("Failed to determine home directory".to_string())
    })?;
    let path = home_dir.join(".stratum/state");

    SledStateStore::open(path)
        .map(Arc::new)
        .map_err(|e| VerifyError::InitializationError(e.to_string()))
}

async fn verify_pipeline(
    pipeline: &Pipeline,
    exec_ctx: &ExecutionContext,
    state: &Arc<SledStateStore>,
    results: &mut Vec<VerificationResult>,
) -> Result<(), VerifyError> {
    let driver = exec_ctx
        .resolve_driver(&pipeline.destination.connection)
        .await?;
    let mapping = TransformationMetadata::new(pipeline);
    let resolved_pagination = resolve_pagination(&pipeline.source.pagination, &mapping);

    if resolved_pagination.is_none() {
        warn!(
            pipeline = %pipeline.name,
            "pipeline has no `paginate` block; verification requires deterministic row \
             ordering to reproduce batch boundaries, so results may show false mismatches. \
             Add a `paginate` block for reliable verification."
        );
    }

    let offset_strategy = OffsetStrategyFactory::from_pagination(&resolved_pagination);

    let cascade_meta = get_graph_expansion(pipeline, &driver, &mapping).await?;
    let cascade_tables = resolve_cascade_tables(pipeline, &mapping, &cascade_meta);

    // Filter empty names and deduplicate so each table is verified exactly once.
    let mut seen = HashSet::new();
    let mut all_tables = Vec::new();

    let mut add_table = |t: &str| {
        if !t.is_empty() && seen.insert(t.to_string()) {
            all_tables.push(t.to_string());
        }
    };

    add_table(&pipeline.destination.table);
    for t in cascade_tables {
        add_table(&t);
    }

    for table in all_tables {
        // Load the receipt written by the most recent `apply --integrity`.
        match state.load_receipt(&pipeline.name, &table).await? {
            Some(receipt) => {
                let result =
                    verify_table(&driver, &table, &receipt, offset_strategy.clone()).await?;
                results.push(result);
            }
            None => {
                results.push(VerificationResult::NoPriorRun {
                    pipeline_name: pipeline.name.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Re-read all rows of `table` from the destination, hash them in batches of
/// `receipt.rows_per_batch` using the `column_order` and `algorithm` from the receipt,
/// and compare the resulting subtree roots against `receipt.batch_roots`.
async fn verify_table(
    driver: &DriverRef,
    table: &str,
    receipt: &VerificationReceipt,
    offset_strategy: Arc<dyn OffsetStrategy>,
) -> Result<VerificationResult, VerifyError> {
    let start = std::time::Instant::now();

    let meta = fetch_table_metadata(driver, table).await?;
    let col_types: HashMap<String, String> = meta
        .columns
        .values()
        .map(|c| (c.name.clone(), c.data_type.clone()))
        .collect();
    let table_reader = create_table_reader(driver, meta, offset_strategy)?;
    let (actual_batch_roots, actual_row_hashes_by_batch) =
        read_and_hash(&table_reader, receipt, &col_types).await?;

    let duration_ms = start.elapsed().as_millis() as u64;
    Ok(build_result(
        receipt,
        actual_batch_roots,
        actual_row_hashes_by_batch,
        duration_ms,
    ))
}

/// Delegates to the appropriate hashing strategy based on whether it is a
/// cascade table (`sorted_hashes = true`) or a primary table.
async fn read_and_hash(
    reader: &TableReader,
    receipt: &VerificationReceipt,
    col_types: &HashMap<String, String>,
) -> Result<(Vec<[u8; 32]>, Vec<Vec<[u8; 32]>>), VerifyError> {
    if receipt.sorted_hashes {
        read_and_hash_sorted(reader, receipt, col_types).await
    } else {
        read_and_hash_batched(reader, receipt, col_types).await
    }
}

/// Cascade table: read all rows, sort hashes, build single Merkle root.
/// This matches the migration path which accumulates unique row hashes,
/// sorts them, and stores a single root - making the result order-independent.
async fn read_and_hash_sorted(
    reader: &TableReader,
    receipt: &VerificationReceipt,
    col_types: &HashMap<String, String>,
) -> Result<(Vec<[u8; 32]>, Vec<Vec<[u8; 32]>>), VerifyError> {
    let mut hasher = RowHasher::new(receipt.column_order.clone(), receipt.algorithm);
    let mut all_hashes = Vec::new();
    let mut cursor = Cursor::None;
    let limit = 1000usize;

    loop {
        let (rows, next_cursor) = reader.next_batch(cursor, limit).await?;
        all_hashes.extend(
            rows.iter()
                .map(|r| hash_row_coerced(&mut hasher, r, col_types)),
        );

        match next_cursor {
            None => break,
            Some(c) => cursor = c,
        }
    }

    all_hashes.sort_unstable();
    let root = MerkleTree::root_from_hashes(&all_hashes, receipt.algorithm);

    // No per-batch row hashes for cascade (sorted mode); row-level verify N/A.
    Ok((vec![root], vec![all_hashes]))
}

/// Primary table: batch-based verification.
/// Uses `receipt.rows_per_batch` as the limit for each fetch so that batch
/// boundaries match exactly what the apply path produced.
/// Row hashes are only collected when the receipt carries them (`--full-integrity`).
async fn read_and_hash_batched(
    reader: &TableReader,
    receipt: &VerificationReceipt,
    col_types: &HashMap<String, String>,
) -> Result<(Vec<[u8; 32]>, Vec<Vec<[u8; 32]>>), VerifyError> {
    let need_row_hashes = receipt.row_hashes.is_some();
    let mut hasher = RowHasher::new(receipt.column_order.clone(), receipt.algorithm);
    let mut actual_roots = Vec::with_capacity(receipt.rows_per_batch.len());
    let mut actual_row_hashes_by_batch: Vec<Vec<[u8; 32]>> = if need_row_hashes {
        Vec::with_capacity(receipt.rows_per_batch.len())
    } else {
        Vec::new()
    };
    let mut cursor = Cursor::None;

    for &expected_rows in &receipt.rows_per_batch {
        let limit = expected_rows as usize;
        let (rows, next_cursor) = reader.next_batch(cursor.clone(), limit).await?;

        if !rows.is_empty() {
            let row_hashes: Vec<[u8; 32]> = rows
                .iter()
                .map(|r| hash_row_coerced(&mut hasher, r, col_types))
                .collect();
            let subtree_root = MerkleTree::root_from_hashes(&row_hashes, receipt.algorithm);
            actual_roots.push(subtree_root);
            if need_row_hashes {
                actual_row_hashes_by_batch.push(row_hashes);
            }
        }

        match next_cursor {
            None => break,
            Some(c) => cursor = c,
        }
    }

    // Sentinel check: fetch one row past the last receipt boundary.
    // A non-empty sentinel result means the destination has more data than recorded.
    let (sentinel_rows, _) = reader.next_batch(cursor, 1).await?;
    if !sentinel_rows.is_empty() {
        actual_roots.push([0xffu8; 32]);
    }

    Ok((actual_roots, actual_row_hashes_by_batch))
}

/// Compare per-batch roots and build the final `VerificationResult`.
/// Row-level detail is filled into `DivergentBatch.divergent_rows` only when
/// both the receipt and `actual_row_hashes_by_batch` carry per-row hashes
/// (i.e. the run used `--full-integrity`).
fn build_result(
    receipt: &VerificationReceipt,
    actual_batch_roots: Vec<[u8; 32]>,
    actual_row_hashes_by_batch: Vec<Vec<[u8; 32]>>,
    duration_ms: u64,
) -> VerificationResult {
    let actual_root = MerkleTree::root_from_hashes(&actual_batch_roots, receipt.algorithm);

    // Batch count mismatch means the destination has a different number of rows.
    // Report as a single divergent span covering the whole table.
    if actual_batch_roots.len() != receipt.batch_roots.len() {
        return VerificationResult::Mismatch {
            receipt: receipt.clone(),
            actual_root,
            divergent_batches: vec![DivergentBatch {
                batch_index: 0,
                expected_root: receipt.table_root,
                actual_root,
                row_start: 0,
                row_end: receipt.total_rows,
                divergent_rows: vec![],
            }],
            duration_ms,
        };
    }

    let divergent_batches =
        find_divergent_batches(receipt, &actual_batch_roots, &actual_row_hashes_by_batch);

    if divergent_batches.is_empty() {
        VerificationResult::Match {
            receipt: receipt.clone(),
            duration_ms,
        }
    } else {
        VerificationResult::Mismatch {
            receipt: receipt.clone(),
            actual_root,
            divergent_batches,
            duration_ms,
        }
    }
}

fn find_divergent_batches(
    receipt: &VerificationReceipt,
    actual_batch_roots: &[[u8; 32]],
    actual_row_hashes_by_batch: &[Vec<[u8; 32]>],
) -> Vec<DivergentBatch> {
    receipt
        .batch_roots
        .iter()
        .zip(actual_batch_roots.iter())
        .enumerate()
        .filter(|(_, (expected, actual))| expected != actual)
        .map(|(i, (expected, actual))| {
            let row_start: u64 = receipt.rows_per_batch[..i].iter().sum();
            let row_end = (row_start + receipt.rows_per_batch[i]).min(receipt.total_rows);

            let divergent_rows = match &receipt.row_hashes {
                Some(receipt_row_hashes) => {
                    let batch_actual = actual_row_hashes_by_batch
                        .get(i)
                        .map(|v| v.as_slice())
                        .unwrap_or(&[]);
                    find_divergent_rows(row_start, row_end, receipt_row_hashes, batch_actual)
                }
                None => vec![],
            };

            DivergentBatch {
                batch_index: i as u64,
                expected_root: *expected,
                actual_root: *actual,
                row_start,
                row_end,
                divergent_rows,
            }
        })
        .collect()
}

fn find_divergent_rows(
    row_start: u64,
    row_end: u64,
    receipt_row_hashes: &[[u8; 32]],
    actual_row_hashes: &[[u8; 32]],
) -> Vec<DivergentRow> {
    let batch_receipt = &receipt_row_hashes[row_start as usize..row_end as usize];
    batch_receipt
        .iter()
        .zip(actual_row_hashes.iter())
        .enumerate()
        .filter(|(_, (e, a))| e != a)
        .map(|(j, (e, a))| DivergentRow {
            row_index: row_start + j as u64,
            expected_hash: *e,
            actual_hash: *a,
        })
        .collect()
}

async fn fetch_table_metadata(
    driver: &DriverRef,
    table: &str,
) -> Result<TableMetadata, VerifyError> {
    let meta = dispatch_driver!(driver, |d| {
        let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
        introspector.table_metadata(table).await?
    });
    Ok(meta)
}

fn create_table_reader(
    driver: &DriverRef,
    meta: TableMetadata,
    offset_strategy: Arc<dyn OffsetStrategy>,
) -> Result<TableReader, VerifyError> {
    let reader = dispatch_driver!(driver, |d| {
        let data_reader: Arc<dyn DataReader> = d.clone() as _;
        TableReader::new(data_reader, meta.clone(), offset_strategy.clone())
    });
    Ok(reader)
}

async fn get_graph_expansion(
    pipeline: &Pipeline,
    src_driver: &DriverRef,
    mapping: &TransformationMetadata,
) -> Result<Option<HashMap<String, TableMetadata>>, VerifyError> {
    if let Some(refs) = &pipeline.source.graph_references {
        expand_graph_references(&pipeline.source.table, src_driver, mapping, refs).await
    } else {
        Ok(None)
    }
}

async fn expand_graph_references(
    root_table: &str,
    src_driver: &DriverRef,
    mapping: &TransformationMetadata,
    refs: &GraphReferences,
) -> Result<Option<HashMap<String, TableMetadata>>, VerifyError> {
    let source_dialect = src_driver.dialect();

    let result = dispatch_driver!(src_driver, |d| {
        let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
        let type_registry = Arc::new(TypeRegistry::new(
            source_dialect,
            Dialect::Postgres, // TODO: derive from destination driver
        ));
        let expander = GraphExpander::new(introspector, type_registry, source_dialect);
        expander
            .expand(root_table, refs, mapping, false, false)
            .await
            .map_err(|e| VerifyError::InitializationError(e.to_string()))?
    });

    let cascade_meta = if matches!(refs.data_mode, DataMode::Cascade) {
        Some(result.discovered_tables)
    } else {
        None
    };

    Ok(cascade_meta)
}

/// Resolve pagination column references from source names to destination names.
fn resolve_pagination(
    pagination: &Option<Pagination>,
    mapping: &TransformationMetadata,
) -> Option<Pagination> {
    let pag = pagination.as_ref()?;

    Some(Pagination {
        strategy: pag.strategy.clone(),
        column: resolve_qualified_column(&pag.column, mapping),
        tiebreaker: pag
            .tiebreaker
            .as_ref()
            .map(|tb| resolve_qualified_column(tb, mapping)),
        timezone: pag.timezone.clone(),
    })
}

/// Resolve a `table.column` reference to destination names via the mapping.
fn resolve_qualified_column(qual_col: &str, mapping: &TransformationMetadata) -> String {
    let parts: Vec<&str> = qual_col.split('.').collect();
    if parts.len() == 2 {
        let src_table = parts[0];
        let src_col = parts[1];
        let dst_table = mapping.entities.resolve(src_table);
        let dst_col = mapping.field_mappings.resolve(&dst_table, src_col);
        format!("{}.{}", dst_table, dst_col)
    } else {
        qual_col.to_string()
    }
}

/// Hash a row, applying column-type coercions when needed.
/// Mirrors the same function in `engine-processing` so that verify
/// produces identical hashes to the write path.
fn hash_row_coerced(
    hasher: &mut RowHasher,
    row: &Record,
    col_types: &HashMap<String, String>,
) -> [u8; 32] {
    if col_types.is_empty() {
        hasher.hash_row(row)
    } else {
        hasher.hash_row(&coerce_row_for_hash(row, col_types))
    }
}
