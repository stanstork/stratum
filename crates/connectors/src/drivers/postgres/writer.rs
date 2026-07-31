use crate::{
    drivers::postgres::{
        config::CopyFormat,
        driver::PgDriver,
        encoding::{
            binary::{BinaryColumnType, BinaryEncodeError, PgBinaryEncoder},
            coercion,
            text::PgCopyEncoder,
        },
        params::PgParamStore,
        types::PgTypeConverter,
    },
    error::DriverError,
    sql::{
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::generator::{QueryGenerator, max_rows_per_insert},
    },
    traits::{driver::Driver, writer::DataWriter},
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, pin_mut};
use model::records::Record;
use query_builder::dialect;
use tracing::debug;

#[async_trait]
impl DataWriter for PgDriver {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let generator = QueryGenerator::new(&dialect::Postgres);
        let max_params = self.capabilities().max_parameters.unwrap_or(usize::MAX);
        let chunk_rows = max_rows_per_insert(meta, max_params);

        // Common case: the whole batch fits in one statement.
        if rows.len() <= chunk_rows {
            let (sql, params) = generator.insert_batch(meta, rows, &PgTypeConverter);

            debug!(rows = rows.len(), table = %meta.name, "inserting rows");

            let client = self.client().read().await;
            let param_store = PgParamStore::from_values(&params);
            let result = client
                .execute(&sql, &param_store.as_refs()[..])
                .await
                .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

            return Ok(result);
        }

        // Batch exceeds the placeholder limit: split into chunks in one
        // transaction so it stays all-or-nothing.

        debug!(rows = rows.len(), chunk_rows, table = %meta.name, "inserting rows (chunked)");

        let mut client = self.client().write().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        let mut affected = 0u64;

        for chunk in rows.chunks(chunk_rows) {
            let (sql, params) = generator.insert_batch(meta, chunk, &PgTypeConverter);
            let param_store = PgParamStore::from_values(&params);

            affected += tx
                .execute(&sql, &param_store.as_refs()[..])
                .await
                .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(affected)
    }

    async fn truncate(&self, table: &str) -> Result<(), DriverError> {
        let (sql, _) = QueryGenerator::new(&dialect::Postgres).truncate_table(table);
        debug!(table = %table, "truncating table");

        let client = self.client().write().await;
        client
            .batch_execute(&sql)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        Ok(())
    }

    /// Write rows using PostgreSQL COPY protocol for maximum throughput.
    /// Transaction handling should be done by the caller (e.g., Sink).
    async fn copy_rows(
        &self,
        table: &str,
        columns: &[ColumnMetadata],
        rows: &[Record],
    ) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Get non-generated columns sorted by ordinal position.
        // Generated columns are computed by the DB and must be excluded from both
        // the COPY header and the data to avoid a column-count mismatch.
        let mut columns: Vec<_> = columns
            .iter()
            .filter(|c| !c.is_generated)
            .cloned()
            .collect();
        columns.sort_by_key(|c| c.ordinal);

        // Only write destination columns the row actually carries.
        columns.retain(|c| rows[0].index_of(&c.name).is_some());

        if columns.is_empty() {
            return Ok(0);
        }

        debug!(rows = rows.len(), table = %table, "COPY rows into table");

        // All rows in a batch share the same field layout, so resolve each output
        // column's field index once. Every retained column is present in the row.
        let field_idx: Vec<Option<usize>> = columns
            .iter()
            .map(|col| rows[0].index_of(&col.name))
            .collect();

        let generator = QueryGenerator::new(&dialect::Postgres);

        // Prefer binary COPY when the driver is configured for it and every
        // column is binary-encodable; otherwise fall back to the CSV text path.
        let binary_cols = if self.copy_format() == CopyFormat::Binary {
            columns
                .iter()
                .map(BinaryColumnType::classify)
                .collect::<Option<Vec<_>>>()
        } else {
            None
        };

        let (statement, payload) = match binary_cols
            .as_deref()
            .map(|bcts| encode_binary(&columns, bcts, &field_idx, rows))
        {
            Some(Ok(buf)) => (generator.copy_from_stdin_binary(table, &columns), buf),
            _ => (
                generator.copy_from_stdin_text(table, &columns),
                encode_text(&columns, &field_idx, rows),
            ),
        };

        let client = self.client().write().await;

        let sink = client
            .copy_in(&statement)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        pin_mut!(sink);

        sink.as_mut()
            .send(payload)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        sink.as_mut()
            .close()
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(rows.len() as u64)
    }
}

/// Encode a batch as a binary COPY stream. Returns `Err(BinaryEncodeError)` if
/// any value fails to encode for its target column, signaling a fallback to CSV
fn encode_binary(
    columns: &[ColumnMetadata],
    bcts: &[BinaryColumnType],
    field_idx: &[Option<usize>],
    rows: &[Record],
) -> Result<Bytes, BinaryEncodeError> {
    let enc = PgBinaryEncoder;
    let mut buf = Vec::with_capacity(rows.len() * 128 + 19);

    PgBinaryEncoder::write_header(&mut buf);

    for row in rows {
        PgBinaryEncoder::begin_row(&mut buf, columns.len());
        for i in 0..columns.len() {
            match field_idx[i].and_then(|idx| row.value_at(idx)) {
                Some(value) => enc.write_field(bcts[i], value, &mut buf)?,
                None => PgBinaryEncoder::write_null(&mut buf),
            }
        }
    }

    PgBinaryEncoder::write_trailer(&mut buf);
    Ok(Bytes::from(buf))
}

/// Encode a batch as a single CSV COPY payload (the text fallback path).
fn encode_text(columns: &[ColumnMetadata], field_idx: &[Option<usize>], rows: &[Record]) -> Bytes {
    let encoder = PgCopyEncoder;
    let coercions: Vec<_> = columns.iter().map(coercion::ColumnCoercion::of).collect();

    let mut buf = String::with_capacity(rows.len() * 128);

    for row in rows {
        for i in 0..columns.len() {
            if i > 0 {
                buf.push(',');
            }
            match field_idx[i].and_then(|idx| row.value_at(idx)) {
                Some(value) => {
                    let coerced = coercions[i].apply(value);
                    encoder.write_value(&coerced, &mut buf);
                }
                None => buf.push_str("\\N"),
            }
        }
        buf.push('\n');
    }
    Bytes::from(buf)
}
