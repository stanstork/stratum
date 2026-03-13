use crate::{
    drivers::postgres::{
        coercion, driver::PgDriver, encoder::PgCopyEncoder, params::PgParamStore,
        types::PgTypeConverter,
    },
    error::DriverError,
    sql::{
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::generator::QueryGenerator,
    },
    traits::{encoder::CopyValueEncoder, writer::DataWriter},
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, pin_mut};
use model::records::Record;
use query_builder::dialect;
use tracing::info;

#[async_trait]
impl DataWriter for PgDriver {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let num_rows = rows.len();
        let generator = QueryGenerator::new(&dialect::Postgres);
        let (sql, params) = generator.insert_batch(meta, rows, &PgTypeConverter);

        info!("Inserting {} rows into {}", num_rows, meta.name);

        let client = self.client().read().await;
        let param_store = PgParamStore::from_values(&params);
        let result = client
            .execute(&sql, &param_store.as_refs()[..])
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(result)
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

        let encoder = PgCopyEncoder;

        // Get non-generated columns sorted by ordinal position.
        // Generated columns are computed by the DB and must be excluded from both
        // the COPY header and the CSV data to avoid a column-count mismatch.
        let mut columns: Vec<_> = columns
            .iter()
            .filter(|c| !c.is_generated)
            .cloned()
            .collect();
        columns.sort_by_key(|c| c.ordinal);

        // Build COPY statement using query generator
        let generator = QueryGenerator::new(&dialect::Postgres);
        let statement = generator.copy_from_stdin(table, &columns);

        info!("COPY {} rows into {}", rows.len(), table);

        let client = self.client().write().await;

        let sink = client
            .copy_in(&statement)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        pin_mut!(sink);

        // Write rows as CSV in column order
        for row in rows {
            let mut line = String::new();
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    line.push(',');
                }
                let field = row.get(&col.name);
                let encoded = match field.and_then(|f| f.value.clone()) {
                    Some(value) => {
                        // Coerce value to match target column type
                        let coerced = coercion::coerce_value(value, col);
                        encoder.encode_value(&coerced)
                    }
                    None => encoder.encode_null(),
                };
                line.push_str(&encoded);
            }
            line.push('\n');
            sink.as_mut()
                .send(Bytes::from(line))
                .await
                .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        }

        sink.as_mut()
            .close()
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(rows.len() as u64)
    }
}
