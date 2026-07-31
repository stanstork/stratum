use crate::{
    drivers::mysql::{
        driver::MySqlDriver, encoder::MySqlCopyEncoder, params::MySqlParamStore,
        types::MySqlTypeConverter,
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
use futures_util::StreamExt;
use model::records::Record;
use mysql_async::{TxOpts, prelude::Queryable};
use query_builder::{ast::load_data::LoadDataConflict, dialect};
use tracing::debug;

#[async_trait]
impl DataWriter for MySqlDriver {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let generator = QueryGenerator::new(&dialect::MySql);
        let max_params = self.capabilities().max_parameters.unwrap_or(usize::MAX);
        let chunk_rows = max_rows_per_insert(meta, max_params);
        let mut conn = self.write_conn().await?;

        // Common case: the whole batch fits in one statement.
        if rows.len() <= chunk_rows {
            let (sql, params) = generator.insert_batch(meta, rows, &MySqlTypeConverter);

            debug!(rows = rows.len(), table = %meta.name, "inserting rows");

            let params = MySqlParamStore::from_values(&params).into_params();
            let result = conn
                .exec_iter(&sql, params)
                .await
                .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

            return Ok(result.affected_rows());
        }

        // Batch exceeds the placeholder limit: split into chunks and run them in
        // one transaction so the batch stays all-or-nothing.

        debug!(rows = rows.len(), chunk_rows, table = %meta.name, "inserting rows (chunked)");

        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        let mut affected = 0u64;

        for chunk in rows.chunks(chunk_rows) {
            let (sql, params) = generator.insert_batch(meta, chunk, &MySqlTypeConverter);
            let params = MySqlParamStore::from_values(&params).into_params();
            let result = tx
                .exec_iter(&sql, params)
                .await
                .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

            affected += result.affected_rows();
        }

        tx.commit()
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(affected)
    }

    async fn truncate(&self, table: &str) -> Result<(), DriverError> {
        let (sql, _) = QueryGenerator::new(&dialect::MySql).truncate_table(table);
        debug!(table = %table, "truncating table");

        let mut conn = self.pool().get_conn().await?;
        conn.query_drop(&sql)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn copy_rows(
        &self,
        table: &str,
        columns: &[ColumnMetadata],
        rows: &[Record],
    ) -> Result<u64, DriverError> {
        self.load_data(table, columns, rows, LoadDataConflict::Default)
            .await
    }
}

impl MySqlDriver {
    /// Acquire a pooled connection configured for bulk writes.
    async fn write_conn(&self) -> Result<mysql_async::Conn, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        // CONCAT_WS + NULLIF avoids a leading comma when @@sql_mode is empty or
        // NULL (plain CONCAT would yield ",NO_AUTO_VALUE_ON_ZERO", a syntax error).
        conn.query_drop(
            "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@sql_mode, ''), 'NO_AUTO_VALUE_ON_ZERO'), \
             unique_checks = 0, foreign_key_checks = 0",
        )
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        // Surface (once) any server settings the DBA controls that throttle bulk
        // loads. Read-only; stratum never changes server config.
        self.bulk_preflight_advisory(&mut conn).await;
        Ok(conn)
    }

    /// Bulk-loads `rows` into `table` via `LOAD DATA LOCAL INFILE`.
    pub async fn load_data(
        &self,
        table: &str,
        columns: &[ColumnMetadata],
        rows: &[Record],
        on_conflict: LoadDataConflict,
    ) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Destination columns the DB actually accepts, in wire order.
        // Generated columns are computed by MySQL and must NOT appear in either
        // the column list or the data - including them is a column-count error.
        let mut cols: Vec<_> = columns
            .iter()
            .filter(|c| !c.is_generated)
            .cloned()
            .collect();
        cols.sort_by_key(|c| c.ordinal);

        // Only write destination columns the row actually carries.
        cols.retain(|c| rows[0].index_of(&c.name).is_some());

        if cols.is_empty() {
            return Ok(0);
        }

        let field_idx: Vec<Option<usize>> =
            cols.iter().map(|c| rows[0].index_of(&c.name)).collect();

        let sql = QueryGenerator::new(&dialect::MySql).load_data_infile(table, &cols, on_conflict);

        // A `LOAD DATA` payload is sent as one logical packet, so it must stay
        // under the server's `max_allowed_packet`.
        let max_chunk = self
            .capabilities()
            .max_query_size
            .map(|m| m.saturating_sub(m / 8).max(1 << 20)) // ~87.5% of max_allowed_packet, min 1 MiB
            .unwrap_or(48 << 20); // conservative default if the server didn't report it

        debug!(rows = rows.len(), table = %table, ?on_conflict, max_chunk, "LOAD DATA rows into table");

        let encoder = MySqlCopyEncoder;

        let mut buff = String::with_capacity(max_chunk.min(rows.len() * 128) + 256);
        let mut conn = self.write_conn().await?;
        let mut affected = 0u64;

        for row in rows {
            for (i, idx) in field_idx.iter().enumerate() {
                if i > 0 {
                    buff.push('\t');
                }

                match idx.and_then(|j| row.value_at(j)) {
                    Some(value) => encoder.write_value(value, &mut buff),
                    None => buff.push_str("\\N"),
                }
            }
            buff.push('\n');

            // Flush once the encoded chunk approaches the packet limit.
            if buff.len() >= max_chunk {
                affected += load_chunk(&mut conn, &sql, std::mem::take(&mut buff)).await?;
            }
        }

        if !buff.is_empty() {
            affected += load_chunk(&mut conn, &sql, buff).await?;
        }

        Ok(affected)
    }
}

/// Send one already-encoded text chunk to the server.
async fn load_chunk(
    conn: &mut mysql_async::Conn,
    sql: &str,
    buff: String,
) -> Result<u64, DriverError> {
    let payload = Bytes::from(buff);

    conn.set_infile_handler(async move {
        Ok(futures_util::stream::once(async move { Ok(payload) }).boxed())
    });

    let result = conn
        .query_iter(sql)
        .await
        .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

    Ok(result.affected_rows())
}
