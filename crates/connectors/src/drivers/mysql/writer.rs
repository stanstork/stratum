use crate::{
    drivers::mysql::{
        driver::MySqlDriver, encoder::MySqlCopyEncoder, params::MySqlParamStore,
        types::MySqlTypeConverter,
    },
    error::DriverError,
    sql::{
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::generator::QueryGenerator,
    },
    traits::writer::DataWriter,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use model::records::Record;
use mysql_async::prelude::Queryable;
use query_builder::{ast::load_data::LoadDataConflict, dialect};
use tracing::debug;

#[async_trait]
impl DataWriter for MySqlDriver {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let num_rows = rows.len();
        let generator = QueryGenerator::new(&dialect::MySql);
        let (sql, params) = generator.insert_batch(meta, rows, &MySqlTypeConverter);

        debug!(rows = num_rows, table = %meta.name, "inserting rows");

        let params = MySqlParamStore::from_values(&params).params();
        let mut conn = self.write_conn().await?;
        let result = conn
            .exec_iter(&sql, params)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(result.affected_rows())
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
    /// Acquire a pooled connection configured for writes. Sets
    /// `NO_AUTO_VALUE_ON_ZERO` so an explicit `0` in an AUTO_INCREMENT column is
    /// stored as-is rather than being replaced by a generated id.
    async fn write_conn(&self) -> Result<mysql_async::Conn, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        // CONCAT_WS + NULLIF avoids a leading comma when @@sql_mode is empty or
        // NULL (plain CONCAT would yield ",NO_AUTO_VALUE_ON_ZERO", a syntax error).
        conn.query_drop(
            "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@sql_mode, ''), 'NO_AUTO_VALUE_ON_ZERO')",
        )
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
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

        // Serialize the whole batch to the tab/newline/backslash text format
        // that MySqlCopyEncoder produces.
        let encoder = MySqlCopyEncoder;
        let mut buff = String::new();

        for row in rows {
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    buff.push('\t');
                }
                // Borrow the value and append straight into the batch buffer.
                match row.get(&col.name).and_then(|f| f.value.as_ref()) {
                    Some(value) => encoder.write_value(value, &mut buff),
                    None => buff.push_str("\\N"),
                }
            }
            buff.push('\n');
        }

        let payload = Bytes::from(buff);
        let sql = QueryGenerator::new(&dialect::MySql).load_data_infile(table, &cols, on_conflict);

        debug!(rows = rows.len(), table = %table, ?on_conflict, "LOAD DATA rows into table");

        let mut conn = self.write_conn().await?;
        conn.set_infile_handler(async move {
            Ok(futures_util::stream::once(async move { Ok(payload) }).boxed())
        });

        let result = conn
            .query_iter(&sql)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(result.affected_rows())
    }
}
