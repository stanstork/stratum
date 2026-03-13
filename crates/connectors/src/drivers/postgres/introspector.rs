use crate::{
    drivers::postgres::{driver::PgDriver, queries, row::PgRowDecoder},
    error::DriverError,
    sql::metadata::{
        column::ColumnMetadata,
        constraint::{CheckConstraintMetadata, UniqueConstraintMetadata},
        fk::ForeignKeyMetadata,
        index::IndexMetadata,
        provider::MetadataProvider,
        table::TableMetadata,
    },
    traits::introspector::SchemaIntrospector,
};
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
impl SchemaIntrospector for PgDriver {
    async fn table_exists(&self, table: &str) -> Result<bool, DriverError> {
        let client = self.client().read().await;
        let row = client
            .query_one(queries::TABLE_EXISTS_SQL, &[&table])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        Ok(row.get(0))
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>, DriverError> {
        let schema_name = schema.unwrap_or("public");
        let client = self.client().read().await;

        let rows = client
            .query(queries::LIST_TABLES_SQL, &[&schema_name])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError> {
        let query = queries::TABLE_METADATA_SQL.replace("{table}", table);
        let client = self.client().read().await;

        let rows = client
            .query(&query, &[])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let columns: HashMap<String, ColumnMetadata> = rows
            .iter()
            .map(|row| {
                let decoder = PgRowDecoder(row);
                let col_meta = ColumnMetadata::from_row(&decoder);
                (col_meta.name.clone(), col_meta)
            })
            .collect();

        let fks = self.fk_metadata(table).await?;

        MetadataProvider::construct_table_metadata(table, columns, fks)
    }

    async fn index_metadata(&self, table: &str) -> Result<Vec<IndexMetadata>, DriverError> {
        let client = self.client().read().await;
        let schema = "public";

        let rows = client
            .query(queries::INDEX_METADATA_SQL, &[&schema, &table])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let indexes = rows
            .iter()
            .map(|row| {
                let decoder = PgRowDecoder(row);
                IndexMetadata::from_row(&decoder)
            })
            .collect();

        Ok(indexes)
    }

    async fn fk_metadata(&self, table: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError> {
        let client = self.client().read().await;

        let rows = client
            .query(queries::FK_METADATA_SQL, &[&table])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let fks = rows
            .iter()
            .map(|row| {
                let decoder = PgRowDecoder(row);
                ForeignKeyMetadata::from_row(&decoder)
            })
            .collect();

        Ok(fks)
    }

    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DriverError> {
        let client = self.client().read().await;

        let rows = client
            .query(queries::REFERRING_TABLES_SQL, &[&table])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| row.get::<_, String>("referencing_table"))
            .collect())
    }

    async fn table_size_bytes(&self, table: &str) -> Result<u64, DriverError> {
        let client = self.client().read().await;

        let row = client
            .query_one(queries::TABLE_SIZE_SQL, &[&table])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;
        let size_bytes: i64 = row.get("size_bytes");

        Ok(size_bytes as u64)
    }

    async fn unique_constraint_metadata(
        &self,
        table: &str,
    ) -> Result<Vec<UniqueConstraintMetadata>, DriverError> {
        let client = self.client().read().await;

        let rows = client
            .query(queries::UNIQUE_CONSTRAINT_METADATA_SQL, &[&table])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let constraints = rows
            .iter()
            .map(|row| {
                let decoder = PgRowDecoder(row);
                UniqueConstraintMetadata::from_row(&decoder)
            })
            .collect();

        Ok(constraints)
    }

    async fn check_constraint_metadata(
        &self,
        table: &str,
    ) -> Result<Vec<CheckConstraintMetadata>, DriverError> {
        let client = self.client().read().await;

        let rows = client
            .query(queries::CHECK_CONSTRAINT_METADATA_SQL, &[&table])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let constraints = rows
            .iter()
            .map(|row| {
                let decoder = PgRowDecoder(row);
                CheckConstraintMetadata::from_row(&decoder)
            })
            .collect();

        Ok(constraints)
    }
}
