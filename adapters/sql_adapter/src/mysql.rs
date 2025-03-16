use crate::{
    adapter::DbAdapter,
    metadata::{
        column::metadata::ColumnMetadata, foreign_key::ForeignKeyMetadata, table::TableMetadata,
    },
    query::{builder::SqlQueryBuilder, loader::QueryLoader},
    requests::FetchRowsRequest,
    row::{
        extract::{MySqlRowDataExt, RowDataExt},
        row::RowData,
    },
};
use async_trait::async_trait;
use sqlx::{MySql, Pool, Row};
use std::collections::HashMap;

#[derive(Clone)]
pub struct MySqlAdapter {
    pool: Pool<MySql>,
}

#[async_trait]
impl DbAdapter for MySqlAdapter {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = Pool::connect(url).await?;
        Ok(MySqlAdapter { pool })
    }

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let query = "SELECT EXISTS (
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'test'
            AND table_name = $1
        )";

        let row = sqlx::query(query).bind(table).fetch_one(&self.pool).await?;
        let exists: bool = row.get(0);
        Ok(exists)
    }

    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>> {
        let query = format!("TRUNCATE TABLE {}", table);
        sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn fetch_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let query = QueryLoader::table_metadata_query()
            .map_err(|_| sqlx::Error::Configuration("Table metadata query not found".into()))?;

        let rows = sqlx::query(&query)
            .bind(table)
            .bind(table)
            .bind(table)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        let columns: HashMap<String, ColumnMetadata> = rows
            .iter()
            .map(|row| ColumnMetadata::from(row))
            .map(|col| (col.name.clone(), col))
            .collect();

        let primary_keys: Vec<String> = columns
            .values()
            .filter(|col| col.is_primary_key)
            .map(|col| col.name.clone())
            .collect();

        let foreign_keys: Vec<ForeignKeyMetadata> = columns
            .values()
            .filter_map(|col| {
                col.referenced_table
                    .as_ref()
                    .zip(col.referenced_column.as_ref())
                    .map(|(ref_table, ref_column)| ForeignKeyMetadata {
                        column: col.name.clone(),
                        referenced_table: ref_table.clone(),
                        referenced_column: ref_column.clone(),
                    })
            })
            .collect();

        Ok(TableMetadata {
            name: table.to_string(),
            schema: None,
            columns,
            primary_keys,
            foreign_keys,
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        })
    }

    async fn fetch_rows(
        &self,
        request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
        let query = SqlQueryBuilder::new()
            .select(&request.columns, request.table.as_str())
            .from(&request.table)
            .limit(request.limit)
            .offset(request.offset.unwrap_or(0))
            .build();

        let rows = sqlx::query(&query.0).fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|row| MySqlRowDataExt::from_row(row))
            .collect())
    }
}
