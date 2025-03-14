use super::data_dest::{DataDestination, DbDataDestination};
use crate::source::record::DataRecord;
use async_trait::async_trait;
use sql_adapter::{
    adapter::DbAdapter,
    metadata::table::TableMetadata,
    postgres::PgAdapter,
    query::builder::{ColumnInfo, ForeignKeyInfo, SqlQueryBuilder},
    row::row::RowData,
};
use std::collections::{HashMap, HashSet};
use tracing::error;

pub struct PgDestination {
    adapter: Box<dyn DbAdapter + Send + Sync>,
}

impl PgDestination {
    pub fn new(
        adapter: Box<dyn DbAdapter + Send + Sync>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(PgDestination { adapter })
    }

    /// Checks if the table exists in the database
    pub async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        if self.adapter.table_exists(table).await? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Maps row data into column names and values
    fn map_columns(&self, row: &Box<dyn DataRecord>) -> Result<Vec<(String, String)>, sqlx::Error> {
        // let row = row
        //     .as_any()
        //     .downcast_ref::<RowData>()
        //     .ok_or_else(|| sqlx::Error::Configuration("Invalid row data type".into()))?;

        // let columns = row
        //     .columns
        //     .iter()
        //     .map(|col| {
        //         let name = self.column_mapping.get(&col.name).ok_or_else(|| {
        //             sqlx::Error::Configuration(format!("Column '{}' not found", col.name).into())
        //         })?;
        //         let value = col.value.clone().ok_or_else(|| {
        //             sqlx::Error::Configuration(
        //                 format!("Null value for column '{}'", col.name).into(),
        //             )
        //         })?;

        //         Ok((name.clone(), value.to_string()))
        //     })
        //     .collect::<Result<Vec<_>, sqlx::Error>>()?;
        // Ok(columns)
        todo!()
    }
}

#[async_trait]
impl DataDestination for PgDestination {
    type Record = Box<dyn DataRecord + Send + Sync>;

    async fn write(
        &self,
        data: Vec<Box<dyn DataRecord + Send + Sync>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // self.ensure_table_exists().await?;

        // To simplify testing, we truncate the table before writing the data
        // In a real-world scenario, you would likely want to append the data
        // self.manager.truncate_table(&self.table).await?;

        // for (i, row) in data.iter().enumerate() {
        //     let columns = match self.map_columns(row) {
        //         Ok(cols) => cols,
        //         Err(e) => {
        //             error!(
        //                 "Failed to map columns for row {} in table '{}': {:?}",
        //                 i, self.table, e
        //             );
        //             continue;
        //         }
        //     };

        //     let query = SqlQueryBuilder::new()
        //         .insert_into(&self.table, &columns)
        //         .build();

        //     if let Err(err) = self.manager.execute(&query.0).await {
        //         error!(
        //             "Error writing row {} to table '{}': {:?}",
        //             i, self.table, err
        //         );
        //     }
        // }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl DbDataDestination for PgDestination {
    async fn infer_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut visited = HashSet::new();
        let mut query_builder = SqlQueryBuilder::new().begin_transaction();

        // Collect tables recursively and ensure correct creation order
        self.collect_referenced_tables(metadata, &mut visited, &mut query_builder);

        query_builder = query_builder.commit_transaction();

        let (query, _) = query_builder.build();
        println!("{}", query);

        // if let Err(err) = self.adapter.execute(&query).await {
        //     eprintln!("Failed to create tables: {:?}", err);
        //     return Err(err);
        // }

        Ok(())
    }
}

impl PgDestination {
    fn collect_referenced_tables<'a>(
        &self,
        table: &'a TableMetadata,
        visited: &mut HashSet<String>,
        query_builder: &mut SqlQueryBuilder,
    ) {
        if visited.contains(&table.name) {
            return;
        }

        for referenced_table in table.referenced_tables.values() {
            self.collect_referenced_tables(referenced_table, visited, query_builder);
        }

        let columns = self.get_columns(table);
        let foreign_keys = self.get_foreign_keys(table);

        *query_builder = query_builder
            .clone()
            .create_table(&table.name, &columns, &foreign_keys);

        visited.insert(table.name.clone());
    }

    fn get_columns(&self, metadata: &TableMetadata) -> Vec<ColumnInfo> {
        metadata
            .columns
            .iter()
            .map(|(name, col)| ColumnInfo {
                name: name.clone(),
                data_type: col.data_type.to_pg_string(),
                is_nullable: col.is_nullable,
                is_primary_key: metadata.primary_keys.contains(name),
                default: col.default_value.as_ref().map(ToString::to_string),
            })
            .collect::<Vec<_>>()
    }

    fn get_foreign_keys(&self, metadata: &TableMetadata) -> Vec<ForeignKeyInfo> {
        metadata
            .foreign_keys
            .iter()
            .map(|fk| ForeignKeyInfo {
                column: fk.column.clone(),
                referenced_table: fk.referenced_table.clone(),
                referenced_column: fk.referenced_column.clone(),
            })
            .collect::<Vec<_>>()
    }
}
