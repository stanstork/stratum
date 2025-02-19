use super::Destination;
use crate::database::{
    connection::{DbConnection, PostgresConnection},
    operations::DbOperations,
    query::QueryBuilder,
    row::RowData,
    utils::pg_table_exists,
};
use async_trait::async_trait;
use std::collections::HashMap;

pub struct PgDestination {
    conn: PostgresConnection,
    table: String,
    column_mapping: HashMap<String, String>,
}

impl PgDestination {
    pub async fn new(
        url: &str,
        table: String,
        column_mapping: HashMap<String, String>,
    ) -> Result<Self, sqlx::Error> {
        let conn = DbConnection::connect(url).await?;
        Ok(Self {
            conn,
            table,
            column_mapping,
        })
    }

    /// Checks if the table exists in the database
    async fn ensure_table_exists(&self) -> Result<(), sqlx::Error> {
        if !self.conn.pool().table_exists(&self.table).await? {
            return Err(sqlx::Error::Configuration(
                format!("Table '{}' does not exist in the database", self.table).into(),
            ));
        }
        Ok(())
    }

    /// Maps row data into column names and values
    fn map_columns(&self, row: &RowData) -> Result<Vec<(String, String)>, sqlx::Error> {
        let columns = row
            .columns
            .iter()
            .map(|col| {
                let name = self.column_mapping.get(&col.name).ok_or_else(|| {
                    sqlx::Error::Configuration(format!("Column '{}' not found", col.name).into())
                })?;
                let value = col.value.clone().ok_or_else(|| {
                    sqlx::Error::Configuration(
                        format!("Null value for column '{}'", col.name).into(),
                    )
                })?;

                Ok((name.clone(), value.to_string()))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        Ok(columns)
    }
}

#[async_trait]
impl Destination for PgDestination {
    async fn write(&self, data: Vec<RowData>) -> Result<(), sqlx::Error> {
        self.ensure_table_exists().await?;
        for row in data.iter() {
            let columns = self.map_columns(row)?;
            let query = QueryBuilder::new()
                .insert_into(&self.table, &columns)
                .build();
            sqlx::query(&query.0).execute(self.conn.pool()).await?;
        }
        Ok(())
    }
}
