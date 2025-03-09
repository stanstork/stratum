use super::data_dest::DataDestination;
use crate::source::record::DataRecord;
use async_trait::async_trait;
use sql_adapter::{
    adapter::DbAdapter, postgres::PgAdapter, query::builder::SqlQueryBuilder, row::row::RowData,
};
use std::collections::HashMap;
use tracing::error;

pub struct PgDestination {
    manager: PgAdapter,
    table: String,
    column_mapping: HashMap<String, String>,
}

impl PgDestination {
    pub async fn new(
        url: &str,
        table: String,
        column_mapping: HashMap<String, String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let manager = PgAdapter::connect(url).await?;
        Ok(Self {
            manager,
            table,
            column_mapping,
        })
    }

    /// Checks if the table exists in the database
    async fn ensure_table_exists(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.manager.table_exists(&self.table).await? {
            return Err("Table does not exist".into());
        }
        Ok(())
    }

    /// Maps row data into column names and values
    fn map_columns(&self, row: &Box<dyn DataRecord>) -> Result<Vec<(String, String)>, sqlx::Error> {
        let row = row
            .as_any()
            .downcast_ref::<RowData>()
            .ok_or_else(|| sqlx::Error::Configuration("Invalid row data type".into()))?;

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
impl DataDestination for PgDestination {
    type Record = Box<dyn DataRecord>;

    async fn write(
        &self,
        data: Vec<Box<dyn DataRecord>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.ensure_table_exists().await?;

        // To simplify testing, we truncate the table before writing the data
        // In a real-world scenario, you would likely want to append the data
        self.manager.truncate_table(&self.table).await?;

        for (i, row) in data.iter().enumerate() {
            let columns = match self.map_columns(row) {
                Ok(cols) => cols,
                Err(e) => {
                    error!(
                        "Failed to map columns for row {} in table '{}': {:?}",
                        i, self.table, e
                    );
                    continue;
                }
            };

            let query = SqlQueryBuilder::new()
                .insert_into(&self.table, &columns)
                .build();

            if let Err(err) = self.manager.execute(&query.0).await {
                error!(
                    "Error writing row {} to table '{}': {:?}",
                    i, self.table, err
                );
            }
        }
        Ok(())
    }
}
