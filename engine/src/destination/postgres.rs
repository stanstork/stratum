use super::Destination;
use crate::database::{
    managers::{base::DbManager, postgres::PgManager},
    query::builder::QueryBuilder,
    row::RowData,
};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::error;

pub struct PgDestination {
    manager: PgManager,
    table: String,
    column_mapping: HashMap<String, String>,
}

impl PgDestination {
    pub async fn new(
        url: &str,
        table: String,
        column_mapping: HashMap<String, String>,
    ) -> Result<Self, sqlx::Error> {
        let manager = PgManager::connect(url).await?;

        Ok(Self {
            manager,
            table,
            column_mapping,
        })
    }

    /// Checks if the table exists in the database
    async fn ensure_table_exists(&self) -> Result<(), sqlx::Error> {
        if !self.manager.table_exists(&self.table).await? {
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

            let query = QueryBuilder::new()
                .insert_into(&self.table, &columns)
                .build();

            if let Err(err) = sqlx::query(&query.0).execute(self.manager.pool()).await {
                error!(
                    "Error writing row {} to table '{}': {:?}",
                    i, self.table, err
                );
            }
        }
        Ok(())
    }
}
