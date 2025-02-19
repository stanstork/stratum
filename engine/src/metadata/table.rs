use super::{
    column::ColumnMetadata,
    foreign_key::ForeignKeyMetadata,
    provider::{base::DbMetadataProvider, mysql::MySqlMetadataProvider},
};
use crate::{
    config::mapping::TableMapping,
    database::managers::{base::DbManager, mysql::MySqlManager},
};
use sqlx::{Column, Row};

#[derive(Debug)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
}

impl TableMetadata {
    pub async fn from_mapping(
        mapping: TableMapping,
        manager: &MySqlManager,
    ) -> Result<Self, sqlx::Error> {
        let query = format!("SELECT * FROM {} LIMIT 1", mapping.table);
        let row = sqlx::query(&query).fetch_one(manager.pool()).await?;

        let columns = row
            .columns()
            .iter()
            .filter_map(|col| {
                mapping
                    .columns
                    .get(col.name())
                    .map(|_| ColumnMetadata::from(col))
            })
            .collect();

        Ok(Self {
            name: mapping.table.clone(),
            columns,
            primary_key: MySqlMetadataProvider::get_primary_key(&mapping.table, manager.pool())
                .await?,
            foreign_keys: MySqlMetadataProvider::get_foreign_keys(&mapping.table, manager.pool())
                .await?,
        })
    }
}
