use crate::database::{
    column::ColumnMetadata,
    connection::{DbConnection, MySqlConnection},
    mapping::TableMapping,
};
use mysql::MySqlMetadataProvider;
use provider::DbMetadataProvider;
use sqlx::{Column, Row};

pub mod mysql;
pub mod postgres;
pub mod provider;

#[derive(Debug)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
}

#[derive(Debug)]
pub struct ForeignKeyMetadata {
    pub column: String,
    pub foreign_table: String,
    pub foreign_column: String,
}

impl TableMetadata {
    pub async fn from_mapping(
        mapping: TableMapping,
        conn: &MySqlConnection,
    ) -> Result<Self, sqlx::Error> {
        let query = format!("SELECT * FROM {} LIMIT 1", mapping.table);
        let row = sqlx::query(&query).fetch_one(conn.pool()).await?;

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
            primary_key: MySqlMetadataProvider::get_primary_key(&mapping.table, conn.pool())
                .await?,
            foreign_keys: MySqlMetadataProvider::get_foreign_keys(&mapping.table, conn.pool())
                .await?,
        })
    }
}
