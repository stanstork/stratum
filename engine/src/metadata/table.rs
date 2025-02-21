use super::{column::ColumnMetadata, foreign_key::ForeignKeyMetadata};
use crate::{
    config::mapping::TableMapping,
    database::{
        managers::{base::DbManager, mysql::MySqlManager},
        query::loader::QueryLoader,
    },
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct TableMetadata {
    pub name: String,
    pub schema: Option<String>,
    pub columns: HashMap<String, ColumnMetadata>,
    pub primary_keys: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
}

impl TableMetadata {
    pub async fn from_mapping(
        mapping: TableMapping,
        manager: &MySqlManager,
    ) -> Result<Self, sqlx::Error> {
        if let Ok(query) = QueryLoader::table_metadata_query() {
            let table_name = mapping.table.clone();
            let rows = sqlx::query(&query)
                .bind(table_name.clone())
                .bind(table_name.clone())
                .bind(table_name.clone())
                .bind(table_name.clone())
                .fetch_all(manager.pool())
                .await?;

            let mut columns = Vec::new();
            let mut primary_keys = Vec::new();
            let mut foreign_keys = Vec::new();

            for row in rows.iter() {
                let column = ColumnMetadata::from(row);
                if column.is_primary_key {
                    primary_keys.push(column.name.clone());
                }
                if column.referenced_table.is_some() {
                    let ref_table = column.referenced_table.clone().unwrap();
                    let fk_metadata = ForeignKeyMetadata {
                        column: column.name.clone(),
                        foreign_table: ref_table,
                        foreign_column: column.referenced_column.clone().unwrap(),
                    };
                    foreign_keys.push(fk_metadata);
                }
                columns.push(column.clone());
            }

            return Ok(Self {
                name: mapping.table.clone(),
                schema: None,
                columns: columns
                    .into_iter()
                    .map(|col| (col.name.clone(), col))
                    .collect(),
                primary_keys,
                foreign_keys,
            });
        }

        Err(sqlx::Error::Configuration(
            "Table metadata query not found".into(),
        ))
    }
}
