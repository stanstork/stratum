use super::{metadata::ForeignKeyMetadata, DbMetadataProvider};
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool, Row};

pub struct MySqlMetadataProvider {
    conn: Pool<MySql>,
}

impl MySqlMetadataProvider {
    pub fn new(conn: Pool<MySql>) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl DbMetadataProvider for MySqlMetadataProvider {
    async fn get_primary_key(&self, table: &str) -> Result<Vec<String>, sqlx::Error> {
        let query = format!(
            "SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_NAME = {} 
            AND CONSTRAINT_NAME = 'PRIMARY'",
            table
        );

        let mut rows = sqlx::query(&query).fetch(&self.conn);
        let mut primary_keys = Vec::new();

        while let Some(row) = rows.try_next().await? {
            let column_name: String = row.get("COLUMN_NAME");
            primary_keys.push(column_name);
        }

        Ok(primary_keys)
    }

    async fn get_foreign_keys(&self, table: &str) -> Result<Vec<ForeignKeyMetadata>, sqlx::Error> {
        let query = format!(
            "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = {} AND REFERENCED_TABLE_NAME IS NOT NULL",
            table
        );

        let mut rows = sqlx::query(&query).fetch(&self.conn);
        let mut foreign_keys = Vec::new();

        while let Some(row) = rows.try_next().await? {
            let column_name: String = row.get("COLUMN_NAME");
            let foreign_table: String = row.get("REFERENCED_TABLE_NAME");
            let foreign_column: String = row.get("REFERENCED_COLUMN_NAME");

            foreign_keys.push(ForeignKeyMetadata {
                column: column_name,
                foreign_table,
                foreign_column,
            });
        }

        Ok(foreign_keys)
    }
}
