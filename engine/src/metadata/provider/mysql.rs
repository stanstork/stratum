use super::base::DbMetadataProvider;
use crate::metadata::foreign_key::ForeignKeyMetadata;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool, Row};

pub struct MySqlMetadataProvider;

#[async_trait]
impl DbMetadataProvider for MySqlMetadataProvider {
    type DB = MySql;

    async fn get_primary_key(table: &str, conn: &Pool<MySql>) -> Result<Vec<String>, sqlx::Error> {
        let query = format!(
            "SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_NAME = '{}' 
            AND CONSTRAINT_NAME = 'PRIMARY'",
            table
        );

        let mut rows = sqlx::query(&query).fetch(conn);
        let mut primary_keys = Vec::new();

        while let Some(row) = rows.try_next().await? {
            let column_name: String = row.get("COLUMN_NAME");
            primary_keys.push(column_name);
        }

        Ok(primary_keys)
    }

    async fn get_foreign_keys(
        table: &str,
        conn: &Pool<MySql>,
    ) -> Result<Vec<ForeignKeyMetadata>, sqlx::Error> {
        let query = format!(
            "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = '{}' AND REFERENCED_TABLE_NAME IS NOT NULL",
            table
        );

        let mut rows = sqlx::query(&query).fetch(conn);
        let mut foreign_keys = Vec::new();

        while let Some(row) = rows.try_next().await? {
            let column_name: String = row.get("COLUMN_NAME");
            let referenced_table: String = row.get("REFERENCED_TABLE_NAME");
            let referenced_column: String = row.get("REFERENCED_COLUMN_NAME");

            foreign_keys.push(ForeignKeyMetadata {
                column: column_name,
                referenced_table,
                referenced_column,
            });
        }

        Ok(foreign_keys)
    }
}
