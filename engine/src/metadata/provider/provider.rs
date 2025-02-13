use crate::metadata::fk::ForeignKeyMetadata;
use async_trait::async_trait;
use sqlx::{Database, Error, Pool};

#[async_trait]
pub trait DbMetadataProvider {
    type DB: Database;

    async fn get_primary_key(table: &str, conn: &Pool<Self::DB>) -> Result<Vec<String>, Error>;
    async fn get_foreign_keys(
        table: &str,
        conn: &Pool<Self::DB>,
    ) -> Result<Vec<ForeignKeyMetadata>, Error>;
}
