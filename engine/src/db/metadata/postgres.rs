use super::{metadata::ForeignKeyMetadata, DbMetadataProvider};
use async_trait::async_trait;
use sqlx::{Pool, Postgres};

pub struct PostgresMetadataProvider {
    conn: Pool<Postgres>,
}

#[async_trait]
impl DbMetadataProvider for PostgresMetadataProvider {
    async fn get_primary_key(&self, table: &str) -> Result<Vec<String>, sqlx::Error> {
        unimplemented!()
    }

    async fn get_foreign_keys(&self, table: &str) -> Result<Vec<ForeignKeyMetadata>, sqlx::Error> {
        unimplemented!()
    }
}
