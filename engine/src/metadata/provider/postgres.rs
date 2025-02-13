use super::base::DbMetadataProvider;
use crate::metadata::foreign_key::ForeignKeyMetadata;
use async_trait::async_trait;
use sqlx::{Pool, Postgres};

pub struct PostgresMetadataProvider {
    conn: Pool<Postgres>,
}

#[async_trait]
impl DbMetadataProvider for PostgresMetadataProvider {
    type DB = Postgres;

    async fn get_primary_key(
        table: &str,
        conn: &Pool<Postgres>,
    ) -> Result<Vec<String>, sqlx::Error> {
        unimplemented!()
    }

    async fn get_foreign_keys(
        table: &str,
        conn: &Pool<Postgres>,
    ) -> Result<Vec<ForeignKeyMetadata>, sqlx::Error> {
        unimplemented!()
    }
}
