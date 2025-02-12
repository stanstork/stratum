use async_trait::async_trait;
use metadata::ForeignKeyMetadata;
use mysql::MySqlMetadataProvider;
use postgres::PostgresMetadataProvider;
use sqlx::Error;

pub mod metadata;
pub mod mysql;
pub mod postgres;

#[async_trait]
pub trait DbMetadataProvider {
    async fn get_primary_key(&self, table: &str) -> Result<Vec<String>, Error>;
    async fn get_foreign_keys(&self, table: &str) -> Result<Vec<ForeignKeyMetadata>, Error>;
}

pub enum DatabaseType {
    MySql(MySqlMetadataProvider),
    Postgres(PostgresMetadataProvider),
}

impl DatabaseType {
    pub async fn get_primary_keys(&self, table: &str) -> Result<Vec<String>, sqlx::Error> {
        match self {
            DatabaseType::MySql(provider) => provider.get_primary_key(table).await,
            DatabaseType::Postgres(provider) => provider.get_primary_key(table).await,
        }
    }

    pub async fn get_foreign_keys(
        &self,
        table: &str,
    ) -> Result<Vec<ForeignKeyMetadata>, sqlx::Error> {
        match self {
            DatabaseType::MySql(provider) => provider.get_foreign_keys(table).await,
            DatabaseType::Postgres(provider) => provider.get_foreign_keys(table).await,
        }
    }
}
