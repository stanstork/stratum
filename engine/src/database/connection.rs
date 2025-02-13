use async_trait::async_trait;
use sqlx::{Database, MySql, Pool, Postgres};

/// Connection trait for different database types
#[async_trait]
pub trait DbConnection {
    type DB: Database;

    async fn connect(url: &str) -> Result<Self, sqlx::Error>
    where
        Self: Sized;

    fn pool(&self) -> &Pool<Self::DB>;
}

pub struct MySqlConnection {
    /// Connection pool for MySQL
    pool: Pool<MySql>,
}

pub struct PostgresConnection {
    /// Connection pool for PostgreSQL
    pool: Pool<Postgres>,
}

#[async_trait]
impl DbConnection for MySqlConnection {
    type DB = MySql;

    async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = Pool::connect(url).await?;
        Ok(MySqlConnection { pool })
    }

    fn pool(&self) -> &Pool<Self::DB> {
        &self.pool
    }
}

#[async_trait]
impl DbConnection for PostgresConnection {
    type DB = Postgres;

    async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = Pool::connect(url).await?;
        Ok(PostgresConnection { pool })
    }

    fn pool(&self) -> &Pool<Self::DB> {
        &self.pool
    }
}
