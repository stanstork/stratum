#![allow(dead_code)]

use connectors::sql::postgres::utils::connect_client;
use mysql_async::Pool;
use std::sync::Arc;
use tokio_postgres::Client;

pub mod integration;
pub mod utils;

// Test database URLs
const TEST_MYSQL_URL_SAKILA: &str = "mysql://sakila_user:qwerty123@localhost:3306/sakila";
const TEST_MYSQL_URL_ORDERS: &str = "mysql://user:password@localhost:3306/testdb";
const TEST_PG_URL: &str = "postgres://user:password@localhost:5432/testdb";

async fn mysql_pool(source_db: &str) -> Pool {
    Pool::from_url(match source_db {
        "sakila" => TEST_MYSQL_URL_SAKILA,
        "orders" => TEST_MYSQL_URL_ORDERS,
        _ => panic!("Unknown source database: {source_db}"),
    })
    .expect("connect mysql")
}

async fn pg_pool() -> Arc<Client> {
    let client = Arc::new(connect_client(TEST_PG_URL).await.expect("connect postgres"));
    client
}

/// Drop & recreate the public schema in Postgres so it's empty.
async fn reset_postgres_schema() {
    let pool = pg_pool().await;
    // This will drop all tables, types, etc. in `public` and re-create it.
    pool.batch_execute(
        r#"
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
    "#,
    )
    .await
    .expect("reset postgres schema");
}
