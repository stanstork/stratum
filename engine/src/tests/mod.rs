#![allow(dead_code)]

use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, Executor, MySqlPool, PgPool};
use std::{env, fs, io, path::PathBuf};

pub mod integration;
pub mod utils;

// Test database URLs
const TEST_MYSQL_URL_SAKILA: &str = "mysql://sakila_user:qwerty123@localhost:3306/sakila";
const TEST_MYSQL_URL_ORDERS: &str = "mysql://user:password@localhost:3306/testdb";
const TEST_PG_URL: &str = "postgres://user:password@localhost:5432/testdb";

async fn mysql_pool(source_db: &str) -> MySqlPool {
    MySqlPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(match source_db {
            "sakila" => TEST_MYSQL_URL_SAKILA,
            "orders" => TEST_MYSQL_URL_ORDERS,
            _ => panic!("Unknown source database: {source_db}"),
        })
        .await
        .expect("connect mysql")
}

async fn pg_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(TEST_PG_URL)
        .await
        .expect("connect postgres")
}

/// Drop & recreate the public schema in Postgres so it's empty.
async fn reset_postgres_schema() {
    let pool = pg_pool().await;
    // This will drop all tables, types, etc. in `public` and re-create it.
    pool.execute(
        r#"
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
    "#,
    )
    .await
    .expect("reset postgres schema");
}

/// Remove all on-disk Sled buffers named `migration_buffer_<…>`
/// in the current directory.
pub fn reset_migration_buffer() -> io::Result<()> {
    // Get working dir
    let base: PathBuf = env::current_dir()?;

    // Read every entry in that dir
    for entry in fs::read_dir(&base)? {
        let entry = entry?;
        let path = entry.path();

        // Check if it's a directory whose name starts with "migration_buffer_"
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("migration_buffer_") {
                    // Recursively delete it
                    fs::remove_dir_all(&path)?;
                    println!("Removed buffer directory: {}", path.display());
                }
            }
        }
    }

    Ok(())
}
