use engine::{config::Config, db::con::DbConnection};
use sqlx::{MySql, Pool, Postgres};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let config = Config::from_file(".cargo/config.toml").expect("Failed to load config file");

    let mysql_pool: Pool<MySql> = DbConnection::connect(&config.mysql_url()).await?;
    let postgres_pool: Pool<Postgres> = DbConnection::connect(&config.postgres_url()).await?;

    // Check if the connection is alive
    if mysql_pool.is_connected().await {
        println!("🔄 MySQL connection is alive!");
    } else {
        println!("⚠️ MySQL connection failed!");
    }

    // Check if the connection is alive
    if postgres_pool.is_connected().await {
        println!("🔄 PostgreSQL connection is alive!");
    } else {
        println!("⚠️ PostgreSQL connection failed!");
    }

    Ok(())
}
