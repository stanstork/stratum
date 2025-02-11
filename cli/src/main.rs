use clap::Parser;
use engine::db::con::DbConnection;
use sqlx::{MySql, Pool, Postgres};

#[derive(Parser)]
struct Cli {
    #[arg(long, help = "MySQL connection URL")]
    mysql_url: String,
    #[arg(long, help = "PostgreSQL connection URL")]
    postgres_url: String,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let cli = Cli::parse();

    let mysql_pool: Pool<MySql> = DbConnection::connect(&cli.mysql_url).await?;
    let postgres_pool: Pool<Postgres> = DbConnection::connect(&cli.postgres_url).await?;

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
