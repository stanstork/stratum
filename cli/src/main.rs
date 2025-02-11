use clap::Parser;
use engine::db::source::{DataSource, MySqlDataSource};

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

    let data_source = MySqlDataSource::new(&cli.mysql_url).await?;
    let query = "SELECT * FROM products";
    let data = data_source.fetch_data(query).await?;

    for row in data {
        println!("{}", row);
    }

    Ok(())
}
