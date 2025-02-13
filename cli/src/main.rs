use clap::Parser;
use engine::{
    config::{self, read_config},
    db::{
        conn::DbConnection,
        metadata::metadata::TableMetadata,
        source::{DataSource, MySqlDataSource},
    },
};
use sqlx::{MySql, Pool};

#[derive(Parser)]
struct Cli {
    #[arg(long, help = "Config file path")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let cli = Cli::parse();
    let config = read_config(&cli.config).expect("Failed to read config file");
    let metadata =
        TableMetadata::from_mapping(config.mappings()[0].clone(), config.source()).await?;

    println!("{:#?}", metadata);

    Ok(())
}
