use clap::Parser;
use commands::Commands;
use engine::{
    config::config::{read_config, Config},
    destination::{data_dest::DataDestination, postgres::PgDestination},
    source::{
        data_source::{create_data_source, DataSource, DataSourceType},
        record::DataRecord,
    },
};
use sql_adapter::db_type::DbType;
use tracing::Level;
pub mod commands;

#[derive(Parser)]
#[command(name = "stratum", version = "0.0.1", about = "Data migration tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Migrate { config } => {
            let config = read_config(&config).expect("Failed to read config file");

            let source = init_source(&config).await?;
            let data = source.fetch_data().await?;

            // Print data
            for record in data.iter() {
                println!("{}", record.debug());
            }

            // Initialize destination
            let dest = init_destination(&config).await?;

            match dest.write(data).await {
                Ok(_) => println!("Data written successfully"),
                Err(e) => println!("Error writing data: {:?}", e),
            }
        }
    }

    Ok(())
}

async fn init_source(
    config: &Config,
) -> Result<Box<dyn DataSource<Record = Box<dyn DataRecord>>>, Box<dyn std::error::Error>> {
    match create_data_source(DataSourceType::Database(DbType::MySql), &config).await {
        Ok(source) => return Ok(source),
        Err(e) => return Err(e),
    }
}

async fn init_destination(config: &Config) -> Result<PgDestination, Box<dyn std::error::Error>> {
    let mapping = config.mappings.first().unwrap();
    let columns = mapping.columns.clone().unwrap();

    match PgDestination::new(config.dest(), mapping.table.clone(), columns).await {
        Ok(dest) => return Ok(dest),
        Err(e) => return Err(e),
    }
}
