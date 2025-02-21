use clap::Parser;
use commands::Commands;
use engine::{
    config::config::read_config,
    destination::{postgres::PgDestination, Destination},
    source::{datasource::DataSource, providers::mysql::MySqlDataSource},
    transform::pipeline::TransformPipeline,
};
use tracing::{error, info, Level};
pub mod commands;

#[derive(Parser)]
#[command(name = "stratum", version = "0.0.1", about = "Data migration tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // Initialize logger
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Migrate { config } => {
            init_source(&config).await?;
        }
    }

    Ok(())
}

async fn init_source(config_path: &str) -> Result<(), sqlx::Error> {
    println!("Reading config file: {}", config_path);
    let config = read_config(config_path).expect("Failed to read config file");

    println!("Config: {:#?}", config);

    for mapping in config.mappings.iter() {
        let table = mapping.table.clone();
        let source = MySqlDataSource::new(config.source(), mapping.clone()).await?;

        println!("Table: {}", table);
        println!("Metadata: {:#?}", source.metadata());
    }

    Ok(())
}

// async fn run_migration(config_path: &str) -> Result<(), sqlx::Error> {
//     info!("Reading config file: {}", config_path);
//     let config = read_config(config_path).expect("Failed to read config file");

//     for mapping in &config.mappings {
//         let table = mapping.table.clone();
//         info!("Starting migration for table: {}", table);

//         // Initialize MySQL source
//         info!("Initializing MySQL source for table: {}", table);
//         let mysql_source = match MySqlDataSource::new(config.source(), mapping.clone()).await {
//             Ok(source) => source,
//             Err(e) => {
//                 error!("Failed to initialize MySQL source for {}: {}", table, e);
//                 continue;
//             }
//         };

//         // Create transformation pipeline
//         info!("Creating transformation pipeline for table: {}", table);
//         let pipeline = TransformPipeline::from_mapping(mapping.transform.clone());

//         // Fetch and transform data
//         info!("Fetching data from MySQL for table: {}", table);
//         let data = match mysql_source.fetch_data().await {
//             Ok(d) => d,
//             Err(e) => {
//                 error!("Failed to fetch data for {}: {}", table, e);
//                 continue;
//             }
//         };

//         info!("Transforming {} rows for table: {}", data.len(), table);
//         let transformed_data: Vec<_> = data.iter().map(|row| pipeline.apply(row)).collect();

//         // Initialize Postgres destination
//         info!("Initializing Postgres destination for table: {}", table);
//         let dest =
//             match PgDestination::new(config.destination(), table.clone(), mapping.columns.clone())
//                 .await
//             {
//                 Ok(d) => d,
//                 Err(e) => {
//                     error!(
//                         "Failed to initialize Postgres destination for {}: {}",
//                         table, e
//                     );
//                     continue;
//                 }
//             };

//         // Attempt to write data and handle errors
//         info!(
//             "Writing {} transformed rows to table: {}",
//             transformed_data.len(),
//             table
//         );
//         match dest.write(transformed_data).await {
//             Ok(_) => info!("Data successfully written to destination: {}", table),
//             Err(err) => error!("Failed to write data to {}: {}", table, err),
//         }
//     }

//     info!("Migration process completed.");
//     Ok(())
// }
