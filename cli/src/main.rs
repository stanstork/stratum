use clap::Parser;
use engine::{
    config::config::read_config,
    destination::{postgres::PgDestination, Destination},
    source::{datasource::DataSource, providers::mysql::MySqlDataSource},
    transform::pipeline::TransformPipeline,
};

#[derive(Parser)]
struct Cli {
    #[arg(long, help = "Config file path")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let cli = Cli::parse();
    let config = read_config(&cli.config).expect("Failed to read config file");

    for mapping in &config.mappings {
        let table = mapping.table.clone();

        // Initialize MySQL source
        let mysql_source = MySqlDataSource::new(config.source(), mapping.clone()).await?;

        // Create transformation pipeline
        let pipeline = TransformPipeline::from_mapping(mapping.transform.clone());

        // Fetch and transform data
        let data = mysql_source.fetch_data().await?;
        let transformed_data: Vec<_> = data.iter().map(|row| pipeline.apply(row)).collect();

        // Initialize Postgres destination
        let dest = PgDestination::new(config.destination(), table.clone(), mapping.columns.clone())
            .await?;

        // Attempt to write data and handle errors
        match dest.write(transformed_data).await {
            Ok(_) => println!(
                "Data successfully written to destination: {}",
                table.clone()
            ),
            Err(err) => eprintln!("Failed to write data to {}: {}", table.clone(), err),
        }
    }

    Ok(())
}
