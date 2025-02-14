use clap::Parser;
use engine::{
    config::config::read_config,
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

    for mapping in config.mappings.iter() {
        let mysql_source = MySqlDataSource::new(config.source(), (*mapping).clone()).await?;
        let pipeline = TransformPipeline::from_mapping(mapping.transform.clone());
        let data = mysql_source.fetch_data().await?;

        for row in data.iter() {
            let transformed = pipeline.apply(row);
            println!("{:#?}", transformed);
        }
    }

    Ok(())
}
