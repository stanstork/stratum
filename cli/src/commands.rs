use clap::Subcommand;

#[derive(Subcommand)]
pub enum Commands {
    Migrate {
        #[arg(long, help = "Config file path")]
        config: String,
    },
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
}

#[derive(Subcommand)]
pub enum SourceCommand {
    Info {
        #[arg(long, help = "Config file path")]
        config: String,

        #[arg(short, long, help = "Show verbose metadata information")]
        verbose: bool,
    },
}
