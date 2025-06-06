use clap::Subcommand;

#[derive(Subcommand)]
pub enum Commands {
    Migrate {
        #[arg(long, help = "Config file path")]
        config: String,

        #[arg(
            long,
            help = "Treat the config file as already parsed AST, skipping parsing step"
        )]
        from_ast: bool,
    },
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    Ast {
        #[arg(long, help = "Config file path")]
        config: String,
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
