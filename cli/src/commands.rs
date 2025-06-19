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
    /// Test a connection string against a given format
    TestConn {
        /// Data format: "mysql", "pg", "ftp", …
        #[arg(long)]
        format: String,

        /// Connection string or address
        #[arg(long)]
        conn_str: String,
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
