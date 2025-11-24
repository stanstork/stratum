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
    Validate {
        #[arg(long, help = "Config file path")]
        config: String,

        #[arg(
            long,
            help = "Treat the config file as already parsed AST, skipping parsing step"
        )]
        from_ast: bool,

        #[arg(
            long,
            help = "If specified, writes the JSON report to this file instead of stdout"
        )]
        output: Option<String>,
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
    Progress {
        #[arg(long, help = "Run ID to inspect")]
        run: String,

        #[arg(long, help = "Item ID within the run")]
        item: String,

        #[arg(
            long,
            help = "If set, prints the progress information as JSON instead of a table"
        )]
        json: bool,
    },
}

#[derive(Subcommand)]
pub enum SourceCommand {
    Info {
        #[arg(short, long, help = "Connection string")]
        conn_str: String,

        #[arg(short, long, help = "Data format")]
        format: String,

        #[arg(
            long,
            help = "If specified, writes metadata to this file instead of stdout"
        )]
        output: Option<String>,
    },
}
