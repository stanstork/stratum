use crate::{Cli, error::CliError};
use clap::{Subcommand, ValueEnum};
use engine_processing::EnvContext;
use model::execution::flags::IntegrityMode;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub mod apply;
pub mod plan;
pub mod test_conn;
pub mod verify;
pub mod version;

/// Sampling method for data preview
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum SampleMethod {
    /// Take first N rows (fastest, deterministic)
    #[default]
    First,
    /// Take random N rows (varied sample)
    Random,
    /// By specific IDs
    Id,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run dry-run migration and show results without making changes
    Plan {
        #[arg(
            short = 'c',
            long,
            help = "Path to SMQL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(
            long,
            short = 'o',
            help = "If specified, writes the report to this file instead of stdout"
        )]
        output: Option<String>,

        /// Enable sample data collection in the plan output
        #[arg(long, short = 's')]
        sample: bool,

        /// Number of rows to sample per pipeline (default: 5)
        #[arg(long, default_value = "5")]
        sample_size: usize,

        /// Sampling method: first (default) or random
        #[arg(long, value_enum, default_value = "first")]
        sample_method: SampleMethod,

        /// Name of the ID column for sampling
        #[arg(long, default_value = "id")]
        id_column: Option<String>,

        /// Specific IDs to sample (comma-separated)
        #[arg(long, value_delimiter = ',')]
        sample_ids: Option<Vec<String>>,

        /// Use exact COUNT for filtered rows (slower but accurate). By default uses EXPLAIN estimates (faster)
        #[arg(long)]
        exact_filter: bool,
    },
    /// Execute the migration
    Apply {
        #[arg(
            short = 'c',
            long,
            help = "Path to SMQL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(long, help = "Run with interactive TUI (experimental)")]
        tui: bool,

        #[arg(long, help = "Run with pretty colored output")]
        pretty: bool,

        /// Use exact COUNT for filtered rows (slower but accurate). By default uses EXPLAIN estimates (faster)
        #[arg(long)]
        exact_filter: bool,

        #[arg(long, help = "Compute integrity hashes and receipts during migration")]
        integrity: bool,

        #[arg(
            long,
            help = "Store individual row hashes in the receipt (implies --integrity). \
                    Enables row-level mismatch reporting during `verify` at the cost of \
                    ~32 bytes per row of additional storage."
        )]
        full_integrity: bool,
    },
    /// Verify migrated data matches source data
    Verify {
        #[arg(
            short = 'c',
            long,
            help = "Path to SMQL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(
            long,
            short = 'o',
            help = "If specified, writes the verification report to this file instead of stdout"
        )]
        output: Option<String>,
    },
    /// Test database connection
    TestConn {
        #[arg(
            long,
            help = "Connection URL (e.g., mysql://user:pass@host:3306/db or postgresql://user:pass@host:5432/db)"
        )]
        url: String,

        #[arg(
            long,
            help = "Database format (mysql, postgres). Auto-detected from URL if not specified"
        )]
        format: Option<String>,
    },
    /// Show version information
    Version,
}

/// Executes the appropriate command based on CLI arguments
pub async fn execute_command(
    cli: &Cli,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    match &cli.command {
        Commands::Plan { .. } => plan::execute(cli, &cli.command, env).await,
        Commands::Apply {
            config,
            tui,
            pretty,
            exact_filter,
            integrity,
            full_integrity,
        } => {
            let integrity_mode = if *full_integrity {
                IntegrityMode::FullHashes
            } else if *integrity {
                IntegrityMode::BatchHashes
            } else {
                IntegrityMode::Off
            };
            apply::execute(
                config.clone(),
                *tui,
                *pretty,
                *exact_filter,
                integrity_mode,
                cancel,
                env,
            )
            .await
        }
        Commands::Verify { config, output } => {
            verify::execute(config.clone(), output.clone(), env.clone()).await
        }
        Commands::TestConn { url, format } => {
            test_conn::execute(cli, url.clone(), format.clone()).await
        }
        Commands::Version => {
            version::execute();
            Ok(())
        }
    }
}
