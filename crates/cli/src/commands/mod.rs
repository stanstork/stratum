use crate::{Cli, error::CliError};
use clap::{Subcommand, ValueEnum};
use engine_infra::shutdown::ShutdownSignal;
use engine_processing::EnvContext;
use engine_state::sled_store::SledStateStore;
use model::execution::flags::IntegrityMode;
use std::{path::PathBuf, sync::Arc};

pub mod apply;
pub mod pause;
pub mod plan;
pub mod reset;
pub mod resume;
pub mod status;
pub mod test_conn;
pub mod verify;
pub mod version;

const STATE_DIR: &str = ".stratum/state";

/// Returns the path to the state directory (~/.stratum/state/).
pub fn state_dir() -> Result<PathBuf, CliError> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::Unknown("Could not determine home directory".to_string()))?;
    Ok(home.join(STATE_DIR))
}

/// Opens the sled state store from the default location.
pub fn open_state_store() -> Result<SledStateStore, CliError> {
    let path = state_dir()?;
    SledStateStore::open(&path).map_err(|e| {
        CliError::Unknown(format!(
            "Failed to open state store at {}: {e}",
            path.display()
        ))
    })
}

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
    /// Show migration run status
    Status {
        #[arg(
            short = 'c',
            long,
            help = "Path to SMQL config file. If provided, shows status for that migration only"
        )]
        config: Option<String>,
    },
    /// Resume a previously paused migration
    Resume {
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

        #[arg(long, help = "Compute integrity hashes and receipts during migration")]
        integrity: bool,

        #[arg(
            long,
            help = "Store individual row hashes in the receipt (implies --integrity)"
        )]
        full_integrity: bool,
    },
    /// Clear all state for a migration (checkpoints, WAL, run state)
    Reset {
        #[arg(
            short = 'c',
            long,
            help = "Path to SMQL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(long, help = "Skip confirmation prompt")]
        force: bool,
    },
    /// Send pause signal to a running migration
    Pause {
        #[arg(short = 'c', long, help = "Path to SMQL config file")]
        config: String,
    },
    /// Show version information
    Version,
}

/// Executes the appropriate command based on CLI arguments
pub async fn execute_command(
    cli: &Cli,
    shutdown: ShutdownSignal,
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
            let integrity_mode = IntegrityMode::new(*integrity, *full_integrity);
            apply::execute(
                config.clone(),
                *tui,
                *pretty,
                *exact_filter,
                integrity_mode,
                shutdown,
                env,
            )
            .await
        }
        Commands::Verify { config, output } => {
            verify::execute(config.clone(), output.clone(), env.clone()).await
        }
        Commands::Status { config } => status::execute(config.clone(), env).await,
        Commands::TestConn { url, format } => {
            test_conn::execute(cli, url.clone(), format.clone()).await
        }
        Commands::Version => {
            version::execute();
            Ok(())
        }
        Commands::Resume {
            config,
            tui,
            pretty,
            integrity,
            full_integrity,
        } => {
            let integrity_mode = IntegrityMode::new(*integrity, *full_integrity);
            resume::execute(config.clone(), *tui, *pretty, integrity_mode, shutdown, env).await
        }
        Commands::Reset { config, force } => reset::execute(config.clone(), *force, env).await,
        Commands::Pause { config } => pause::execute(Some(config.clone()), env).await,
    }
}
