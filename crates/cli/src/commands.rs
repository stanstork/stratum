use clap::{Subcommand, ValueEnum};

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
        id_column: String,

        /// Specific IDs to sample (comma-separated)
        #[arg(long, value_delimiter = ',')]
        sample_ids: Option<Vec<String>>,

        /// Use exact COUNT for filtered rows (slower but accurate). By default uses EXPLAIN estimates (faster)
        #[arg(long)]
        exact_where: bool,
    },
    /// Execute the migration
    Apply {
        #[arg(
            short = 'c',
            long,
            help = "Path to SMQL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,
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
