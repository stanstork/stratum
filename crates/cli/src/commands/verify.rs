use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::Arc,
};

use crate::{config, error::CliError};
use engine_processing::EnvContext;
use engine_verify::{error::VerifyError, verifier::verify};
use model::integrity::result::VerificationResult;
use tracing::info;

/// Executes the verify command (post-migration verification)
pub async fn execute(
    config_path: Option<String>,
    output: Option<String>,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let config_path = config::resolve_path(config_path)?;
    info!(config = %config_path, "verifying migrated data");

    let plan = config::load_plan(&config_path, false, env.clone()).await?;
    let results = verify(plan, env).await?;

    let mut writer: Option<BufWriter<File>> = if let Some(ref path) = output {
        let file = File::create(path).map_err(CliError::ConfigFileRead)?;
        Some(BufWriter::new(file))
    } else {
        None
    };

    let mut all_match = true;
    for result in &results {
        println!("{}", format_result(result));

        if let Some(ref mut w) = writer {
            writeln!(w, "{}", format_result(result)).map_err(CliError::ConfigFileRead)?;
        }

        if matches!(result, VerificationResult::Mismatch { .. }) {
            all_match = false;
        }
    }

    if let Some(ref mut w) = writer {
        w.flush().map_err(CliError::ConfigFileRead)?;
        info!(
            path = output.as_deref().unwrap_or(""),
            "verification report written"
        );
    }

    if !all_match {
        return Err(CliError::Verification(VerifyError::Mismatch));
    }

    Ok(())
}

/// Format a single `VerificationResult` as a human-readable string.
pub fn format_result(result: &VerificationResult) -> String {
    match result {
        VerificationResult::Match {
            receipt,
            duration_ms,
        } => format!(
            "✓ {}/{} - match ({} batches, {} rows, {}ms)",
            receipt.pipeline_name,
            receipt.table_name,
            receipt.batch_roots.len(),
            receipt.total_rows,
            duration_ms,
        ),
        VerificationResult::Mismatch {
            receipt,
            divergent_batches,
            duration_ms,
            ..
        } => {
            let mut out = format!(
                "✗ {}/{} - MISMATCH ({} divergent batches, {}ms)",
                receipt.pipeline_name,
                receipt.table_name,
                divergent_batches.len(),
                duration_ms,
            );
            for b in divergent_batches {
                out.push_str(&format!(
                    "\n  batch {} (rows {}-{}): expected {:02x?}... actual {:02x?}...",
                    b.batch_index,
                    b.row_start,
                    b.row_end,
                    &b.expected_root[..4],
                    &b.actual_root[..4],
                ));
                for r in &b.divergent_rows {
                    out.push_str(&format!(
                        "\n    row {}: expected {:02x?}... actual {:02x?}...",
                        r.row_index,
                        &r.expected_hash[..4],
                        &r.actual_hash[..4],
                    ));
                }
            }
            out
        }
        VerificationResult::NoPriorRun { pipeline_name } => format!(
            "? {} - no integrity receipt (run `apply --integrity` first)",
            pipeline_name
        ),
    }
}
