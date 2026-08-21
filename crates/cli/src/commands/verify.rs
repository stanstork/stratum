use crate::{config, error::CliError};
use engine_processing::EnvContext;
use engine_verify::{error::VerifyError, verifier::verify};
use model::integrity::result::{DivergenceKind, VerificationResult};
use std::{
    fmt::Write as _,
    fs::File,
    io::{BufWriter, Write},
    sync::Arc,
};
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

    let mut writer = output
        .as_ref()
        .map(|path| File::create(path).map(BufWriter::new))
        .transpose()
        .map_err(CliError::ConfigFileRead)?;

    for result in &results {
        let formatted = format_result(result);

        println!("{formatted}");

        if let Some(w) = writer.as_mut() {
            writeln!(w, "{formatted}").map_err(CliError::ConfigFileRead)?;
        }
    }

    if let Some(mut w) = writer {
        w.flush().map_err(CliError::ConfigFileRead)?;
        info!(
            path = output.as_deref().unwrap_or_default(),
            "verification report written"
        );
    }

    let has_mismatch = results
        .iter()
        .any(|r| matches!(r, VerificationResult::Mismatch { .. }));

    if has_mismatch {
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
            "✓ {}/{} - match ({} rows, root {}, {}ms)",
            receipt.pipeline_name,
            receipt.table_name,
            receipt.total_rows,
            short_root(&receipt.table_root),
            duration_ms,
        ),
        VerificationResult::Mismatch {
            receipt,
            actual_root,
            summary,
            divergences,
            duration_ms,
        } => {
            let mut out = format!(
                "✗ {}/{} - MISMATCH ({} missing, {} changed, {} extra; \
                 {} rows expected, {} found; {}ms)\n  expected root {}\n  actual   root {}",
                receipt.pipeline_name,
                receipt.table_name,
                summary.missing,
                summary.changed,
                summary.extra,
                summary.expected_rows,
                summary.actual_rows,
                duration_ms,
                short_root(&receipt.table_root),
                short_root(actual_root),
            );

            for d in divergences {
                match &d.kind {
                    DivergenceKind::Missing { .. } => {
                        write!(out, "\n  {} - missing from destination", d.key).unwrap();
                    }
                    DivergenceKind::Extra { .. } => {
                        write!(out, "\n  {} - not in the migration", d.key).unwrap();
                    }
                    DivergenceKind::Changed {
                        expected_hash,
                        actual_hash,
                    } => {
                        write!(
                            out,
                            "\n  {} - changed: expected {} actual {}",
                            d.key,
                            short_root(expected_hash),
                            short_root(actual_hash)
                        )
                        .unwrap();
                    }
                }
            }

            let reported = divergences.len() as u64;
            let total = summary.missing + summary.changed + summary.extra;

            if total > reported {
                out.push_str(&format!("\n  ... and {} more", total - reported));
            }

            out
        }
        VerificationResult::NoPriorRun {
            pipeline: pipeline_name,
        } => {
            format!("? {pipeline_name} - no integrity receipt (run `apply --integrity` first)")
        }
    }
}

/// First 8 bytes of a 32-byte root - enough to compare by eye.
fn short_root(root: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(16);
    for byte in root.iter().take(8) {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
