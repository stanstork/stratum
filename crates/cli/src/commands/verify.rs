use crate::{args::Cli, config, error::CliError};
use crossterm::execute;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use engine_processing::EnvContext;
use engine_verify::{VerifyProgress, error::VerifyError, verifier::verify_with_progress};
use model::integrity::result::{DivergenceKind, VerificationResult};
use std::{
    fmt::Write as _,
    fs::File,
    io::{self, BufWriter, IsTerminal, Stdout, Write, stdout},
    sync::Arc,
};
use tracing::info;

/// Executes the verify command (post-migration verification)
pub async fn execute(
    cli: &Cli,
    config_path: Option<String>,
    output: Option<String>,
    pretty: bool,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let config_path = config::resolve_path(config_path)?;
    info!(config = %config_path, "verifying migrated data");

    let plan = config::load_plan(&config_path, false, env.clone()).await?;

    let color = pretty && !cli.no_color && stdout().is_terminal();
    let mut printer = VerifyPrinter {
        out: stdout(),
        pretty,
        color,
    };

    printer.header(&config_path);

    let results = verify_with_progress(plan, env, &mut printer).await?;

    if let Some(path) = output.as_ref() {
        write_report(path, &results)?;
        info!(path = %path, "verification report written");
    }

    printer.summary(&results);

    // Check for fatal conditions
    if results
        .iter()
        .any(|r| matches!(r, VerificationResult::Mismatch { .. }))
    {
        return Err(CliError::Verification(VerifyError::Mismatch));
    }

    // A receipt with no diffable log can't confirm the destination; surface it
    // as a non-zero exit rather than a silent pass.
    if results
        .iter()
        .any(|r| matches!(r, VerificationResult::LogUnavailable { .. }))
    {
        return Err(CliError::Verification(VerifyError::Inconclusive));
    }

    Ok(())
}

/// Writes the plain-text report (no color) to `path`.
fn write_report(path: &str, results: &[VerificationResult]) -> Result<(), CliError> {
    let file = File::create(path).map_err(CliError::ConfigFileRead)?;
    let mut writer = BufWriter::new(file);

    for result in results {
        writeln!(writer, "{}", format_result(result)).map_err(CliError::ConfigFileRead)?;
    }

    writer.flush().map_err(CliError::ConfigFileRead)
}

/// Streams verification progress and results to a sink, table by table.
struct VerifyPrinter<W: Write = Stdout> {
    out: W,
    pretty: bool,
    color: bool,
}

impl<W: Write> VerifyPrinter<W> {
    fn header(&mut self, config_path: &str) {
        if self.pretty {
            let _ = self.line(Color::Cyan, &format!("◆ Verifying: {config_path}"));
        }
    }

    /// One line, best-effort (a broken pipe must not fail verification). Colored
    /// only when `color` is set; otherwise written verbatim.
    fn line(&mut self, color: Color, text: &str) -> io::Result<()> {
        if self.color {
            execute!(
                self.out,
                SetForegroundColor(color),
                Print(text),
                ResetColor,
                Print("\n")
            )?;
        } else {
            writeln!(self.out, "{text}")?;
        }
        self.out.flush()
    }

    /// Final tally across all tables. `--pretty` only.
    fn summary(&mut self, results: &[VerificationResult]) {
        if !self.pretty {
            return;
        }

        let (mut matched, mut mismatched, mut inconclusive, mut no_receipt) = (0, 0, 0, 0);

        for r in results {
            match r {
                VerificationResult::Match { .. } => matched += 1,
                VerificationResult::Mismatch { .. } => mismatched += 1,
                VerificationResult::LogUnavailable { .. } => inconclusive += 1,
                VerificationResult::NoPriorRun { .. } => no_receipt += 1,
            }
        }

        let mut parts = vec![format!("{matched} matched")];
        if mismatched > 0 {
            parts.push(format!("{mismatched} mismatched"));
        }
        if inconclusive > 0 {
            parts.push(format!("{inconclusive} inconclusive"));
        }
        if no_receipt > 0 {
            parts.push(format!("{no_receipt} without a receipt"));
        }

        let ok = mismatched == 0 && inconclusive == 0;
        let color = if ok { Color::Green } else { Color::Red };
        let symbol = if ok { "✓" } else { "✗" };

        let _ = self.line(color, &format!("{symbol} {}", parts.join(", ")));
    }
}

impl<W: Write + Send> VerifyProgress for VerifyPrinter<W> {
    fn table_started(&mut self, pipeline: &str, table: &str) {
        if self.pretty {
            let _ = self.line(Color::Yellow, &format!("⧗ {pipeline}/{table}"));
        }
    }

    fn table_phase(&mut self, phase: &str) {
        if self.pretty {
            let _ = self.line(Color::DarkGrey, &format!("    {phase}…"));
        }
    }

    fn table_finished(&mut self, result: &VerificationResult) {
        let _ = self.line(result_color(result), &format_result(result));
    }
}

/// The color for a result's status, applied only in `--pretty` output.
fn result_color(result: &VerificationResult) -> Color {
    match result {
        VerificationResult::Match { .. } => Color::Green,
        VerificationResult::Mismatch { .. } => Color::Red,
        VerificationResult::LogUnavailable { .. } => Color::Yellow,
        VerificationResult::NoPriorRun { .. } => Color::DarkGrey,
    }
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
            commas(receipt.total_rows),
            short_root(&receipt.table_root),
            commas(*duration_ms),
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
                commas(summary.missing),
                commas(summary.changed),
                commas(summary.extra),
                commas(summary.expected_rows),
                commas(summary.actual_rows),
                commas(*duration_ms),
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
        VerificationResult::LogUnavailable {
            pipeline,
            table,
            expected_rows,
            found_rows,
        } => {
            format!(
                "? {pipeline}/{table} - INCONCLUSIVE: row-hash log is missing or truncated \
                 ({found_rows} of {expected_rows} committed rows on disk); re-run \
                 `apply --integrity` before verifying"
            )
        }
    }
}

/// Thousands-separated integer, e.g. 1234567 -> "1,234,567".
fn commas(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);

    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use model::integrity::{
        algorithm::HashAlgorithm,
        result::{Divergence, DivergenceKind, DivergenceSummary},
    };

    fn receipt(rows: u64) -> model::integrity::receipt::VerificationReceipt {
        model::integrity::receipt::VerificationReceipt {
            run_id: "run-1".into(),
            pipeline_name: "migrate_actor".into(),
            table_name: "actor".into(),
            table_root: [0xab; 32],
            column_order: vec!["actor_id".into()],
            key_columns: vec!["actor_id".into()],
            total_rows: rows,
            skipped_rows: 0,
            algorithm: HashAlgorithm::Sha256,
            created_at: chrono::Utc::now(),
        }
    }

    /// Render a sequence of progress calls through an in-memory sink, as the CLI
    /// would drive it. `pretty` toggles the decorated output; color is always off
    /// here so assertions read the plain text.
    fn render(pretty: bool, f: impl FnOnce(&mut VerifyPrinter<&mut Vec<u8>>)) -> String {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut printer = VerifyPrinter {
                out: &mut buf,
                pretty,
                color: false,
            };
            f(&mut printer);
        }
        String::from_utf8(buf).expect("utf8")
    }

    fn drive(p: &mut VerifyPrinter<&mut Vec<u8>>, results: &[VerificationResult]) {
        p.header("migration.ppl");
        for r in results {
            if let VerificationResult::Match { receipt, .. } = r {
                p.table_started(&receipt.pipeline_name, &receipt.table_name);
                p.table_phase("reading destination");
                p.table_phase("sorting row hashes");
                p.table_phase("comparing");
            }
            p.table_finished(r);
        }
        p.summary(results);
    }

    #[test]
    fn pretty_mode_streams_icons_headers_and_summary() {
        let results = vec![
            VerificationResult::Match {
                receipt: receipt(200),
                duration_ms: 45,
            },
            VerificationResult::NoPriorRun {
                pipeline: "migrate_film".into(),
            },
        ];

        let out = render(true, |p| drive(p, &results));
        println!("\n----- verify --pretty -----\n{out}---------------------------\n");

        assert!(out.contains("◆ Verifying: migration.ppl"));
        assert!(out.contains("⧗ migrate_actor/actor"));
        assert!(out.contains("    reading destination…"));
        assert!(out.contains("    sorting row hashes…"));
        assert!(out.contains("    comparing…"));
        assert!(out.contains("✓ migrate_actor/actor - match (200 rows"));
        assert!(out.contains("✓ 1 matched, 1 without a receipt"));
    }

    #[test]
    fn plain_mode_keeps_status_markers_but_no_decoration() {
        let results = vec![
            VerificationResult::Match {
                receipt: receipt(200),
                duration_ms: 45,
            },
            VerificationResult::NoPriorRun {
                pipeline: "migrate_film".into(),
            },
        ];

        let out = render(false, |p| drive(p, &results));
        println!("\n----- verify (plain) -----\n{out}--------------------------\n");

        // The ✓/? status markers stay (this is the documented default output),
        // but none of the --pretty decoration: no header, phase lines, summary.
        assert_eq!(
            out,
            "✓ migrate_actor/actor - match (200 rows, root abababababababab, 45ms)\n\
             ? migrate_film - no integrity receipt (run `apply --integrity` first)\n"
        );
        for deco in ["◆", "⧗", "matched"] {
            assert!(
                !out.contains(deco),
                "plain output must not contain '{deco}'"
            );
        }
    }

    #[test]
    fn numbers_are_thousands_separated() {
        let results = vec![VerificationResult::Mismatch {
            receipt: receipt(127_491),
            actual_root: [0xcd; 32],
            summary: DivergenceSummary {
                missing: 0,
                changed: 1,
                extra: 0,
                expected_rows: 127_491,
                actual_rows: 127_491,
            },
            divergences: vec![Divergence {
                key: "order_id=3412".into(),
                kind: DivergenceKind::Changed {
                    expected_hash: [0xab; 32],
                    actual_hash: [0xcd; 32],
                },
            }],
            duration_ms: 2_841,
        }];

        let out = render(false, |p| drive(p, &results));
        println!("\n----- verify (mismatch) -----\n{out}-----------------------------\n");

        assert!(out.starts_with("✗ migrate_actor/actor - MISMATCH"));
        assert!(out.contains("127,491 rows expected, 127,491 found; 2,841ms"));
        assert!(out.contains("order_id=3412 - changed:"));
        // Still no pretty-only decoration in the default output.
        assert!(!out.contains("◆") && !out.contains("⧗"));
    }
}
