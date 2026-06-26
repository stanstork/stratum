use crossterm::ExecutableCommand;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use engine_core::event_bus::bus::EventBus;
use model::events::migration::MigrationEvent;
use std::collections::HashMap;
use std::io::{self, Write, stdout};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Pretty-prints migration progress to stdout with colors and Unicode symbols
pub struct PrettyPrinter {
    start_time: Instant,
    pipeline_start_times: HashMap<String, Instant>,
    pipeline_names: HashMap<String, String>,
    stats: GlobalStats,
}

/// Accumulated statistics across all pipelines
#[derive(Debug, Default)]
struct GlobalStats {
    total_rows: u64,
    total_skipped: u64,
    total_failed: u64,
    pipelines_completed: usize,
}

impl PrettyPrinter {
    /// Creates a new pretty printer with pipeline name mappings
    pub fn new(pipeline_names: HashMap<String, String>) -> Self {
        Self {
            start_time: Instant::now(),
            pipeline_start_times: HashMap::new(),
            pipeline_names,
            stats: GlobalStats::default(),
        }
    }

    /// Runs the pretty printer, subscribing to events from the event bus
    pub async fn run(
        event_bus: EventBus,
        shutdown: CancellationToken,
        pipeline_names: HashMap<String, String>,
    ) -> io::Result<()> {
        let (tx, mut rx) = mpsc::channel(1000);

        // Subscribe to all migration events
        let _subscription = event_bus.subscribe::<MigrationEvent>(tx).await;

        // Drop event bus reference so it can be cleaned up
        drop(event_bus);

        let mut printer = Self::new(pipeline_names);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }
                event = rx.recv() => {
                    match event {
                        Some(evt) => printer.handle_event(&evt)?,
                        None => break,
                    }
                }
            }
        }

        // Print final summary
        printer.print_final_summary()?;

        Ok(())
    }

    /// Handles a single migration event
    pub fn handle_event(&mut self, event: &MigrationEvent) -> io::Result<()> {
        match event {
            MigrationEvent::Started { run_id, .. } => {
                self.print_started(run_id)?;
            }

            MigrationEvent::ProducerStarted { item_id, mode, .. } => {
                self.handle_producer_started(item_id, &mode.to_string())?;
            }

            MigrationEvent::SnapshotStarted {
                item_id,
                estimated_rows,
                ..
            } => {
                self.handle_snapshot_started(item_id, estimated_rows)?;
            }

            MigrationEvent::Progress {
                item_id,
                rows_processed,
                rows_skipped,
                rows_failed,
                ..
            } => {
                self.print_progress(item_id, *rows_processed, *rows_skipped, *rows_failed)?;
            }

            MigrationEvent::Completed {
                item_id,
                rows_processed,
                rows_skipped,
                rows_failed,
                duration_ms,
                ..
            } => {
                self.handle_completed(
                    item_id,
                    *rows_processed,
                    *rows_skipped,
                    *rows_failed,
                    *duration_ms,
                )?;
            }

            MigrationEvent::Failed { item_id, error, .. } => {
                self.print_failed(item_id, error)?;
            }

            MigrationEvent::SnapshotCompleted {
                rows_processed,
                duration_ms,
                ..
            } => {
                self.print_snapshot_completed(*rows_processed, *duration_ms)?;
            }

            // Ignore other events in pretty mode
            _ => {}
        }

        Ok(())
    }

    /// Handles producer started event
    fn handle_producer_started(&mut self, item_id: &str, mode: &str) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id).to_string();
        self.pipeline_start_times
            .insert(item_id.to_string(), Instant::now());
        self.print_line(
            Color::Cyan,
            "◉",
            &format!("Pipeline '{}' started ({:?} mode)", name, mode),
        )
    }

    /// Handles snapshot started event
    fn handle_snapshot_started(
        &self,
        item_id: &str,
        estimated_rows: &Option<u64>,
    ) -> io::Result<()> {
        if let Some(est) = estimated_rows {
            let name = self.get_pipeline_name(item_id);
            self.print_line(
                Color::Cyan,
                "◉",
                &format!("Pipeline '{}' started ({} rows)", name, format_number(*est)),
            )?;
        }
        Ok(())
    }

    /// Handles pipeline completed event
    fn handle_completed(
        &mut self,
        item_id: &str,
        rows_processed: u64,
        rows_skipped: u64,
        rows_failed: u64,
        duration_ms: u64,
    ) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id);
        let duration = Duration::from_millis(duration_ms);
        let throughput = format_throughput(rows_processed, duration);

        let summary = build_summary(rows_processed, rows_skipped, rows_failed, Some(duration));

        self.print_line(
            Color::Green,
            "✓",
            &format!(
                "Pipeline '{}' completed: {} ({})",
                name, summary, throughput
            ),
        )?;

        // Accumulate totals
        self.stats.total_rows += rows_processed;
        self.stats.total_skipped += rows_skipped;
        self.stats.total_failed += rows_failed;
        self.stats.pipelines_completed += 1;

        Ok(())
    }

    /// Prints migration started message
    fn print_started(&self, run_id: &str) -> io::Result<()> {
        self.print_line(Color::Blue, "▶", &format!("Starting migration: {}", run_id))
    }

    /// Prints progress update
    fn print_progress(
        &self,
        item_id: &str,
        rows_processed: u64,
        rows_skipped: u64,
        rows_failed: u64,
    ) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id);
        let details = build_row_details(rows_processed, rows_skipped, rows_failed);
        let progress_msg = format!("{} {}", name, details);

        self.print_line(Color::Yellow, "->", &progress_msg)
    }

    /// Prints pipeline failed message
    fn print_failed(&self, item_id: &str, error: &str) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id);
        self.print_line(
            Color::Red,
            "✗",
            &format!("Pipeline '{}' failed: {}", name, error),
        )
    }

    /// Prints snapshot completed summary
    fn print_snapshot_completed(&self, rows_processed: u64, duration_ms: u64) -> io::Result<()> {
        let duration = Duration::from_millis(duration_ms);
        let throughput = format_throughput(rows_processed, duration);

        self.print_line(Color::Green, "✓", "Snapshot completed!")?;

        let mut out = stdout();
        out.execute(Print(&format!(
            "   Total rows: {}\n",
            format_number(rows_processed)
        )))?;
        out.execute(Print(&format!(
            "   Duration:   {}\n",
            format_duration(duration)
        )))?;
        out.execute(Print(&format!("   Throughput: {}\n", throughput)))?;
        out.flush()?;

        Ok(())
    }

    /// Prints final migration summary
    fn print_final_summary(&self) -> io::Result<()> {
        if self.stats.pipelines_completed == 0 {
            return Ok(());
        }

        let total_duration = self.start_time.elapsed();
        let throughput = format_throughput(self.stats.total_rows, total_duration);

        self.print_line(Color::Green, "✓", "Migration completed!")?;

        let mut out = stdout();

        let summary = build_row_details(
            self.stats.total_rows,
            self.stats.total_skipped,
            self.stats.total_failed,
        );

        out.execute(Print(&format!("   Total:      {}\n", summary)))?;
        out.execute(Print(&format!(
            "   Pipelines:  {}\n",
            self.stats.pipelines_completed
        )))?;
        out.execute(Print(&format!(
            "   Duration:   {}\n",
            format_duration(total_duration)
        )))?;
        out.execute(Print(&format!("   Throughput: {}\n", throughput)))?;
        out.flush()?;

        Ok(())
    }

    /// Prints a colored line to stdout
    fn print_line(&self, color: Color, symbol: &str, message: &str) -> io::Result<()> {
        let mut out = stdout();
        out.execute(SetForegroundColor(Color::DarkGrey))?;
        out.execute(Print(&self.format_timestamp()))?;
        out.execute(Print(" "))?;
        out.execute(SetForegroundColor(color))?;
        out.execute(Print(symbol))?;
        out.execute(Print(" "))?;
        out.execute(ResetColor)?;
        out.execute(Print(message))?;
        out.execute(Print("\n"))?;
        out.flush()?;
        Ok(())
    }

    /// Gets the display name for a pipeline (falls back to item_id if not found)
    fn get_pipeline_name<'a>(&'a self, item_id: &'a str) -> &'a str {
        self.pipeline_names
            .get(item_id)
            .map(|s| s.as_str())
            .unwrap_or(item_id)
    }

    /// Formats elapsed time in seconds with 3 decimal places
    fn format_timestamp(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        format!("[{:>7.3}s]", elapsed)
    }
}

// Helper functions

/// Formats large numbers with commas (e.g., 1,234,567)
fn format_number(n: u64) -> String {
    n.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap()
        .join(",")
}

/// Formats duration in seconds with 2 decimal places
fn format_duration(duration: Duration) -> String {
    format!("{:.2}s", duration.as_secs_f64())
}

/// Formats throughput as rows/sec
fn format_throughput(rows: u64, duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs > 0.001 {
        format!("{:.0}/s", rows as f64 / secs)
    } else {
        "-".to_string()
    }
}

/// Builds row details string (e.g., "1,234 rows, 5 skipped")
fn build_row_details(rows_processed: u64, rows_skipped: u64, rows_failed: u64) -> String {
    let mut parts = vec![format!("{} rows", format_number(rows_processed))];

    if rows_skipped > 0 {
        parts.push(format!("{} skipped", format_number(rows_skipped)));
    }
    if rows_failed > 0 {
        parts.push(format!("{} failed", format_number(rows_failed)));
    }

    parts.join(", ")
}

/// Builds complete summary with duration
fn build_summary(
    rows_processed: u64,
    rows_skipped: u64,
    rows_failed: u64,
    duration: Option<Duration>,
) -> String {
    let mut parts = vec![format!("{} rows", format_number(rows_processed))];

    if rows_skipped > 0 {
        parts.push(format!("{} skipped", format_number(rows_skipped)));
    }
    if rows_failed > 0 {
        parts.push(format!("{} failed", format_number(rows_failed)));
    }
    if let Some(d) = duration {
        parts.push(format!("in {}", format_duration(d)));
    }

    parts.join(", ")
}
