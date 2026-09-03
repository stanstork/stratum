use crate::tui::{
    app::{
        command::MigrationCommand,
        handlers::{
            events::{self, TerminalEvent},
            modal, terminal,
        },
        state::{AppState, ErrorEntry, IntegrityProgress, View},
        stats::GlobalStats,
    },
    pipeline::PipelineState,
    tasks::ExecOutcome,
    ui::{render::render, widgets::modal::ModalState},
};
use chrono::{DateTime, Utc};
use engine_planner::plan::execution::{
    execution_settings::FailureStrategy, migration_report::MigrationReport,
};
use model::events::migration::MigrationEvent;
use ratatui::{Terminal, prelude::Backend};
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc;

/// The core Application controller for the TUI
pub struct App {
    // --- State ---
    pub state: AppState,
    pub current_view: View,
    pub modal_state: ModalState,
    pub pipelines: HashMap<String, PipelineState>,
    pub report: MigrationReport,
    pub errors: Vec<ErrorEntry>,
    pub selected_pipeline: usize,
    pub start_time: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub global_stats: GlobalStats,
    pub integrity: IntegrityProgress,
    pub run_finished: bool,

    /// The user dismissed the integrity finalizing modal; the app may now show completion.
    finalizing_acknowledged: bool,
    /// Quit was requested while a migration was running.
    quitting: bool,
    /// Set once the app should leave its run loop and exit.
    should_quit: bool,

    // --- Communication ---
    event_rx: mpsc::Receiver<MigrationEvent>,
    command_tx: mpsc::Sender<MigrationCommand>,
    terminal_rx: mpsc::Receiver<TerminalEvent>,
    outcome_rx: mpsc::Receiver<ExecOutcome>,
}

impl App {
    pub fn new(
        event_rx: mpsc::Receiver<MigrationEvent>,
        command_tx: mpsc::Sender<MigrationCommand>,
        terminal_rx: mpsc::Receiver<TerminalEvent>,
        pipelines: HashMap<String, PipelineState>,
        report: MigrationReport,
        outcome_rx: mpsc::Receiver<ExecOutcome>,
        integrity_enabled: bool,
    ) -> Self {
        let mut app = Self {
            state: AppState::Running, // Start in Running state since pipelines are loaded
            event_rx,
            command_tx,
            terminal_rx,
            outcome_rx,
            current_view: View::Overview,
            modal_state: ModalState::None,
            pipelines,
            report,
            errors: Vec::new(),
            selected_pipeline: 0,
            start_time: Some(Utc::now()),
            ended_at: None,
            global_stats: GlobalStats::default(),
            integrity: IntegrityProgress {
                enabled: integrity_enabled,
                ..Default::default()
            },
            run_finished: false,
            finalizing_acknowledged: false,
            quitting: false,
            should_quit: false,
        };

        app.sync_global_stats();
        app
    }

    /// Primary execution loop
    pub async fn run(
        &mut self,
        terminal: &mut Terminal<impl Backend>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            // Draw the UI
            terminal.draw(|frame| render(frame, self))?;

            tokio::select! {
                // Biased so migration events are fully drained before the
                // run-finished outcome, keeping the integrity finalizing modal
                // ahead of the completion modal. Terminal input stays first for
                // responsiveness.
                biased;
                Some(event) = self.terminal_rx.recv() => {
                    if self.handle_terminal_event(event)? {
                        return Ok(()); // Exit requested
                    }
                }
                Some(event) = self.event_rx.recv() => {
                    self.handle_migration_event(event);
                }
                Some(outcome) = self.outcome_rx.recv() => {
                    self.handle_exec_outcome(outcome);
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    self.tick();
                }
            }

            // A graceful quit completes once the engine has drained and reported.
            if self.should_quit {
                return Ok(());
            }
        }
    }

    fn handle_migration_event(&mut self, event: MigrationEvent) {
        match &event {
            MigrationEvent::IntegrityStarted { .. } => {
                self.integrity.active = true;
                self.integrity.current_table = None;
            }
            MigrationEvent::IntegritySealing { table, .. } => {
                self.integrity.current_table = Some(table.clone());
            }
            MigrationEvent::IntegrityReceipt { .. } => {
                self.integrity.receipts_done += 1;
                self.integrity.current_table = None;
            }
            MigrationEvent::IntegrityCompleted { .. } => {
                self.integrity.current_table = None;
            }
            _ => {}
        }

        events::handle_migration_event(&mut self.pipelines, &mut self.errors, event);
        self.update_app_lifecycle();
        self.sync_global_stats();
    }

    fn begin_quit(&mut self) -> bool {
        if self.state.is_running() {
            self.quitting = true;
            let _ = self.send_command(MigrationCommand::CancelAll);
            self.modal_state = ModalState::Notice {
                title: "Stopping…".to_string(),
                message: "Draining the current batch and saving a checkpoint before exit…"
                    .to_string(),
            };
            false
        } else {
            true
        }
    }

    /// React to how the background migration ended.
    fn handle_exec_outcome(&mut self, outcome: ExecOutcome) {
        if self.quitting {
            self.should_quit = true;
            return;
        }

        match outcome {
            ExecOutcome::Completed => {
                self.run_finished = true;
                self.update_app_lifecycle();
            }
            ExecOutcome::Failed(error) => self.handle_fatal_error(error),
            ExecOutcome::Paused => {
                self.stamp_end();
                self.state = AppState::Paused;
                self.modal_state = ModalState::Notice {
                    title: "⏸ Migration Paused".to_string(),
                    message: "Stopped at a clean checkpoint after the current batch.\n\n\
                              To resume, press [q] to quit and re-run the same command - \
                              it continues automatically from where it left off."
                        .to_string(),
                };
            }
            ExecOutcome::Cancelled => {
                self.stamp_end();
                self.state = AppState::Failed("cancelled".to_string());
                self.modal_state = ModalState::Notice {
                    title: "■ Migration Cancelled".to_string(),
                    message: "The migration was aborted.\n\n\
                              Progress up to the last completed batch is on disk. Re-run to \
                              continue from there, or `pag reset` to discard it and start over."
                        .to_string(),
                };
            }
        }
    }

    fn handle_fatal_error(&mut self, error: String) {
        self.errors.insert(0, ErrorEntry::new(error.clone(), None));

        if self.modal_state == ModalState::None && !self.state.is_terminal() {
            self.state = AppState::Failed(error.clone());
            self.modal_state = ModalState::MigrationFailed {
                pipeline_name: "engine".to_string(),
                error_message: error,
                error_count: self.errors.len(),
            };
        }
    }

    fn handle_terminal_event(
        &mut self,
        event: TerminalEvent,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        if self.modal_state != ModalState::None {
            return self.handle_modal_input(event);
        }

        let action = terminal::handle_terminal_event(
            event,
            &self.state,
            &mut self.current_view,
            &mut self.selected_pipeline,
            self.pipelines.len(),
        );

        self.process_terminal_action(action)
    }

    fn handle_modal_input(
        &mut self,
        event: TerminalEvent,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let TerminalEvent::Key(key) = event else {
            return Ok(false);
        };

        if self.modal_state == ModalState::IntegrityFinalizing {
            return self.handle_finalizing_key(key);
        }

        match modal::handle_modal_key(&mut self.modal_state, &mut self.current_view, key) {
            modal::ModalAction::None => Ok(false),
            modal::ModalAction::Quit => Ok(self.begin_quit()),
            modal::ModalAction::SendCommand(cmd) => {
                self.send_command(cmd)?;
                Ok(false)
            }
        }
    }

    fn handle_finalizing_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        use crossterm::event::KeyCode::*;
        match key.code {
            Char('q') | Char('Q') => Ok(self.begin_quit()),
            Enter | Char(' ') | Esc if self.run_finished => {
                // Dismiss -> allow completion (which shows the completion modal).
                self.finalizing_acknowledged = true;
                self.update_app_lifecycle();
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    fn process_terminal_action(
        &mut self,
        action: terminal::TerminalAction,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match action {
            terminal::TerminalAction::None => Ok(false),
            terminal::TerminalAction::Quit => Ok(self.begin_quit()),
            terminal::TerminalAction::QuitConfirm => {
                self.modal_state = ModalState::QuitConfirmation;
                Ok(false)
            }
            terminal::TerminalAction::PauseConfirm => {
                self.modal_state = ModalState::ConfirmAction {
                    title: "Pause Migration".to_string(),
                    message: "Finish the current batch, save a checkpoint, and stop?\n\
                              You can resume by re-running the same command."
                        .to_string(),
                    command: MigrationCommand::PauseAll,
                };
                Ok(false)
            }
            terminal::TerminalAction::CancelConfirm => {
                self.modal_state = ModalState::ConfirmAction {
                    title: "Cancel Migration".to_string(),
                    message: "Abort the migration now, without finishing the current batch?\n\
                              Progress up to the last checkpoint is kept."
                        .to_string(),
                    command: MigrationCommand::CancelAll,
                };
                Ok(false)
            }
        }
    }

    fn sync_global_stats(&mut self) {
        if !self.state.is_running() {
            return;
        }

        self.global_stats
            .calculate_current_throughput(self.pipelines.values());
        self.global_stats
            .sync_from_pipelines(self.pipelines.values());
        self.global_stats.total_pipelines = self.pipelines.len();
    }

    fn update_app_lifecycle(&mut self) {
        if self.pipelines.is_empty() {
            return;
        }

        let new_state = self.determine_app_state();

        if new_state != self.state {
            self.handle_state_transition(&new_state);
        }

        self.state = new_state;
    }

    fn determine_app_state(&self) -> AppState {
        if matches!(
            self.state,
            AppState::Paused | AppState::Completed | AppState::Failed(_)
        ) {
            return self.state.clone();
        }

        let total = self.pipelines.len();
        let mut running = 0;
        let mut failed = 0;
        let mut terminal = 0;
        let mut paused = 0;

        for p in self.pipelines.values() {
            use crate::tui::pipeline::PipelineStatus;
            match &p.status {
                PipelineStatus::Running => running += 1,
                PipelineStatus::Failed(_) => {
                    failed += 1;
                    terminal += 1;
                }
                PipelineStatus::Paused => paused += 1,
                status if status.is_terminal() => terminal += 1,
                _ => {}
            }
        }

        let fail_fast = matches!(
            self.report.execution_settings.on_failure,
            FailureStrategy::FailFast
        );

        if failed > 0 && terminal == total && fail_fast {
            AppState::Failed(format!("{failed} pipeline(s) failed"))
        } else if terminal == total {
            if self.integrity.enabled && !self.finalizing_acknowledged {
                AppState::Finalizing
            } else {
                AppState::Completed
            }
        } else if paused == total {
            AppState::Paused
        } else if running > 0 {
            AppState::Running
        } else {
            self.state.clone()
        }
    }

    fn handle_state_transition(&mut self, new_state: &AppState) {
        match new_state {
            AppState::Finalizing => {
                if self.modal_state == ModalState::None {
                    self.modal_state = ModalState::IntegrityFinalizing;
                }
            }
            AppState::Completed => {
                self.stamp_end();
                self.show_completion_modal();
            }
            AppState::Failed(_) => {
                self.stamp_end();
                self.show_failure_modal();
            }
            _ => {}
        }
    }

    fn stamp_end(&mut self) {
        if self.ended_at.is_none() {
            self.ended_at = Some(Utc::now());
        }
    }

    fn show_completion_modal(&mut self) {
        let duration = self.global_stats.started_at.elapsed();
        let failed = self
            .pipelines
            .values()
            .filter(|p| matches!(p.status, crate::tui::pipeline::PipelineStatus::Failed(_)))
            .count();

        self.modal_state = ModalState::MigrationCompleted {
            total_rows: self.global_stats.total_processed_rows,
            duration,
            avg_throughput: self.global_stats.average_throughput,
            warnings: self.errors.len(),
            errors: failed,
            skipped: self.global_stats.total_skipped_rows,
        };
    }

    fn show_failure_modal(&mut self) {
        if let Some((pipeline_name, error_message)) = self.pipelines.values().find_map(|p| {
            if let crate::tui::pipeline::PipelineStatus::Failed(err) = &p.status {
                Some((p.name.clone(), err.clone()))
            } else {
                None
            }
        }) {
            self.modal_state = ModalState::MigrationFailed {
                pipeline_name,
                error_message,
                error_count: self.errors.len(),
            };
        }
    }

    fn tick(&mut self) {
        self.sync_global_stats();
        self.update_app_lifecycle();
    }

    fn send_command(&self, cmd: MigrationCommand) -> Result<(), Box<dyn std::error::Error>> {
        self.command_tx.try_send(cmd)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::test_app;
    use super::*;
    use crate::tui::pipeline::{PipelineState, PipelineStatus};
    use crossterm::event::{KeyCode, KeyEvent};

    fn integrity_app(name: &str) -> App {
        let mut pipelines = HashMap::new();
        let mut p = PipelineState::new(name.to_string(), 0);
        p.status = PipelineStatus::Completed;
        pipelines.insert(name.to_string(), p);
        let mut app = test_app(pipelines);
        app.integrity.enabled = true;
        app
    }

    #[test]
    fn integrity_modal_stays_until_user_dismisses() {
        let mut app = integrity_app("orders");

        // Data written: Finalizing, integrity modal shown.
        app.update_app_lifecycle();
        assert_eq!(app.state, AppState::Finalizing);
        assert_eq!(app.modal_state, ModalState::IntegrityFinalizing);

        // Engine reports done - the modal STAYS (no auto-close), just becomes
        // dismissible.
        app.handle_exec_outcome(ExecOutcome::Completed);
        assert_eq!(app.state, AppState::Finalizing);
        assert!(app.run_finished);
        assert_eq!(app.modal_state, ModalState::IntegrityFinalizing);

        // User presses Enter -> completion.
        let _ = app.handle_finalizing_key(KeyEvent::from(KeyCode::Enter));
        assert_eq!(app.state, AppState::Completed);
        assert!(matches!(
            app.modal_state,
            ModalState::MigrationCompleted { .. }
        ));
    }

    #[test]
    fn finalizing_modal_dismissible_only_once_done() {
        let mut app = integrity_app("orders");
        app.update_app_lifecycle();
        assert_eq!(app.state, AppState::Finalizing);

        // Enter before the run is done does nothing (sealing must finish).
        let _ = app.handle_finalizing_key(KeyEvent::from(KeyCode::Enter));
        assert_eq!(app.state, AppState::Finalizing);

        // After done, Enter dismisses.
        app.handle_exec_outcome(ExecOutcome::Completed);
        let _ = app.handle_finalizing_key(KeyEvent::from(KeyCode::Enter));
        assert_eq!(app.state, AppState::Completed);
    }

    #[test]
    fn completed_outcome_waits_for_pipelines_to_be_terminal() {
        // The engine's run-done signal can arrive before the last pipelines'
        // Completed events are processed - the modal must not pop early.
        let mut pipelines = HashMap::new();
        let mut running = PipelineState::new("a".to_string(), 0);
        running.status = PipelineStatus::Running;
        pipelines.insert("a".to_string(), running);

        let mut app = test_app(pipelines);
        app.handle_exec_outcome(ExecOutcome::Completed);

        assert_ne!(app.state, AppState::Completed, "must wait for pipelines");
        assert!(!matches!(
            app.modal_state,
            ModalState::MigrationCompleted { .. }
        ));
    }

    #[test]
    fn finalizing_completes_even_if_a_pipeline_skipped_finalization() {
        // A failed/continue pipeline never emits IntegrityCompleted, but the
        // engine's run-done signal must still let the modal be dismissed.
        let mut pipelines = HashMap::new();
        let mut ok = PipelineState::new("ok".to_string(), 0);
        ok.status = PipelineStatus::Completed;
        let mut bad = PipelineState::new("bad".to_string(), 0);
        bad.status = PipelineStatus::Failed("boom".to_string());
        pipelines.insert("ok".to_string(), ok);
        pipelines.insert("bad".to_string(), bad);

        let mut app = test_app(pipelines);
        app.integrity.enabled = true;

        app.update_app_lifecycle();
        assert_eq!(app.state, AppState::Finalizing);

        app.handle_exec_outcome(ExecOutcome::Completed);
        let _ = app.handle_finalizing_key(KeyEvent::from(KeyCode::Enter));
        assert_eq!(app.state, AppState::Completed);
        assert!(!matches!(app.modal_state, ModalState::IntegrityFinalizing));
    }

    fn failed_pipeline_app(name: &str) -> App {
        let mut pipelines = HashMap::new();
        let mut p = PipelineState::new(name.to_string(), 0);
        p.status = PipelineStatus::Failed("boom".to_string());
        pipelines.insert("p".to_string(), p);
        test_app(pipelines)
    }

    #[test]
    fn continue_strategy_completes_despite_a_failed_pipeline() {
        // test_report defaults to FailureStrategy::Continue.
        let mut app = failed_pipeline_app("orders");
        app.update_app_lifecycle();
        assert_eq!(app.state, AppState::Completed);
        // The completion summary still reports the failure.
        match app.modal_state {
            ModalState::MigrationCompleted { errors, .. } => assert_eq!(errors, 1),
            other => panic!("expected completion modal, got {other:?}"),
        }
    }

    #[test]
    fn fail_fast_strategy_fails_the_migration_on_a_failed_pipeline() {
        let mut app = failed_pipeline_app("orders");
        app.report.execution_settings.on_failure = FailureStrategy::FailFast;
        app.update_app_lifecycle();
        assert!(matches!(app.state, AppState::Failed(_)));
    }

    #[test]
    fn quit_while_running_stops_gracefully_then_exits() {
        let mut app = test_app(HashMap::new());
        assert!(app.state.is_running());

        // Quit doesn't exit immediately - it asks the engine to stop first.
        let exit_now = app.begin_quit();
        assert!(!exit_now, "should wait for graceful stop");
        assert!(app.quitting);
        assert!(matches!(app.modal_state, ModalState::Notice { .. }));

        // Once the engine reports it stopped, the app exits (no lingering notice).
        app.handle_exec_outcome(ExecOutcome::Cancelled);
        assert!(app.should_quit);
    }

    #[test]
    fn quit_when_not_running_exits_immediately() {
        let mut app = test_app(HashMap::new());
        app.state = AppState::Completed;
        assert!(app.begin_quit());
    }

    #[test]
    fn pause_outcome_shows_a_paused_notice_not_a_failure() {
        let mut app = test_app(HashMap::new());
        assert!(app.ended_at.is_none());

        app.handle_exec_outcome(ExecOutcome::Paused);

        assert_eq!(app.state, AppState::Paused);
        // The elapsed clock is frozen, and stats stop accumulating once not running.
        assert!(
            app.ended_at.is_some(),
            "elapsed clock should freeze on pause"
        );
        assert!(!app.state.is_running());
        match &app.modal_state {
            ModalState::Notice { title, .. } => assert!(title.contains("Paused")),
            other => panic!("expected a paused notice, got {other:?}"),
        }
    }

    #[test]
    fn pause_sticks_and_does_not_revert_to_running() {
        // A running pipeline that never gets a per-pipeline Paused event: after
        // the pause outcome, ticks/late events must keep the app Paused so the
        // bottom stats stay frozen.
        let mut pipelines = HashMap::new();
        let mut p = PipelineState::new("a".to_string(), 0);
        p.status = PipelineStatus::Running;
        pipelines.insert("a".to_string(), p);
        let mut app = test_app(pipelines);

        app.handle_exec_outcome(ExecOutcome::Paused);
        assert_eq!(app.state, AppState::Paused);
        assert!(app.ended_at.is_some());

        // A tick re-evaluates lifecycle - must not flip back to Running.
        app.tick();
        assert_eq!(app.state, AppState::Paused);
        assert!(!app.state.is_running());
    }

    #[test]
    fn fatal_error_raises_a_failure_modal() {
        let mut app = test_app(HashMap::new());
        assert_eq!(app.modal_state, ModalState::None);

        app.handle_fatal_error("Failed to initialize the engine: connection refused".into());

        assert!(matches!(app.state, AppState::Failed(_)));
        assert_eq!(app.errors.len(), 1);
        match &app.modal_state {
            ModalState::MigrationFailed { error_message, .. } => {
                assert!(error_message.contains("connection refused"));
            }
            other => panic!("expected a failure modal, got {other:?}"),
        }
    }

    #[test]
    fn fatal_error_does_not_clobber_an_existing_failure_modal() {
        let mut pipelines = HashMap::new();
        pipelines.insert("p".to_string(), PipelineState::new("p".to_string(), 0));
        let mut app = test_app(pipelines);

        // A per-pipeline failure already surfaced its own, more specific modal.
        app.modal_state = ModalState::MigrationFailed {
            pipeline_name: "p".to_string(),
            error_message: "row 5 rejected".to_string(),
            error_count: 1,
        };

        app.handle_fatal_error("Migration execution failed: aborted".into());

        // The error is still recorded, but the specific modal is preserved.
        assert_eq!(app.errors.len(), 1);
        match &app.modal_state {
            ModalState::MigrationFailed { pipeline_name, .. } => {
                assert_eq!(pipeline_name, "p", "specific modal must be kept");
            }
            other => panic!("expected the original modal, got {other:?}"),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use engine_planner::plan::execution::migration_report::MigrationReport;

    /// A minimal report for render tests. Only the detail view reads it; the
    /// other views ignore it, so the fields are left at their defaults.
    pub fn test_report() -> MigrationReport {
        MigrationReport {
            plan_id: "test-plan".to_string(),
            generated_at: Utc::now(),
            engine_version: "0.0.0-test".to_string(),
            config_hash: String::new(),
            config_path: "test.ppl".to_string(),
            execution_settings: Default::default(),
            defines: Default::default(),
            connections: Vec::new(),
            pipelines: Vec::new(),
            execution_order: Vec::new(),
            summary: Default::default(),
            diagnostics: Vec::new(),
            estimations: Default::default(),
            is_executable: true,
            blocking_reason: None,
        }
    }

    /// Build an `App` wired to dummy channels, for rendering tests. The channels
    /// are never exercised - `render` only reads the public state fields.
    pub fn test_app(pipelines: HashMap<String, PipelineState>) -> App {
        let (_event_tx, event_rx) = mpsc::channel(1);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_term_tx, terminal_rx) = mpsc::channel(1);
        let (_outcome_tx, outcome_rx) = mpsc::channel(1);
        App::new(
            event_rx,
            command_tx,
            terminal_rx,
            pipelines,
            test_report(),
            outcome_rx,
            false,
        )
    }
}
