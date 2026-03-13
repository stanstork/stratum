use crate::tui::{
    app::{
        command::MigrationCommand,
        handlers::{
            events::{self, TerminalEvent},
            modal, terminal,
        },
        state::{AppState, ErrorEntry, View},
        stats::GlobalStats,
    },
    pipeline::PipelineState,
    ui::{render::render, widgets::modal::ModalState},
};
use chrono::{DateTime, Utc};
use engine_planner::plan::execution::migration_report::MigrationReport;
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
    pub global_stats: GlobalStats,

    // --- Communication ---
    event_rx: mpsc::Receiver<MigrationEvent>,
    command_tx: mpsc::Sender<MigrationCommand>,
    terminal_rx: mpsc::Receiver<TerminalEvent>,
}

impl App {
    pub fn new(
        event_rx: mpsc::Receiver<MigrationEvent>,
        command_tx: mpsc::Sender<MigrationCommand>,
        terminal_rx: mpsc::Receiver<TerminalEvent>,
        pipelines: HashMap<String, PipelineState>,
        report: MigrationReport,
    ) -> Self {
        let mut app = Self {
            state: AppState::Running, // Start in Running state since pipelines are loaded
            event_rx,
            command_tx,
            terminal_rx,
            current_view: View::Overview,
            modal_state: ModalState::None,
            pipelines,
            report,
            errors: Vec::new(),
            selected_pipeline: 0,
            start_time: Some(Utc::now()),
            global_stats: GlobalStats::default(),
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
                Some(event) = self.event_rx.recv() => {
                    self.handle_migration_event(event);
                }
                Some(event) = self.terminal_rx.recv() => {
                    if self.handle_terminal_event(event)? {
                        return Ok(()); // Exit requested
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    self.tick();
                }
            }
        }
    }

    fn handle_migration_event(&mut self, event: MigrationEvent) {
        events::handle_migration_event(&mut self.pipelines, &mut self.errors, event);
        self.update_app_lifecycle();
        self.sync_global_stats();
    }

    fn handle_terminal_event(
        &mut self,
        event: TerminalEvent,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // Handle modal input first if modal is active
        if self.modal_state != ModalState::None {
            return self.handle_modal_input(event);
        }

        // Handle normal terminal input
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
        if let TerminalEvent::Key(key) = event {
            let action =
                modal::handle_modal_key(&mut self.modal_state, &mut self.current_view, key);

            match action {
                modal::ModalAction::None => Ok(false),
                modal::ModalAction::Quit => Ok(true),
                modal::ModalAction::SendCommand(cmd) => {
                    self.send_command(cmd)?;
                    Ok(false)
                }
            }
        } else {
            Ok(false)
        }
    }

    fn process_terminal_action(
        &mut self,
        action: terminal::TerminalAction,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match action {
            terminal::TerminalAction::None => Ok(false),
            terminal::TerminalAction::Quit => Ok(true),
            terminal::TerminalAction::QuitConfirm => {
                self.modal_state = ModalState::QuitConfirmation;
                Ok(false)
            }
            terminal::TerminalAction::SendCommand(cmd) => {
                // Replace "selected" placeholder with actual pipeline name
                let cmd = self.resolve_selected_command(cmd);
                self.send_command(cmd)?;
                Ok(false)
            }
        }
    }

    fn resolve_selected_command(&self, cmd: MigrationCommand) -> MigrationCommand {
        match cmd {
            MigrationCommand::PausePipeline(ref name)
            | MigrationCommand::ResumePipeline(ref name)
            | MigrationCommand::CancelPipeline(ref name)
                if name == "selected" =>
            {
                if let Some(pipeline) = self.get_selected_pipeline() {
                    match cmd {
                        MigrationCommand::PausePipeline(_) => {
                            MigrationCommand::PausePipeline(pipeline.name.clone())
                        }
                        MigrationCommand::ResumePipeline(_) => {
                            MigrationCommand::ResumePipeline(pipeline.name.clone())
                        }
                        MigrationCommand::CancelPipeline(_) => {
                            MigrationCommand::CancelPipeline(pipeline.name.clone())
                        }
                        _ => cmd,
                    }
                } else {
                    cmd
                }
            }
            _ => cmd,
        }
    }

    fn sync_global_stats(&mut self) {
        // Don't update stats if migration is complete or failed
        if self.state.is_terminal() {
            return;
        }

        // Calculate current throughput first
        self.global_stats
            .calculate_current_throughput(self.pipelines.values());

        // Sync all other stats
        self.global_stats
            .sync_from_pipelines(self.pipelines.values());

        // Update total pipeline count
        self.global_stats.total_pipelines = self.pipelines.len();
    }

    fn update_app_lifecycle(&mut self) {
        if self.pipelines.is_empty() {
            return;
        }

        let new_state = self.determine_app_state();

        // Trigger modals on state transitions
        if new_state != self.state {
            self.handle_state_transition(&new_state);
        }

        self.state = new_state;
    }

    fn determine_app_state(&self) -> AppState {
        let statuses: Vec<_> = self.pipelines.values().map(|p| &p.status).collect();

        let has_running = statuses
            .iter()
            .any(|s| **s == crate::tui::pipeline::PipelineStatus::Running);
        let has_failed = statuses
            .iter()
            .any(|s| matches!(s, crate::tui::pipeline::PipelineStatus::Failed(_)));
        let all_terminal = statuses.iter().all(|s| s.is_terminal());
        let all_paused = statuses
            .iter()
            .all(|s| **s == crate::tui::pipeline::PipelineStatus::Paused);

        if has_failed && all_terminal {
            let failed_count = statuses
                .iter()
                .filter(|s| matches!(s, crate::tui::pipeline::PipelineStatus::Failed(_)))
                .count();
            AppState::Failed(format!("{} pipeline(s) failed", failed_count))
        } else if all_terminal {
            AppState::Completed
        } else if all_paused {
            AppState::Paused
        } else if has_running {
            AppState::Running
        } else {
            self.state.clone() // Keep current
        }
    }

    fn handle_state_transition(&mut self, new_state: &AppState) {
        match new_state {
            AppState::Completed => {
                self.show_completion_modal();
            }
            AppState::Failed(_) => {
                self.show_failure_modal();
            }
            _ => {}
        }
    }

    fn show_completion_modal(&mut self) {
        let duration = self.global_stats.started_at.elapsed();
        let warnings = self.errors.len();

        self.modal_state = ModalState::MigrationCompleted {
            total_rows: self.global_stats.total_processed_rows,
            duration,
            avg_throughput: self.global_stats.average_throughput,
            warnings,
            errors: 0,
            skipped: self.global_stats.total_skipped_rows,
        };
    }

    fn show_failure_modal(&mut self) {
        if let Some(failed_pipeline) = self
            .pipelines
            .values()
            .find(|p| matches!(p.status, crate::tui::pipeline::PipelineStatus::Failed(_)))
        {
            let error_message = if let crate::tui::pipeline::PipelineStatus::Failed(err) =
                &failed_pipeline.status
            {
                err.clone()
            } else {
                "Unknown error".to_string()
            };

            self.modal_state = ModalState::MigrationFailed {
                pipeline_name: failed_pipeline.name.clone(),
                error_message,
                error_count: self.errors.len(),
            };
        }
    }

    fn get_selected_pipeline(&self) -> Option<&PipelineState> {
        let mut pipelines: Vec<_> = self.pipelines.iter().collect();

        // Sort by execution order: stage first, then by name
        pipelines.sort_by(|(name_a, pipeline_a), (name_b, pipeline_b)| {
            pipeline_a
                .stage
                .cmp(&pipeline_b.stage)
                .then_with(|| name_a.cmp(name_b))
        });

        pipelines
            .get(self.selected_pipeline)
            .map(|(_, pipeline)| *pipeline)
    }

    fn tick(&mut self) {
        self.sync_global_stats();
    }

    fn send_command(&self, cmd: MigrationCommand) -> Result<(), Box<dyn std::error::Error>> {
        self.command_tx.try_send(cmd)?;
        Ok(())
    }
}
