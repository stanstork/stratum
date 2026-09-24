use crate::tui::{
    app::{core::App, state::View},
    ui::{
        constants::{FOOTER_HEIGHT, HEADER_HEIGHT, SPACER_HEIGHT, STATS_HEIGHT, styles},
        views,
        widgets::{footer, header, modal, stats},
    },
};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin},
    widgets::{Block, Borders},
};

/// Entry point for TUI rendering
pub fn render(frame: &mut Frame, app: &App) {
    // Main background block
    let root_block = Block::default()
        .borders(Borders::ALL)
        .border_style(styles::border());
    frame.render_widget(root_block, frame.area());

    // Define the safe area inside the root borders
    let safe_area = frame.area().inner(Margin {
        vertical: 1,
        horizontal: 1,
    });

    // Layout partitioning - hide stats dashboard in detail view
    let show_stats = !matches!(app.current_view, View::PipelineDetail);

    let chunks = create_layout(safe_area, show_stats);

    header::render(frame, chunks[0], app);
    render_main_content(frame, chunks[1], app);

    if show_stats {
        stats::render_dashboard(frame, chunks[2], app);
        // chunks[3] is left empty as a spacer
        footer::render(frame, chunks[4]);
    } else {
        footer::render(frame, chunks[2]);
    }

    // Render modal overlay on top if active
    modal::render(frame, app);
}

fn create_layout(
    area: ratatui::layout::Rect,
    show_stats: bool,
) -> std::rc::Rc<[ratatui::layout::Rect]> {
    if show_stats {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(HEADER_HEIGHT),
                Constraint::Min(10),
                Constraint::Length(STATS_HEIGHT),
                Constraint::Length(SPACER_HEIGHT),
                Constraint::Length(FOOTER_HEIGHT),
            ])
            .split(area)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(HEADER_HEIGHT),
                Constraint::Min(10),
                Constraint::Length(FOOTER_HEIGHT),
            ])
            .split(area)
    }
}

fn render_main_content(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    match app.current_view {
        View::Overview => views::overview::render(frame, area, app),
        View::PipelineDetail => views::detail::render(frame, area, app),
        View::Errors => views::errors::render(frame, area, app),
        View::Help => views::help::render(frame, area),
    }
}

#[cfg(test)]
mod tests {
    // `App` and `View` come from the parent module via `super::*`.
    use super::*;
    use crate::tui::{
        app::core::test_support::test_app,
        pipeline::{PipelineState, PipelineStatus},
    };
    use ratatui::{Terminal, backend::TestBackend};
    use std::collections::HashMap;

    fn pipeline(
        name: &str,
        stage: u32,
        status: PipelineStatus,
        src: u64,
        done: u64,
    ) -> PipelineState {
        let mut p = PipelineState::new(name.to_string(), stage);
        p.status = status;
        p.source_rows = src;
        p.processed_rows = done;
        p
    }

    /// Render one frame at the given size and return it as text (via TestBackend).
    fn dump(
        w: u16,
        h: u16,
        pipelines: Vec<PipelineState>,
        mutate: impl FnOnce(&mut App),
    ) -> String {
        let map: HashMap<String, PipelineState> =
            pipelines.into_iter().map(|p| (p.name.clone(), p)).collect();
        let mut app = test_app(map);
        mutate(&mut app);

        let mut terminal = Terminal::new(TestBackend::new(w, h)).expect("terminal");
        terminal.draw(|f| render(f, &app)).expect("draw");
        format!("{}", terminal.backend())
    }

    #[test]
    fn overview_renders_a_running_migration() {
        let out = dump(
            100,
            30,
            vec![
                pipeline("migrate_actor", 0, PipelineStatus::Completed, 200, 200),
                pipeline(
                    "migrate_customers",
                    1,
                    PipelineStatus::Running,
                    13_842,
                    8_000,
                ),
                pipeline("migrate_orders", 1, PipelineStatus::Pending, 127_491, 0),
            ],
            |_app| {},
        );

        println!("\n{out}");
        assert!(out.contains("PAGANEL"));
        assert!(out.contains("migrate_customers"));
    }

    #[test]
    fn progress_bar_caps_at_100_when_actual_exceeds_estimate() {
        // Source count is an estimate; here the actual (11K) exceeds it (10K).
        let out = dump(
            100,
            30,
            vec![pipeline(
                "orders",
                0,
                PipelineStatus::Running,
                10_000,
                11_000,
            )],
            |_app| {},
        );
        println!("\n{out}");
        // The stats Progress panel renders "{:.1}%" - must be capped at 100.0%.
        assert!(out.contains("100.0%"), "progress should cap at 100.0%");
        assert!(
            !out.contains("110.0%"),
            "progress must not exceed 100% even when actual > estimate"
        );
    }

    #[test]
    fn detail_view_renders() {
        let out = dump(
            100,
            30,
            vec![
                pipeline("migrate_actor", 0, PipelineStatus::Completed, 200, 200),
                pipeline(
                    "migrate_customers",
                    1,
                    PipelineStatus::Running,
                    13_842,
                    8_000,
                ),
            ],
            |app| {
                app.current_view = View::PipelineDetail;
                app.selected_pipeline = 1;
            },
        );
        println!("\n{out}");
        assert!(out.contains("PAGANEL"));
    }

    #[test]
    fn errors_view_renders() {
        use crate::tui::app::state::ErrorEntry;
        let out = dump(
            100,
            30,
            vec![pipeline(
                "migrate_orders",
                0,
                PipelineStatus::Failed("connection reset".into()),
                100,
                40,
            )],
            |app| {
                app.current_view = View::Errors;
                app.errors.push(ErrorEntry::new(
                    "Batch 7 failed: connection reset by peer".into(),
                    Some("migrate_orders".into()),
                ));
            },
        );
        println!("\n{out}");
        assert!(out.contains("PAGANEL"));
    }

    #[test]
    fn integrity_finalization_shows_a_modal_not_premature_completion() {
        use crate::tui::app::state::{AppState, IntegrityProgress};
        use crate::tui::ui::widgets::modal::ModalState;
        let out = dump(
            100,
            30,
            vec![pipeline("orders", 0, PipelineStatus::Completed, 200, 200)],
            |app| {
                // Data written, integrity still sealing -> Finalizing + its modal.
                app.state = AppState::Finalizing;
                app.modal_state = ModalState::IntegrityFinalizing;
                app.integrity = IntegrityProgress {
                    enabled: true,
                    active: true,
                    current_table: Some("orders".into()),
                    receipts_done: 2,
                };
            },
        );
        println!("\n{out}");
        assert!(out.contains("FINALIZING"), "status should read FINALIZING");
        assert!(
            out.contains("SEALING INTEGRITY RECEIPTS"),
            "finalizing modal"
        );
        assert!(out.contains("Sealing 'orders'"));
        assert!(
            out.contains("Receipts committed:  2"),
            "cumulative receipts"
        );
        // The premature completion modal must not be up while sealing.
        assert!(
            !out.contains("MIGRATION COMPLETED"),
            "completion modal must not show during finalization"
        );
    }

    #[test]
    fn paused_notice_renders_with_resume_guidance() {
        use crate::tui::app::state::AppState;
        use crate::tui::ui::widgets::modal::ModalState;
        let out = dump(
            100,
            30,
            vec![pipeline("orders", 0, PipelineStatus::Paused, 1000, 400)],
            |app| {
                app.state = AppState::Paused;
                app.modal_state = ModalState::Notice {
                    title: "⏸ Migration Paused".into(),
                    message: "The current batch was drained and a checkpoint was saved.\n\n\
                              To resume: press [q] to quit, then re-run the same command -\n\
                              it continues automatically from the last checkpoint."
                        .into(),
                };
            },
        );
        println!("\n{out}");
        assert!(out.contains("Migration Paused"));
        assert!(out.contains("To resume"));
        // (the rest of the sentence wraps across lines in the modal)
        assert!(out.contains("re-run"));
    }

    #[test]
    fn cancel_confirmation_modal_renders() {
        use crate::tui::app::command::MigrationCommand;
        use crate::tui::ui::widgets::modal::ModalState;
        let out = dump(
            100,
            30,
            vec![pipeline("orders", 0, PipelineStatus::Running, 1000, 400)],
            |app| {
                app.modal_state = ModalState::ConfirmAction {
                    title: "Cancel Migration".into(),
                    message: "Abort the migration now? Data already written is checkpointed."
                        .into(),
                    command: MigrationCommand::CancelAll,
                };
            },
        );
        println!("\n{out}");
        assert!(out.contains("Cancel Migration"));
        assert!(out.contains("Abort the migration now"));
        assert!(out.contains("[y]") && out.contains("[n]"));
    }

    #[test]
    fn executor_failure_modal_renders() {
        use crate::tui::ui::widgets::modal::ModalState;
        let out = dump(
            100,
            30,
            vec![pipeline("orders", 0, PipelineStatus::Pending, 0, 0)],
            |app| {
                app.modal_state = ModalState::MigrationFailed {
                    pipeline_name: "engine".into(),
                    error_message: "Failed to initialize the engine: connection refused".into(),
                    error_count: 1,
                };
            },
        );
        println!("\n{out}");
        assert!(out.contains("MIGRATION FAILED"));
        assert!(out.contains("connection refused"));
    }

    #[test]
    fn help_view_renders() {
        let out = dump(100, 30, vec![], |app| app.current_view = View::Help);
        println!("\n{out}");
        assert!(out.contains("PAGANEL"));
    }

    #[test]
    fn narrow_terminal_keeps_pipeline_names() {
        // 80x24 is the classic minimum. The name column must survive - dropping
        // secondary columns (rate/eta) is fine, losing the pipeline identity is
        // not. Regression guard for the responsive column layout.
        let out = dump(
            80,
            24,
            vec![
                pipeline("migrate_actor", 0, PipelineStatus::Completed, 200, 200),
                pipeline(
                    "migrate_customers",
                    1,
                    PipelineStatus::Running,
                    13_842,
                    8_000,
                ),
            ],
            |_app| {},
        );
        println!("\n{out}");
        assert!(out.contains("PAGANEL"));
        assert!(
            out.contains("Pipeline"),
            "name column header must be present"
        );
        assert!(
            out.contains("migrate_actor"),
            "pipeline name must be visible"
        );
    }
}
