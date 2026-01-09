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
    modal::render(frame, &app.modal_state);
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
