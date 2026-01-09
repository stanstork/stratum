use crate::tui::{
    app::core::App,
    ui::{
        constants::styles,
        widgets::{dag, pipeline_table},
    },
};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    widgets::Paragraph,
};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    if app.pipelines.is_empty() {
        render_empty_state(frame, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(10), // Pipeline table
            Constraint::Min(3),  // DAG visualization
        ])
        .split(area);

    pipeline_table::render(frame, chunks[0], app);
    dag::render(frame, chunks[1], app);
}

fn render_empty_state(frame: &mut Frame, area: Rect) {
    let placeholder = Paragraph::new("\n\nNo active pipelines found.\nWaiting for engine...")
        .alignment(Alignment::Center)
        .style(styles::muted());
    frame.render_widget(placeholder, area);
}
