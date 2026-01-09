use crate::tui::{
    app::{core::App, state::AppState},
    ui::constants::styles,
};
use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    render_title(frame, area, app);
    render_view_indicator(frame, area, app);
}

fn render_title(frame: &mut Frame, area: Rect, app: &App) {
    let (status_text, color) = get_app_status(&app.state);

    let title = Line::from(vec![
        Span::styled(" STRATUM ", styles::header_title()),
        Span::styled(
            format!(" {} ", status_text),
            Style::default()
                .bg(color)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        ),
    ]);

    let header = Paragraph::new(title).alignment(Alignment::Left);
    frame.render_widget(header, area);
}

fn render_view_indicator(frame: &mut Frame, area: Rect, app: &App) {
    let view_indicator = Paragraph::new(format!("View: {:?} ", app.current_view))
        .alignment(Alignment::Right)
        .style(styles::muted());
    frame.render_widget(view_indicator, area);
}

fn get_app_status(state: &AppState) -> (&'static str, Color) {
    match state {
        AppState::Initializing => ("INITIALIZING", Color::Yellow),
        AppState::Running => ("RUNNING", Color::Green),
        AppState::Paused => ("PAUSED", Color::Yellow),
        AppState::Completed => ("COMPLETED", Color::Blue),
        AppState::Failed(_) => ("FAILED", Color::Red),
    }
}
