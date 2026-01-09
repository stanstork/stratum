use crate::tui::{app::core::App, ui::constants::styles};
use ratatui::{
    Frame,
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let lines: Vec<Line> = app
        .errors
        .iter()
        .map(|err| {
            Line::from(vec![
                Span::styled(
                    format!("[{}] ", err.timestamp.format("%H:%M:%S")),
                    styles::muted(),
                ),
                Span::styled(
                    err.item_id.as_deref().unwrap_or("SYSTEM"),
                    styles::warning(),
                ),
                Span::raw(": "),
                Span::raw(&err.message),
            ])
        })
        .collect();

    let widget = Paragraph::new(lines)
        .block(Block::default().title(" Error Log ").borders(Borders::TOP))
        .wrap(ratatui::widgets::Wrap { trim: true });

    frame.render_widget(widget, area);
}
