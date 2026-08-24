use crate::tui::ui::constants::styles;
use ratatui::{
    Frame,
    layout::Rect,
    text::Line,
    widgets::{Block, Borders, Paragraph},
};

pub fn render(frame: &mut Frame, area: Rect) {
    let help_text = vec![
        Line::from("Navigation:"),
        Line::from("  ↑/↓   : Select Pipeline"),
        Line::from("  Tab   : Switch Views"),
        Line::from("  1-4   : Quick View Switch"),
        Line::from(""),
        Line::from("Controls:"),
        Line::from("  Space : Pause & checkpoint the migration"),
        Line::from("  c     : Cancel the migration"),
        Line::from("  q     : Quit application"),
    ];

    let widget = Paragraph::new(help_text).block(
        Block::default()
            .title(" Keyboard Shortcuts ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}
