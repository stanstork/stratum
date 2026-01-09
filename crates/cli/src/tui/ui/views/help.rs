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
        Line::from("  p/r   : Pause/Resume All"),
        Line::from("  P/R   : Pause/Resume Selected"),
        Line::from("  c/C   : Cancel Selected/All"),
        Line::from("  q     : Quit Application"),
    ];

    let widget = Paragraph::new(help_text).block(Block::default().borders(Borders::TOP));
    frame.render_widget(widget, area);
}
