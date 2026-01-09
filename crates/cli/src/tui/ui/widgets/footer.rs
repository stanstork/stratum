use crate::tui::ui::constants::colors;
use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    widgets::Paragraph,
};

pub fn render(frame: &mut Frame, area: Rect) {
    let keys = " [Q]uit  [Tab]View  [P]ause  [R]esume  [C]ancel ";
    let footer = Paragraph::new(keys)
        .alignment(Alignment::Center)
        .style(Style::default().bg(colors::BACKGROUND).fg(colors::BORDER));
    frame.render_widget(footer, area);
}
