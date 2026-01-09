use crate::tui::{
    app::core::App,
    ui::{
        constants::{colors, styles},
        formatters::{create_sparkline, format_bytes, format_compact_number, format_duration},
    },
};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};
use std::time::Instant;

pub fn render_dashboard(frame: &mut Frame, area: Rect, app: &App) {
    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(area);

    render_progress_panel(frame, panels[0], app);
    render_throughput_panel(frame, panels[1], app);
    render_timing_panel(frame, panels[2], app);
    render_data_volume_panel(frame, panels[3], app);
}

fn render_progress_panel(frame: &mut Frame, area: Rect, app: &App) {
    let stats = &app.global_stats;
    let progress_pct = if stats.total_source_rows > 0 {
        (stats.total_processed_rows as f64 / stats.total_source_rows as f64) * 100.0
    } else {
        0.0
    };

    let bar_width = (area.width.saturating_sub(8)) as usize;
    let filled = ((progress_pct / 100.0) * bar_width as f64).round() as usize;
    let empty = bar_width.saturating_sub(filled);
    let bar_str = format!("{}{}", "█".repeat(filled), "░".repeat(empty));

    let content = vec![
        Line::from(vec![
            Span::styled(bar_str, Style::default().fg(colors::PROGRESS_BAR)),
            Span::styled(
                format!(" {:.1}%", progress_pct),
                Style::default().fg(colors::PROGRESS_BAR).bold(),
            ),
        ]),
        Line::from(vec![Span::raw(format!(
            "{} / {} pipelines",
            stats.completed_pipelines, stats.total_pipelines
        ))]),
        Line::from(format!(
            "Rows: {} / {}",
            format_compact_number(stats.total_processed_rows),
            format_compact_number(stats.total_source_rows)
        )),
    ];

    let widget = Paragraph::new(content).block(
        Block::default()
            .title(" Progress ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn render_throughput_panel(frame: &mut Frame, area: Rect, app: &App) {
    let stats = &app.global_stats;
    let sparkline_data: Vec<u64> = stats
        .throughput_history
        .iter()
        .map(|(_, val)| *val as u64)
        .collect();
    let sparkline = create_sparkline(&sparkline_data, area.width.saturating_sub(4) as usize);

    let content = vec![
        Line::from(vec![
            Span::raw("Rate: "),
            Span::styled(
                format!(
                    "{}/s",
                    format_compact_number(stats.current_throughput as u64)
                ),
                Style::default().fg(colors::THROUGHPUT).bold(),
            ),
        ]),
        Line::from(vec![
            Span::raw("Peak: "),
            Span::styled(
                format!("{}/s", format_compact_number(stats.peak_throughput as u64)),
                styles::muted(),
            ),
        ]),
        Line::from(Span::styled(
            sparkline,
            Style::default().fg(colors::THROUGHPUT),
        )),
    ];

    let widget = Paragraph::new(content).block(
        Block::default()
            .title(" Throughput ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn render_timing_panel(frame: &mut Frame, area: Rect, app: &App) {
    let stats = &app.global_stats;

    let elapsed = app
        .start_time
        .map(|t| {
            let duration = chrono::Utc::now() - t;
            format_duration(duration.to_std().unwrap_or_default())
        })
        .unwrap_or_else(|| "00:00:00".to_string());

    let eta = stats
        .estimated_completion
        .map(|t| {
            let dur = t.duration_since(Instant::now());
            format_duration(dur)
        })
        .unwrap_or_else(|| "--".to_string());

    let content = vec![
        Line::from(vec![
            Span::raw("Elapsed: "),
            Span::styled(elapsed, styles::value_bold()),
        ]),
        Line::from(vec![
            Span::raw("ETA:     "),
            Span::styled(eta, Style::default().fg(colors::TIMING_ETA)),
        ]),
    ];

    let widget = Paragraph::new(content).block(
        Block::default()
            .title(" Timing ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn render_data_volume_panel(frame: &mut Frame, area: Rect, app: &App) {
    let stats = &app.global_stats;

    let content = vec![
        Line::from(vec![
            Span::raw("Volume: "),
            Span::styled(
                format_bytes(stats.total_bytes_transferred),
                Style::default().fg(colors::DATA_VOLUME).bold(),
            ),
        ]),
        Line::from(vec![
            Span::raw("Rate:   "),
            Span::styled(
                format!("{}/s", format_bytes(stats.current_bytes_per_second as u64)),
                Style::default().fg(colors::PROGRESS_BAR),
            ),
        ]),
        Line::from(vec![
            Span::raw("Peak:   "),
            Span::styled(
                format!("{}/s", format_bytes(stats.peak_bytes_per_second as u64)),
                styles::muted(),
            ),
        ]),
    ];

    let widget = Paragraph::new(content).block(
        Block::default()
            .title(" Data Volume ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}
