use crate::tui::{
    app::core::App,
    pipeline::PipelineStatus,
    ui::{
        constants::{
            ETA_COLUMN_WIDTH, PROGRESS_BAR_WIDTH_SMALL, PROGRESS_COLUMN_WIDTH, RATE_COLUMN_WIDTH,
            ROWS_COLUMN_WIDTH, SELECTION_INDICATOR, STATUS_COLUMN_WIDTH, styles,
        },
        formatters::{
            format_compact_number, format_duration, format_progress_bar, format_rate,
            format_row_counts,
        },
    },
};
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::Style,
    text::Line,
    widgets::{Row, Table},
};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let cols = ColumnLayout::for_width(area.width);
    let sorted_names = get_sorted_pipeline_names(app);
    let header = cols.header();
    let rows = create_rows(&sorted_names, app, &cols);

    let table = Table::new(rows, cols.constraints())
        .header(header)
        .column_spacing(2);

    frame.render_widget(table, area);
}

/// Which columns the pipeline table shows at a given width.
struct ColumnLayout {
    show_progress: bool,
    show_rate: bool,
    show_eta: bool,
}

impl ColumnLayout {
    fn for_width(width: u16) -> Self {
        Self {
            show_progress: width >= 74,
            show_rate: width >= 86,
            show_eta: width >= 96,
        }
    }

    fn constraints(&self) -> Vec<Constraint> {
        let mut c = vec![
            Constraint::Min(14),                     // Pipeline name
            Constraint::Length(STATUS_COLUMN_WIDTH), // Status
        ];

        if self.show_progress {
            c.push(Constraint::Length(PROGRESS_COLUMN_WIDTH));
        }

        c.push(Constraint::Length(ROWS_COLUMN_WIDTH)); // Rows

        if self.show_rate {
            c.push(Constraint::Length(RATE_COLUMN_WIDTH));
        }
        if self.show_eta {
            c.push(Constraint::Length(ETA_COLUMN_WIDTH));
        }

        c
    }

    fn header(&self) -> Row<'static> {
        let mut cells = vec!["Pipeline", "Status"];
        if self.show_progress {
            cells.push("Progress");
        }
        cells.push("Rows");
        if self.show_rate {
            cells.push("Rate");
        }
        if self.show_eta {
            cells.push("ETA");
        }

        Row::new(cells).style(styles::table_header()).height(1)
    }
}

fn get_sorted_pipeline_names(app: &App) -> Vec<String> {
    let mut pipelines: Vec<_> = app.pipelines.iter().collect();

    // Sort by execution order: stage first, then by name
    pipelines.sort_by(|(name_a, pipeline_a), (name_b, pipeline_b)| {
        pipeline_a
            .stage
            .cmp(&pipeline_b.stage)
            .then_with(|| name_a.cmp(name_b))
    });

    pipelines
        .into_iter()
        .map(|(name, _)| name.clone())
        .collect()
}

fn create_rows<'a>(sorted_names: &[String], app: &App, cols: &ColumnLayout) -> Vec<Row<'a>> {
    sorted_names
        .iter()
        .enumerate()
        .filter_map(|(i, name)| {
            let p = app.pipelines.get(name)?;
            let is_selected = i == app.selected_pipeline;
            let style = if is_selected {
                styles::selected_row()
            } else {
                Style::default()
            };

            let status_display = get_status_display(&p.status);

            // Cells must be built in the same order and count as the constraints.
            let mut cells = vec![
                Line::from(format!(
                    "{}{}",
                    if is_selected {
                        SELECTION_INDICATOR
                    } else {
                        "  "
                    },
                    p.name
                )),
                Line::from(vec![
                    ratatui::text::Span::styled(
                        status_display.symbol,
                        Style::default().fg(status_display.color),
                    ),
                    ratatui::text::Span::raw(format!(" {}", status_display.text)),
                ]),
            ];

            if cols.show_progress {
                cells.push(Line::from(format_progress_bar(
                    p.progress_fraction(),
                    PROGRESS_BAR_WIDTH_SMALL,
                )));
            }

            cells.push(Line::from(if p.status == PipelineStatus::Completed {
                // When completed, show just processed rows
                format_compact_number(p.processed_rows)
            } else {
                // When running, show processed/total
                format_row_counts(p.processed_rows, p.source_rows)
            }));

            if cols.show_rate {
                cells.push(Line::from(format_rate(p.throughput.current_throughput())));
            }
            if cols.show_eta {
                cells.push(Line::from(
                    p.eta().map(format_duration).unwrap_or_else(|| "--".into()),
                ));
            }

            Some(Row::new(cells).style(style))
        })
        .collect()
}

pub struct StatusDisplay {
    pub symbol: &'static str,
    pub text: &'static str,
    pub color: ratatui::style::Color,
}

pub fn get_status_display(status: &PipelineStatus) -> StatusDisplay {
    use crate::tui::ui::constants::StatusDisplay as SD;

    match status {
        PipelineStatus::Pending => StatusDisplay {
            symbol: SD::PENDING.symbol,
            text: SD::PENDING.text,
            color: SD::PENDING.color,
        },
        PipelineStatus::Queued => StatusDisplay {
            symbol: SD::QUEUED.symbol,
            text: SD::QUEUED.text,
            color: SD::QUEUED.color,
        },
        PipelineStatus::Running => StatusDisplay {
            symbol: SD::RUNNING.symbol,
            text: SD::RUNNING.text,
            color: SD::RUNNING.color,
        },
        PipelineStatus::Paused => StatusDisplay {
            symbol: SD::PAUSED.symbol,
            text: SD::PAUSED.text,
            color: SD::PAUSED.color,
        },
        PipelineStatus::Completed => StatusDisplay {
            symbol: SD::COMPLETED.symbol,
            text: SD::COMPLETED.text,
            color: SD::COMPLETED.color,
        },
        PipelineStatus::Failed(_) => StatusDisplay {
            symbol: SD::FAILED.symbol,
            text: SD::FAILED.text,
            color: SD::FAILED.color,
        },
        PipelineStatus::Skipped => StatusDisplay {
            symbol: SD::SKIPPED.symbol,
            text: SD::SKIPPED.text,
            color: SD::SKIPPED.color,
        },
    }
}
