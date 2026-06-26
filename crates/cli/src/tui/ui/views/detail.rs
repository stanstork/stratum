use crate::tui::{
    app::core::App,
    pipeline::{PipelineState, PipelineStatus},
    ui::{
        constants::{
            DETAIL_HEADER_HEIGHT, DETAIL_SOURCE_DEST_HEIGHT, DETAIL_STATS_HEIGHT, INDEXED_MARKER,
            MAX_MAPPINGS_DISPLAY, PROGRESS_BAR_WIDTH_LARGE, colors, styles,
        },
        formatters::{format_bytes, format_compact_number, format_duration, format_progress_bar},
        widgets::pipeline_table::get_status_display,
    },
};
use engine_planner::plan::{
    pipeline::{destination::WriteMode, plan::PipelinePlan},
    transform::{
        join::JoinType,
        mapping::{MappingSource, MappingType},
    },
    validation::types::{ValidationAction, ValidationLevel},
};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let pipeline = get_selected_pipeline(app);

    let Some(pipeline) = pipeline else {
        render_empty_state(frame, area);
        return;
    };

    let pipeline_plan = app
        .report
        .pipelines
        .iter()
        .find(|p| p.name == pipeline.name);

    let chunks = create_layout(area);

    render_header_section(frame, chunks[0], pipeline);
    render_source_dest_section(frame, chunks[1], pipeline_plan);
    render_details_section(frame, chunks[2], pipeline_plan);
    render_statistics_section(frame, chunks[3], pipeline);
}

fn get_selected_pipeline(app: &App) -> Option<&PipelineState> {
    let mut pipelines: Vec<_> = app.pipelines.iter().collect();

    // Sort by execution order: stage first, then by name
    pipelines.sort_by(|(name_a, pipeline_a), (name_b, pipeline_b)| {
        pipeline_a
            .stage
            .cmp(&pipeline_b.stage)
            .then_with(|| name_a.cmp(name_b))
    });

    pipelines
        .get(app.selected_pipeline)
        .map(|(_, pipeline)| *pipeline)
}

fn render_empty_state(frame: &mut Frame, area: Rect) {
    let placeholder = Paragraph::new("\n\nNo pipeline selected.")
        .alignment(Alignment::Center)
        .style(styles::muted());
    frame.render_widget(placeholder, area);
}

fn create_layout(area: Rect) -> std::rc::Rc<[Rect]> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(DETAIL_HEADER_HEIGHT),
            Constraint::Length(DETAIL_SOURCE_DEST_HEIGHT),
            Constraint::Min(8),
            Constraint::Length(DETAIL_STATS_HEIGHT),
        ])
        .margin(1)
        .split(area)
}

fn render_header_section(frame: &mut Frame, area: Rect, pipeline: &PipelineState) {
    let status_display = get_status_display(&pipeline.status);
    let progress_fraction = pipeline.progress_fraction();
    let progress_bar = format_progress_bar(progress_fraction, PROGRESS_BAR_WIDTH_LARGE);

    let lines = vec![
        Line::from(vec![
            Span::styled("Pipeline: ", styles::label()),
            Span::styled(&pipeline.name, styles::header_title()),
        ]),
        Line::from(vec![
            Span::styled("Status:   ", styles::label()),
            Span::styled(
                status_display.symbol,
                Style::default().fg(status_display.color),
            ),
            Span::raw(format!(" {}", status_display.text)),
            Span::raw("  "),
            Span::styled("Progress: ", styles::label()),
            Span::styled(progress_bar, Style::default().fg(colors::PROGRESS_BAR)),
            Span::raw(if pipeline.status == PipelineStatus::Completed {
                // When completed, show just processed rows
                format!(" ({})", format_compact_number(pipeline.processed_rows))
            } else {
                // When running, show processed/total
                format!(
                    " ({}/{})",
                    format_compact_number(pipeline.processed_rows),
                    format_compact_number(pipeline.source_rows)
                )
            }),
        ]),
    ];

    let widget = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn render_source_dest_section(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    render_source_panel(frame, chunks[0], pipeline_plan);
    render_destination_panel(frame, chunks[1], pipeline_plan);
}

fn render_source_panel(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let lines = if let Some(plan) = pipeline_plan {
        vec![
            Line::from(vec![
                Span::styled("Connection: ", styles::label()),
                Span::styled(
                    &plan.source.connection,
                    Style::default().fg(colors::PROGRESS_BAR),
                ),
            ]),
            Line::from(vec![
                Span::styled("Table:      ", styles::label()),
                Span::styled(&plan.source.fqn, styles::value_bold()),
            ]),
            Line::from(vec![
                Span::styled("Driver:     ", styles::label()),
                Span::styled(
                    format!("{:?}", plan.source.driver),
                    Style::default().fg(colors::THROUGHPUT),
                ),
            ]),
            Line::from(vec![
                Span::styled("Rows:       ", styles::label()),
                Span::styled(
                    plan.source.effective_row_count().display(),
                    styles::warning(),
                ),
            ]),
        ]
    } else {
        vec![Line::from(Span::styled(
            "No plan data available",
            styles::muted(),
        ))]
    };

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(" Source ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn render_destination_panel(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let lines = if let Some(plan) = pipeline_plan {
        let mode_str = get_write_mode_display(&plan.destination.mode);

        vec![
            Line::from(vec![
                Span::styled("Connection: ", styles::label()),
                Span::styled(
                    &plan.destination.connection,
                    Style::default().fg(colors::PROGRESS_BAR),
                ),
            ]),
            Line::from(vec![
                Span::styled("Table:      ", styles::label()),
                Span::styled(&plan.destination.fqn, styles::value_bold()),
            ]),
            Line::from(vec![
                Span::styled("Mode:       ", styles::label()),
                Span::styled(mode_str, styles::success()),
            ]),
            Line::from(vec![
                Span::styled("Exists:     ", styles::label()),
                Span::styled(
                    if plan.destination.exists {
                        "Yes"
                    } else {
                        "No (will create)"
                    },
                    if plan.destination.exists {
                        styles::success()
                    } else {
                        styles::warning()
                    },
                ),
            ]),
        ]
    } else {
        vec![Line::from(Span::styled(
            "No plan data available",
            styles::muted(),
        ))]
    };

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(" Destination ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn get_write_mode_display(mode: &WriteMode) -> &'static str {
    match mode {
        WriteMode::Replace => "replace (truncate+insert)",
        WriteMode::Append => "append (insert)",
        WriteMode::Upsert => "upsert (conflict update)",
        WriteMode::Merge => "merge",
    }
}

fn render_details_section(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),
            Constraint::Min(3),
            Constraint::Min(3),
            Constraint::Min(4),
        ])
        .split(area);

    render_joins_panel(frame, chunks[0], pipeline_plan);
    render_filters_panel(frame, chunks[1], pipeline_plan);
    render_validations_panel(frame, chunks[2], pipeline_plan);
    render_mappings_panel(frame, chunks[3], pipeline_plan);
}

fn render_joins_panel(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let (lines, count) = if let Some(plan) = pipeline_plan {
        if plan.joins.is_empty() {
            (vec![Line::from("  No joins configured")], 0)
        } else {
            let mut lines = Vec::new();
            for join in &plan.joins {
                for condition in &join.conditions {
                    let join_type = get_join_type_display(&join.join_type);

                    let mut spans = vec![
                        Span::raw("  • "),
                        Span::styled(
                            join_type,
                            Style::default()
                                .fg(colors::PROGRESS_BAR)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(" "),
                        Span::styled(&join.alias, styles::warning()),
                        Span::raw(" ON "),
                        Span::raw(&condition.expression),
                    ];

                    if condition.indexed {
                        spans.push(Span::styled(INDEXED_MARKER, styles::success()));
                    }

                    lines.push(Line::from(spans));
                }
            }
            (lines, plan.joins.len())
        }
    } else {
        (vec![Line::from("  No plan data available")], 0)
    };

    let title = if count > 0 {
        format!(" Joins ({}) ", count)
    } else {
        " Joins ".to_string()
    };

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn get_join_type_display(join_type: &JoinType) -> &'static str {
    match join_type {
        JoinType::Inner => "INNER",
        JoinType::Left => "LEFT",
        JoinType::Right => "RIGHT",
        JoinType::Full => "FULL",
    }
}

fn render_filters_panel(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let (lines, count) = if let Some(plan) = pipeline_plan {
        if plan.filters.is_empty() {
            (vec![Line::from("  No filters configured")], 0)
        } else {
            let mut lines = Vec::new();
            for filter in &plan.filters {
                let selectivity_pct = (filter.selectivity.selectivity * 100.0) as u32;

                let mut spans = vec![
                    Span::raw("  • "),
                    Span::raw(&filter.expression),
                    Span::raw(" ("),
                    Span::styled(format!("{}%", selectivity_pct), styles::warning()),
                    Span::raw(" selectivity)"),
                ];

                if filter.uses_index {
                    spans.push(Span::styled(INDEXED_MARKER, styles::success()));
                }

                lines.push(Line::from(spans));
            }
            (lines, plan.filters.len())
        }
    } else {
        (vec![Line::from("  No plan data available")], 0)
    };

    let title = if count > 0 {
        format!(" Filters ({}) ", count)
    } else {
        " Filters ".to_string()
    };

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn render_validations_panel(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let (lines, count) = if let Some(plan) = pipeline_plan {
        if plan.validations.is_empty() {
            (vec![Line::from("  No validations configured")], 0)
        } else {
            let mut lines = Vec::new();
            for validation in &plan.validations {
                let (level_str, level_color) = get_validation_level_display(&validation.level);
                let action_str = validation
                    .action
                    .as_ref()
                    .map(get_validation_action_display)
                    .unwrap_or("");

                let spans = vec![
                    Span::raw("  • ["),
                    Span::styled(
                        level_str,
                        Style::default()
                            .fg(level_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("] "),
                    Span::raw(&validation.check.expression),
                    Span::styled(action_str, styles::muted()),
                ];

                lines.push(Line::from(spans));
            }
            (lines, plan.validations.len())
        }
    } else {
        (vec![Line::from("  No plan data available")], 0)
    };

    let title = if count > 0 {
        format!(" Validations ({}) ", count)
    } else {
        " Validations ".to_string()
    };

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn get_validation_level_display(level: &ValidationLevel) -> (&'static str, ratatui::style::Color) {
    match level {
        ValidationLevel::Assert => ("ASSERT", colors::STATUS_FAILED),
        ValidationLevel::Warn => ("WARN", colors::STATUS_PAUSED),
    }
}

fn get_validation_action_display(action: &ValidationAction) -> &'static str {
    match action {
        ValidationAction::Skip => " (skip on fail)",
        ValidationAction::Fail => " (fail on error)",
    }
}

fn render_mappings_panel(frame: &mut Frame, area: Rect, pipeline_plan: Option<&PipelinePlan>) {
    let (lines, count) = if let Some(plan) = pipeline_plan {
        if plan.mappings.is_empty() {
            (vec![Line::from("  No column mappings configured")], 0)
        } else {
            let mut lines = Vec::new();

            for (i, mapping) in plan.mappings.iter().enumerate() {
                if i >= MAX_MAPPINGS_DISPLAY {
                    let remaining = plan.mappings.len() - MAX_MAPPINGS_DISPLAY;
                    lines.push(Line::from(format!("  ...{} more", remaining)));
                    break;
                }

                let source_expr = get_mapping_source_display(&mapping.source);
                let (mapping_type, type_color) = get_mapping_type_display(&mapping.mapping_type);

                let spans = vec![
                    Span::raw(format!(
                        "  {:<20} -> {:<20} (",
                        source_expr.chars().take(20).collect::<String>(),
                        mapping.target.chars().take(20).collect::<String>()
                    )),
                    Span::styled(mapping_type, Style::default().fg(type_color)),
                    Span::raw(")"),
                ];

                lines.push(Line::from(spans));
            }
            (lines, plan.mappings.len())
        }
    } else {
        (vec![Line::from("  No plan data available")], 0)
    };

    let title = if count > 0 {
        format!(" Column Mappings ({}) ", count)
    } else {
        " Column Mappings ".to_string()
    };

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}

fn get_mapping_source_display(source: &MappingSource) -> String {
    match source {
        MappingSource::Column { column, .. } => column.clone(),
        MappingSource::Renamed { column, .. } => column.clone(),
        MappingSource::Expression { expression, .. } => expression.clone(),
        MappingSource::Lookup { join_alias, column } => {
            format!("{}.{}", join_alias, column)
        }
        MappingSource::Function { name, args } => {
            if args.is_empty() {
                format!("{}()", name)
            } else {
                format!("{}({})", name, args.join(", "))
            }
        }
        MappingSource::Constant { value, .. } => value.clone(),
        MappingSource::Conditional { sql_preview, .. } => sql_preview.clone(),
    }
}

fn get_mapping_type_display(mapping_type: &MappingType) -> (&'static str, ratatui::style::Color) {
    match mapping_type {
        MappingType::Direct => ("direct", colors::TEXT_MUTED),
        MappingType::Renamed => ("renamed", colors::STATUS_COMPLETED),
        MappingType::Computed => ("computed", colors::STATUS_PAUSED),
        MappingType::Conditional => ("conditional", colors::THROUGHPUT),
        MappingType::Lookup => ("lookup", colors::PROGRESS_BAR),
        MappingType::Generated => ("generated", colors::THROUGHPUT),
        MappingType::Constant => ("constant", colors::DATA_VOLUME),
    }
}

fn render_statistics_section(frame: &mut Frame, area: Rect, pipeline: &PipelineState) {
    let throughput = pipeline.throughput.current_throughput();
    let duration_str = pipeline
        .duration()
        .map(format_duration)
        .unwrap_or_else(|| "--".into());

    let throughput_val = format!("{} rows/sec", format_compact_number(throughput as u64));
    let data_val = format_bytes(pipeline.bytes_transferred);

    let lines = vec![
        Line::from(vec![
            Span::styled("Throughput: ", styles::label()),
            Span::styled(format!("{:<18}", throughput_val), styles::success()),
            Span::styled("Batches: ", styles::label()),
            Span::raw(format!(
                "{}/{} completed",
                pipeline.current_batch, pipeline.total_batches
            )),
        ]),
        Line::from(vec![
            Span::styled("Duration:   ", styles::label()),
            Span::raw(format!("{:<18}", duration_str)),
            Span::styled("Failed:  ", styles::label()),
            Span::styled(
                format!("{} rows", format_compact_number(pipeline.failed_rows)),
                if pipeline.failed_rows > 0 {
                    styles::error()
                } else {
                    Style::default()
                },
            ),
        ]),
        Line::from(vec![
            Span::styled("Data:       ", styles::label()),
            Span::styled(
                format!("{:<18}", data_val),
                Style::default().fg(colors::PROGRESS_BAR),
            ),
            Span::styled("Skipped: ", styles::label()),
            Span::styled(
                format!("{} rows", format_compact_number(pipeline.skipped_rows)),
                if pipeline.skipped_rows > 0 {
                    styles::warning()
                } else {
                    Style::default()
                },
            ),
        ]),
    ];

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(" Statistics ")
            .borders(Borders::ALL)
            .border_style(styles::border()),
    );
    frame.render_widget(widget, area);
}
