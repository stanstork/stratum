use crate::tui::{
    app::core::App,
    pipeline::PipelineStatus,
    ui::constants::{ACTIVE_MARKER, COMPLETED_MARKER, colors, styles},
};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};
use std::collections::{BTreeMap, HashSet};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let (stages, active_stages, completed_stages) = analyze_stages(app);
    let lines = build_stage_lines(&stages, &active_stages, &completed_stages);

    let block = Block::default()
        .title(" Execution Stages ")
        .borders(Borders::ALL)
        .border_style(styles::border());

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn analyze_stages(app: &App) -> (BTreeMap<u32, Vec<&str>>, HashSet<u32>, HashSet<u32>) {
    let mut stages: BTreeMap<u32, Vec<&str>> = BTreeMap::new();
    let mut active_stages = HashSet::new();
    let mut completed_stages = HashSet::new();

    // Group pipelines by stage
    for pipeline in app.pipelines.values() {
        stages
            .entry(pipeline.stage)
            .or_default()
            .push(pipeline.name.as_str());

        if pipeline.status == PipelineStatus::Running {
            active_stages.insert(pipeline.stage);
        }
    }

    // Determine which stages are fully completed
    for (stage_num, pipeline_names) in stages.iter() {
        let all_completed = pipeline_names.iter().all(|name| {
            app.pipelines
                .get(*name)
                .map(|p| p.status == PipelineStatus::Completed)
                .unwrap_or(false)
        });
        if all_completed {
            completed_stages.insert(*stage_num);
        }
    }

    (stages, active_stages, completed_stages)
}

fn build_stage_lines(
    stages: &BTreeMap<u32, Vec<&str>>,
    active_stages: &HashSet<u32>,
    completed_stages: &HashSet<u32>,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    for (stage_num, pipeline_names) in stages.iter() {
        let is_active = active_stages.contains(stage_num);
        let is_completed = completed_stages.contains(stage_num);

        let marker = if is_active {
            ACTIVE_MARKER
        } else if is_completed {
            COMPLETED_MARKER
        } else {
            ""
        };

        let pipeline_list = pipeline_names.join("  ");
        let stage_line = format!("Stage {}: {}{}", stage_num, pipeline_list, marker);

        let style = if is_active {
            Style::default()
                .fg(colors::STATUS_RUNNING)
                .add_modifier(Modifier::BOLD)
        } else if is_completed {
            Style::default().fg(colors::STATUS_COMPLETED)
        } else {
            styles::muted()
        };

        lines.push(Line::from(Span::styled(stage_line, style)));
    }

    lines
}
