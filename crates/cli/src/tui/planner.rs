use crate::tui::pipeline::{PipelineState, PipelineStatus};
use engine_core::utils::make_item_id;
use engine_planner::plan::{
    execution::execution_plan::ExecutionPlan, pipeline::plan::PipelinePlan,
};
use std::collections::HashMap;

/// Converts planner's ExecutionPlan into TUI pipeline states
///
/// This pre-populates the TUI with pipeline metadata from the plan,
/// including row counts, execution stages, and dependencies.
pub fn initialize_pipelines_from_plan(
    plan: &ExecutionPlan,
    plan_hash: &str,
) -> HashMap<String, PipelineState> {
    let mut pipelines = HashMap::new();

    for (idx, pipeline_plan) in plan.pipelines.iter().enumerate() {
        let state = create_pipeline_state(pipeline_plan);

        // Compute item_id the same way the executor does
        // This ensures events from the executor can find the right pipeline
        let item_id = make_item_id(plan_hash, &pipeline_plan.destination.table, idx);

        // Store pipeline by item_id (used in migration events)
        pipelines.insert(item_id, state);
    }

    pipelines
}

/// Creates a pipeline state from a pipeline plan
fn create_pipeline_state(pipeline_plan: &PipelinePlan) -> PipelineState {
    let source_rows = pipeline_plan.source.effective_row_count().value;

    let mut state = PipelineState::new(
        pipeline_plan.name.clone(),
        pipeline_plan.execution_stage as u32,
    );

    // Set source row count from planner metadata
    state.source_rows = source_rows;

    // Set total batches estimate
    state.total_batches = pipeline_plan.estimations.batches as u32;

    // Set initial status based on dependencies and execution stage
    state.status = determine_initial_status(pipeline_plan);

    state
}

/// Determines the initial status of a pipeline based on its dependencies
fn determine_initial_status(pipeline_plan: &PipelinePlan) -> PipelineStatus {
    if pipeline_plan.depends_on.is_empty() && pipeline_plan.execution_stage == 0 {
        // No dependencies and in first stage -> ready to queue
        PipelineStatus::Queued
    } else {
        // Has dependencies or in later stage -> pending
        PipelineStatus::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_planner::plan::{
        connection::plan::DatabaseDriver,
        estimation::pipeline::PipelineEstimations,
        execution::types::RowCount,
        pipeline::{
            data_flow_summary::DataFlowSummary,
            destination::{DataImpact, DataImpactAction, DestinationPlan, WriteMode},
            plan::PipelinePlan,
            settings::PipelineSettings,
            source::SourcePlan,
        },
    };

    fn create_test_source_plan(total_rows: u64) -> SourcePlan {
        SourcePlan {
            connection: "test_conn".to_string(),
            table: "test_table".to_string(),
            schema: None,
            fqn: "test_table".to_string(),
            driver: DatabaseDriver::MySql,
            total_rows: RowCount::exact(total_rows),
            filtered_rows: None,
            columns: vec![],
            primary_key: vec![],
            indexes: vec![],
            size_bytes: 0,
            last_analyzed: chrono::Utc::now(),
        }
    }

    fn create_test_destination_plan() -> DestinationPlan {
        DestinationPlan {
            connection: "test_conn".to_string(),
            table: "test_dest_table".to_string(),
            schema: None,
            fqn: "test_dest_table".to_string(),
            driver: DatabaseDriver::MySql,
            exists: true,
            current_rows: RowCount::exact(0),
            mode: WriteMode::Append,
            conflict_keys: vec![],
            columns: vec![],
            data_impact: DataImpact {
                action: DataImpactAction::Append,
                description: "Test append".to_string(),
                is_destructive: false,
                affected_rows: None,
            },
        }
    }

    #[test]
    fn test_pipeline_initialization_with_no_dependencies() {
        let pipeline_plan = PipelinePlan {
            name: "test_pipeline".to_string(),
            description: None,
            execution_order: 0,
            execution_stage: 0,
            depends_on: vec![],
            source: create_test_source_plan(1000),
            destination: create_test_destination_plan(),
            filters: vec![],
            joins: vec![],
            mappings: vec![],
            validations: vec![],
            error_handling: Default::default(),
            pagination: Default::default(),
            hooks: Default::default(),
            settings: PipelineSettings::default(),
            data_flow_summary: DataFlowSummary::default(),
            schema_changes: vec![],
            diagnostics: vec![],
            estimations: PipelineEstimations {
                duration: Default::default(),
                rows_per_second: 100,
                batches: 10,
                memory_mb: 50,
            },
            sample: None,
        };

        let plan = engine_planner::plan::execution::execution_plan::ExecutionPlan {
            plan_id: "test".to_string(),
            generated_at: chrono::Utc::now(),
            engine_version: "0.1.0".to_string(),
            config_hash: "abc123".to_string(),
            config_path: "test.smql".to_string(),
            execution_settings: Default::default(),
            defines: Default::default(),
            connections: vec![],
            pipelines: vec![pipeline_plan],
            execution_order: vec![],
            summary: Default::default(),
            diagnostics: vec![],
            estimations: Default::default(),
            is_executable: true,
            blocking_reason: None,
        };

        let pipelines = initialize_pipelines_from_plan(&plan, "test-plan-hash");

        assert_eq!(pipelines.len(), 1);
        // Pipeline is keyed by item_id (hash), not name
        let state = pipelines.values().next().unwrap();
        assert_eq!(state.name, "test_pipeline");
        assert_eq!(state.source_rows, 1000);
        assert_eq!(state.total_batches, 10);
        assert_eq!(state.status, PipelineStatus::Queued);
        assert_eq!(state.stage, 0);
    }

    #[test]
    fn test_pipeline_initialization_with_dependencies() {
        let pipeline_plan = PipelinePlan {
            name: "dependent_pipeline".to_string(),
            description: None,
            execution_order: 1,
            execution_stage: 1,
            depends_on: vec!["other_pipeline".to_string()],
            source: create_test_source_plan(5000),
            destination: create_test_destination_plan(),
            filters: vec![],
            joins: vec![],
            mappings: vec![],
            validations: vec![],
            error_handling: Default::default(),
            pagination: Default::default(),
            hooks: Default::default(),
            settings: PipelineSettings::default(),
            data_flow_summary: DataFlowSummary::default(),
            schema_changes: vec![],
            diagnostics: vec![],
            estimations: PipelineEstimations {
                duration: Default::default(),
                rows_per_second: 200,
                batches: 25,
                memory_mb: 100,
            },
            sample: None,
        };

        let plan = engine_planner::plan::execution::execution_plan::ExecutionPlan {
            plan_id: "test".to_string(),
            generated_at: chrono::Utc::now(),
            engine_version: "0.1.0".to_string(),
            config_hash: "abc123".to_string(),
            config_path: "test.smql".to_string(),
            execution_settings: Default::default(),
            defines: Default::default(),
            connections: vec![],
            pipelines: vec![pipeline_plan],
            execution_order: vec![],
            summary: Default::default(),
            diagnostics: vec![],
            estimations: Default::default(),
            is_executable: true,
            blocking_reason: None,
        };

        let pipelines = initialize_pipelines_from_plan(&plan, "test-plan-hash");

        assert_eq!(pipelines.len(), 1);
        // Pipeline is keyed by item_id (hash), not name
        let state = pipelines.values().next().unwrap();
        assert_eq!(state.name, "dependent_pipeline");
        assert_eq!(state.source_rows, 5000);
        assert_eq!(state.total_batches, 25);
        assert_eq!(state.status, PipelineStatus::Pending);
        assert_eq!(state.stage, 1);
    }

    #[test]
    fn test_make_item_id_consistency() {
        let id1 = make_item_id("hash1", "table1", 0);
        let id2 = make_item_id("hash1", "table1", 0);
        let id3 = make_item_id("hash2", "table1", 0);

        // Same inputs should produce same ID
        assert_eq!(id1, id2);

        // Different inputs should produce different IDs
        assert_ne!(id1, id3);

        // ID should have correct format
        assert!(id1.starts_with("itm-"));
        assert_eq!(id1.len(), 20); // "itm-" + 16 hex chars
    }

    #[test]
    fn test_determine_initial_status() {
        let mut plan = PipelinePlan {
            name: "test".to_string(),
            description: None,
            execution_order: 0,
            execution_stage: 0,
            depends_on: vec![],
            source: create_test_source_plan(100),
            destination: create_test_destination_plan(),
            filters: vec![],
            joins: vec![],
            mappings: vec![],
            validations: vec![],
            error_handling: Default::default(),
            pagination: Default::default(),
            hooks: Default::default(),
            settings: PipelineSettings::default(),
            data_flow_summary: DataFlowSummary::default(),
            schema_changes: vec![],
            diagnostics: vec![],
            estimations: PipelineEstimations {
                duration: Default::default(),
                rows_per_second: 100,
                batches: 10,
                memory_mb: 50,
            },
            sample: None,
        };

        // No dependencies, stage 0 -> Queued
        assert_eq!(determine_initial_status(&plan), PipelineStatus::Queued);

        // Has dependencies -> Pending
        plan.depends_on = vec!["other".to_string()];
        assert_eq!(determine_initial_status(&plan), PipelineStatus::Pending);

        // Later stage -> Pending
        plan.depends_on = vec![];
        plan.execution_stage = 1;
        assert_eq!(determine_initial_status(&plan), PipelineStatus::Pending);
    }
}
