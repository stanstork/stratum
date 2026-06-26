use super::PlanDestinationEndpoint;
use crate::{
    builder::{
        endpoint::{dialect_driver, plugin_column_info},
        errors::{ReportBuilderError, SourceAnalyzerError},
    },
    plan::{
        connection::plan::DatabaseDriver,
        pipeline::{
            destination::{DataImpact, DataImpactAction, DestinationPlan, WriteMode},
            source::ColumnInfo,
        },
    },
};
use connectors::traits::introspector::SchemaIntrospector;
use engine_core::{dispatch_driver, drivers::DriverRef};
use engine_wasm::registry::PluginRegistry;
use model::{
    core::types::Type,
    execution::{pipeline::Pipeline, row_count::RowCount},
};
use std::{collections::HashMap, sync::Arc};

pub struct DbPlanDestinationEndpoint {
    driver: DriverRef,
    plan: DestinationPlan,
    column_types: HashMap<String, Type>,
}

impl DbPlanDestinationEndpoint {
    pub async fn new(pipeline: &Pipeline, driver: DriverRef) -> Result<Self, ReportBuilderError> {
        let table = &pipeline.destination.table;
        let metadata = dispatch_driver!(&driver, |d| { d.table_metadata(table).await.ok() });

        let dialect = driver.dialect();
        let db_driver = dialect_driver(dialect);

        let (columns, column_types, exists, schema, primary_keys) = if let Some(meta) = &metadata {
            let mut columns: Vec<ColumnInfo> = meta
                .columns
                .values()
                .map(ColumnInfo::from_metadata)
                .collect();
            columns.sort_by_key(|c| meta.columns.get(&c.name).map(|m| m.ordinal).unwrap_or(0));
            let column_types: HashMap<String, Type> = meta
                .columns
                .values()
                .map(|col| (col.name.clone(), dialect.to_canonical(col)))
                .collect();
            (
                columns,
                column_types,
                true,
                meta.schema.clone(),
                meta.primary_keys.clone(),
            )
        } else {
            (Vec::new(), HashMap::new(), false, None, Vec::new())
        };

        let plan = DestinationPlan {
            connection: pipeline.destination.connection.name.clone(),
            table: table.clone(),
            schema,
            fqn: table.clone(),
            driver: db_driver,
            exists,
            current_rows: RowCount::unknown(),
            mode: WriteMode::Append,
            conflict_keys: primary_keys,
            columns,
            data_impact: DataImpact {
                action: if exists {
                    DataImpactAction::Append
                } else {
                    DataImpactAction::Create
                },
                description: if exists {
                    "Append rows to existing table".to_string()
                } else {
                    "Table will be created".to_string()
                },
                is_destructive: false,
                affected_rows: None,
            },
        };

        Ok(Self {
            driver,
            plan,
            column_types,
        })
    }
}

impl PlanDestinationEndpoint for DbPlanDestinationEndpoint {
    fn destination_plan(&self) -> &DestinationPlan {
        &self.plan
    }
    fn column_types(&self) -> &HashMap<String, Type> {
        &self.column_types
    }
    fn db_driver(&self) -> Option<&DriverRef> {
        Some(&self.driver)
    }
    fn plugin_name(&self) -> Option<&str> {
        None
    }
}

pub struct WasmPlanDestinationEndpoint {
    plugin: String,
    plan: DestinationPlan,
    column_types: HashMap<String, Type>,
}

impl WasmPlanDestinationEndpoint {
    pub fn new(
        pipeline: &Pipeline,
        registry: Arc<PluginRegistry>,
        plugin: String,
    ) -> Result<Self, ReportBuilderError> {
        let metadata = registry.metadata(&plugin).map_err(|e| {
            ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                "could not load sink plugin '{}': {}",
                plugin, e
            )))
        })?;

        let columns: Vec<ColumnInfo> = metadata
            .input_schema
            .iter()
            .enumerate()
            .map(plugin_column_info)
            .collect();
        let column_types: HashMap<String, Type> = metadata
            .input_schema
            .iter()
            .map(|f| (f.name.clone(), f.to_canonical_type()))
            .collect();

        let plan = DestinationPlan {
            connection: pipeline.destination.connection.name.clone(),
            table: pipeline.destination.table.clone(),
            schema: None,
            fqn: format!("plugin://{}", plugin),
            driver: DatabaseDriver::Other("wasm".to_string()),
            exists: true, // the plugin always exists from the planner's POV
            current_rows: RowCount::unknown(),
            mode: WriteMode::Append,
            conflict_keys: Vec::new(),
            columns,
            data_impact: DataImpact {
                action: DataImpactAction::Append,
                description: "Rows handed to WASM sink plugin".to_string(),
                is_destructive: false,
                affected_rows: None,
            },
        };

        Ok(Self {
            plugin,
            plan,
            column_types,
        })
    }
}

impl PlanDestinationEndpoint for WasmPlanDestinationEndpoint {
    fn destination_plan(&self) -> &DestinationPlan {
        &self.plan
    }
    fn column_types(&self) -> &HashMap<String, Type> {
        &self.column_types
    }
    fn db_driver(&self) -> Option<&DriverRef> {
        None
    }
    fn plugin_name(&self) -> Option<&str> {
        Some(&self.plugin)
    }
}
