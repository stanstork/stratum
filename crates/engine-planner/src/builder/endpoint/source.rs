use super::PlanSourceEndpoint;
use crate::{
    builder::{
        endpoint::{dialect_driver, plugin_column_info},
        errors::{ReportBuilderError, SourceAnalyzerError},
    },
    plan::{
        connection::plan::DatabaseDriver,
        pipeline::source::{ColumnInfo, IndexInfo, SourcePlan},
    },
};
use chrono::Utc;
use connectors::traits::introspector::SchemaIntrospector;
use engine_core::{dispatch_driver, drivers::DriverRef};
use engine_wasm::registry::PluginRegistry;
use model::{
    core::types::Type,
    execution::{pipeline::Pipeline, row_count::RowCount},
};
use std::{collections::HashMap, sync::Arc};

pub struct DbPlanSourceEndpoint {
    driver: DriverRef,
    plan: SourcePlan,
    column_types: HashMap<String, Type>,
}

impl DbPlanSourceEndpoint {
    pub async fn new(pipeline: &Pipeline, driver: DriverRef) -> Result<Self, ReportBuilderError> {
        let table = &pipeline.source.table;
        let metadata = dispatch_driver!(&driver, |d| {
            d.table_metadata(table).await.map_err(|e| {
                ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                    "could not introspect source table '{}': {}",
                    table, e
                )))
            })?
        });

        let dialect = driver.dialect();
        let db_driver = dialect_driver(dialect);

        let columns: Vec<ColumnInfo> = metadata
            .columns
            .values()
            .map(ColumnInfo::from_metadata)
            .collect();
        let column_types: HashMap<String, Type> = metadata
            .columns
            .values()
            .map(|col| (col.name.clone(), dialect.to_canonical(col)))
            .collect();

        // Sort columns by ordinal for stable output.
        let mut columns = columns;
        columns.sort_by_key(|c| {
            metadata
                .columns
                .get(&c.name)
                .map(|m| m.ordinal)
                .unwrap_or(0)
        });

        let plan = SourcePlan {
            connection: pipeline.source.connection.name.clone(),
            table: table.clone(),
            schema: metadata.schema.clone(),
            fqn: table.clone(),
            driver: db_driver,
            total_rows: RowCount::unknown(),
            filtered_rows: None,
            columns,
            primary_key: metadata.primary_keys.clone(),
            indexes: Vec::<IndexInfo>::new(),
            size_bytes: 0,
            last_analyzed: Utc::now(),
        };

        Ok(Self {
            driver,
            plan,
            column_types,
        })
    }
}

impl PlanSourceEndpoint for DbPlanSourceEndpoint {
    fn source_plan(&self) -> &SourcePlan {
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

pub struct WasmPlanSourceEndpoint {
    plugin: String,
    plan: SourcePlan,
    column_types: HashMap<String, Type>,
}

impl WasmPlanSourceEndpoint {
    pub fn new(
        pipeline: &Pipeline,
        registry: Arc<PluginRegistry>,
        plugin: String,
    ) -> Result<Self, ReportBuilderError> {
        let metadata = registry.metadata(&plugin).map_err(|e| {
            ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                "could not load source plugin '{}': {}",
                plugin, e
            )))
        })?;

        let columns: Vec<ColumnInfo> = metadata
            .output_schema
            .iter()
            .enumerate()
            .map(plugin_column_info)
            .collect();
        let column_types: HashMap<String, Type> = metadata
            .output_schema
            .iter()
            .map(|f| (f.name.clone(), f.to_canonical_type()))
            .collect();

        let plan = SourcePlan {
            connection: pipeline.source.connection.name.clone(),
            table: pipeline.source.table.clone(),
            schema: None,
            fqn: format!("plugin://{}", plugin),
            driver: DatabaseDriver::Other("wasm".to_string()),
            total_rows: RowCount::unknown(),
            filtered_rows: None,
            columns,
            primary_key: Vec::new(),
            indexes: Vec::new(),
            size_bytes: 0,
            last_analyzed: Utc::now(),
        };

        Ok(Self {
            plugin,
            plan,
            column_types,
        })
    }
}

impl PlanSourceEndpoint for WasmPlanSourceEndpoint {
    fn source_plan(&self) -> &SourcePlan {
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
