use super::{SourceArtifacts, SourceEndpoint};
use crate::error::MigrationError;
use async_trait::async_trait;
use connectors::{sql::metadata::table::TableMetadata, traits::introspector::SchemaIntrospector};
use engine_core::{
    dispatch_driver,
    drivers::DriverRef,
    plan::cascade::resolve_cascade_tables,
    schema::{
        graph_expander::GraphExpander,
        schema_ops::SchemaOps,
        type_registry::{Dialect, TypeRegistry},
    },
};
use engine_processing::io::source::{Source, plugin_introspector::PluginIntrospector};
use engine_wasm::registry::PluginRegistry;
use model::{
    execution::{
        pipeline::Pipeline,
        references::{DataMode, GraphReferences},
    },
    transform::mapping::TransformationMetadata,
};
use query_builder::offsets::OffsetStrategy;
use std::{collections::HashMap, sync::Arc};

pub struct DbSourceEndpoint(pub DriverRef);

pub struct WasmSourceEndpoint {
    pub registry: Arc<PluginRegistry>,
    pub plugin: String,
}

impl DbSourceEndpoint {
    async fn expand_graph(
        &self,
        root_table: &str,
        mapping: &TransformationMetadata,
        refs: &GraphReferences,
    ) -> Result<(Option<SchemaOps>, Option<HashMap<String, TableMetadata>>), MigrationError> {
        let source_dialect = self.0.dialect();
        let result = dispatch_driver!(&self.0, |d| {
            let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
            let type_registry = Arc::new(TypeRegistry::new(source_dialect, Dialect::Postgres)); // TODO: from dest
            let expander = GraphExpander::new(introspector, type_registry, source_dialect);
            expander
                .expand(root_table, refs, mapping, false, false)
                .await
                .map_err(MigrationError::from)?
        });
        let cascade_meta =
            matches!(refs.data_mode, DataMode::Cascade).then_some(result.discovered_tables);
        Ok((Some(result.schema_ops), cascade_meta))
    }
}

#[async_trait]
impl SourceEndpoint for DbSourceEndpoint {
    async fn build(
        &self,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<SourceArtifacts, MigrationError> {
        let (schema_ops, cascade_meta) = match &pipeline.source.graph_references {
            Some(refs) => {
                self.expand_graph(&pipeline.source.table, mapping, refs)
                    .await?
            }
            None => (None, None),
        };
        let cascade_tables = resolve_cascade_tables(pipeline, mapping, &cascade_meta);

        let source = dispatch_driver!(&self.0, |d| {
            Source::with_cascade(d.clone(), pipeline, mapping, offset_strategy, cascade_meta).await
        })?;

        Ok(SourceArtifacts {
            source,
            schema_ops,
            cascade_tables,
        })
    }

    fn dialect(&self) -> Option<Dialect> {
        Some(self.0.dialect())
    }

    fn schema_introspector(
        &self,
        _dest_dialect: Dialect,
    ) -> Option<(Arc<dyn SchemaIntrospector>, Dialect)> {
        let introspector = dispatch_driver!(&self.0, |d| d.clone() as Arc<dyn SchemaIntrospector>);
        Some((introspector, self.0.dialect()))
    }
}

#[async_trait]
impl SourceEndpoint for WasmSourceEndpoint {
    async fn build(
        &self,
        pipeline: &Pipeline,
        _mapping: &TransformationMetadata,
        _offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<SourceArtifacts, MigrationError> {
        let instance = self.registry.instantiate(&self.plugin)?; // WasmError -> MigrationError
        let source = Source::from_plugin(instance, pipeline)?; // DriverError -> MigrationError
        Ok(SourceArtifacts {
            source,
            schema_ops: None,
            cascade_tables: Vec::new(),
        })
    }

    fn dialect(&self) -> Option<Dialect> {
        None
    }

    fn schema_introspector(
        &self,
        dest_dialect: Dialect,
    ) -> Option<(Arc<dyn SchemaIntrospector>, Dialect)> {
        // Synthesize a schema from the plugin's declared `output` columns so a
        // `wasm -> db` pipeline can create the destination table.
        let meta = self.registry.metadata(&self.plugin).ok()?;
        if meta.output_schema.is_empty() {
            return None;
        }
        let introspector =
            Arc::new(PluginIntrospector::new(&meta.output_schema, dest_dialect)) as Arc<_>;
        Some((introspector, dest_dialect))
    }
}
