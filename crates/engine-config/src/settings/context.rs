use super::{
    driver::SchemaDriver,
    endpoint::{Endpoint, SchemaSource},
    error::SettingsError,
};
use crate::settings::validated::ValidatedSettings;
use engine_schema::planner::SchemaPlanner;
use engine_schema::{
    plan::{SchemaObjectFlags, SchemaPlan},
    type_registry::TypeRegistry,
    types::TypeEngine,
};
use model::transform::mapping::TransformationMetadata;
use std::sync::Arc;

#[derive(Clone)]
pub struct SchemaSettingContext<D: SchemaDriver> {
    pub source: SchemaSource,
    pub destination: Endpoint<D>,
    pub mapping: TransformationMetadata,
    pub settings: ValidatedSettings,
}

impl<D: SchemaDriver> SchemaSettingContext<D> {
    pub fn new(
        source: SchemaSource,
        destination: Endpoint<D>,
        mapping: &TransformationMetadata,
        settings: &ValidatedSettings,
    ) -> Self {
        Self {
            source,
            destination,
            mapping: mapping.clone(),
            settings: settings.clone(),
        }
    }

    pub async fn destination_exists(&self) -> Result<bool, SettingsError> {
        self.destination
            .driver
            .table_exists(&self.destination.name)
            .await
            .map_err(SettingsError::Driver)
    }

    pub fn type_registry(&self) -> TypeRegistry {
        TypeRegistry::new(self.source.dialect, self.destination.dialect)
    }

    fn schema_object_flags(&self) -> SchemaObjectFlags {
        SchemaObjectFlags {
            skip_pk: self.settings.skip_pk(),
            skip_fk: self.settings.skip_fk(),
            skip_idx: self.settings.skip_idx(),
            skip_seq: self.settings.skip_seq(),
            skip_unique: self.settings.skip_unique(),
            skip_check: self.settings.skip_check(),
        }
    }

    pub async fn init_schema_planner(&self) -> Result<SchemaPlanner, SettingsError> {
        let mapped_columns_only = self.mapping.has_projection;
        let introspector = self.source.introspector.clone();

        Ok(SchemaPlanner::new(
            introspector,
            self.source.dialect,
            self.mapping.clone(),
            self.schema_object_flags(),
            mapped_columns_only,
            self.type_registry(),
        ))
    }

    pub async fn build_schema_plan(&self) -> Result<SchemaPlan, SettingsError> {
        let mapped_columns_only = self.mapping.has_projection;

        let introspector = self.source.introspector.clone();
        let registry = Arc::new(self.type_registry());

        let type_engine =
            TypeEngine::new(introspector.clone(), registry.clone(), self.source.dialect);

        Ok(SchemaPlan::new(
            type_engine,
            self.schema_object_flags(),
            mapped_columns_only,
            self.mapping.clone(),
        ))
    }
}
