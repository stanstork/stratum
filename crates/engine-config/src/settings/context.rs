use super::{driver::SchemaDriver, endpoint::Endpoint, error::SettingsError};
use crate::settings::CopyColumns;
use crate::settings::validated::ValidatedSettings;
use connectors::traits::introspector::SchemaIntrospector;
use engine_core::schema::planner::SchemaPlanner;
use engine_core::schema::{plan::SchemaPlan, type_registry::TypeRegistry, types::TypeEngine};
use model::transform::mapping::TransformationMetadata;
use std::sync::Arc;

#[derive(Clone)]
pub struct SchemaSettingContext<S: SchemaDriver, D: SchemaDriver> {
    pub source: Endpoint<S>,
    pub destination: Endpoint<D>,
    pub mapping: TransformationMetadata,
    pub settings: ValidatedSettings,
}

impl<S: SchemaDriver, D: SchemaDriver> SchemaSettingContext<S, D> {
    pub fn new(
        source: Endpoint<S>,
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

    pub async fn init_schema_planner(&self) -> Result<SchemaPlanner, SettingsError> {
        let ignore_constraints = self.settings.ignore_constraints();
        let mapped_columns_only = *self.settings.copy_columns() == CopyColumns::MapOnly;
        let introspector = self.source.driver.clone() as Arc<dyn SchemaIntrospector>;

        Ok(SchemaPlanner::new(
            introspector,
            self.source.dialect,
            self.mapping.clone(),
            ignore_constraints,
            mapped_columns_only,
            self.type_registry(),
        ))
    }

    pub async fn build_schema_plan(&self) -> Result<SchemaPlan, SettingsError> {
        let ignore_constraints = self.settings.ignore_constraints();
        let mapped_columns_only = *self.settings.copy_columns() == CopyColumns::MapOnly;

        let introspector = self.source.driver.clone() as Arc<dyn SchemaIntrospector>;
        let registry = Arc::new(self.type_registry());

        let type_engine =
            TypeEngine::new(introspector.clone(), registry.clone(), self.source.dialect);

        Ok(SchemaPlan::new(
            type_engine,
            ignore_constraints,
            mapped_columns_only,
            self.mapping.clone(),
        ))
    }
}
