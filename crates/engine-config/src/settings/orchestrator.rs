use super::{
    create_cols::CreateMissingColumnsSetting, create_tables::CreateMissingTablesSetting,
    driver::SchemaDriver, endpoint::Endpoint, endpoint::SchemaSource, error::SettingsError,
    infer_schema::InferSchemaSetting, traits::MigrationSetting, types::Settings,
    validated::ValidatedSettings, validator::SettingsValidator,
};
use crate::settings::SchemaSettingContext;
use connectors::traits::introspector::SchemaIntrospector;
use engine_core::schema::{schema_ops::SchemaOps, type_registry::Dialect};
use engine_processing::context::PipelineContext;
use model::{core::value::Value, execution::flags::IntegrityMode};
use std::{collections::HashMap, sync::Arc};

/// Validate settings and collect schema operations without executing DDL.
///
/// Returns validated settings (for non-schema config like batch_size) and
/// the collected schema operations split into pre/post migration phases.
pub async fn validate_and_plan<D>(
    ctx: &mut PipelineContext,
    src_introspector: Arc<dyn SchemaIntrospector>,
    src_dialect: Dialect,
    dst_driver: Arc<D>,
    settings: &HashMap<String, Value>,
    is_dry_run: bool,
    integrity: IntegrityMode,
) -> Result<(ValidatedSettings, SchemaOps), SettingsError>
where
    D: SchemaDriver,
{
    let settings = Settings::from_map(settings);

    let introspector = dst_driver.clone() as Arc<dyn SchemaIntrospector>;
    let validator = SettingsValidator::new(
        &ctx.source,
        &ctx.destination,
        introspector.as_ref(),
        is_dry_run,
        integrity,
    );
    let validated_settings = validator.validate(&settings).await?;

    let mut all_settings = collect_settings(
        ctx,
        src_introspector,
        src_dialect,
        dst_driver.clone(),
        &validated_settings,
    )
    .await;

    let mut schema_ops = SchemaOps::empty();

    for setting in all_settings.iter_mut() {
        if setting.can_apply(ctx) {
            // Collect schema ops (no-op for non-schema settings)
            let ops = setting.plan(ctx).await?;
            schema_ops.merge(ops);
        }
    }

    Ok((validated_settings, schema_ops))
}

pub async fn collect_settings<D>(
    ctx: &PipelineContext,
    src_introspector: Arc<dyn SchemaIntrospector>,
    src_dialect: Dialect,
    dst_driver: Arc<D>,
    validated: &ValidatedSettings,
) -> Vec<Box<dyn MigrationSetting>>
where
    D: SchemaDriver,
{
    let source_info = SchemaSource::new(src_introspector, ctx.source.name.clone(), src_dialect);

    let dest_info = Endpoint::new(
        dst_driver,
        ctx.destination.name.clone(),
        ctx.destination.format.to_dialect(),
    );

    let schema_ctx = SchemaSettingContext::new(source_info, dest_info, &ctx.mapping, validated);
    let mut all_settings: Vec<Box<dyn MigrationSetting>> = Vec::new();

    if validated.infer_schema() {
        let infer_schema_setting = InferSchemaSetting::new(schema_ctx.clone()).await;
        all_settings.push(Box::new(infer_schema_setting));
    }

    if validated.create_missing_tables() {
        let missing_tables_setting = CreateMissingTablesSetting::new(schema_ctx.clone()).await;
        all_settings.push(Box::new(missing_tables_setting));
    }

    if validated.create_missing_columns() {
        let missing_cols_setting = CreateMissingColumnsSetting::new(schema_ctx.clone()).await;
        all_settings.push(Box::new(missing_cols_setting));
    }

    // Settings are already created in phase order due to enum ordering
    all_settings.sort_by_key(|s| s.phase());

    all_settings
}
