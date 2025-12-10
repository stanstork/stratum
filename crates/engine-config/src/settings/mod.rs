use crate::{
    report::dry_run::DryRunReport,
    settings::{error::SettingsError, validated::ValidatedSettings, validator::SettingsValidator},
};
use async_trait::async_trait;
use batch_size::BatchSizeSetting;
use constraints::IgnoreConstraintsSettings;
use copy_cols::CopyColumnsSetting;
use create_cols::CreateMissingColumnsSetting;
use create_tables::CreateMissingTablesSetting;
use engine_core::context::item::ItemContext;
use futures::lock::Mutex;
use infer_schema::InferSchemaSetting;
use model::core::value::Value;
use phase::MigrationSettingsPhase;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, sync::Arc};
use tracing::info;

pub mod batch_size;
pub mod cascade;
pub mod constraints;
pub mod context;
pub mod copy_cols;
pub mod create_cols;
pub mod create_tables;
pub mod error;
pub mod infer_schema;
pub mod phase;
pub mod schema_manager;
pub mod validated;
pub mod validator;

/// Migration settings structure
#[derive(Debug, Clone)]
pub struct Settings {
    pub infer_schema: bool,
    pub ignore_constraints: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub copy_columns: CopyColumns,
    pub batch_size: usize,
    pub cascade_schema: bool,
    pub csv_header: bool,
    pub csv_delimiter: char,
    pub csv_id_column: Option<String>,
}

impl Settings {
    pub fn from_map(map: &HashMap<String, Value>) -> Settings {
        Settings {
            infer_schema: map
                .get("infer_schema")
                .and_then(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false),
            ignore_constraints: map
                .get("ignore_constraints")
                .and_then(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false),
            create_missing_columns: map
                .get("create_missing_columns")
                .and_then(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false),
            create_missing_tables: map
                .get("create_missing_tables")
                .and_then(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false),
            copy_columns: map
                .get("copy_columns")
                .and_then(|v| match v {
                    Value::String(s) => match s.to_uppercase().as_str() {
                        "ALL" => Some(CopyColumns::All),
                        "MAP_ONLY" => Some(CopyColumns::MapOnly),
                        _ => None,
                    },
                    _ => None,
                })
                .unwrap_or(CopyColumns::All),
            batch_size: map
                .get("batch_size")
                .and_then(|v| match v {
                    Value::Int(i) => Some(*i as usize),
                    Value::Uint(u) => Some(*u as usize),
                    _ => None,
                })
                .unwrap_or(0),
            cascade_schema: map
                .get("cascade_schema")
                .and_then(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false),
            csv_header: map
                .get("csv_header")
                .and_then(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(true),
            csv_delimiter: map
                .get("csv_delimiter")
                .and_then(|v| match v {
                    Value::String(s) => s.chars().next(),
                    _ => None,
                })
                .unwrap_or(','),
            csv_id_column: map.get("csv_id_column").and_then(|v| match v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            }),
        }
    }
}

/// Copy columns strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyColumns {
    All,
    MapOnly,
}

impl fmt::Display for CopyColumns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyColumns::All => write!(f, "ALL"),
            CopyColumns::MapOnly => write!(f, "MAP_ONLY"),
        }
    }
}

#[async_trait]
pub trait MigrationSetting: Send + Sync {
    fn phase(&self) -> MigrationSettingsPhase;

    /// Check whether this setting should be applied in the given context.
    /// By default, all settings are applicable.
    fn can_apply(&self, _ctx: &ItemContext) -> bool {
        true
    }

    async fn apply(&mut self, _ctx: &mut ItemContext) -> Result<(), SettingsError> {
        Ok(())
    }
}

/// Validate and apply all migration settings.
pub async fn validate_and_apply(
    ctx: &mut ItemContext,
    settings: &HashMap<String, Value>,
    is_dry_run: bool,
    dry_run_report: &Arc<Mutex<DryRunReport>>,
) -> Result<ValidatedSettings, SettingsError> {
    let settings_struct = Settings::from_map(settings);

    let validator = SettingsValidator::new(&ctx.source, &ctx.destination, is_dry_run);
    let validated_settings = validator.validate(&settings_struct).await?;

    let mut all_settings = collect_settings(ctx, dry_run_report, &validated_settings).await;
    for setting in all_settings.iter_mut() {
        if setting.can_apply(ctx) {
            let phase = setting.phase();
            info!("Applying setting: {:?}", phase);
            setting.apply(ctx).await?;
        }
    }

    Ok(validated_settings)
}

pub async fn collect_settings(
    ctx: &ItemContext,
    dry_run_report: &Arc<Mutex<DryRunReport>>,
    validated: &ValidatedSettings,
) -> Vec<Box<dyn MigrationSetting>> {
    let src = ctx.source.clone();
    let dest = ctx.destination.clone();
    let mapping = ctx.mapping.clone();

    let mut all_settings: Vec<Box<dyn MigrationSetting>> = Vec::new();

    if validated.batch_size() > 0 {
        all_settings.push(Box::new(BatchSizeSetting(validated.batch_size() as i64)));
    }

    all_settings.push(Box::new(CopyColumnsSetting(*validated.copy_columns())));

    if validated.ignore_constraints() {
        all_settings.push(Box::new(IgnoreConstraintsSettings(true)));
    }

    if validated.infer_schema() {
        let infer_schema_setting =
            InferSchemaSetting::new(&src, &dest, &mapping, validated, dry_run_report).await;
        all_settings.push(Box::new(infer_schema_setting));
    }

    if validated.create_missing_tables() {
        let missing_tables_setting =
            CreateMissingTablesSetting::new(&src, &dest, &mapping, validated, dry_run_report).await;
        all_settings.push(Box::new(missing_tables_setting));
    }

    if validated.create_missing_columns() {
        let missing_cols_setting =
            CreateMissingColumnsSetting::new(&src, &dest, &mapping, validated, dry_run_report)
                .await;
        all_settings.push(Box::new(missing_cols_setting));
    }

    // Settings are already created in phase order due to enum ordering
    all_settings.sort_by_key(|s| s.phase());

    all_settings
}
