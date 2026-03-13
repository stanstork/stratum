pub mod context;
pub mod create_cols;
pub mod create_tables;
pub mod driver;
pub mod endpoint;
pub mod error;
pub mod infer_schema;
pub mod orchestrator;
pub mod phase;
pub mod schema_manager;
pub mod traits;
pub mod types;
pub mod validated;
pub mod validator;
pub(crate) mod value_ext;

// Public API re-exports (preserve external usage)
pub use context::SchemaSettingContext;
pub use driver::SchemaDriver;
pub use error::SettingsError;
pub use orchestrator::{collect_settings, validate_and_plan};
pub use phase::MigrationSettingsPhase;
pub use schema_manager::apply_schema_ops;
pub use traits::MigrationSetting;
pub use types::{CopyColumns, Settings};
pub use validated::{ValidatedSettings, ValidatedSettingsBuilder};
pub use validator::SettingsValidator;
