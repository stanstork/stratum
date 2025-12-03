use crate::sql::base::{
    adapter::SqlAdapter, capabilities::DbCapabilities, error::DbError, probe::CapabilityProbe,
};
use async_trait::async_trait;
use tracing::warn;

const MYSQL_CAPABILITIES_SQL: &str = include_str!("sql/capabilities.sql");

pub struct MySqlCapabilityProbe;

#[async_trait]
impl CapabilityProbe for MySqlCapabilityProbe {
    async fn detect(adapter: &(dyn SqlAdapter + Send + Sync)) -> Result<DbCapabilities, DbError> {
        let mut capabilities = DbCapabilities::default();
        let rows = adapter.query_rows(MYSQL_CAPABILITIES_SQL).await?;

        for row in rows {
            let capability_name = row.get_value("capability");
            let enabled = row.get_value("enabled");

            // Process each capability and update DbCapabilities accordingly
            if let Some(name) = capability_name.as_string() {
                match name.as_str() {
                    "copy_streaming" => {
                        if let Some(is_enabled) = enabled.as_bool() {
                            capabilities.copy_streaming = is_enabled;
                        }
                    }
                    "upsert_native" => {
                        if let Some(is_enabled) = enabled.as_bool() {
                            capabilities.upsert_native = is_enabled;
                        }
                    }
                    "merge_statements" => {
                        if let Some(is_enabled) = enabled.as_bool() {
                            capabilities.merge_statements = is_enabled;
                        }
                    }
                    "transactions" => {
                        if let Some(is_enabled) = enabled.as_bool() {
                            capabilities.transactions = is_enabled;
                        }
                    }
                    "ddl_online" => {
                        if let Some(is_enabled) = enabled.as_bool() {
                            capabilities.ddl_online = is_enabled;
                        }
                    }
                    "temp_tables" => {
                        if let Some(is_enabled) = enabled.as_bool() {
                            capabilities.temp_tables = is_enabled;
                        }
                    }
                    _ => {
                        warn!(
                            "Unknown capability '{}' encountered during MySQL capability detection",
                            name
                        );
                    }
                }
            }
        }

        Ok(capabilities)
    }
}
