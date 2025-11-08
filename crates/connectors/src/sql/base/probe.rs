use crate::sql::base::{adapter::SqlAdapter, capabilities::DbCapabilities, error::DbError};

pub(crate) const COPY_STREAMING_COL: &str = "copy_streaming";
pub(crate) const UPSERT_NATIVE_COL: &str = "upsert_native";
pub(crate) const TRANSACTIONS_COL: &str = "transactions";
pub(crate) const MERGE_STATEMENTS_COL: &str = "merge_statements";
pub(crate) const DDL_ONLINE_COL: &str = "ddl_online";
pub(crate) const TEMP_TABLES_COL: &str = "temp_tables";

pub trait CapabilityProbe {
    async fn detect(adapter: &impl SqlAdapter) -> Result<DbCapabilities, DbError>;
}
