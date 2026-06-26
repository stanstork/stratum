use async_trait::async_trait;
use connectors::{
    drivers::{mysql::types::MySqlTypeConverter, postgres::types::PgTypeConverter},
    error::DriverError,
    sql::metadata::{
        capabilities::Capabilities,
        column::ColumnMetadata,
        constraint::{CheckConstraintMetadata, UniqueConstraintMetadata},
        fk::ForeignKeyMetadata,
        index::IndexMetadata,
        table::TableMetadata,
    },
    traits::{
        driver::{Driver, DriverInfo},
        introspector::SchemaIntrospector,
    },
};
use engine_core::schema::type_registry::Dialect;
use engine_wasm::schema::PluginField;
use model::core::convert::FromCanonical;
use std::collections::HashMap;

static INFO: DriverInfo = DriverInfo {
    id: "wasm",
    name: "WASM plugin source",
    schemes: &["wasm"],
};

/// Read-only introspector backed by a plugin's declared output schema.
pub struct PluginIntrospector {
    meta: TableMetadata,
    capabilities: Capabilities,
}

impl PluginIntrospector {
    /// Build an introspector from the plugin's `output` schema, rendering each
    /// column's type in `dest_dialect`'s native DDL form.
    pub fn new(output_schema: &[PluginField], dest_dialect: Dialect) -> Self {
        let columns = output_schema
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                let canonical = field.to_canonical_type();
                let ddl = match dest_dialect {
                    Dialect::Postgres => PgTypeConverter.to_ddl(&canonical).ddl,
                    Dialect::MySql => MySqlTypeConverter.to_ddl(&canonical).ddl,
                };
                (
                    field.name.clone(),
                    ColumnMetadata {
                        ordinal,
                        name: field.name.clone(),
                        data_type: ddl,
                        is_nullable: field.nullable,
                        ..Default::default()
                    },
                )
            })
            .collect();

        let meta = TableMetadata {
            name: String::new(),
            schema: None,
            columns,
            primary_keys: Vec::new(),
            foreign_keys: Vec::new(),
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        };

        Self {
            meta,
            capabilities: Capabilities::default(),
        }
    }
}

impl Driver for PluginIntrospector {
    fn info(&self) -> &DriverInfo {
        &INFO
    }

    fn version(&self) -> &str {
        "0"
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
}

#[async_trait]
impl SchemaIntrospector for PluginIntrospector {
    async fn table_exists(&self, _table: &str) -> Result<bool, DriverError> {
        Ok(true)
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>, DriverError> {
        Ok(Vec::new())
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError> {
        let mut meta = self.meta.clone();
        meta.name = table.to_string();
        Ok(meta)
    }

    async fn index_metadata(&self, _table: &str) -> Result<Vec<IndexMetadata>, DriverError> {
        Ok(Vec::new())
    }

    async fn fk_metadata(&self, _table: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError> {
        Ok(Vec::new())
    }

    async fn referencing_tables(&self, _table: &str) -> Result<Vec<String>, DriverError> {
        Ok(Vec::new())
    }

    async fn table_size_bytes(&self, _table: &str) -> Result<u64, DriverError> {
        Ok(0)
    }

    async fn unique_constraint_metadata(
        &self,
        _table: &str,
    ) -> Result<Vec<UniqueConstraintMetadata>, DriverError> {
        Ok(Vec::new())
    }

    async fn check_constraint_metadata(
        &self,
        _table: &str,
    ) -> Result<Vec<CheckConstraintMetadata>, DriverError> {
        Ok(Vec::new())
    }
}
