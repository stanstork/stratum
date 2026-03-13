use connectors::traits::{
    ddl::DdlWriter, executor::QueryExecutor, introspector::SchemaIntrospector,
};

/// Composite trait for drivers supporting schema operations.
///
/// This trait combines the capabilities needed for migration settings:
/// - `DdlWriter`: CREATE TABLE, ALTER TABLE, CREATE INDEX, etc.
/// - `QueryExecutor`: Execute arbitrary SQL queries
/// - `SchemaIntrospector`: Read table/column metadata
/// - `Clone`: Allow sharing across async tasks
pub trait SchemaDriver: DdlWriter + QueryExecutor + SchemaIntrospector + Clone {}

/// Blanket implementation: any type satisfying the bounds is a SchemaDriver.
impl<T> SchemaDriver for T where T: DdlWriter + QueryExecutor + SchemaIntrospector + Clone {}
