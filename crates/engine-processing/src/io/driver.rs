use connectors::traits::{
    ddl::DdlWriter, executor::QueryExecutor, introspector::SchemaIntrospector, reader::DataReader,
};

/// Trait alias combining all capabilities needed for schema operations.
///
/// Note: This trait is not object-safe because SchemaIntrospector has generic methods.
/// For dynamic dispatch, use `Arc<dyn Driver>` and downcast when specific capabilities are needed.
pub trait SchemaDriver:
    DdlWriter + QueryExecutor + DataReader + SchemaIntrospector + Clone
{
}

/// Blanket implementation: any type satisfying the bounds is a SchemaDriver.
impl<T> SchemaDriver for T where
    T: DdlWriter + QueryExecutor + DataReader + SchemaIntrospector + Clone
{
}
