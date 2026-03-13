use crate::type_registry::ConversionResult;
use connectors::sql::metadata::index::IndexType;
use model::core::types::Type;
use std::collections::HashMap;

pub mod mysql_to_pg;
pub mod pg_to_mysql;

/// Trait that each dialect-pair converter must implement.
///
/// Provides column type conversion and index type mapping
/// for a specific (source -> target) dialect pair.
pub trait DialectConverter: Send + Sync {
    /// Convert a source column type to the target dialect's equivalent.
    fn convert_type(&self, source: &Type) -> ConversionResult;

    /// Return index type overrides for this dialect pair.
    /// Missing entries pass through unchanged.
    fn index_type_map(&self) -> HashMap<IndexType, IndexType>;

    /// Whether the target dialect requires explicit sequences for auto-increment columns.
    /// E.g., PostgreSQL uses sequences, MySQL uses AUTO_INCREMENT natively.
    fn use_explicit_sequences(&self) -> bool {
        false
    }
}
