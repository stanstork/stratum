use crate::type_registry::ConversionResult;
use connectors::sql::metadata::index::IndexType;
use model::core::types::Type;
use std::collections::HashMap;

pub mod to_mysql;
pub mod to_postgres;

/// Trait implemented by each target-dialect converter.
///
/// Input is always a canonical [`Type`] (produced by the source dialect's
/// `IntoCanonical`), so a converter only needs to know its *target* dialect -
/// one converter per target.
pub trait DialectConverter: Send + Sync {
    /// Convert a canonical column type to the target dialect's equivalent.
    fn convert_type(&self, source: &Type) -> ConversionResult;

    /// Return index type overrides for the target dialect.
    /// Missing entries pass through unchanged.
    fn index_type_map(&self) -> HashMap<IndexType, IndexType>;

    /// Whether the target dialect requires explicit sequences for auto-increment columns.
    /// E.g., PostgreSQL uses sequences, MySQL uses AUTO_INCREMENT natively.
    fn use_explicit_sequences(&self) -> bool {
        false
    }
}
