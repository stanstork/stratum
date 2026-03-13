use crate::core::types::Type;

#[derive(Debug, Clone, PartialEq)]
pub struct TypeMapping {
    pub canonical: Type,
    pub fidelity: Fidelity,
    pub value_transform: Option<Transform>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fidelity {
    Lossless,   // Perfect round-trip
    Equivalent, // Same semantics, minor differences (unsigned -> signed)
    Lossy,      // May lose data (enum -> varchar)
    BestEffort, // Significant differences
}

#[derive(Debug, Clone, PartialEq)]
pub enum Transform {
    None,
    ToString,
    FromString,
    Truncate(u32),
    ScalePrecision {
        precision: u8,
        scale: u8,
    },
    ArrayToJson,
    JsonToArray,
    /// MySQL TINYINT(1) → Boolean: coerce 0 → false, non-zero → true
    IntToBool,
    Custom(String), // SQL expression
}

/// Implemented by source database connectors
pub trait IntoCanonical {
    /// The native column metadata type
    type ColumnMeta;

    /// Convert native column metadata to canonical type
    fn to_canonical(&self, col: &Self::ColumnMeta) -> TypeMapping;
}

/// Implemented by target database connectors
pub trait FromCanonical {
    /// Generate DDL type string (e.g., "VARCHAR(255)", "BIGINT")
    fn to_ddl(&self, canonical: &Type) -> DdlMapping;
}

#[derive(Debug, Clone)]
pub struct DdlMapping {
    /// The DDL type string (e.g., "BIGINT", "VARCHAR(255)")
    pub ddl: String,
    /// Overall fidelity of the mapping
    pub fidelity: Fidelity,
    /// Value transformation needed during data transfer
    pub transform: Option<Transform>,
    /// Warnings for user
    pub warnings: Vec<String>,
    /// Additional DDL to execute before table creation (e.g., CREATE TYPE for enums)
    pub pre_ddl: Option<String>,
}
