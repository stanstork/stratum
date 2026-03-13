use super::DialectConverter;
use crate::type_registry::{ConversionResult, TypeTransform};
use connectors::sql::metadata::index::IndexType;
use model::core::types::{IntSize, Type};
use std::collections::HashMap;

pub struct MysqlToPg;

impl DialectConverter for MysqlToPg {
    fn index_type_map(&self) -> HashMap<IndexType, IndexType> {
        HashMap::from([(IndexType::FullText, IndexType::Gin)])
    }

    fn convert_type(&self, source: &Type) -> ConversionResult {
        convert_type(source)
    }

    fn use_explicit_sequences(&self) -> bool {
        true
    }
}

fn convert_type(source: &Type) -> ConversionResult {
    match source {
        Type::Int {
            bits,
            unsigned,
            auto_increment,
        } => {
            let target = match (bits, auto_increment) {
                (IntSize::I8, _) | (IntSize::I16, false) => Type::Int {
                    bits: IntSize::I16,
                    unsigned: false,
                    auto_increment: false,
                },
                (IntSize::I16, true) => Type::Int {
                    bits: IntSize::I16,
                    unsigned: false,
                    auto_increment: true,
                },
                (IntSize::I24, _) | (IntSize::I32, false) => Type::Int {
                    bits: IntSize::I32,
                    unsigned: false,
                    auto_increment: false,
                },
                (IntSize::I32, true) => Type::Int {
                    bits: IntSize::I32,
                    unsigned: false,
                    auto_increment: true,
                },
                (IntSize::I64, false) => Type::Int {
                    bits: IntSize::I64,
                    unsigned: false,
                    auto_increment: false,
                },
                (IntSize::I64, true) => Type::Int {
                    bits: IntSize::I64,
                    unsigned: false,
                    auto_increment: true,
                },
            };

            if *unsigned {
                let warning = if matches!(bits, IntSize::I64) {
                    // BIGINT UNSIGNED range is 0..2^64-1; PostgreSQL BIGINT is signed 0..2^63-1.
                    // Values above 9223372036854775807 (i64::MAX) will overflow or error on insert.
                    "BIGINT UNSIGNED mapped to BIGINT: values > 9223372036854775807 will overflow"
                        .to_string()
                } else {
                    "PostgreSQL does not support unsigned integers; sign bit is dropped".to_string()
                };
                ConversionResult::Compatible {
                    target,
                    warnings: vec![warning],
                }
            } else {
                ConversionResult::Exact(target)
            }
        }

        Type::Float { .. } => ConversionResult::Exact(source.clone()),
        Type::Decimal { .. } => ConversionResult::Exact(source.clone()),

        Type::Char { .. } | Type::Varchar { .. } | Type::Text { .. } => {
            ConversionResult::Exact(source.clone())
        }

        // Binary types -> BYTEA in PostgreSQL
        Type::Binary { .. } | Type::Varbinary { .. } | Type::Blob { .. } => {
            ConversionResult::Exact(Type::Blob { max_bytes: None })
        }

        Type::Date | Type::Time { .. } | Type::Timestamp { .. } => {
            ConversionResult::Exact(source.clone())
        }

        Type::Year => ConversionResult::Compatible {
            target: Type::Int {
                bits: IntSize::I16,
                unsigned: false,
                auto_increment: false,
            },
            warnings: vec!["YEAR mapped to SMALLINT".to_string()],
        },

        // MySQL JSON -> PostgreSQL JSONB
        Type::Json { .. } => ConversionResult::Exact(Type::Json { binary: true }),

        Type::Boolean => ConversionResult::Exact(source.clone()),
        Type::Uuid => ConversionResult::Exact(source.clone()),

        // ENUM -> VARCHAR (PostgreSQL has native ENUM but requires CREATE TYPE)
        Type::Enum { .. } => ConversionResult::RequiresTransform {
            target: Type::Varchar {
                length: Some(255),
                charset: None,
            },
            transform: TypeTransform::CastToString,
        },

        // SET -> Array of text
        Type::Set { .. } => ConversionResult::RequiresTransform {
            target: Type::Array {
                element: Box::new(Type::Text { charset: None }),
            },
            transform: TypeTransform::Custom("string_to_array".to_string()),
        },

        Type::Bit { .. } => ConversionResult::Exact(source.clone()),

        // MySQL GEOMETRY/POINT/etc. → BYTEA (WKB binary).
        // PostGIS geometry type is not available without the extension.
        Type::Geometry { .. } => ConversionResult::Compatible {
            target: Type::Blob { max_bytes: None },
            warnings: vec![
                "GEOMETRY mapped to BYTEA (WKB). Install PostGIS for native geometry support."
                    .to_string(),
            ],
        },
        Type::Inet | Type::Cidr | Type::MacAddr => ConversionResult::Exact(source.clone()),

        // Recurse for array element type
        Type::Array { element } => {
            let elem_result = convert_type(element);
            ConversionResult::Exact(Type::Array {
                element: Box::new(elem_result.target_type()),
            })
        }

        Type::Interval { .. } => ConversionResult::Exact(source.clone()),
        Type::Composite { .. } | Type::Domain { .. } => ConversionResult::Exact(source.clone()),

        Type::Unknown { source_name, .. } => ConversionResult::Unsupported(format!(
            "Unknown type '{}' has no PostgreSQL mapping",
            source_name
        )),
    }
}
