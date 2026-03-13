use super::DialectConverter;
use crate::type_registry::{ConversionResult, TypeTransform};
use connectors::sql::metadata::index::IndexType;
use model::core::types::Type;
use std::collections::HashMap;

pub struct PgToMysql;

impl DialectConverter for PgToMysql {
    fn index_type_map(&self) -> HashMap<IndexType, IndexType> {
        HashMap::from([
            (IndexType::Gin, IndexType::BTree),
            (IndexType::Gist, IndexType::BTree),
            (IndexType::SpGist, IndexType::BTree),
            (IndexType::Brin, IndexType::BTree),
        ])
    }

    fn convert_type(&self, source: &Type) -> ConversionResult {
        convert_type(source)
    }
}

fn convert_type(source: &Type) -> ConversionResult {
    match source {
        Type::Int {
            bits,
            auto_increment,
            ..
        } => ConversionResult::Exact(Type::Int {
            bits: *bits,
            unsigned: false,
            auto_increment: *auto_increment,
        }),

        Type::Float { .. } => ConversionResult::Exact(source.clone()),
        Type::Decimal { .. } => ConversionResult::Exact(source.clone()),

        Type::Char { .. } | Type::Varchar { .. } | Type::Text { .. } => {
            ConversionResult::Exact(source.clone())
        }

        // BYTEA -> LONGBLOB
        Type::Blob { .. } => ConversionResult::Exact(Type::Blob {
            max_bytes: Some(4294967295),
        }),

        Type::Binary { .. } | Type::Varbinary { .. } => ConversionResult::Exact(source.clone()),

        Type::Date => ConversionResult::Exact(source.clone()),
        Type::Time { precision, .. } => ConversionResult::Exact(Type::Time {
            precision: *precision,
            with_tz: false,
        }),
        Type::Timestamp { precision, .. } => ConversionResult::Exact(Type::Timestamp {
            precision: *precision,
            with_tz: false,
        }),

        Type::Interval { .. } => {
            ConversionResult::Unsupported("PostgreSQL INTERVAL has no MySQL equivalent".to_string())
        }

        Type::Year => ConversionResult::Exact(source.clone()),

        // JSONB -> JSON
        Type::Json { .. } => ConversionResult::Exact(Type::Json { binary: false }),

        Type::Boolean => ConversionResult::Exact(source.clone()),

        // UUID -> CHAR(36)
        Type::Uuid => ConversionResult::Compatible {
            target: Type::Char {
                length: Some(36),
                charset: None,
            },
            warnings: vec!["UUID mapped to CHAR(36)".to_string()],
        },

        Type::Bit { .. } => ConversionResult::Exact(source.clone()),

        // Arrays -> JSON
        Type::Array { .. } => ConversionResult::RequiresTransform {
            target: Type::Json { binary: false },
            transform: TypeTransform::Custom("JSON_ARRAY".to_string()),
        },

        // Network types -> VARCHAR
        Type::Inet | Type::Cidr => ConversionResult::RequiresTransform {
            target: Type::Varchar {
                length: Some(45),
                charset: None,
            },
            transform: TypeTransform::CastToString,
        },
        Type::MacAddr => ConversionResult::RequiresTransform {
            target: Type::Varchar {
                length: Some(17),
                charset: None,
            },
            transform: TypeTransform::CastToString,
        },

        Type::Geometry { .. } => ConversionResult::Exact(source.clone()),
        Type::Enum { .. } => ConversionResult::Exact(source.clone()),
        Type::Set { .. } => ConversionResult::Exact(source.clone()),

        // Composite -> JSON
        Type::Composite { .. } => ConversionResult::RequiresTransform {
            target: Type::Json { binary: false },
            transform: TypeTransform::Custom("to_json".to_string()),
        },

        // Domain -> recurse on base type
        Type::Domain { base_type, .. } => convert_type(base_type),

        Type::Unknown { source_name, .. } => ConversionResult::Unsupported(format!(
            "Unknown type '{}' has no MySQL mapping",
            source_name
        )),
    }
}
