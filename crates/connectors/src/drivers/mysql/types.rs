use crate::sql::metadata::column::ColumnMetadata;
use model::core::convert::{
    DdlMapping, Fidelity, FromCanonical, IntoCanonical, Transform, TypeMapping,
};
use model::core::types::{FloatSize, GeomKind, IntSize, Type};

pub struct MySqlTypeConverter;

impl IntoCanonical for MySqlTypeConverter {
    type ColumnMeta = ColumnMetadata;

    fn to_canonical(&self, col: &Self::ColumnMeta) -> TypeMapping {
        let mysql_type = col.data_type.as_str().to_lowercase();
        let is_unsigned = mysql_type.contains("unsigned");
        let auto_inc = col.is_auto_increment;

        match mysql_type.as_str() {
            // Integer types
            // TINYINT(1) is the conventional MySQL boolean - map it to Boolean.
            // Both signed and unsigned variants are treated as boolean (0/1).
            "tinyint" | "tinyint unsigned"
                if col
                    .full_column_type
                    .as_deref()
                    .map(|s| {
                        s.eq_ignore_ascii_case("tinyint(1)")
                            || s.eq_ignore_ascii_case("tinyint(1) unsigned")
                    })
                    .unwrap_or(false) =>
            {
                TypeMapping {
                    canonical: Type::Boolean,
                    fidelity: Fidelity::Lossless,
                    value_transform: Some(Transform::IntToBool),
                    warnings: vec![],
                }
            }
            "tinyint" | "tinyint unsigned" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I8,
                    unsigned: is_unsigned,
                    auto_increment: auto_inc,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "smallint" | "smallint unsigned" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I16,
                    unsigned: is_unsigned,
                    auto_increment: auto_inc,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "mediumint" | "mediumint unsigned" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I24,
                    unsigned: is_unsigned,
                    auto_increment: auto_inc,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "int" | "integer" | "int unsigned" | "integer unsigned" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I32,
                    unsigned: is_unsigned,
                    auto_increment: auto_inc,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "bigint" | "bigint unsigned" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I64,
                    unsigned: is_unsigned,
                    auto_increment: auto_inc,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Floating point types
            "float" => TypeMapping {
                canonical: Type::Float {
                    bits: FloatSize::F32,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "double" | "double precision" | "real" => TypeMapping {
                canonical: Type::Float {
                    bits: FloatSize::F64,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Decimal types
            "decimal" | "numeric" | "dec" | "fixed" => TypeMapping {
                canonical: Type::Decimal {
                    precision: col.num_precision.map(|p| p as u8),
                    scale: col.num_scale.map(|s| s as u8),
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // String types
            "char" => TypeMapping {
                canonical: Type::Char {
                    length: col.char_max_length,
                    charset: col.charset.clone(),
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "varchar" => TypeMapping {
                canonical: Type::Varchar {
                    length: col.char_max_length,
                    charset: col.charset.clone(),
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "tinytext" | "text" | "mediumtext" | "longtext" => TypeMapping {
                canonical: Type::Text {
                    charset: col.charset.clone(),
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Binary types
            "binary" => TypeMapping {
                canonical: Type::Binary {
                    length: col.char_max_length,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "varbinary" => TypeMapping {
                canonical: Type::Varbinary {
                    length: col.char_max_length,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "tinyblob" | "blob" | "mediumblob" | "longblob" => TypeMapping {
                canonical: Type::Blob {
                    max_bytes: col.char_max_length,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Temporal types
            "date" => TypeMapping {
                canonical: Type::Date,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "time" => TypeMapping {
                canonical: Type::Time {
                    precision: col.num_precision.map(|p| p as u8),
                    with_tz: false,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "datetime" => TypeMapping {
                canonical: Type::Timestamp {
                    precision: col.num_precision.map(|p| p as u8),
                    with_tz: false,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "timestamp" => TypeMapping {
                canonical: Type::Timestamp {
                    precision: col.num_precision.map(|p| p as u8),
                    with_tz: true, // MySQL TIMESTAMP is stored in UTC
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "year" => TypeMapping {
                canonical: Type::Year,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Boolean (MySQL uses TINYINT(1))
            "bool" | "boolean" => TypeMapping {
                canonical: Type::Boolean,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // JSON
            "json" => TypeMapping {
                canonical: Type::Json { binary: true },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Bit
            "bit" => TypeMapping {
                canonical: Type::Bit {
                    length: col.char_max_length,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Enum
            "enum" => TypeMapping {
                canonical: Type::Enum {
                    name: col.name.clone(),
                    values: vec![], // Values would need to be extracted from column_type
                },
                fidelity: Fidelity::Equivalent,
                value_transform: None,
                warnings: vec!["Enum values need to be extracted from column_type".to_string()],
            },

            // Set
            "set" => TypeMapping {
                canonical: Type::Set {
                    values: vec![], // Values would need to be extracted from column_type
                },
                fidelity: Fidelity::Equivalent,
                value_transform: None,
                warnings: vec!["Set values need to be extracted from column_type".to_string()],
            },

            // Geometry types
            "geometry" => TypeMapping {
                canonical: Type::Geometry {
                    kind: None,
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "point" => TypeMapping {
                canonical: Type::Geometry {
                    kind: Some(GeomKind::Point),
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "linestring" => TypeMapping {
                canonical: Type::Geometry {
                    kind: Some(GeomKind::LineString),
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "polygon" => TypeMapping {
                canonical: Type::Geometry {
                    kind: Some(GeomKind::Polygon),
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "multipoint" => TypeMapping {
                canonical: Type::Geometry {
                    kind: Some(GeomKind::MultiPoint),
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "multilinestring" => TypeMapping {
                canonical: Type::Geometry {
                    kind: Some(GeomKind::MultiLineString),
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "multipolygon" => TypeMapping {
                canonical: Type::Geometry {
                    kind: Some(GeomKind::MultiPolygon),
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "geometrycollection" => TypeMapping {
                canonical: Type::Geometry {
                    kind: Some(GeomKind::GeometryCollection),
                    srid: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Unknown type - fallback
            other => TypeMapping {
                canonical: Type::Unknown {
                    source_name: other.to_string(),
                    fallback_ddl: format!("VARCHAR({})", col.char_max_length.unwrap_or(255)),
                },
                fidelity: Fidelity::BestEffort,
                value_transform: Some(Transform::ToString),
                warnings: vec![format!(
                    "Unknown MySQL type '{}', falling back to VARCHAR",
                    other
                )],
            },
        }
    }
}

impl FromCanonical for MySqlTypeConverter {
    fn to_ddl(&self, canonical: &Type) -> DdlMapping {
        match canonical {
            // Integer types
            Type::Int {
                bits,
                unsigned,
                auto_increment,
            } => {
                let base = match bits {
                    IntSize::I8 => "TINYINT",
                    IntSize::I16 => "SMALLINT",
                    IntSize::I24 => "MEDIUMINT",
                    IntSize::I32 => "INT",
                    IntSize::I64 => "BIGINT",
                };
                let mut ddl = base.to_string();
                if *unsigned {
                    ddl.push_str(" UNSIGNED");
                }
                if *auto_increment {
                    ddl.push_str(" AUTO_INCREMENT");
                }
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Floating point types
            Type::Float { bits } => {
                let ddl = match bits {
                    FloatSize::F32 => "FLOAT".to_string(),
                    FloatSize::F64 => "DOUBLE".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Decimal types
            Type::Decimal { precision, scale } => {
                let ddl = match (precision, scale) {
                    (Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
                    (Some(p), None) => format!("DECIMAL({})", p),
                    _ => "DECIMAL".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // String types
            Type::Char { length, .. } => {
                let ddl = match length {
                    Some(len) => format!("CHAR({})", len),
                    None => "CHAR(255)".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }
            Type::Varchar { length, .. } => {
                let ddl = match length {
                    Some(len) => format!("VARCHAR({})", len),
                    None => "VARCHAR(255)".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }
            Type::Text { .. } => DdlMapping {
                ddl: "TEXT".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },

            // Binary types
            Type::Binary { length } => {
                let ddl = match length {
                    Some(len) => format!("BINARY({})", len),
                    None => "BINARY(255)".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }
            Type::Varbinary { length } => {
                let ddl = match length {
                    Some(len) => format!("VARBINARY({})", len),
                    None => "VARBINARY(255)".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }
            Type::Blob { max_bytes } => {
                let ddl = match max_bytes {
                    Some(b) if *b <= 255 => "TINYBLOB".to_string(),
                    Some(b) if *b <= 65535 => "BLOB".to_string(),
                    Some(b) if *b <= 16777215 => "MEDIUMBLOB".to_string(),
                    _ => "LONGBLOB".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Temporal types
            Type::Date => DdlMapping {
                ddl: "DATE".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },
            Type::Time { precision, .. } => {
                let ddl = match precision {
                    Some(p) => format!("TIME({})", p),
                    None => "TIME".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }
            Type::Timestamp { precision, with_tz } => {
                // MySQL TIMESTAMP stores in UTC, DATETIME does not
                let base = if *with_tz { "TIMESTAMP" } else { "DATETIME" };
                let ddl = match precision {
                    Some(p) => format!("{}({})", base, p),
                    None => base.to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }
            Type::Year => DdlMapping {
                ddl: "YEAR".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },
            Type::Interval { .. } => DdlMapping {
                ddl: "VARCHAR(100)".to_string(),
                fidelity: Fidelity::Lossy,
                transform: Some(Transform::ToString),
                warnings: vec!["MySQL does not support INTERVAL type, using VARCHAR".to_string()],
                pre_ddl: None,
            },

            // Boolean
            Type::Boolean => DdlMapping {
                ddl: "TINYINT(1)".to_string(),
                fidelity: Fidelity::Equivalent,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },

            // JSON
            Type::Json { .. } => DdlMapping {
                ddl: "JSON".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },

            // UUID - MySQL doesn't have native UUID, use CHAR(36) or BINARY(16)
            Type::Uuid => DdlMapping {
                ddl: "CHAR(36)".to_string(),
                fidelity: Fidelity::Equivalent,
                transform: Some(Transform::ToString),
                warnings: vec!["MySQL does not have native UUID type, using CHAR(36)".to_string()],
                pre_ddl: None,
            },

            // Bit
            Type::Bit { length } => {
                let ddl = match length {
                    Some(len) => format!("BIT({})", len),
                    None => "BIT(1)".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Enum
            Type::Enum { values, .. } => {
                let quoted_values: Vec<String> =
                    values.iter().map(|v| format!("'{}'", v)).collect();
                let ddl = format!("ENUM({})", quoted_values.join(", "));
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Set
            Type::Set { values } => {
                let quoted_values: Vec<String> =
                    values.iter().map(|v| format!("'{}'", v)).collect();
                let ddl = format!("SET({})", quoted_values.join(", "));
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Array - MySQL doesn't support arrays natively, use JSON
            Type::Array { .. } => DdlMapping {
                ddl: "JSON".to_string(),
                fidelity: Fidelity::Equivalent,
                transform: Some(Transform::ArrayToJson),
                warnings: vec!["MySQL does not support arrays, using JSON".to_string()],
                pre_ddl: None,
            },

            // Geometry types
            Type::Geometry { kind, .. } => {
                let ddl = match kind {
                    Some(GeomKind::Point) => "POINT".to_string(),
                    Some(GeomKind::LineString) => "LINESTRING".to_string(),
                    Some(GeomKind::Polygon) => "POLYGON".to_string(),
                    Some(GeomKind::MultiPoint) => "MULTIPOINT".to_string(),
                    Some(GeomKind::MultiLineString) => "MULTILINESTRING".to_string(),
                    Some(GeomKind::MultiPolygon) => "MULTIPOLYGON".to_string(),
                    Some(GeomKind::GeometryCollection) => "GEOMETRYCOLLECTION".to_string(),
                    None => "GEOMETRY".to_string(),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Network types - MySQL doesn't have native support
            Type::Inet => DdlMapping {
                ddl: "VARCHAR(45)".to_string(), // IPv6 max length
                fidelity: Fidelity::Equivalent,
                transform: Some(Transform::ToString),
                warnings: vec!["MySQL does not support INET type, using VARCHAR(45)".to_string()],
                pre_ddl: None,
            },
            Type::Cidr => DdlMapping {
                ddl: "VARCHAR(49)".to_string(), // IPv6/prefix max length
                fidelity: Fidelity::Equivalent,
                transform: Some(Transform::ToString),
                warnings: vec!["MySQL does not support CIDR type, using VARCHAR(49)".to_string()],
                pre_ddl: None,
            },
            Type::MacAddr => DdlMapping {
                ddl: "CHAR(17)".to_string(), // XX:XX:XX:XX:XX:XX
                fidelity: Fidelity::Equivalent,
                transform: Some(Transform::ToString),
                warnings: vec!["MySQL does not support MACADDR type, using CHAR(17)".to_string()],
                pre_ddl: None,
            },

            // Composite types - MySQL doesn't support, use JSON
            Type::Composite { .. } => DdlMapping {
                ddl: "JSON".to_string(),
                fidelity: Fidelity::Lossy,
                transform: Some(Transform::Custom("TO_JSON".to_string())),
                warnings: vec!["MySQL does not support composite types, using JSON".to_string()],
                pre_ddl: None,
            },

            // Domain types - MySQL doesn't support, use base type
            Type::Domain { base_type, .. } => {
                let base_mapping = self.to_ddl(base_type);
                DdlMapping {
                    ddl: base_mapping.ddl,
                    fidelity: Fidelity::Equivalent,
                    transform: base_mapping.transform,
                    warnings: vec![
                        "MySQL does not support domain types, using base type".to_string(),
                    ],
                    pre_ddl: None,
                }
            }

            // Unknown - use fallback DDL
            Type::Unknown { fallback_ddl, .. } => DdlMapping {
                ddl: fallback_ddl.clone(),
                fidelity: Fidelity::BestEffort,
                transform: None,
                warnings: vec!["Using fallback DDL for unknown type".to_string()],
                pre_ddl: None,
            },
        }
    }
}
