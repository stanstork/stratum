use crate::sql::metadata::column::ColumnMetadata;
use model::core::convert::{
    DdlMapping, Fidelity, FromCanonical, IntoCanonical, Transform, TypeMapping,
};
use model::core::types::{FloatSize, GeomKind, IntSize, Type};

pub struct PgTypeConverter;

impl IntoCanonical for PgTypeConverter {
    type ColumnMeta = ColumnMetadata;

    fn to_canonical(&self, col: &Self::ColumnMeta) -> TypeMapping {
        let pg_type = col.data_type.as_str().to_lowercase();

        match pg_type.as_str() {
            // Integer types
            "smallint" | "int2" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I16,
                    unsigned: false,
                    auto_increment: false,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "integer" | "int" | "int4" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I32,
                    unsigned: false,
                    auto_increment: false,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "bigint" | "int8" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I64,
                    unsigned: false,
                    auto_increment: col.is_auto_increment,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "serial" | "serial4" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I32,
                    unsigned: false,
                    auto_increment: true,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "bigserial" | "serial8" => TypeMapping {
                canonical: Type::Int {
                    bits: IntSize::I64,
                    unsigned: false,
                    auto_increment: true,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Floating point
            "real" | "float4" => TypeMapping {
                canonical: Type::Float {
                    bits: FloatSize::F32,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "double precision" | "float8" => TypeMapping {
                canonical: Type::Float {
                    bits: FloatSize::F64,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Decimal
            "numeric" | "decimal" => TypeMapping {
                canonical: Type::Decimal {
                    precision: col.num_precision.map(|p| p as u8),
                    scale: col.num_scale.map(|s| s as u8),
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // String types
            "character" | "char" | "bpchar" => TypeMapping {
                canonical: Type::Char {
                    length: col.char_max_length,
                    charset: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "character varying" | "varchar" => TypeMapping {
                canonical: Type::Varchar {
                    length: col.char_max_length,
                    charset: None,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "text" => TypeMapping {
                canonical: Type::Text { charset: None },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Binary
            "bytea" => TypeMapping {
                canonical: Type::Blob { max_bytes: None },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Temporal
            "date" => TypeMapping {
                canonical: Type::Date,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "time" | "time without time zone" => TypeMapping {
                canonical: Type::Time {
                    precision: col.num_precision.map(|p| p as u8),
                    with_tz: false,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "time with time zone" | "timetz" => TypeMapping {
                canonical: Type::Time {
                    precision: col.num_precision.map(|p| p as u8),
                    with_tz: true,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "timestamp" | "timestamp without time zone" => TypeMapping {
                canonical: Type::Timestamp {
                    precision: col.num_precision.map(|p| p as u8),
                    with_tz: false,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "timestamp with time zone" | "timestamptz" => TypeMapping {
                canonical: Type::Timestamp {
                    precision: col.num_precision.map(|p| p as u8),
                    with_tz: true,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "interval" => TypeMapping {
                canonical: Type::Interval { fields: None },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Boolean
            "boolean" | "bool" => TypeMapping {
                canonical: Type::Boolean,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // JSON
            "json" => TypeMapping {
                canonical: Type::Json { binary: false },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "jsonb" => TypeMapping {
                canonical: Type::Json { binary: true },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // UUID
            "uuid" => TypeMapping {
                canonical: Type::Uuid,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Network types
            "inet" => TypeMapping {
                canonical: Type::Inet,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "cidr" => TypeMapping {
                canonical: Type::Cidr,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },
            "macaddr" | "macaddr8" => TypeMapping {
                canonical: Type::MacAddr,
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Bit types
            "bit" | "bit varying" | "varbit" => TypeMapping {
                canonical: Type::Bit {
                    length: col.char_max_length,
                },
                fidelity: Fidelity::Lossless,
                value_transform: None,
                warnings: vec![],
            },

            // Geometry (PostGIS)
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

            // Arrays - detect by prefix
            _ if pg_type.ends_with("[]") || pg_type.starts_with("_") => {
                let element_type = pg_type.trim_end_matches("[]").trim_start_matches('_');
                TypeMapping {
                    canonical: Type::Array {
                        element: Box::new(Type::Unknown {
                            source_name: element_type.to_string(),
                            fallback_ddl: "TEXT".to_string(),
                        }),
                    },
                    fidelity: Fidelity::Lossless,
                    value_transform: None,
                    warnings: vec![],
                }
            }

            // Unknown
            other => TypeMapping {
                canonical: Type::Unknown {
                    source_name: other.to_string(),
                    fallback_ddl: "TEXT".to_string(),
                },
                fidelity: Fidelity::BestEffort,
                value_transform: Some(Transform::ToString),
                warnings: vec![format!("Unknown PostgreSQL type '{}'", other)],
            },
        }
    }
}

impl FromCanonical for PgTypeConverter {
    fn to_ddl(&self, canonical: &Type) -> DdlMapping {
        match canonical {
            // Integer types
            Type::Int {
                bits,
                auto_increment,
                ..
            } => {
                let ddl = if *auto_increment {
                    match bits {
                        IntSize::I16 => "SMALLSERIAL",
                        IntSize::I32 => "SERIAL",
                        _ => "BIGSERIAL",
                    }
                } else {
                    match bits {
                        IntSize::I8 | IntSize::I16 => "SMALLINT",
                        IntSize::I24 | IntSize::I32 => "INTEGER",
                        IntSize::I64 => "BIGINT",
                    }
                };
                DdlMapping {
                    ddl: ddl.to_string(),
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Floating point
            Type::Float { bits } => {
                let ddl = match bits {
                    FloatSize::F32 => "REAL",
                    FloatSize::F64 => "DOUBLE PRECISION",
                };
                DdlMapping {
                    ddl: ddl.to_string(),
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Decimal
            Type::Decimal { precision, scale } => {
                let ddl = match (precision, scale) {
                    (Some(p), Some(s)) => format!("NUMERIC({},{})", p, s),
                    (Some(p), None) => format!("NUMERIC({})", p),
                    _ => "NUMERIC".to_string(),
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
                    None => "CHAR(1)".to_string(),
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
                    None => "TEXT".to_string(),
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

            // Binary
            Type::Binary { .. } | Type::Varbinary { .. } | Type::Blob { .. } => DdlMapping {
                ddl: "BYTEA".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },

            // Temporal
            Type::Date => DdlMapping {
                ddl: "DATE".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },
            Type::Time { precision, with_tz } => {
                let base = if *with_tz {
                    "TIME WITH TIME ZONE"
                } else {
                    "TIME"
                };
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
            Type::Timestamp { precision, with_tz } => {
                let base = if *with_tz {
                    "TIMESTAMP WITH TIME ZONE"
                } else {
                    "TIMESTAMP"
                };
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
                ddl: "SMALLINT".to_string(),
                fidelity: Fidelity::Equivalent,
                transform: None,
                warnings: vec!["PostgreSQL has no YEAR type, using SMALLINT".to_string()],
                pre_ddl: None,
            },
            Type::Interval { fields } => {
                let ddl = match fields {
                    Some(model::core::types::IntervalFields::YearMonth) => "INTERVAL YEAR TO MONTH",
                    Some(model::core::types::IntervalFields::DayTime) => "INTERVAL DAY TO SECOND",
                    _ => "INTERVAL",
                };
                DdlMapping {
                    ddl: ddl.to_string(),
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Boolean
            Type::Boolean => DdlMapping {
                ddl: "BOOLEAN".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },

            // JSON
            Type::Json { binary } => DdlMapping {
                ddl: if *binary { "JSONB" } else { "JSON" }.to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },

            // UUID
            Type::Uuid => DdlMapping {
                ddl: "UUID".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
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

            // Enum - PostgreSQL has native ENUM support
            Type::Enum { name, values } => {
                let type_name = if name.is_empty() {
                    "custom_enum".to_string()
                } else {
                    name.clone()
                };
                let create_type = format!(
                    "CREATE TYPE {} AS ENUM ({})",
                    type_name,
                    values
                        .iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                DdlMapping {
                    ddl: type_name.clone(),
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: Some(create_type),
                }
            }

            // Set - PostgreSQL uses arrays for this
            Type::Set { .. } => DdlMapping {
                ddl: "TEXT[]".to_string(),
                fidelity: Fidelity::Equivalent,
                transform: None, // CanonicalValue::Set already contains array values
                warnings: vec!["PostgreSQL has no SET type, using TEXT[]".to_string()],
                pre_ddl: None,
            },

            // Array
            Type::Array { element } => {
                let element_ddl = self.to_ddl(element);
                DdlMapping {
                    ddl: format!("{}[]", element_ddl.ddl),
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: element_ddl.pre_ddl,
                }
            }

            // Geometry
            Type::Geometry { kind, srid } => {
                let geom_type = match kind {
                    Some(GeomKind::Point) => "POINT",
                    Some(GeomKind::LineString) => "LINESTRING",
                    Some(GeomKind::Polygon) => "POLYGON",
                    Some(GeomKind::MultiPoint) => "MULTIPOINT",
                    Some(GeomKind::MultiLineString) => "MULTILINESTRING",
                    Some(GeomKind::MultiPolygon) => "MULTIPOLYGON",
                    Some(GeomKind::GeometryCollection) => "GEOMETRYCOLLECTION",
                    None => "GEOMETRY",
                };
                let ddl = match srid {
                    Some(s) => format!("geometry({}, {})", geom_type, s),
                    None => format!("geometry({})", geom_type),
                };
                DdlMapping {
                    ddl,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: None,
                }
            }

            // Network types
            Type::Inet => DdlMapping {
                ddl: "INET".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },
            Type::Cidr => DdlMapping {
                ddl: "CIDR".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },
            Type::MacAddr => DdlMapping {
                ddl: "MACADDR".to_string(),
                fidelity: Fidelity::Lossless,
                transform: None,
                warnings: vec![],
                pre_ddl: None,
            },

            // Composite
            Type::Composite { name, fields } => {
                let type_name = if name.is_empty() {
                    "custom_composite".to_string()
                } else {
                    name.clone()
                };
                let field_defs: Vec<String> = fields
                    .iter()
                    .map(|(fname, ftype)| {
                        let fddl = self.to_ddl(ftype);
                        format!("{} {}", fname, fddl.ddl)
                    })
                    .collect();
                let create_type =
                    format!("CREATE TYPE {} AS ({})", type_name, field_defs.join(", "));
                DdlMapping {
                    ddl: type_name,
                    fidelity: Fidelity::Lossless,
                    transform: None,
                    warnings: vec![],
                    pre_ddl: Some(create_type),
                }
            }

            // Domain
            Type::Domain { base_type, .. } => self.to_ddl(base_type),

            // Unknown
            Type::Unknown { fallback_ddl, .. } => DdlMapping {
                ddl: fallback_ddl.clone(),
                fidelity: Fidelity::BestEffort,
                transform: None,
                warnings: vec!["Using fallback DDL".to_string()],
                pre_ddl: None,
            },
        }
    }
}
