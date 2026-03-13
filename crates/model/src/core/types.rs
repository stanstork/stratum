use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Type {
    // Numeric
    Int {
        bits: IntSize,
        unsigned: bool,
        auto_increment: bool,
    },
    Decimal {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Float {
        bits: FloatSize,
    },

    // String
    Char {
        length: Option<usize>,
        charset: Option<String>,
    },
    Varchar {
        length: Option<usize>,
        charset: Option<String>,
    },
    Text {
        charset: Option<String>,
    },

    // Binary
    Binary {
        length: Option<usize>,
    },
    Varbinary {
        length: Option<usize>,
    },
    Blob {
        max_bytes: Option<usize>,
    },

    // Temporal
    Date,
    Time {
        precision: Option<u8>,
        with_tz: bool,
    },
    Timestamp {
        precision: Option<u8>,
        with_tz: bool,
    },
    Interval {
        fields: Option<IntervalFields>,
    },
    Year,

    // Other scalar
    Boolean,
    Uuid,
    Json {
        binary: bool,
    },
    Bit {
        length: Option<usize>,
    },

    // Complex
    Array {
        element: Box<Type>,
    },
    Enum {
        name: String,
        values: Vec<String>,
    },
    Set {
        values: Vec<String>,
    },
    Geometry {
        kind: Option<GeomKind>,
        srid: Option<u32>,
    },

    // Network
    Inet,
    Cidr,
    MacAddr,

    // Composite / custom
    Composite {
        name: String,
        fields: Vec<(String, Type)>,
    },
    Domain {
        name: String,
        base_type: Box<Type>,
    },

    // Unknown
    Unknown {
        source_name: String,
        fallback_ddl: String, // Best guess DDL for target
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IntSize {
    I8,
    I16,
    I24,
    I32,
    I64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FloatSize {
    F32,
    F64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IntervalFields {
    Full,
    YearMonth,
    DayTime,
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GeomKind {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

impl Type {
    /// Get a simple type name for lookup purposes (used by custom mappings)
    pub fn name(&self) -> String {
        match self {
            Type::Int { bits, unsigned, .. } => {
                let base = match bits {
                    IntSize::I8 => "tinyint",
                    IntSize::I16 => "smallint",
                    IntSize::I24 => "mediumint",
                    IntSize::I32 => "int",
                    IntSize::I64 => "bigint",
                };
                if *unsigned {
                    format!("{} unsigned", base)
                } else {
                    base.to_string()
                }
            }
            Type::Decimal { .. } => "decimal".to_string(),
            Type::Float { bits } => match bits {
                FloatSize::F32 => "float".to_string(),
                FloatSize::F64 => "double".to_string(),
            },
            Type::Char { .. } => "char".to_string(),
            Type::Varchar { .. } => "varchar".to_string(),
            Type::Text { .. } => "text".to_string(),
            Type::Binary { .. } => "binary".to_string(),
            Type::Varbinary { .. } => "varbinary".to_string(),
            Type::Blob { .. } => "blob".to_string(),
            Type::Date => "date".to_string(),
            Type::Time { .. } => "time".to_string(),
            Type::Timestamp { .. } => "timestamp".to_string(),
            Type::Interval { .. } => "interval".to_string(),
            Type::Year => "year".to_string(),
            Type::Boolean => "boolean".to_string(),
            Type::Uuid => "uuid".to_string(),
            Type::Json { binary } => {
                if *binary {
                    "jsonb".to_string()
                } else {
                    "json".to_string()
                }
            }
            Type::Bit { .. } => "bit".to_string(),
            Type::Array { .. } => "array".to_string(),
            Type::Enum { name, .. } => {
                if name.is_empty() {
                    "enum".to_string()
                } else {
                    name.clone()
                }
            }
            Type::Set { .. } => "set".to_string(),
            Type::Geometry { .. } => "geometry".to_string(),
            Type::Inet => "inet".to_string(),
            Type::Cidr => "cidr".to_string(),
            Type::MacAddr => "macaddr".to_string(),
            Type::Composite { name, .. } => name.clone(),
            Type::Domain { name, .. } => name.clone(),
            Type::Unknown { source_name, .. } => source_name.clone(),
        }
    }
}
