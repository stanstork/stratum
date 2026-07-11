use crate::traits::row_decoder::RowDecoder;
use serde::Serialize;

const COL_ORDINAL_POSITION: &str = "ordinal_position";
const COL_COLUMN_NAME: &str = "column_name";
const COL_DATA_TYPE: &str = "data_type";
const COL_IS_NULLABLE: &str = "is_nullable";
const COL_COLUMN_DEFAULT: &str = "column_default";
const COL_CHAR_MAX_LENGTH: &str = "character_maximum_length";
const COL_NUMERIC_PRECISION: &str = "numeric_precision";
const COL_NUMERIC_SCALE: &str = "numeric_scale";
const COL_IS_PRIMARY_KEY: &str = "is_primary_key";
const COL_PK_POSITION: &str = "pk_position";
const COL_IS_UNIQUE: &str = "is_unique";
const COL_IS_AUTO_INCREMENT: &str = "is_auto_increment";
const COL_COMMENT: &str = "column_comment";
const COL_COLLATION: &str = "collation_name";
const COL_CHARSET: &str = "character_set_name";
const COL_IS_GENERATED: &str = "is_generated";
const COL_IS_STORED: &str = "is_stored";
const COL_GENERATED_EXPRESSION: &str = "generated_expression";
const COL_FULL_COLUMN_TYPE: &str = "full_column_type";

#[derive(Debug, Clone, Serialize, Default)]
pub struct ColumnMetadata {
    pub ordinal: usize,
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub has_default: bool,
    pub default_value: Option<String>,
    pub char_max_length: Option<usize>,
    pub num_precision: Option<u32>,
    pub num_scale: Option<u32>,
    pub is_primary_key: bool,
    pub pk_position: Option<u32>,
    pub is_unique: bool,
    pub is_auto_increment: bool,
    pub comment: Option<String>,
    pub collation: Option<String>,
    pub charset: Option<String>,
    pub is_generated: bool,
    pub is_stored: bool,
    pub generated_expression: Option<String>,

    /// Full column type string (e.g. "enum('G','PG','PG-13','R','NC-17')").
    pub full_column_type: Option<String>,
}

impl ColumnMetadata {
    pub fn is_enum(&self) -> bool {
        self.data_type.eq_ignore_ascii_case("enum")
            || self
                .full_column_type
                .as_deref()
                .map(|t| t.trim_start().to_lowercase().starts_with("enum("))
                .unwrap_or(false)
    }

    pub fn enum_values(&self) -> Vec<String> {
        self.full_column_type
            .as_deref()
            .map(parse_enum_values)
            .unwrap_or_default()
    }

    pub fn from_row<R: RowDecoder>(row: &R) -> Self {
        Self {
            ordinal: row.get_u32(COL_ORDINAL_POSITION).unwrap_or(0) as usize,
            name: row.get_string(COL_COLUMN_NAME).unwrap_or_default(),
            data_type: row.get_string(COL_DATA_TYPE).unwrap_or_default(),
            is_nullable: row.get_string(COL_IS_NULLABLE).unwrap_or_default() == "YES",
            has_default: row.get_string(COL_COLUMN_DEFAULT).is_some(),
            default_value: row.get_string(COL_COLUMN_DEFAULT),
            char_max_length: row.get_i32(COL_CHAR_MAX_LENGTH).map(|v| v as usize),
            num_precision: row.get_i32(COL_NUMERIC_PRECISION).map(|v| v as u32),
            num_scale: row.get_i32(COL_NUMERIC_SCALE).map(|v| v as u32),
            is_primary_key: row.get_bool(COL_IS_PRIMARY_KEY).unwrap_or(false),
            pk_position: row.get_u32(COL_PK_POSITION),
            is_unique: row.get_bool(COL_IS_UNIQUE).unwrap_or(false),
            is_auto_increment: row.get_bool(COL_IS_AUTO_INCREMENT).unwrap_or(false),
            comment: row.get_string(COL_COMMENT),
            collation: row.get_string(COL_COLLATION),
            charset: row.get_string(COL_CHARSET),
            is_generated: row.get_bool(COL_IS_GENERATED).unwrap_or(false),
            is_stored: row.get_bool(COL_IS_STORED).unwrap_or(false),
            generated_expression: row
                .get_string(COL_GENERATED_EXPRESSION)
                .filter(|s| !s.is_empty()),
            full_column_type: row.get_string(COL_FULL_COLUMN_TYPE),
        }
    }
}

/// Parse variants from an `enum('a','b','c')` type string into `["a", "b", "c"]`.
pub(crate) fn parse_enum_values(full_type: &str) -> Vec<String> {
    let inner = match (full_type.find('('), full_type.rfind(')')) {
        (Some(open), Some(close)) if close > open => &full_type[open + 1..close],
        _ => return Vec::new(),
    };

    let mut values = Vec::new();
    let mut chars = inner.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c == '\'' {
            chars.next(); // opening quote
            let mut value = String::new();
            while let Some(ch) = chars.next() {
                if ch == '\'' {
                    // Doubled '' is an escaped single quote; otherwise end of value.
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        value.push('\'');
                    } else {
                        break;
                    }
                } else {
                    value.push(ch);
                }
            }
            values.push(value);
        } else {
            chars.next(); // skip commas / whitespace between values
        }
    }
    values
}
