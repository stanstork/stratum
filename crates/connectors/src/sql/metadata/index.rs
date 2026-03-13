use crate::traits::row_decoder::RowDecoder;
use serde::{Deserialize, Serialize};

const COL_INDEX_NAME: &str = "index_name";
const COL_TABLE_NAME: &str = "table_name";
const COL_SCHEMA_NAME: &str = "schema_name";
const COL_INDEX_TYPE: &str = "index_type";
const COL_COLUMNS: &str = "columns";
const COL_IS_UNIQUE: &str = "is_unique";
const COL_IS_PRIMARY: &str = "is_primary";
const COL_INDEX_CONDITION: &str = "index_condition";
const COL_TABLESPACE: &str = "tablespace";
const COL_FILL_FACTOR: &str = "fill_factor";
const COL_SIZE_BYTES: &str = "size_bytes";
const COL_COMMENT: &str = "comment";

#[derive(Debug, Clone, Serialize)]
pub struct IndexMetadata {
    pub name: String,
    pub table: String,
    pub schema: String,
    pub index_type: IndexType,

    /// Ordered columns with metadata
    pub columns: Vec<IndexColumn>,

    pub is_unique: bool,
    pub is_primary: bool,

    /// Partial index predicate (WHERE clause)
    pub condition: Option<String>,

    pub tablespace: Option<String>,

    /// Fill factor (PostgreSQL, 10-100)
    pub fill_factor: Option<u8>,

    pub size_bytes: Option<u64>,
    pub comment: Option<String>,
}

impl IndexMetadata {
    pub fn from_row<R: RowDecoder>(row: &R) -> Self {
        let columns_json = row.get_string(COL_COLUMNS).unwrap_or("[]".to_string());
        let columns: Vec<IndexColumn> = serde_json::from_str(&columns_json).unwrap_or_default();

        Self {
            name: row.get_string(COL_INDEX_NAME).unwrap_or_default(),
            table: row.get_string(COL_TABLE_NAME).unwrap_or_default(),
            schema: row.get_string(COL_SCHEMA_NAME).unwrap_or_default(),
            index_type: row
                .get_string(COL_INDEX_TYPE)
                .and_then(|s| IndexType::parse(&s))
                .unwrap_or(IndexType::BTree),
            columns,
            is_unique: row.get_bool(COL_IS_UNIQUE).unwrap_or(false),
            is_primary: row.get_bool(COL_IS_PRIMARY).unwrap_or(false),
            condition: row.get_string(COL_INDEX_CONDITION),
            tablespace: row.get_string(COL_TABLESPACE),
            fill_factor: row.get_i32(COL_FILL_FACTOR).and_then(|v| {
                if (10..=100).contains(&v) {
                    Some(v as u8)
                } else {
                    None
                }
            }),
            size_bytes: row.get_i64(COL_SIZE_BYTES).map(|v| v as u64),
            comment: row.get_string(COL_COMMENT),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IndexType {
    BTree,
    Hash,
    Gin,      // PostgreSQL
    Gist,     // PostgreSQL
    SpGist,   // PostgreSQL
    Brin,     // PostgreSQL
    FullText, // MySQL
}

impl IndexType {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "btree" => Some(Self::BTree),
            "hash" => Some(Self::Hash),
            "gin" => Some(Self::Gin),
            "gist" => Some(Self::Gist),
            "spgist" => Some(Self::SpGist),
            "brin" => Some(Self::Brin),
            "fulltext" => Some(Self::FullText),
            _ => None,
        }
    }

    /// Returns the SQL string for use in DDL (e.g. `USING` clause).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BTree => "btree",
            Self::Hash => "hash",
            Self::Gin => "gin",
            Self::Gist => "gist",
            Self::SpGist => "spgist",
            Self::Brin => "brin",
            Self::FullText => "fulltext",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexColumn {
    pub name: String,
    pub sort_order: SortOrder,

    /// NULLS positioning (PostgreSQL)
    pub nulls_order: NullsOrder,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    pub fn to_string(&self) -> Option<String> {
        match self {
            SortOrder::Asc => None,
            SortOrder::Desc => Some("DESC".to_string()),
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "asc" => Self::Asc,
            "desc" => Self::Desc,
            _ => Self::Asc, // Default to Asc if unrecognized
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NullsOrder {
    First,
    Last,
    Default,
}

impl NullsOrder {
    pub fn to_string(&self) -> Option<String> {
        match self {
            NullsOrder::First => Some("NULLS FIRST".to_string()),
            NullsOrder::Last => Some("NULLS LAST".to_string()),
            NullsOrder::Default => None,
        }
    }
}
