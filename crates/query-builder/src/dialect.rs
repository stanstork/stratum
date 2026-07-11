//! Defines the `Dialect` trait for database-specific SQL syntax.

use model::core::types::Type;

pub trait Dialect: Send + Sync {
    /// Wraps an identifier (like a table or column name) in the correct
    /// quotation marks for the dialect.
    ///
    /// - PostgreSQL uses double quotes: `"my_column"`
    /// - MySQL uses backticks: `` `my_column` ``
    fn quote_identifier(&self, ident: &str) -> String;

    /// Returns the placeholder for a parameterized query.
    ///
    /// - PostgreSQL uses `$1`, `$2`, etc.
    /// - MySQL uses `?`
    fn placeholder(&self, index: usize) -> String;

    /// Renders a generic `Type` into a database-specific SQL type string.
    fn format_type(&self, data_type: &Type, max_length: Option<usize>) -> String;

    /// Returns the name of the dialect (e.g., "PostgreSQL", "MySQL").
    fn name(&self) -> String;

    /// Generates the SQL query and a corresponding list of parameters to bind
    /// for efficiently checking the existence of multiple composite keys.
    fn key_existence_query(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String;

    /// Returns the random function name for this dialect.
    ///
    /// - PostgreSQL uses `RANDOM()`
    /// - MySQL uses `RAND()`
    /// - SQLite uses `RANDOM()`
    fn random_fn(&self) -> &'static str;

    /// Whether `CREATE INDEX ... IF NOT EXISTS` is supported.
    /// MySQL has no such clause (MariaDB does); PostgreSQL does.
    fn supports_index_if_not_exists(&self) -> bool {
        true
    }

    /// Whether `CREATE INDEX CONCURRENTLY` is supported (PostgreSQL only).
    fn supports_index_concurrently(&self) -> bool {
        true
    }

    /// Whether partial indexes (`CREATE INDEX ... WHERE <cond>`) are supported.
    /// PostgreSQL yes; MySQL no.
    fn supports_partial_index(&self) -> bool {
        true
    }

    /// Whether per-column `NULLS FIRST/LAST` ordering is supported in an index.
    /// PostgreSQL yes; MySQL no.
    fn supports_index_nulls(&self) -> bool {
        true
    }

    /// Whether the index method (`USING btree`) is written *before* the column
    /// list, as in PostgreSQL (`ON tbl USING btree (col)`). MySQL requires it
    /// after the column list (`ON tbl (col) USING BTREE`).
    fn index_method_before_cols(&self) -> bool {
        true
    }

    /// Whether an index column may carry a key prefix length, e.g. `` `col`(255) ``.
    /// MySQL requires one to index TEXT/BLOB columns; PostgreSQL has no such syntax.
    fn supports_index_prefix(&self) -> bool {
        false
    }

    /// Whether an auto-incrementing column must belong to a key.
    ///
    /// MySQL rejects `AUTO_INCREMENT` on a column that is not the first column of
    /// some key (error 1075), so the attribute must be dropped when a table is
    /// created without constraints. PostgreSQL's `SERIAL` is only a column default
    /// and needs no key.
    fn auto_inc_requires_key(&self) -> bool {
        false
    }

    /// Whether the dialect has standalone enum types (`CREATE TYPE ... AS ENUM`).
    ///
    /// PostgreSQL does. MySQL spells the variants inline in the column
    /// (`ENUM('a','b')`), so emitting a `CREATE TYPE` for it is a syntax error.
    fn supports_enums(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub struct Postgres;

impl Dialect for Postgres {
    fn quote_identifier(&self, ident: &str) -> String {
        format!(r#""{ident}""#)
    }

    fn placeholder(&self, index: usize) -> String {
        // PostgreSQL uses $1, $2, etc.
        format!("${}", index + 1)
    }

    fn format_type(&self, data_type: &Type, max_length: Option<usize>) -> String {
        use model::core::types::{FloatSize, GeomKind, IntSize, IntervalFields};

        match data_type {
            Type::Int {
                bits,
                auto_increment,
                ..
            } => {
                if *auto_increment {
                    match bits {
                        IntSize::I8 | IntSize::I16 => "smallserial".to_string(),
                        IntSize::I24 | IntSize::I32 => "serial".to_string(),
                        IntSize::I64 => "bigserial".to_string(),
                    }
                } else {
                    match bits {
                        IntSize::I8 | IntSize::I16 => "smallint".to_string(),
                        IntSize::I24 | IntSize::I32 => "integer".to_string(),
                        IntSize::I64 => "bigint".to_string(),
                    }
                }
            }
            Type::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("numeric({p},{s})"),
                (Some(p), None) => format!("numeric({p})"),
                _ => "numeric".to_string(),
            },
            Type::Float { bits } => match bits {
                FloatSize::F32 => "real".to_string(),
                FloatSize::F64 => "double precision".to_string(),
            },
            Type::Char { length, .. } => match length.or(max_length) {
                Some(l) => format!("char({l})"),
                None => "char".to_string(),
            },
            Type::Varchar { length, .. } => match length.or(max_length) {
                Some(l) => format!("varchar({l})"),
                None => "varchar".to_string(),
            },
            Type::Text { .. } => "text".to_string(),
            Type::Binary { .. } | Type::Varbinary { .. } | Type::Blob { .. } => "bytea".to_string(),
            Type::Date => "date".to_string(),
            Type::Time { precision, with_tz } => {
                let base = if *with_tz {
                    "time with time zone"
                } else {
                    "time"
                };
                match precision {
                    Some(p) => format!("{base}({p})"),
                    None => base.to_string(),
                }
            }
            Type::Timestamp { precision, with_tz } => {
                let base = if *with_tz {
                    "timestamp with time zone"
                } else {
                    "timestamp"
                };
                match precision {
                    Some(p) => format!("{base}({p})"),
                    None => base.to_string(),
                }
            }
            Type::Interval { fields } => match fields {
                Some(IntervalFields::Year) => "interval year".to_string(),
                Some(IntervalFields::Month) => "interval month".to_string(),
                Some(IntervalFields::Day) => "interval day".to_string(),
                Some(IntervalFields::Hour) => "interval hour".to_string(),
                Some(IntervalFields::Minute) => "interval minute".to_string(),
                Some(IntervalFields::Second) => "interval second".to_string(),
                Some(IntervalFields::YearMonth) => "interval year to month".to_string(),
                Some(IntervalFields::DayTime) => "interval day to second".to_string(),
                Some(IntervalFields::Full) | None => "interval".to_string(),
            },
            Type::Year => "smallint".to_string(), // No YEAR in PostgreSQL
            Type::Boolean => "boolean".to_string(),
            Type::Uuid => "uuid".to_string(),
            Type::Json { binary } => {
                if *binary {
                    "jsonb".to_string()
                } else {
                    "json".to_string()
                }
            }
            Type::Bit { length } => match length {
                Some(l) => format!("bit({l})"),
                None => "bit".to_string(),
            },
            Type::Array { element } => {
                format!("{}[]", self.format_type(element, None))
            }
            Type::Enum { name, .. } => name.clone(),
            Type::Set { .. } => "text[]".to_string(),
            Type::Geometry { kind, .. } => match kind {
                Some(GeomKind::Point) => "point".to_string(),
                Some(GeomKind::LineString) => "path".to_string(),
                Some(GeomKind::Polygon) => "polygon".to_string(),
                _ => "geometry".to_string(),
            },
            Type::Inet => "inet".to_string(),
            Type::Cidr => "cidr".to_string(),
            Type::MacAddr => "macaddr".to_string(),
            Type::Composite { name, .. } => name.clone(),
            Type::Domain { name, .. } => name.clone(),
            Type::Unknown { fallback_ddl, .. } => fallback_ddl.clone(),
        }
    }

    fn name(&self) -> String {
        "PostgreSQL".into()
    }

    fn key_existence_query(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String {
        if keys_batch == 0 || key_columns.is_empty() {
            return String::new();
        }

        let select_clause = key_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| format!("v.c{} AS {}", i + 1, self.quote_identifier(col_name)))
            .collect::<Vec<_>>()
            .join(", ");

        let value_columns: String = (1..=key_columns.len())
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ");

        let join_conditions = key_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| format!("t.{} = v.c{}", self.quote_identifier(col_name), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let mut placeholder_idx = 1;
        let placeholders: String = (0..keys_batch)
            .map(|_| {
                let p = (0..key_columns.len())
                    .map(|_| {
                        let p_str = format!("${placeholder_idx}");
                        placeholder_idx += 1;
                        p_str
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({p})")
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {} FROM (VALUES {}) AS v({}) INNER JOIN {} AS t ON {}",
            select_clause,
            placeholders,
            value_columns,
            self.quote_identifier(table_name),
            join_conditions
        )
    }

    fn random_fn(&self) -> &'static str {
        "RANDOM()"
    }

    fn supports_enums(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct MySql;

impl Dialect for MySql {
    fn quote_identifier(&self, ident: &str) -> String {
        format!(r#"`{ident}`"#)
    }

    fn placeholder(&self, _index: usize) -> String {
        // MySQL uses ?
        "?".into()
    }

    fn format_type(&self, data_type: &Type, max_length: Option<usize>) -> String {
        use model::core::types::{FloatSize, GeomKind, IntSize};

        match data_type {
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
                let mut result = base.to_string();
                if *unsigned {
                    result.push_str(" UNSIGNED");
                }
                if *auto_increment {
                    result.push_str(" AUTO_INCREMENT");
                }
                result
            }
            Type::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("DECIMAL({p},{s})"),
                (Some(p), None) => format!("DECIMAL({p})"),
                _ => "DECIMAL".to_string(),
            },
            Type::Float { bits } => match bits {
                FloatSize::F32 => "FLOAT".to_string(),
                FloatSize::F64 => "DOUBLE".to_string(),
            },
            Type::Char { length, .. } => match length.or(max_length) {
                Some(l) => format!("CHAR({l})"),
                None => "CHAR".to_string(),
            },
            Type::Varchar { length, .. } => match length.or(max_length) {
                Some(l) => format!("VARCHAR({l})"),
                None => "VARCHAR(255)".to_string(),
            },
            Type::Text { .. } => "TEXT".to_string(),
            Type::Binary { length } => match length.or(max_length) {
                Some(l) => format!("BINARY({l})"),
                None => "BINARY".to_string(),
            },
            Type::Varbinary { length } => match length.or(max_length) {
                Some(l) => format!("VARBINARY({l})"),
                None => "VARBINARY(255)".to_string(),
            },
            Type::Blob { max_bytes } => match max_bytes {
                Some(b) if *b <= 255 => "TINYBLOB".to_string(),
                Some(b) if *b <= 65535 => "BLOB".to_string(),
                Some(b) if *b <= 16777215 => "MEDIUMBLOB".to_string(),
                _ => "LONGBLOB".to_string(),
            },
            Type::Date => "DATE".to_string(),
            Type::Time { precision, .. } => match precision {
                Some(p) => format!("TIME({p})"),
                None => "TIME".to_string(),
            },
            Type::Timestamp { precision, .. } => match precision {
                Some(p) => format!("DATETIME({p})"),
                None => "DATETIME".to_string(),
            },
            Type::Interval { .. } => "VARCHAR(255)".to_string(), // No interval in MySQL
            Type::Year => "YEAR".to_string(),
            Type::Boolean => "TINYINT(1)".to_string(),
            Type::Uuid => "CHAR(36)".to_string(),
            Type::Json { .. } => "JSON".to_string(),
            Type::Bit { length } => match length {
                Some(l) => format!("BIT({l})"),
                None => "BIT".to_string(),
            },
            Type::Array { .. } => "JSON".to_string(), // No native arrays in MySQL
            Type::Enum { values, .. } => {
                let vals = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("ENUM({vals})")
            }
            Type::Set { values } => {
                let vals = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("SET({vals})")
            }
            Type::Geometry { kind, .. } => match kind {
                Some(GeomKind::Point) => "POINT".to_string(),
                Some(GeomKind::LineString) => "LINESTRING".to_string(),
                Some(GeomKind::Polygon) => "POLYGON".to_string(),
                Some(GeomKind::MultiPoint) => "MULTIPOINT".to_string(),
                Some(GeomKind::MultiLineString) => "MULTILINESTRING".to_string(),
                Some(GeomKind::MultiPolygon) => "MULTIPOLYGON".to_string(),
                Some(GeomKind::GeometryCollection) => "GEOMETRYCOLLECTION".to_string(),
                None => "GEOMETRY".to_string(),
            },
            Type::Inet => "VARCHAR(45)".to_string(),
            Type::Cidr => "VARCHAR(45)".to_string(),
            Type::MacAddr => "VARCHAR(17)".to_string(),
            Type::Composite { .. } => "JSON".to_string(),
            Type::Domain { base_type, .. } => self.format_type(base_type, max_length),
            Type::Unknown { fallback_ddl, .. } => fallback_ddl.clone(),
        }
    }

    fn name(&self) -> String {
        "MySQL".into()
    }

    fn key_existence_query(
        &self,
        _table_name: &str,
        _key_columns: &[String],
        _keys_batch: usize,
    ) -> String {
        todo!("Implement batch key existence query for MySQL")
    }

    fn random_fn(&self) -> &'static str {
        "RAND()"
    }

    fn supports_index_if_not_exists(&self) -> bool {
        false
    }

    fn supports_index_concurrently(&self) -> bool {
        false
    }

    fn supports_partial_index(&self) -> bool {
        false
    }

    fn supports_index_nulls(&self) -> bool {
        false
    }

    fn index_method_before_cols(&self) -> bool {
        false
    }

    fn supports_index_prefix(&self) -> bool {
        true
    }

    fn auto_inc_requires_key(&self) -> bool {
        true
    }
}
