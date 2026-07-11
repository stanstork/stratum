use crate::converters::{DialectConverter, mysql_to_pg::MysqlToPg, pg_to_mysql::PgToMysql};
use connectors::{
    drivers::{mysql::types::MySqlTypeConverter, postgres::types::PgTypeConverter},
    sql::metadata::{column::ColumnMetadata, index::IndexType},
};
use model::core::{convert::IntoCanonical, types::Type};
use std::{collections::HashMap, sync::Arc};

/// Describes how to transform a value during type conversion
#[derive(Debug, Clone, PartialEq)]
pub enum TypeTransform {
    /// Cast to string (for ENUM -> VARCHAR, etc.)
    CastToString,
    /// Parse from string (for VARCHAR -> ENUM)
    ParseFromString,
    /// Truncate to length
    Truncate { max_length: u32 },
    /// Scale decimal precision
    ScalePrecision { precision: u8, scale: u8 },
    /// Custom SQL expression
    Custom(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConversionResult {
    /// Direct mapping, no data loss
    Exact(Type),

    /// Compatible but may have differences (e.g., precision)
    Compatible { target: Type, warnings: Vec<String> },

    /// Requires transformation (e.g., ENUM -> VARCHAR)
    RequiresTransform {
        target: Type,
        transform: TypeTransform,
    },

    /// Not supported
    Unsupported(String),
}

impl ConversionResult {
    /// Extract the target Type, falling back to Text for unsupported types
    pub fn target_type(&self) -> Type {
        match self {
            ConversionResult::Exact(t) => t.clone(),
            ConversionResult::Compatible { target, .. } => target.clone(),
            ConversionResult::RequiresTransform { target, .. } => target.clone(),
            ConversionResult::Unsupported(_) => Type::Text { charset: None },
        }
    }

    /// Check if the conversion is exact (no data loss)
    pub fn is_exact(&self) -> bool {
        matches!(self, ConversionResult::Exact(_))
    }

    /// Get warnings if any
    pub fn warnings(&self) -> Vec<String> {
        match self {
            ConversionResult::Compatible { warnings, .. } => warnings.clone(),
            ConversionResult::Unsupported(msg) => vec![msg.clone()],
            _ => vec![],
        }
    }
}

/// Database dialect for type conversion
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    MySql,
    Postgres,
}

impl Dialect {
    pub fn to_canonical(&self, col: &ColumnMetadata) -> Type {
        match self {
            Dialect::MySql => MySqlTypeConverter.to_canonical(col).canonical,
            Dialect::Postgres => PgTypeConverter.to_canonical(col).canonical,
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "mysql" => Some(Dialect::MySql),
            "postgres" | "postgresql" => Some(Dialect::Postgres),
            _ => None,
        }
    }

    /// Normalize a generated column expression from this dialect's syntax to plain SQL.
    /// MySQL quotes identifiers with backticks (e.g. `` char_length(`title`) ``);
    /// strip them so the expression is valid in any target dialect.
    pub fn normalize_generated_expression(&self, expr: &str) -> String {
        match self {
            Dialect::MySql => expr.replace('`', ""),
            Dialect::Postgres => expr.to_owned(),
        }
    }

    /// Normalize a column DEFAULT expression from this dialect's syntax.
    /// PostgreSQL annotates default literals with `::type` casts (e.g.
    /// `'G'::mpaa_rating`, `'{}'::text[]`, `0::numeric`), which are invalid
    /// elsewhere; strip them, leaving the bare literal.
    pub fn normalize_default_expression(&self, expr: &str) -> String {
        match self {
            Dialect::MySql => expr.to_owned(),
            Dialect::Postgres => strip_pg_casts(expr),
        }
    }

    /// Adjust a (source-normalized) DEFAULT expression for this dialect.
    /// MySQL 8 requires non-literal/function defaults to be parenthesized, e.g.
    /// `DEFAULT (CURRENT_DATE)`, `DEFAULT (now())` - unlike bare literals such as
    /// `'G'` or `3`. PostgreSQL accepts either form, so it is left unchanged.
    pub fn finalize_default_expression(&self, expr: &str) -> String {
        match self {
            Dialect::Postgres => expr.to_owned(),
            Dialect::MySql => {
                let trimmed = expr.trim();
                if is_sql_literal(trimmed) || trimmed.starts_with('(') {
                    trimmed.to_owned()
                } else {
                    // Function call or keyword expression (CURRENT_DATE, now(), ...).
                    format!("({trimmed})")
                }
            }
        }
    }

    /// Convert to the query_builder Dialect trait object for SQL generation
    pub fn as_query_dialect(&self) -> Box<dyn query_builder::dialect::Dialect + Send + Sync> {
        match self {
            Dialect::MySql => Box::new(query_builder::dialect::MySql),
            Dialect::Postgres => Box::new(query_builder::dialect::Postgres),
        }
    }
}

/// True when a DEFAULT expression is a bare SQL literal (string, number, or the
/// keywords NULL/TRUE/FALSE) that needs no parenthesization in MySQL.
fn is_sql_literal(expr: &str) -> bool {
    let e = expr.trim();
    if e.is_empty() {
        return true; // nothing to wrap
    }
    // Quoted string literal.
    if e.starts_with('\'') {
        return true;
    }
    // Numeric literal (optionally signed / decimal).
    if e.trim_start_matches(['-', '+'])
        .chars()
        .all(|c| c.is_ascii_digit() || c == '.')
        && e.chars().any(|c| c.is_ascii_digit())
    {
        return true;
    }
    // Boolean / null keywords.
    matches!(e.to_ascii_uppercase().as_str(), "NULL" | "TRUE" | "FALSE")
}

/// Strip PostgreSQL `::type` cast annotations from a default expression, e.g.
/// `'G'::mpaa_rating` -> `'G'`, `'{}'::text[]` -> `'{}'`, `0::numeric(10,2)` -> `0`.
/// Only the cast suffix is removed; the underlying literal/function is preserved.
fn strip_pg_casts(expr: &str) -> String {
    let chars: Vec<char> = expr.chars().collect();
    let mut out = String::with_capacity(expr.len());
    let mut i = 0;
    while i < chars.len() {
        // Inside a single-quoted string literal, `::` is data, not a cast (e.g.
        // `'foo::bar'::text`). Copy the literal verbatim; a doubled `''` is an
        // escaped quote that stays within the string, a lone `'` closes it.
        if chars[i] == '\'' {
            out.push(chars[i]);
            i += 1;
            while i < chars.len() {
                out.push(chars[i]);
                if chars[i] == '\'' {
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        out.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if chars[i] == ':' && i + 1 < chars.len() && chars[i + 1] == ':' {
            i += 2; // skip "::"
            // Type name: identifier chars, dots (schema-qualified), and spaces so
            // multi-word types like `character varying` are consumed whole.
            while i < chars.len()
                && (chars[i].is_alphanumeric()
                    || chars[i] == '_'
                    || chars[i] == '.'
                    || chars[i] == ' ')
            {
                i += 1;
            }
            // Optional length/precision modifier, e.g. numeric(10,2), varchar(50).
            if i < chars.len() && chars[i] == '(' {
                let mut depth = 0;
                while i < chars.len() {
                    match chars[i] {
                        '(' => depth += 1,
                        ')' => depth -= 1,
                        _ => {}
                    }
                    i += 1;
                    if depth == 0 {
                        break;
                    }
                }
            }
            // Optional array marker(s), e.g. text[] or int[][].
            while i + 1 < chars.len() && chars[i] == '[' && chars[i + 1] == ']' {
                i += 2;
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// Registry for type mappings between source and destination databases.
///
/// Uses the canonical Type enum as the universal type representation.
/// Dialect-pair converters are registered at construction time; adding a new
/// database only requires a new module in `converters/` implementing
/// `DialectConverter` and a registration entry in `build_converters()`.
#[derive(Clone)]
pub struct TypeRegistry {
    /// Custom type overrides (source_type_name -> Type)
    custom_mappings: HashMap<String, Type>,
    /// Source database dialect
    source_dialect: Dialect,
    /// Target database dialect
    target_dialect: Dialect,
    /// Dialect-pair converter (None = same dialect, passthrough)
    converter: Option<Arc<dyn DialectConverter>>,
    /// Cached index type overrides (built once from converter)
    index_type_map: HashMap<IndexType, IndexType>,
}

impl std::fmt::Debug for TypeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeRegistry")
            .field("source_dialect", &self.source_dialect)
            .field("target_dialect", &self.target_dialect)
            .field("custom_mappings", &self.custom_mappings)
            .field("has_converter", &self.converter.is_some())
            .finish()
    }
}

impl TypeRegistry {
    /// Create a new TypeRegistry for the given source and target dialects.
    ///
    /// Looks up the converter for (source, target) from the global registry.
    /// Same-dialect pairs use passthrough (no converter needed).
    pub fn new(source: Dialect, target: Dialect) -> Self {
        let converter: Option<Arc<dyn DialectConverter>> = match (source, target) {
            (Dialect::MySql, Dialect::Postgres) => Some(Arc::new(MysqlToPg)),
            (Dialect::Postgres, Dialect::MySql) => Some(Arc::new(PgToMysql)),
            _ => None,
        };

        let index_type_map = converter
            .as_ref()
            .map(|c| c.index_type_map())
            .unwrap_or_default();

        Self {
            custom_mappings: HashMap::new(),
            source_dialect: source,
            target_dialect: target,
            converter,
            index_type_map,
        }
    }

    /// Add a custom type mapping override
    pub fn add_custom_mapping(&mut self, source_type_name: &str, target_type: Type) {
        self.custom_mappings
            .insert(source_type_name.to_lowercase(), target_type);
    }

    /// Convert a source Type to the target database type
    pub fn convert(&self, source_type: &Type) -> ConversionResult {
        // Check custom mappings first by type name
        let type_name = source_type.name().to_lowercase();
        if let Some(target) = self.custom_mappings.get(&type_name) {
            return ConversionResult::Exact(target.clone());
        }

        // Use registered converter, or passthrough if none
        match &self.converter {
            Some(converter) => converter.convert_type(source_type),
            None => ConversionResult::Exact(source_type.clone()),
        }
    }

    /// Convert a source index type to the target dialect's equivalent.
    ///
    /// Looks up the index type in the dialect pair's map. If not found,
    /// passes through unchanged (the index type exists in both dialects).
    pub fn convert_index_type(&self, source: &IndexType) -> IndexType {
        self.index_type_map
            .get(source)
            .cloned()
            .unwrap_or_else(|| source.clone())
    }

    /// Check if a conversion is safe (no data loss)
    pub fn is_safe_conversion(&self, source: &Type) -> bool {
        matches!(
            self.convert(source),
            ConversionResult::Exact(_) | ConversionResult::Compatible { .. }
        )
    }

    /// Get warnings for a conversion
    pub fn get_conversion_warnings(&self, source: &Type) -> Vec<String> {
        match self.convert(source) {
            ConversionResult::Compatible { warnings, .. } => warnings,
            ConversionResult::RequiresTransform { transform, .. } => {
                vec![format!("Requires transformation: {:?}", transform)]
            }
            ConversionResult::Unsupported(msg) => vec![msg],
            ConversionResult::Exact(_) => vec![],
        }
    }

    pub fn source_dialect(&self) -> Dialect {
        self.source_dialect
    }

    pub fn target_dialect(&self) -> Dialect {
        self.target_dialect
    }

    /// Whether the target dialect requires explicit sequences for auto-increment columns.
    pub fn use_explicit_sequences(&self) -> bool {
        self.converter
            .as_ref()
            .is_some_and(|c| c.use_explicit_sequences())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::core::types::IntSize;

    #[test]
    fn strip_pg_casts_enum_default() {
        assert_eq!(strip_pg_casts("'G'::mpaa_rating"), "'G'");
    }

    #[test]
    fn strip_pg_casts_various() {
        assert_eq!(strip_pg_casts("'{}'::text[]"), "'{}'");
        assert_eq!(strip_pg_casts("0::numeric(10,2)"), "0");
        assert_eq!(strip_pg_casts("''::character varying"), "''"); // multi-word type
        assert_eq!(strip_pg_casts("'foo'::public.my_enum"), "'foo'");
        // nextval keeps its call; the inner ::regclass cast is stripped.
        assert_eq!(
            strip_pg_casts("nextval('actor_actor_id_seq'::regclass)"),
            "nextval('actor_actor_id_seq')"
        );
        // No cast: unchanged.
        assert_eq!(strip_pg_casts("now()"), "now()");
        assert_eq!(strip_pg_casts("false"), "false");
    }

    #[test]
    fn strip_pg_casts_preserves_colons_in_string_literals() {
        // `::` inside a string literal is data, not a cast, and must survive
        // while the trailing cast is still stripped.
        assert_eq!(strip_pg_casts("'foo::bar'::text"), "'foo::bar'");
        assert_eq!(strip_pg_casts("'a::b::c'::varchar(10)"), "'a::b::c'");
        // Doubled '' escape keeps the literal intact.
        assert_eq!(strip_pg_casts("'it''s::ok'::text"), "'it''s::ok'");
        // Bare literal with colons, no cast.
        assert_eq!(strip_pg_casts("'x::y'"), "'x::y'");
    }

    #[test]
    fn mysql_wraps_function_defaults_but_not_literals() {
        let my = Dialect::MySql;
        assert_eq!(
            my.finalize_default_expression("CURRENT_DATE"),
            "(CURRENT_DATE)"
        );
        assert_eq!(my.finalize_default_expression("now()"), "(now())");
        // Literals stay bare.
        assert_eq!(my.finalize_default_expression("'G'"), "'G'");
        assert_eq!(my.finalize_default_expression("4.99"), "4.99");
        assert_eq!(my.finalize_default_expression("-3"), "-3");
        assert_eq!(my.finalize_default_expression("false"), "false");
        // Already parenthesized is untouched.
        assert_eq!(my.finalize_default_expression("(now())"), "(now())");
    }

    #[test]
    fn postgres_leaves_defaults_unchanged() {
        assert_eq!(
            Dialect::Postgres.finalize_default_expression("CURRENT_DATE"),
            "CURRENT_DATE"
        );
    }

    #[test]
    fn normalize_default_only_affects_postgres() {
        assert_eq!(
            Dialect::Postgres.normalize_default_expression("'G'::mpaa_rating"),
            "'G'"
        );
        // MySQL source defaults have no `::` casts; leave untouched.
        assert_eq!(Dialect::MySql.normalize_default_expression("'G'"), "'G'");
    }

    #[test]
    fn test_mysql_to_postgres_integer_types() {
        let registry = TypeRegistry::new(Dialect::MySql, Dialect::Postgres);

        let int_type = Type::Int {
            bits: IntSize::I32,
            unsigned: false,
            auto_increment: false,
        };
        assert!(registry.convert(&int_type).is_exact());

        // Unsigned should have warning
        let unsigned_type = Type::Int {
            bits: IntSize::I64,
            unsigned: true,
            auto_increment: false,
        };
        let result = registry.convert(&unsigned_type);
        assert!(matches!(result, ConversionResult::Compatible { .. }));
    }

    /// A MySQL ENUM maps to a native PostgreSQL enum with its variants intact; the
    /// `CREATE TYPE ... AS ENUM` is emitted separately by `SchemaPlan::enum_ops`.
    #[test]
    fn test_mysql_enum_to_postgres() {
        let registry = TypeRegistry::new(Dialect::MySql, Dialect::Postgres);

        let enum_type = Type::Enum {
            name: String::new(),
            values: vec!["active".to_string(), "inactive".to_string()],
        };
        match registry.convert(&enum_type) {
            ConversionResult::Exact(Type::Enum { values, .. }) => {
                assert_eq!(values, vec!["active", "inactive"]);
            }
            other => panic!("expected an exact enum conversion, got {other:?}"),
        }
    }

    #[test]
    fn test_custom_mapping() {
        let mut registry = TypeRegistry::new(Dialect::MySql, Dialect::Postgres);
        registry.add_custom_mapping("tinyint", Type::Boolean);

        let tinyint = Type::Int {
            bits: IntSize::I8,
            unsigned: false,
            auto_increment: false,
        };
        let result = registry.convert(&tinyint);
        assert!(result.is_exact());
        assert_eq!(result.target_type(), Type::Boolean);
    }

    #[test]
    fn test_postgres_to_mysql_uuid() {
        let registry = TypeRegistry::new(Dialect::Postgres, Dialect::MySql);

        let result = registry.convert(&Type::Uuid);
        assert!(matches!(result, ConversionResult::Compatible { .. }));
    }

    #[test]
    fn test_safe_conversion_check() {
        let registry = TypeRegistry::new(Dialect::MySql, Dialect::Postgres);

        let int_type = Type::Int {
            bits: IntSize::I32,
            unsigned: false,
            auto_increment: false,
        };
        assert!(registry.is_safe_conversion(&int_type));
        assert!(registry.is_safe_conversion(&Type::Text { charset: None }));
    }

    #[test]
    fn test_roundtrip() {
        let types = vec![
            Type::Int {
                bits: IntSize::I32,
                unsigned: false,
                auto_increment: false,
            },
            Type::Int {
                bits: IntSize::I64,
                unsigned: false,
                auto_increment: false,
            },
            Type::Text { charset: None },
            Type::Boolean,
        ];

        // Same dialect should always be exact (no converter registered)
        let registry = TypeRegistry::new(Dialect::MySql, Dialect::MySql);
        for t in &types {
            assert!(registry.convert(t).is_exact(), "Failed for {:?}", t);
        }
    }

    #[test]
    fn test_postgres_network_types() {
        let registry = TypeRegistry::new(Dialect::Postgres, Dialect::MySql);

        let result = registry.convert(&Type::Inet);
        assert!(matches!(result, ConversionResult::RequiresTransform { .. }));
    }

    #[test]
    fn test_same_dialect_passthrough() {
        let registry = TypeRegistry::new(Dialect::MySql, Dialect::MySql);

        let enum_type = Type::Enum {
            name: String::new(),
            values: vec!["a".to_string()],
        };
        let result = registry.convert(&enum_type);
        assert!(result.is_exact());
    }

    #[test]
    fn test_index_type_mysql_to_postgres() {
        let registry = TypeRegistry::new(Dialect::MySql, Dialect::Postgres);

        assert_eq!(
            registry.convert_index_type(&IndexType::FullText),
            IndexType::Gin
        );
        assert_eq!(
            registry.convert_index_type(&IndexType::BTree),
            IndexType::BTree
        );
    }

    #[test]
    fn test_index_type_postgres_to_mysql() {
        let registry = TypeRegistry::new(Dialect::Postgres, Dialect::MySql);

        assert_eq!(
            registry.convert_index_type(&IndexType::Gin),
            IndexType::BTree
        );
        assert_eq!(
            registry.convert_index_type(&IndexType::Gist),
            IndexType::BTree
        );
        assert_eq!(
            registry.convert_index_type(&IndexType::SpGist),
            IndexType::BTree
        );
        assert_eq!(
            registry.convert_index_type(&IndexType::Brin),
            IndexType::BTree
        );
        assert_eq!(
            registry.convert_index_type(&IndexType::Hash),
            IndexType::Hash
        );
    }

    #[test]
    fn test_index_type_same_dialect_passthrough() {
        let registry = TypeRegistry::new(Dialect::Postgres, Dialect::Postgres);

        assert_eq!(registry.convert_index_type(&IndexType::Gin), IndexType::Gin);
        assert_eq!(
            registry.convert_index_type(&IndexType::Brin),
            IndexType::Brin
        );
    }
}
