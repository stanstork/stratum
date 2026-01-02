use model::transform::mapping::TransformationMetadata;

/// A parsed column reference with table and column names
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub table: String,
    pub column: String,
}

/// Errors that can occur during column reference parsing
#[derive(Debug, thiserror::Error)]
pub enum ColumnRefError {
    #[error("Invalid column reference format: {0}")]
    InvalidFormat(String),
    #[error("Ambiguous column reference: {0}")]
    Ambiguous(String),
}

/// Utility for parsing column references in various formats
pub struct ColumnRefParser;

impl ColumnRefParser {
    pub fn parse(input: &str, table: &str) -> Result<ColumnRef, ColumnRefError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Err(ColumnRefError::InvalidFormat(
                "Column reference cannot be empty".into(),
            ));
        }

        match trimmed.split_once('.') {
            Some((table, column)) => {
                let table = table.trim();
                let column = column.trim();

                if table.is_empty() || column.is_empty() {
                    return Err(ColumnRefError::InvalidFormat(format!(
                        "Invalid table.column format: '{}'",
                        input
                    )));
                }

                Ok(ColumnRef {
                    table: table.to_string(),
                    column: column.to_string(),
                })
            }
            None => Ok(ColumnRef {
                table: table.to_string(),
                column: trimmed.to_string(),
            }),
        }
    }

    pub fn parse_with_mapping(
        input: &str,
        table: &str,
        mapping: &TransformationMetadata,
    ) -> Result<ColumnRef, ColumnRefError> {
        let trimmed = input.trim();

        if let Some((alias, field_name)) = trimmed.split_once('.') {
            // Look up in foreign_fields to find the actual table and field
            for refs in mapping.foreign_fields.values() {
                for cross_ref in refs {
                    if cross_ref.entity.eq_ignore_ascii_case(alias)
                        && cross_ref.field.eq_ignore_ascii_case(field_name)
                    {
                        return Ok(ColumnRef {
                            table: cross_ref.entity.clone(),
                            column: cross_ref.field.clone(),
                        });
                    }
                }
            }
        }

        // Fallback to standard parsing
        Self::parse(input, table)
    }

    pub fn strip_table(input: &str) -> &str {
        input.split('.').next_back().unwrap_or(input)
    }

    pub fn parse_many(
        inputs: &[String],
        default_table: &str,
    ) -> Result<Vec<ColumnRef>, ColumnRefError> {
        inputs
            .iter()
            .map(|input| Self::parse(input, default_table))
            .collect()
    }

    pub fn parse_many_with_mapping(
        inputs: &[String],
        default_table: &str,
        mapping: &TransformationMetadata,
    ) -> Result<Vec<ColumnRef>, ColumnRefError> {
        inputs
            .iter()
            .map(|input| Self::parse_with_mapping(input, default_table, mapping))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_qualified() {
        let result = ColumnRefParser::parse("users.id", "customers").unwrap();
        assert_eq!(result.table, "users");
        assert_eq!(result.column, "id");
    }

    #[test]
    fn test_parse_unqualified() {
        let result = ColumnRefParser::parse("id", "customers").unwrap();
        assert_eq!(result.table, "customers");
        assert_eq!(result.column, "id");
    }

    #[test]
    fn test_parse_empty() {
        let result = ColumnRefParser::parse("", "customers");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_format() {
        let result = ColumnRefParser::parse("users.", "customers");
        assert!(result.is_err());

        let result = ColumnRefParser::parse(".id", "customers");
        assert!(result.is_err());
    }

    #[test]
    fn test_strip_table() {
        assert_eq!(ColumnRefParser::strip_table("users.id"), "id");
        assert_eq!(ColumnRefParser::strip_table("id"), "id");
        assert_eq!(
            ColumnRefParser::strip_table("schema.table.column"),
            "column"
        );
    }
}
