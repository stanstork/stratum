use crate::{metadata::column::metadata::ColumnMetadata, requests::JoinClause};

#[derive(Debug, Clone)]
pub struct SqlQueryBuilder {
    pub query: String,
    pub params: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default: Option<String>,
    pub char_max_length: Option<usize>,
}

pub struct ForeignKeyInfo {
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
}

#[derive(Debug, Clone)]
pub struct SelectColumn {
    pub table: String,
    pub column: String,
    pub alias: Option<String>,
    pub data_type: String,
}

impl ColumnInfo {
    pub fn new(metadata: &ColumnMetadata) -> Self {
        Self {
            name: metadata.name.clone(),
            data_type: metadata.data_type.to_string(),
            is_nullable: metadata.is_nullable,
            is_primary_key: metadata.is_primary_key,
            default: metadata.default_value.as_ref().map(|v| v.to_string()),
            char_max_length: metadata.char_max_length,
        }
    }

    pub fn set_name(mut self, name: &str) -> Self {
        self.name = name.to_owned();
        self
    }

    pub fn is_array(&self) -> bool {
        self.data_type.eq_ignore_ascii_case("ARRAY")
    }
}

impl SelectColumn {
    pub fn is_geometry(&self) -> bool {
        self.data_type.eq_ignore_ascii_case("geometry")
    }
}

impl Default for SqlQueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlQueryBuilder {
    pub fn new() -> Self {
        Self {
            query: String::new(),
            params: Vec::new(),
        }
    }

    pub fn begin_transaction(mut self) -> Self {
        self.query.push_str("BEGIN;\n");
        self
    }

    pub fn commit_transaction(mut self) -> Self {
        self.query.push_str("\nCOMMIT;");
        self
    }

    pub fn select(mut self, columns: &[SelectColumn]) -> Self {
        let columns: Vec<String> = columns
            .iter()
            .map(|col| {
                let column_expr = if col.is_geometry() {
                    format!("ST_AsBinary({}.{})", col.table, col.column)
                } else {
                    format!("{}.{}", col.table, col.column)
                };

                if let Some(alias) = &col.alias {
                    format!("{} AS {}", column_expr, alias)
                } else {
                    column_expr
                }
            })
            .collect();

        self.query
            .push_str(&format!("SELECT {}", columns.join(", ")));
        self
    }

    pub fn join(mut self, joins: &Vec<JoinClause>) -> Self {
        for join in joins {
            self.query.push_str(&format!(
                " {} JOIN {} AS {} ON {}.{} = {}.{}",
                join.join_type,
                join.table,
                join.alias,
                join.from_alias,
                join.from_col,
                join.alias,
                join.to_col
            ));
        }
        self
    }

    pub fn from(mut self, table: &str, alias: &str) -> Self {
        self.query
            .push_str(&format!(" FROM {} AS {}", table, alias));
        self
    }

    pub fn insert_into(mut self, table: &str, columns: &[(String, String)]) -> Self {
        let (cols, values): (Vec<String>, Vec<String>) = columns.iter().cloned().unzip();
        self.query.push_str(&format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            cols.join(", "),
            values.join(", ")
        ));
        self
    }

    pub fn insert_batch(
        mut self,
        table: &str,
        columns: Vec<ColumnInfo>,
        values: Vec<Vec<String>>,
    ) -> Self {
        let column_names = columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        let mut value_str = String::new();

        for (i, row) in values.iter().enumerate() {
            if i > 0 {
                value_str.push_str(", ");
            }

            let formatted_values: Vec<String> = row
                .iter()
                .zip(&columns)
                .map(|(val, col)| {
                    if col.is_array() {
                        Self::format_array_literal(val)
                    } else {
                        val.clone()
                    }
                })
                .collect();

            value_str.push_str(&format!("({})", formatted_values.join(", ")));
        }

        let query = format!(
            "INSERT INTO {} ({}) VALUES\n{};",
            table, column_names, value_str
        );
        self.query.push_str(&query);
        self
    }

    pub fn create_table(
        mut self,
        table: &str,
        columns: &[ColumnInfo],
        foreign_keys: &[ForeignKeyInfo],
    ) -> Self {
        self.query
            .push_str(&format!("\nCREATE TABLE {} (\n", table));

        let composite_pk = columns.iter().filter(|c| c.is_primary_key).count() > 1;

        let column_defs: Vec<String> = columns
            .iter()
            .map(|col| {
                let mut definition = col.name.clone();
                if col.char_max_length.is_some() {
                    definition.push_str(&format!(
                        " {}({})",
                        col.data_type,
                        col.char_max_length.unwrap()
                    ));
                } else {
                    definition.push_str(&format!(" {}", col.data_type));
                }

                if !composite_pk && col.is_primary_key {
                    definition.push_str(" PRIMARY KEY");
                }
                if !col.is_nullable {
                    definition.push_str(" NOT NULL");
                }
                if let Some(default_val) = &col.default {
                    definition.push_str(&format!(" DEFAULT {}", default_val));
                }
                definition
            })
            .collect();

        let pk_columns = if composite_pk {
            let pk_columns: Vec<String> = columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect();
            vec![format!("\tPRIMARY KEY ({})", pk_columns.join(", "))]
        } else {
            vec![]
        };

        let foreign_key_defs: Vec<String> = foreign_keys
            .iter()
            .map(|fk| {
                format!(
                    "\tFOREIGN KEY ({}) REFERENCES {}({})",
                    fk.column, fk.referenced_table, fk.referenced_column
                )
            })
            .collect();

        let all_defs = column_defs
            .into_iter()
            .chain(pk_columns)
            .chain(foreign_key_defs)
            .collect::<Vec<_>>()
            .join(",\n");

        self.query.push_str(&all_defs);
        self.query.push_str("\n);\n");

        self
    }

    pub fn add_foreign_key(mut self, table: &str, foreign_keys: &ForeignKeyInfo) -> Self {
        self.query.push_str(&format!(
            "ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}({});",
            table,
            foreign_keys.column,
            foreign_keys.referenced_table,
            foreign_keys.referenced_column
        ));
        self
    }

    pub fn create_enum(mut self, name: &str, values: &[String]) -> Self {
        self.query.push_str(&format!(
            "CREATE TYPE {} AS ENUM ('{}');",
            name,
            values.join("', '")
        ));
        self
    }

    pub fn toggle_trigger(mut self, table: &str, enabled: bool) -> Self {
        self.query.push_str(&format!(
            "ALTER TABLE {} {} TRIGGER ALL;",
            table,
            if enabled { "ENABLE" } else { "DISABLE" }
        ));
        self
    }

    pub fn order_by(mut self, column: &str, direction: &str) -> Self {
        self.query
            .push_str(&format!(" ORDER BY {} {}", column, direction));
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.query.push_str(&format!(" LIMIT {}", limit));
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.query.push_str(&format!(" OFFSET {}", offset));
        self
    }

    pub fn add_param(mut self, param: &str) -> Self {
        self.params.push(param.to_owned());
        self
    }

    pub fn build(self) -> (String, Vec<String>) {
        (self.query, self.params)
    }

    fn format_array_literal(value: &str) -> String {
        let trimmed = value
            .trim_matches(&['[', ']'][..])
            .trim_start_matches('\'')
            .trim_end_matches('\'');

        if trimmed.is_empty() {
            return "'{}'".to_string();
        }

        let elements: Vec<String> = trimmed
            .split(',')
            .map(|s| s.trim().replace('"', r#"\""#))
            .map(|e| format!(r#""{}""#, e))
            .collect();

        format!("'{{{}}}'", elements.join(","))
    }
}
