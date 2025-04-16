use super::{column::ColumnDef, fk::ForeignKeyDef, select::SelectField};
use crate::join::{JoinClause, JoinType};

#[derive(Debug, Clone)]
pub struct SqlQueryBuilder {
    pub query: String,
    pub params: Vec<String>,
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

    pub fn select(mut self, fields: &[SelectField]) -> Self {
        let columns: Vec<String> = fields
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
            let join_type = match join.join_type {
                JoinType::Inner => "INNER",
                JoinType::Left => "LEFT",
                JoinType::Right => "RIGHT",
                JoinType::Full => "FULL",
            };
            self.query.push_str(&format!(
                " {} JOIN {} AS {} ON ",
                join_type, join.left.table, join.left.alias
            ));

            let conditions: Vec<String> = join
                .conditions
                .iter()
                .map(|cond| {
                    format!(
                        "{}.{} = {}.{}",
                        join.left.alias, cond.left.column, join.right.alias, cond.right.column
                    )
                })
                .collect();

            self.query.push_str(&conditions.join(" AND "));
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
        columns: Vec<ColumnDef>,
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
        columns: &[ColumnDef],
        foreign_keys: &[ForeignKeyDef],
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

    pub fn add_foreign_key(mut self, table: &str, foreign_keys: &ForeignKeyDef) -> Self {
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

    pub fn add_column(mut self, table: &str, column: &ColumnDef) -> Self {
        let mut query = format!("ALTER TABLE {} ADD COLUMN {} ", table, column.name);
        if column.char_max_length.is_some() {
            query.push_str(&format!(
                "{}({})",
                column.data_type,
                column.char_max_length.unwrap()
            ));
        } else {
            query.push_str(&format!("{}", column.data_type));
        }
        if !column.is_nullable {
            query.push_str(" NOT NULL");
        }
        if let Some(default_val) = &column.default {
            query.push_str(&format!(" DEFAULT {}", default_val));
        }
        self.query.push_str(&query);
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
