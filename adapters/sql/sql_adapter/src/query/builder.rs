use super::{column::ColumnDef, fk::ForeignKeyDef, select::SelectField};
use crate::{
    filter::SqlFilter,
    join::clause::{JoinClause, JoinType},
};

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

    pub fn create_table(
        mut self,
        table: &str,
        columns: &[ColumnDef],
        foreign_keys: &[ForeignKeyDef],
        ignore_constraints: bool,
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

                if !ignore_constraints && !composite_pk && col.is_primary_key {
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

        let pk_columns = if !ignore_constraints && composite_pk {
            let pk_columns: Vec<String> = columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect();
            vec![format!("\tPRIMARY KEY ({})", pk_columns.join(", "))]
        } else {
            vec![]
        };

        let foreign_key_defs: Vec<String> = if !ignore_constraints {
            foreign_keys
                .iter()
                .map(|fk| {
                    format!(
                        "\tFOREIGN KEY ({}) REFERENCES {}({})",
                        fk.column, fk.referenced_table, fk.referenced_column
                    )
                })
                .collect()
        } else {
            vec![]
        };

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
            query.push_str(&column.data_type.to_string());
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

    pub fn where_clause(mut self, filter: &Option<SqlFilter>) -> Self {
        if filter.is_none() {
            return self;
        }

        let filter = filter.as_ref().unwrap();
        self.query.push_str(&filter.to_sql());

        self
    }

    pub fn add_param(mut self, param: &str) -> Self {
        self.params.push(param.to_owned());
        self
    }

    pub fn build(self) -> (String, Vec<String>) {
        (self.query, self.params)
    }
}
