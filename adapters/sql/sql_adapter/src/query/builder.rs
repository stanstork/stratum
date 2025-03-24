use crate::requests::JoinClause;

#[derive(Debug, Clone)]
pub struct SqlQueryBuilder {
    pub query: String,
    pub params: Vec<String>,
}

pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default: Option<String>,
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
                if let Some(alias) = &col.alias {
                    format!("{}.{} AS {}_{}", col.table, col.column, alias, col.column)
                } else {
                    format!("{}.{}", col.table, col.column)
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
        columns: Vec<String>,
        values: Vec<Vec<String>>,
    ) -> Self {
        let query = format!("INSERT INTO {} ({}) VALUES\n", table, columns.join(", "));
        let mut value_str = String::new();
        for (i, value) in values.iter().enumerate() {
            if i > 0 {
                value_str.push_str(", ");
            }
            value_str.push_str(&format!("({})", value.join(", ")));
        }
        self.query.push_str(&format!("{}{};", query, value_str));
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
                let mut definition = format!("\t{} {}", col.name, col.data_type);
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

        let pk_columns: Vec<String> = if composite_pk {
            columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect()
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

    pub fn add_foreign_keys(mut self, table: &str, foreign_keys: &[ForeignKeyInfo]) -> Self {
        for fk in foreign_keys {
            self.query.push_str(&format!(
                "ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}({});\n",
                table, fk.column, fk.referenced_table, fk.referenced_column
            ));
        }
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
}
