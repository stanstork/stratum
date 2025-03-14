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

    pub fn select(mut self, columns: &Vec<String>, alias: &str) -> Self {
        let formatted_columns: Vec<String> = columns
            .iter()
            .map(|col| format!("{}.{} AS {}", alias, col, col))
            .collect();

        self.query
            .push_str(&format!("SELECT {}", formatted_columns.join(", ")));
        self
    }

    pub fn from(mut self, table: &str) -> Self {
        self.query.push_str(" FROM ");
        self.query.push_str(table);
        self
    }

    pub fn insert_into(mut self, table: &str, columns: &[(&str, &str)]) -> Self {
        let (cols, values): (Vec<&str>, Vec<&str>) = columns.iter().cloned().unzip();

        self.query.push_str(&format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            cols.join(", "),
            values.join(", ")
        ));
        self
    }

    pub fn create_table(
        mut self,
        table: &str,
        columns: &Vec<ColumnInfo>,
        foreign_keys: &Vec<ForeignKeyInfo>,
    ) -> Self {
        self.query
            .push_str(&format!("\nCREATE TABLE {} (\n", table));

        let column_defs: Vec<String> = columns
            .iter()
            .map(|col| {
                let mut definition = format!("\t{} {}", col.name, col.data_type);
                if col.is_primary_key {
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
            .chain(foreign_key_defs.into_iter())
            .collect::<Vec<_>>()
            .join(",\n");

        self.query.push_str(&all_defs);
        self.query.push_str("\n);\n");

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
