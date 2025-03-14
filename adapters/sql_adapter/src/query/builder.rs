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

impl SqlQueryBuilder {
    pub fn new() -> Self {
        Self {
            query: String::new(),
            params: Vec::new(),
        }
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

    pub fn create_table(mut self, table: &str, columns: &Vec<ColumnInfo>) -> Self {
        self.query.push_str(&format!("CREATE TABLE {} (", table));

        let column_defs: Vec<String> = columns
            .iter()
            .map(|col| {
                let mut definition = format!("{} {}", col.name, col.data_type);
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

        self.query.push_str(&column_defs.join(", "));
        self.query.push(')');
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
