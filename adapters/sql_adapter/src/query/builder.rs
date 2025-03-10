pub struct SqlQueryBuilder {
    pub query: String,
    pub params: Vec<String>,
}

impl SqlQueryBuilder {
    pub fn new() -> Self {
        Self {
            query: String::new(),
            params: Vec::new(),
        }
    }

    pub fn select(&mut self, columns: &Vec<String>, alias: &str) -> &mut Self {
        self.query.push_str("SELECT ");

        let formatted_columns: Vec<String> = columns
            .iter()
            .map(|col| format!("{}.{} AS {}", alias, col, col))
            .collect();

        self.query.push_str(&formatted_columns.join(", "));
        self
    }

    pub fn from(&mut self, table: String) -> &mut Self {
        self.query.push_str(" FROM ");
        self.query.push_str(&table);
        self
    }

    pub fn where_clause(&mut self, condition: &str) -> &mut Self {
        self.query.push_str(" WHERE ");
        self.query.push_str(condition);
        self
    }

    pub fn order_by(&mut self, column: &str, direction: &str) -> &mut Self {
        self.query.push_str(" ORDER BY ");
        self.query.push_str(column);
        self.query.push_str(" ");
        self.query.push_str(direction);
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.query.push_str(" LIMIT ");
        self.query.push_str(&limit.to_string());
        self
    }

    pub fn offset(&mut self, offset: usize) -> &mut Self {
        self.query.push_str(" OFFSET ");
        self.query.push_str(&offset.to_string());
        self
    }

    pub fn insert_into(&mut self, table: &str, columns: &Vec<(String, String)>) -> &mut Self {
        self.query.push_str("INSERT INTO ");
        self.query.push_str(table);
        self.query.push_str(" (");

        let column_names = columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        self.query.push_str(&column_names.join(", "));
        self.query.push_str(") VALUES (");

        let mut params = Vec::new();
        for (_, value) in columns.iter() {
            params.push(value.clone());
        }

        self.query.push_str(&params.join(", "));
        self.query.push_str(")");
        self
    }

    pub fn build(&self) -> (String, Vec<String>) {
        (self.query.clone(), self.params.clone())
    }
}
