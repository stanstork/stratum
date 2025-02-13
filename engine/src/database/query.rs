pub struct QueryBuilder {
    pub query: String,
    pub params: Vec<String>,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            query: String::new(),
            params: Vec::new(),
        }
    }

    pub fn select(&mut self, columns: &Vec<String>) -> &mut Self {
        self.query.push_str("SELECT ");
        self.query.push_str(&columns.join(", "));
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

    pub fn build(&self) -> (String, Vec<String>) {
        (self.query.clone(), self.params.clone())
    }
}
