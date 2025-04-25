#[derive(Debug, Clone)]
pub struct Condition {
    pub table: String,
    pub column: String,
    pub comparator: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct SqlFilter {
    pub conditions: Vec<Condition>,
}

impl SqlFilter {
    pub fn new() -> Self {
        SqlFilter {
            conditions: Vec::new(),
        }
    }

    pub fn add_condition(&mut self, condition: Condition) {
        self.conditions.push(condition);
    }

    pub fn to_sql(&self) -> String {
        let mut sql = String::new();
        for (i, condition) in self.conditions.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str(&format!(
                "{}.{} {} {}",
                condition.table, condition.column, condition.comparator, condition.value
            ));
        }
        sql
    }
}
