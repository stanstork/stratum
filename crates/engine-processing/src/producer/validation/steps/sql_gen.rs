use connectors::sql::base::query::generator::QueryGenerator;
use engine_config::report::{
    finding::Finding,
    sql::{SqlKind, SqlStatement},
};
use engine_core::connectors::source::{DataSource, Source};
use model::pagination::cursor::Cursor;

/// Step responsible for generating SQL statements for the dry run report
pub struct SqlGenerationStep {
    source: Source,
    sample_size: usize,
    cursor: Cursor,
}

impl SqlGenerationStep {
    pub fn new(source: Source, sample_size: usize, cursor: Cursor) -> Self {
        Self {
            source,
            sample_size,
            cursor,
        }
    }

    pub async fn generate_statements(&self) -> (Vec<SqlStatement>, Vec<Finding>) {
        match &self.source.primary {
            DataSource::Database(db_arc) => {
                let db = db_arc.lock().await;
                let dialect = self.source.dialect();
                let generator = QueryGenerator::new(dialect.as_ref());

                let requests = db.build_fetch_rows_requests(self.sample_size, self.cursor.clone());
                let statements = requests
                    .into_iter()
                    .map(|req| {
                        let (sql, params) = generator.select(&req);
                        SqlStatement {
                            dialect: dialect.name(),
                            kind: SqlKind::Data,
                            sql,
                            params,
                        }
                    })
                    .collect();
                (statements, Vec::new())
            }
            _ => (
                Vec::new(),
                Vec::new(), // No SQL statements for non-database sources
            ),
        }
    }
}
