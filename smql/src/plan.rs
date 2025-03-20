use crate::statements::{
    aggregate::Aggregation, connection::Connection, filter::Filter, load::Load, mapping::Mapping,
    migrate::Migrate, statement::Statement,
};

#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub connections: Connection,
    pub migration: Migrate,
    pub filter: Option<Filter>,
    pub mapping: Vec<Mapping>,
    pub aggregations: Vec<Aggregation>,
    pub load: Option<Load>,
}

impl MigrationPlan {
    pub fn from_statements(statements: Vec<Statement>) -> Self {
        let mut connections = None;
        let mut migration = None;
        let mut filter = None;
        let mut mapping = vec![];
        let mut aggregations = vec![];
        let mut load = None;

        for statement in statements {
            match statement {
                Statement::Connections(c) => connections = Some(c),
                Statement::Migrate(m) => migration = Some(m),
                Statement::Filter(f) => filter = Some(f),
                Statement::Map(m) => mapping.extend(m.mappings),
                Statement::Aggregate(a) => aggregations.extend(a.aggregations),
                Statement::Load(l) => load = Some(l),
            }
        }

        MigrationPlan {
            connections: connections.expect("Connections statement is required"),
            migration: migration.expect("Migrate statement is required"),
            filter,
            mapping,
            aggregations,
            load,
        }
    }
}
