use super::connection::Connection;

#[derive(Debug)]
pub enum Statement {
    Connections(Connection),
    // Migrate(Migrate),
    // Filter(Filter),
    // Load(Load),
    // Map(Map),
    // Aggregate(Aggregate),
}
