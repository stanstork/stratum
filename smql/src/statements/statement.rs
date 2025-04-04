use super::{
    aggregate::Aggregate, connection::Connection, filter::Filter, load::Load, mapping::Map,
    migrate::MigrateBlock,
};

#[derive(Debug)]
pub enum Statement {
    Connections(Connection),
    Migrate(MigrateBlock),
    Filter(Filter),
    Load(Load),
    Map(Map),
    Aggregate(Aggregate),
}
