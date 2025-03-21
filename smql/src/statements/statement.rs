use super::{
    aggregate::Aggregate, connection::Connection, filter::Filter, load::Load, mapping::Map,
    migrate::Migrate,
};

#[derive(Debug)]
pub enum Statement {
    Connections(Connection),
    Migrate(Migrate),
    Filter(Filter),
    Load(Load),
    Map(Map),
    Aggregate(Aggregate),
}
