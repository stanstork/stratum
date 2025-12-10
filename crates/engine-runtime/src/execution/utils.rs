use model::execution::pipeline::Pagination;
use model::pagination::{cursor::QualCol, offset_config::OffsetConfig};
use planner::query::offsets::{OffsetStrategy, OffsetStrategyFactory};
use std::str::FromStr;
use std::sync::Arc;

pub fn offset_from_pagination(pagination: &Option<Pagination>) -> Arc<dyn OffsetStrategy> {
    if let Some(pagination) = pagination {
        let mut cursor: Option<QualCol> = None;
        let mut tiebreaker: Option<QualCol> = None;
        let mut timezone: Option<String> = None;

        if !pagination.cursor.is_empty() {
            cursor = Some(QualCol::from_str(&pagination.cursor).unwrap());
        }

        if let Some(tb) = &pagination.tiebreaker {
            tiebreaker = Some(QualCol::from_str(tb).unwrap());
        }

        if let Some(tz) = &pagination.timezone {
            timezone = Some(tz.clone());
        }

        let config = OffsetConfig {
            strategy: Some(pagination.strategy.clone()),
            cursor,
            tiebreaker,
            timezone,
        };

        OffsetStrategyFactory::from_config(&config)
    } else {
        OffsetStrategyFactory::default_strategy()
    }
}
