use connectors::{
    sql::{
        metadata::table::TableMetadata,
        request::{FetchRowsRequest, FetchRowsRequestBuilder},
    },
    traits::reader::DataReader,
};
use model::{pagination::cursor::Cursor, records::Record};
use query_builder::offsets::OffsetStrategy;
use std::sync::Arc;

pub struct TableReader {
    driver: Arc<dyn DataReader>,
    table: TableMetadata,
    offset_strategy: Arc<dyn OffsetStrategy>,
}

impl TableReader {
    pub fn new(
        driver: Arc<dyn DataReader>,
        table: TableMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Self {
        Self {
            driver,
            table,
            offset_strategy,
        }
    }

    /// Fetch the next batch of rows starting from `cursor`, with `limit` rows.
    /// Returns `(rows, next_cursor)` where `next_cursor = None` signals the table is exhausted.
    pub async fn next_batch(
        &self,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Cursor>), crate::error::VerifyError> {
        let request = self.build_request(&cursor, limit);
        let rows = self.driver.fetch(request).await?;

        let reached_end = rows.len() < limit;
        let last_row = rows.last().cloned();
        let next_cursor = self.compute_next_cursor(last_row.as_ref(), &cursor, reached_end, limit);

        Ok((rows, next_cursor))
    }

    fn build_request(&self, cursor: &Cursor, limit: usize) -> FetchRowsRequest {
        let table = self.table.name.clone();
        let columns = self.table.select_fields();

        FetchRowsRequestBuilder::new(table.clone())
            .alias(table)
            .columns(columns)
            .limit(limit)
            .cursor(cursor.clone())
            .strategy(self.offset_strategy.clone())
            .build()
    }

    // TODO: this logic is currently duplicated. Refactor to a shared utility.
    fn compute_next_cursor(
        &self,
        last_row: Option<&Record>,
        current_cursor: &Cursor,
        reached_end: bool,
        limit: usize,
    ) -> Option<Cursor> {
        if reached_end {
            return None;
        }

        last_row.map(|row| {
            let next = self.offset_strategy.next_cursor(row);
            // Default strategy tracks offset by row count, not by row content.
            // next_cursor() returns the row-level offset; we advance by limit instead.
            match (current_cursor, &next) {
                (Cursor::None, Cursor::Default { .. }) => Cursor::Default { offset: limit },
                (Cursor::Default { offset }, Cursor::Default { .. }) => Cursor::Default {
                    offset: offset + limit,
                },
                _ => next,
            }
        })
    }
}
