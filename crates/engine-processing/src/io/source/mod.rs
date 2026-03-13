use crate::io::{
    filter::{
        Filter,
        compiler::{FilterCompiler, csv::CsvFilterCompiler, sql::SqlFilterCompiler},
        utils::combine_filters,
    },
    format::DataFormat,
    linked::LinkedSource,
    source::{db_reader::DbSourceReader, reader::SourceReader},
};
use connectors::{
    error::DriverError,
    sql::metadata::table::TableMetadata,
    traits::{introspector::SchemaIntrospector, reader::DataReader},
};
use model::{
    execution::pipeline::Pipeline,
    pagination::{cursor::Cursor, page::FetchResult},
    transform::mapping::TransformationMetadata,
};
use query_builder::{
    dialect::{self, Dialect},
    offsets::OffsetStrategy,
};
use std::{collections::HashMap, sync::Arc};

pub mod db_reader;
pub mod reader;

#[derive(Clone)]
pub struct Source {
    pub name: String,
    pub format: DataFormat,
    pub primary: Arc<dyn SourceReader>,
    pub linked: Option<LinkedSource>,
    pub filter: Option<Filter>,
}

impl Source {
    pub async fn new<D>(
        driver: Arc<D>,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<Self, DriverError>
    where
        D: DataReader + SchemaIntrospector,
    {
        Self::with_cascade(driver, pipeline, mapping, offset_strategy, None).await
    }

    /// Create a source with optional cascade metadata for graph-based migration.
    pub async fn with_cascade<D>(
        driver: Arc<D>,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
        cascade_meta: Option<HashMap<String, TableMetadata>>,
    ) -> Result<Self, DriverError>
    where
        D: DataReader + SchemaIntrospector,
    {
        let name = pipeline.source.table.clone();
        let format = DataFormat::parse(&pipeline.source.connection.driver).ok_or_else(|| {
            DriverError::UnsupportedFormat(pipeline.source.connection.driver.clone())
        })?;

        let linked =
            LinkedSource::new(driver.clone(), &format, &pipeline.source.joins, mapping).await?;
        let filter = Self::create_filter(pipeline, &format)?;

        // Fetch primary table metadata upfront so the reader always knows which
        // columns to select, even for simple (non-cascade) pipelines.
        let primary_meta = driver.table_metadata(&name).await.ok();

        let primary = Self::build_primary_reader(
            &name,
            &format,
            driver,
            &linked,
            &filter,
            offset_strategy,
            cascade_meta,
            primary_meta,
        )?;

        Ok(Source {
            name,
            format,
            primary,
            linked,
            filter,
        })
    }

    pub async fn fetch(
        &self,
        batch_size: usize,
        cursor: Cursor,
    ) -> Result<FetchResult, DriverError> {
        self.primary.fetch(batch_size, cursor).await
    }

    pub fn format(&self) -> DataFormat {
        self.format
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn query_dialect(&self) -> Box<dyn Dialect> {
        match self.format {
            DataFormat::MySql => Box::new(dialect::MySql),
            DataFormat::Postgres => Box::new(dialect::Postgres),
            _ => panic!("Unsupported dialect for source"),
        }
    }

    /// Helper to isolate the complex logic of constructing the primary data reader
    #[allow(clippy::too_many_arguments)]
    fn build_primary_reader<D>(
        name: &str,
        format: &DataFormat,
        driver: Arc<D>,
        linked: &Option<LinkedSource>,
        filter: &Option<Filter>,
        offset_strategy: Arc<dyn OffsetStrategy>,
        cascade_meta: Option<HashMap<String, TableMetadata>>,
        primary_meta_fallback: Option<TableMetadata>,
    ) -> Result<Arc<dyn SourceReader>, DriverError>
    where
        D: DataReader + SchemaIntrospector,
    {
        match format {
            DataFormat::MySql | DataFormat::Postgres => {
                let sql_filter = match filter {
                    Some(Filter::Sql(sf)) => Some(sf.clone()),
                    _ => None,
                };

                let join = match linked {
                    Some(LinkedSource::Table(j)) => Some((**j).clone()),
                    _ => None,
                };

                let mut reader = DbSourceReader::new(
                    driver as Arc<dyn DataReader>,
                    join,
                    sql_filter,
                    offset_strategy,
                );

                if let Some(mut cascade) = cascade_meta {
                    if let Some(primary_meta) = cascade.remove(name) {
                        reader.set_primary_meta(primary_meta);
                    }

                    // The remaining map natively represents all related tables.
                    if !cascade.is_empty() {
                        reader.set_related_meta(cascade);
                    }
                }

                // If cascade didn't provide primary metadata (non-cascade pipeline),
                // use the directly-fetched metadata so the reader knows which columns to select.
                if !reader.has_primary_meta()
                    && let Some(meta) = primary_meta_fallback
                {
                    reader.set_primary_meta(meta);
                }

                Ok(Arc::new(reader))
            }
            _ => Err(DriverError::UnsupportedFormat(format!("{:?}", format))),
        }
    }

    fn create_filter(
        pipeline: &Pipeline,
        format: &DataFormat,
    ) -> Result<Option<Filter>, DriverError> {
        let combined_condition = match combine_filters(&pipeline.source.filters) {
            Some(cond) => cond,
            None => return Ok(None),
        };

        match format {
            DataFormat::MySql | DataFormat::Postgres => Ok(Some(Filter::Sql(
                SqlFilterCompiler::compile(&combined_condition),
            ))),
            DataFormat::Csv => Ok(Some(Filter::Csv(CsvFilterCompiler::compile(
                &combined_condition,
            )))),
        }
    }
}
