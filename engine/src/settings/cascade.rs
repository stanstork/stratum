use super::{context::SchemaSettingContext, phase::MigrationSettingsPhase, MigrationSetting};
use crate::{
    context::item::ItemContext,
    destination::{data::DataDestination, Destination},
    error::MigrationError,
    filter::Filter,
    source::{data::DataSource, Source},
    state::MigrationState,
};
use async_trait::async_trait;
use common::mapping::EntityMapping;
use smql::statements::connection::DataFormat;
use sql_adapter::{
    filter::SqlFilter,
    join::{
        clause::JoinType,
        utils::{build_join_clauses, combine_join_paths, find_join_path},
    },
    metadata::{provider::MetadataProvider, table::TableMetadata},
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

pub struct CascadeSchemaSetting {
    context: SchemaSettingContext,
}

#[async_trait]
impl MigrationSetting for CascadeSchemaSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CascadeSchema
    }

    fn can_apply(&self, ctx: &ItemContext) -> bool {
        matches!(
            (ctx.source.format, ctx.destination.format),
            (DataFormat::MySql, DataFormat::Postgres)
        )
    }

    async fn apply(&self, _ctx: &mut ItemContext) -> Result<(), MigrationError> {
        // Handle source metadata & cascade‐joins
        self.apply_source().await?;

        // Handle destination metadata
        self.apply_destination().await?;

        // Set the cascade flag to global state
        {
            let mut state = self.context.state.lock().await;
            state.cascade_schema = true;
        }

        info!("Cascade schema setting applied");
        Ok(())
    }
}

impl CascadeSchemaSetting {
    pub fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        state: &Arc<Mutex<MigrationState>>,
    ) -> Self {
        Self {
            context: SchemaSettingContext::new(src, dest, mapping, state),
        }
    }

    /// Build the graph for the source table, apply any SQL‐filter‐driven joins
    /// into the source’s DataSource, and store related_meta & cascade_joins.
    async fn apply_source(&self) -> Result<(), MigrationError> {
        let table_name = self.context.source.name.clone();
        let adapter = self.context.source_adapter().await?;

        // Build just the one‐table graph
        let meta_graph = {
            let tables = &[table_name.clone()];
            MetadataProvider::build_metadata_graph(&*adapter, tables).await?
        };

        let sql_filter = match &self.context.source.filter {
            Some(Filter::Sql(sql_filter)) => Some(sql_filter.clone()),
            _ => None,
        };

        self.apply_cascade(&meta_graph, &sql_filter, &self.context.source.primary)
            .await?;

        Ok(())
    }

    /// Build the graph for the destination table and register it on the dest DataSource.
    async fn apply_destination(&self) -> Result<(), MigrationError> {
        let dest_name = self.context.destination.name.clone();
        let adapter = self.context.destination_adapter().await?;

        let meta_graph = {
            let tables = &[dest_name.clone()];
            MetadataProvider::build_metadata_graph(&*adapter, tables).await?
        };

        let DataDestination::Database(db_mutex) = &self.context.destination.data_dest;
        let mut db = db_mutex.lock().await;
        db.set_related_meta(meta_graph);

        Ok(())
    }

    async fn apply_cascade(
        &self,
        meta_graph: &HashMap<String, TableMetadata>,
        sql_filter: &Option<SqlFilter>,
        primary: &DataSource,
    ) -> Result<(), MigrationError> {
        let root_table = self.context.source.name.clone();

        let db_mutex = match primary {
            DataSource::Database(db) => db,
            _ => panic!("Expected a database data source"),
        };
        let mut db = db_mutex.lock().await;

        if let Some(sql_filter) = sql_filter {
            for meta in meta_graph.values() {
                // skip the root table
                if meta.name.eq_ignore_ascii_case(&root_table) {
                    continue;
                }

                // find all join‐paths to tables mentioned in the filter
                let joins = sql_filter
                    .tables()
                    .iter()
                    .filter_map(|t| find_join_path(meta_graph, &meta.name, t))
                    .collect::<Vec<_>>();

                // combine + build clauses
                let paths = combine_join_paths(joins, &meta.name);
                let clauses = build_join_clauses(&meta.name, &paths, meta_graph, JoinType::Inner);

                // push them into the locked DB
                db.set_cascade_joins(meta.name.clone(), clauses);
            }
        }

        // store the full graph for later use
        db.set_related_meta(meta_graph.clone());

        Ok(())
    }
}
