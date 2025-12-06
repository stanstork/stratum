use super::{MigrationSetting, context::SchemaSettingContext, phase::MigrationSettingsPhase};
use crate::{
    report::dry_run::DryRunReport,
    settings::{error::SettingsError, validated::ValidatedSettings},
};
use async_trait::async_trait;
use connectors::sql::base::{
    filter::SqlFilter,
    join::{
        clause::JoinType,
        utils::{build_join_clauses, combine_join_paths, find_join_path},
    },
    metadata::{provider::MetadataProvider, table::TableMetadata},
};
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        filter::Filter,
        source::{DataSource, Source},
    },
    context::item::ItemContext,
};
use futures::lock::Mutex;
use model::transform::mapping::EntityMapping;
use smql_syntax::ast_v2::connection::DataFormat;
use std::{collections::HashMap, slice, sync::Arc};
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

    async fn apply(&mut self, _ctx: &mut ItemContext) -> Result<(), SettingsError> {
        // Handle source metadata & cascade‐joins
        self.apply_source().await?;

        // Handle destination metadata
        self.apply_destination().await?;

        info!("Cascade schema setting applied");
        Ok(())
    }
}

impl CascadeSchemaSetting {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        settings: &ValidatedSettings,
        dry_run_report: &Arc<Mutex<DryRunReport>>,
    ) -> Self {
        Self {
            context: SchemaSettingContext::new(src, dest, mapping, settings, dry_run_report).await,
        }
    }

    /// Build the graph for the source table, apply any SQL‐filter‐driven joins
    /// into the source's DataSource, and store related_meta & cascade_joins.
    async fn apply_source(&self) -> Result<(), SettingsError> {
        let table_name = self.context.source.name.clone();
        let adapter = self.context.source_adapter().await?;

        // Build just the one‐table graph
        let meta_graph = {
            let tables = slice::from_ref(&table_name);
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
    async fn apply_destination(&self) -> Result<(), SettingsError> {
        let dest_name = self.context.destination.name.clone();
        let adapter = self.context.destination_adapter().await?;

        let meta_graph = {
            let tables = slice::from_ref(&dest_name);
            MetadataProvider::build_metadata_graph(&*adapter, tables).await?
        };

        let DataDestination::Database(db_mutex) = &self.context.destination.data_dest;
        let mut db = db_mutex.data.lock().await;
        db.set_related_meta(meta_graph);

        Ok(())
    }

    async fn apply_cascade(
        &self,
        meta_graph: &HashMap<String, TableMetadata>,
        sql_filter: &Option<SqlFilter>,
        primary: &DataSource,
    ) -> Result<(), SettingsError> {
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
