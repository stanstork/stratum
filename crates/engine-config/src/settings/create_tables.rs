use super::{
    MigrationSetting, context::SchemaSettingContext, driver::SchemaDriver,
    phase::MigrationSettingsPhase,
};
use crate::settings::error::SettingsError;
use async_trait::async_trait;
use connectors::drivers::postgres::config::PkCreation;
use engine_core::schema::schema_ops::{SchemaOp, SchemaOps};
use engine_processing::context::PipelineContext;
use tracing::{info, warn};

pub struct CreateMissingTablesSetting<D: SchemaDriver> {
    context: SchemaSettingContext<D>,
}

#[async_trait]
impl<D: SchemaDriver> MigrationSetting for CreateMissingTablesSetting<D> {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingTables
    }

    async fn plan(&mut self, _ctx: &PipelineContext) -> Result<SchemaOps, SettingsError> {
        self.build_schema_ops().await
    }
}

impl<D: SchemaDriver> CreateMissingTablesSetting<D> {
    pub async fn new(ctx: SchemaSettingContext<D>) -> Self {
        Self { context: ctx }
    }

    async fn build_schema_ops(&self) -> Result<SchemaOps, SettingsError> {
        let defer_pk = self.context.settings.pk_creation() == PkCreation::Post
            && !self.context.settings.skip_primary_keys();

        // If the table already exists, bail out. `pk_creation = "post"` only
        // applies to tables we create, so warn and leave an existing PK as-is.
        if self.context.destination_exists().await? {
            if defer_pk {
                warn!(
                    table = %self.context.destination.name,
                    "pk_creation=\"post\" ignored: destination table already exists; leaving its primary key in place"
                );
            }
            info!("destination table already exists, skipping schema creation");
            return Ok(SchemaOps::empty());
        }

        // Resolve source name from the destination
        let dest_name = &self.context.destination.name;
        let src_name = self.context.mapping.entities.reverse_resolve(dest_name);

        let schema_planner = self.context.init_schema_planner().await?;
        let plan = schema_planner.plan_schema(&src_name).await?;

        let mut ops = SchemaOps::empty();

        // Enum queries -> pre (idempotent)
        for (sql, name) in plan.enum_queries() {
            ops.pre.push(SchemaOp {
                sql,
                description: format!("Create enum type '{}'", name),
                idempotent: true,
                skip_if_missing_ref: false,
            });
        }

        // Table queries -> pre. With `pk_creation = "post"` the tables are
        // created without their primary key and it is added back after the load,
        // so per-row index maintenance doesn't slow the bulk COPY.
        let table_queries = if defer_pk {
            plan.table_queries_no_pk().await
        } else {
            plan.table_queries().await
        };
        for (sql, name) in table_queries {
            ops.pre.push(SchemaOp {
                sql,
                description: format!("Create table '{}'", name),
                idempotent: false,
                skip_if_missing_ref: false,
            });
        }

        // Deferred primary keys -> post, *before* the FKs (an FK may reference a
        // PK/unique that must already exist).
        if defer_pk {
            for (sql, name) in plan.pk_queries() {
                ops.post.push(SchemaOp {
                    sql,
                    description: format!("Add primary key on '{}'", name),
                    idempotent: false,
                    skip_if_missing_ref: false,
                });
            }
        }

        // FK queries -> post
        for (sql, name) in plan.fk_queries() {
            ops.post.push(SchemaOp {
                sql,
                description: format!("Add foreign key constraint on '{}'", name),
                idempotent: false,
                skip_if_missing_ref: true,
            });
        }

        info!("planned create-missing-tables");
        Ok(ops)
    }
}
