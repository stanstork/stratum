use crate::error::MigrationError;
use engine_core::context::item::ItemContext;

pub async fn load(ctx: &mut ItemContext) -> Result<(), MigrationError> {
    ctx.set_src_meta().await?;
    ctx.set_dest_meta().await?;
    Ok(())
}
