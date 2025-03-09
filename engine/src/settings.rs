use async_trait::async_trait;

#[async_trait]
pub trait MigrationSetting {
    async fn apply(&self) -> Result<(), Box<dyn std::error::Error>>;
}
