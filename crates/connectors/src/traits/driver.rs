use crate::sql::metadata::capabilities::Capabilities;

/// Static metadata for driver registration
#[derive(Debug, Clone)]
pub struct DriverInfo {
    /// Unique identifier (e.g., "mysql", "postgres", "csv")
    pub id: &'static str,
    /// Human-readable name
    pub name: &'static str,
    /// Supported URI schemes (e.g., ["mysql", "mariadb"])
    pub schemes: &'static [&'static str],
}

/// Core driver trait (object-safe)
pub trait Driver: Send + Sync + 'static {
    /// Get driver info
    fn info(&self) -> &DriverInfo;

    /// Driver version
    fn version(&self) -> &str;

    /// Get driver capabilities
    fn capabilities(&self) -> &Capabilities;
}
