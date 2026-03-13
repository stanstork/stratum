use crate::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    error::DriverError,
    traits::driver::{Driver, DriverInfo},
};
use futures_util::future::BoxFuture;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

/// Factory function type for creating driver instances
pub type DriverFactory =
    Arc<dyn Fn(&str) -> BoxFuture<'static, Result<Arc<dyn Driver>, DriverError>> + Send + Sync>;

/// Global driver registry
pub struct DriverRegistry {
    drivers: RwLock<HashMap<String, RegisteredDriver>>,
    scheme_map: RwLock<HashMap<String, String>>, // scheme -> driver_id
}

struct RegisteredDriver {
    factory: DriverFactory,
}

impl Default for DriverRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DriverRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            drivers: RwLock::new(HashMap::new()),
            scheme_map: RwLock::new(HashMap::new()),
        }
    }

    /// Get the global registry instance
    pub fn global() -> &'static Self {
        static REGISTRY: OnceLock<DriverRegistry> = OnceLock::new();
        REGISTRY.get_or_init(|| {
            let registry = DriverRegistry::new();
            // Register built-in drivers
            registry.register_builtin();
            registry
        })
    }

    /// Register a driver with its metadata and factory
    pub fn register<F>(&self, info: DriverInfo, factory: F)
    where
        F: Fn(&str) -> BoxFuture<'static, Result<Arc<dyn Driver>, DriverError>>
            + Send
            + Sync
            + 'static,
    {
        // Map schemes to driver id
        {
            let mut scheme_map = self.scheme_map.write().unwrap();
            for scheme in info.schemes {
                scheme_map.insert(scheme.to_string(), info.id.to_string());
            }
        }

        // Register factory
        {
            let mut drivers = self.drivers.write().unwrap();
            drivers.insert(
                info.id.to_string(),
                RegisteredDriver {
                    factory: Arc::new(factory),
                },
            );
        }
    }

    /// Create a driver instance from connection URL
    pub async fn connect(&self, url: &str) -> Result<Arc<dyn Driver>, DriverError> {
        let scheme = extract_scheme(url)?;

        let factory = {
            let scheme_map = self.scheme_map.read().unwrap();
            let driver_id = scheme_map
                .get(&scheme)
                .ok_or_else(|| DriverError::UnsupportedScheme(scheme.clone()))?;

            let drivers = self.drivers.read().unwrap();
            let registered = drivers
                .get(driver_id)
                .ok_or_else(|| DriverError::DriverNotFound(driver_id.clone()))?;

            registered.factory.clone()
        };

        factory(url).await
    }

    fn register_builtin(&self) {
        self.register(MySqlDriver::INFO, |url| {
            let url = url.to_owned();
            Box::pin(async move {
                MySqlDriver::connect(&url)
                    .await
                    .map(|d| Arc::new(d) as Arc<dyn Driver>)
            })
        });

        self.register(PgDriver::INFO, |url| {
            let url = url.to_owned();
            Box::pin(async move {
                PgDriver::connect(&url)
                    .await
                    .map(|d| Arc::new(d) as Arc<dyn Driver>)
            })
        });
    }
}

fn extract_scheme(url: &str) -> Result<String, DriverError> {
    url.split("://")
        .next()
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or_else(|| DriverError::InvalidUrl(url.to_string()))
}
