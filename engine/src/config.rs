use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub mysql: MysqlConfig,
    pub postgres: PostgresConfig,
}

#[derive(Debug, Deserialize)]
pub struct MysqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub db: DbConfig,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, &'static str> {
        let contents = std::fs::read_to_string(path).map_err(|_| "Failed to read config file")?;
        toml::from_str(&contents).map_err(|_| "Failed to parse config file")
    }

    pub fn mysql_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.db.mysql.user,
            self.db.mysql.password,
            self.db.mysql.host,
            self.db.mysql.port,
            self.db.mysql.database
        )
    }

    pub fn postgres_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.db.postgres.user,
            self.db.postgres.password,
            self.db.postgres.host,
            self.db.postgres.port,
            self.db.postgres.database
        )
    }
}
