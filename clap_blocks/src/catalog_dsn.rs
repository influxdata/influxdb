//! Catalog-DSN-related configs.
use iox_catalog::sqlite::{SqliteCatalog, SqliteConnectionOptions};
use iox_catalog::{
    create_or_get_default_records,
    interface::Catalog,
    mem::MemCatalog,
    postgres::{PostgresCatalog, PostgresConnectionOptions},
};
use observability_deps::tracing::*;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{ops::DerefMut, sync::Arc, time::Duration};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("A Postgres connection string in --catalog-dsn is required."))]
    ConnectionStringRequired,

    #[snafu(display("A SQLite connection string in --catalog-dsn is required."))]
    ConnectionStringSqliteRequired,

    #[snafu(display("A catalog error occurred: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },
}

fn default_max_connections() -> &'static str {
    let s = PostgresConnectionOptions::DEFAULT_MAX_CONNS.to_string();
    Box::leak(Box::new(s))
}

fn default_connect_timeout() -> &'static str {
    let s =
        humantime::format_duration(PostgresConnectionOptions::DEFAULT_CONNECT_TIMEOUT).to_string();
    Box::leak(Box::new(s))
}

fn default_idle_timeout() -> &'static str {
    let s = humantime::format_duration(PostgresConnectionOptions::DEFAULT_IDLE_TIMEOUT).to_string();
    Box::leak(Box::new(s))
}

fn default_hotswap_poll_interval_timeout() -> &'static str {
    let s = humantime::format_duration(PostgresConnectionOptions::DEFAULT_HOTSWAP_POLL_INTERVAL)
        .to_string();
    Box::leak(Box::new(s))
}

/// CLI config for catalog DSN.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct CatalogDsnConfig {
    /// The type of catalog to use. "memory" is only useful for testing purposes.
    #[clap(
        value_enum,
        long = "catalog",
        env = "INFLUXDB_IOX_CATALOG_TYPE",
        default_value = "postgres",
        action
    )]
    pub(crate) catalog_type_: CatalogType,

    /// Postgres connection string. Required if catalog is set to postgres.
    #[clap(long = "catalog-dsn", env = "INFLUXDB_IOX_CATALOG_DSN", action)]
    pub dsn: Option<String>,

    /// Maximum number of connections allowed to the catalog at any one time.
    #[clap(
        long = "catalog-max-connections",
        env = "INFLUXDB_IOX_CATALOG_MAX_CONNECTIONS",
        default_value = default_max_connections(),
        action,
    )]
    pub max_catalog_connections: u32,

    /// Schema name for PostgreSQL-based catalogs.
    #[clap(
        long = "catalog-postgres-schema-name",
        env = "INFLUXDB_IOX_CATALOG_POSTGRES_SCHEMA_NAME",
        default_value = PostgresConnectionOptions::DEFAULT_SCHEMA_NAME,
        action,
    )]
    pub postgres_schema_name: String,

    /// Set the amount of time to attempt connecting to the database.
    #[clap(
        long = "catalog-connect-timeout",
        env = "INFLUXDB_IOX_CATALOG_CONNECT_TIMEOUT",
        default_value = default_connect_timeout(),
        value_parser = humantime::parse_duration,
    )]
    pub connect_timeout: Duration,

    /// Set a maximum idle duration for individual connections.
    #[clap(
        long = "catalog-idle-timeout",
        env = "INFLUXDB_IOX_CATALOG_IDLE_TIMEOUT",
        default_value = default_idle_timeout(),
        value_parser = humantime::parse_duration,
    )]
    pub idle_timeout: Duration,

    /// If the DSN points to a file (i.e. starts with `dsn-file://`), this sets the interval how often the the file
    /// should be polled for updates.
    ///
    /// If an update is encountered, the underlying connection pool will be hot-swapped.
    #[clap(
        long = "catalog-hotswap-poll-interval",
        env = "INFLUXDB_IOX_CATALOG_HOTSWAP_POLL_INTERVAL",
        default_value = default_hotswap_poll_interval_timeout(),
        value_parser = humantime::parse_duration,
    )]
    pub hotswap_poll_interval: Duration,
}

/// Catalog type.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub enum CatalogType {
    /// PostgreSQL.
    #[default]
    Postgres,

    /// In-memory.
    Memory,

    /// SQLite.
    Sqlite,
}

impl CatalogDsnConfig {
    /// Create a new memory instance for all-in-one mode if a catalog DSN is not specified, setting
    /// the default for arguments that are irrelevant
    pub fn new_memory() -> Self {
        info!("Catalog: In-memory");

        Self {
            catalog_type_: CatalogType::Memory,
            ..Self::default()
        }
    }

    /// Create a new Postgres instance for all-in-one mode if a catalog DSN is specified
    pub fn new_postgres(dsn: String, postgres_schema_name: String) -> Self {
        info!("Catalog: Postgres at `{}`", dsn);

        Self {
            catalog_type_: CatalogType::Postgres,
            dsn: Some(dsn),
            max_catalog_connections: PostgresConnectionOptions::DEFAULT_MAX_CONNS,
            postgres_schema_name,
            connect_timeout: PostgresConnectionOptions::DEFAULT_CONNECT_TIMEOUT,
            idle_timeout: PostgresConnectionOptions::DEFAULT_IDLE_TIMEOUT,
            hotswap_poll_interval: PostgresConnectionOptions::DEFAULT_HOTSWAP_POLL_INTERVAL,
        }
    }

    /// Create a new Postgres instance for all-in-one mode if a catalog DSN is specified
    pub fn new_sqlite(dsn: String) -> Self {
        info!("Catalog: SQLite at `{}`", dsn);

        Self {
            catalog_type_: CatalogType::Sqlite,
            dsn: Some(dsn),
            ..Self::default()
        }
    }

    /// Get config-dependent catalog.
    pub async fn get_catalog(
        &self,
        app_name: &'static str,
        metrics: Arc<metric::Registry>,
    ) -> Result<Arc<dyn Catalog>, Error> {
        let catalog = match self.catalog_type_ {
            CatalogType::Postgres => {
                let options = PostgresConnectionOptions {
                    app_name: app_name.to_string(),
                    schema_name: self.postgres_schema_name.clone(),
                    dsn: self
                        .dsn
                        .as_ref()
                        .context(ConnectionStringRequiredSnafu)?
                        .clone(),
                    max_conns: self.max_catalog_connections,
                    connect_timeout: self.connect_timeout,
                    idle_timeout: self.idle_timeout,
                    hotswap_poll_interval: self.hotswap_poll_interval,
                };
                Arc::new(
                    PostgresCatalog::connect(options, metrics)
                        .await
                        .context(CatalogSnafu)?,
                ) as Arc<dyn Catalog>
            }
            CatalogType::Memory => {
                let mem = MemCatalog::new(metrics);

                let mut txn = mem.start_transaction().await.context(CatalogSnafu)?;
                create_or_get_default_records(1, txn.deref_mut())
                    .await
                    .context(CatalogSnafu)?;
                txn.commit().await.context(CatalogSnafu)?;

                Arc::new(mem) as Arc<dyn Catalog>
            }
            CatalogType::Sqlite => {
                let options = SqliteConnectionOptions {
                    dsn: self
                        .dsn
                        .as_ref()
                        .context(ConnectionStringSqliteRequiredSnafu)?
                        .clone(),
                };
                Arc::new(
                    SqliteCatalog::connect(options, metrics)
                        .await
                        .context(CatalogSnafu)?,
                ) as Arc<dyn Catalog>
            }
        };

        Ok(catalog)
    }
}
