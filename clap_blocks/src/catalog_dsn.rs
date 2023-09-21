//! Catalog-DSN-related configs.
use iox_catalog::sqlite::{SqliteCatalog, SqliteConnectionOptions};
use iox_catalog::{
    interface::Catalog,
    mem::MemCatalog,
    postgres::{PostgresCatalog, PostgresConnectionOptions},
};
use observability_deps::tracing::*;
use snafu::{ResultExt, Snafu};
use std::{sync::Arc, time::Duration};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Unknown Catalog DSN {dsn}. Expected a string like 'postgresql://postgres@localhost:5432/postgres' or 'sqlite:///tmp/catalog.sqlite'"))]
    UnknownCatalogDsn { dsn: String },

    #[snafu(display("Catalog DSN not specified. Expected a string like 'postgresql://postgres@localhost:5432/postgres' or 'sqlite:///tmp/catalog.sqlite'"))]
    DsnNotSpecified {},

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
    /// Catalog connection string.
    ///
    /// The dsn determines the type of catalog used.
    ///
    /// PostgreSQL: `postgresql://postgres@localhost:5432/postgres`
    ///
    /// Sqlite (a local filename /tmp/foo.sqlite): `sqlite:///tmp/foo.sqlite`
    ///
    /// Memory (ephemeral, only useful for testing): `memory`
    ///
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

impl CatalogDsnConfig {
    /// Get config-dependent catalog.
    pub async fn get_catalog(
        &self,
        app_name: &'static str,
        metrics: Arc<metric::Registry>,
    ) -> Result<Arc<dyn Catalog>, Error> {
        let Some(dsn) = self.dsn.as_ref() else {
            return Err(Error::DsnNotSpecified {});
        };

        if dsn.starts_with("postgres") || dsn.starts_with("dsn-file://") {
            // do not log entire postgres dsn as it may contain credentials
            info!(postgres_schema_name=%self.postgres_schema_name, "Catalog: Postgres");
            let options = PostgresConnectionOptions {
                app_name: app_name.to_string(),
                schema_name: self.postgres_schema_name.clone(),
                dsn: dsn.clone(),
                max_conns: self.max_catalog_connections,
                connect_timeout: self.connect_timeout,
                idle_timeout: self.idle_timeout,
                hotswap_poll_interval: self.hotswap_poll_interval,
            };
            Ok(Arc::new(
                PostgresCatalog::connect(options, metrics)
                    .await
                    .context(CatalogSnafu)?,
            ))
        } else if dsn == "memory" {
            info!("Catalog: In-memory");
            let mem = MemCatalog::new(metrics);
            Ok(Arc::new(mem))
        } else if let Some(file_path) = dsn.strip_prefix("sqlite://") {
            info!(file_path, "Catalog: Sqlite");
            let options = SqliteConnectionOptions {
                file_path: file_path.to_string(),
            };
            Ok(Arc::new(
                SqliteCatalog::connect(options, metrics)
                    .await
                    .context(CatalogSnafu)?,
            ))
        } else {
            Err(Error::UnknownCatalogDsn {
                dsn: dsn.to_string(),
            })
        }
    }
}
