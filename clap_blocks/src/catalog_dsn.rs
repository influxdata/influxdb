use iox_catalog::{
    create_or_get_default_records, interface::Catalog, mem::MemCatalog, postgres::PostgresCatalog,
};
use observability_deps::tracing::*;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{ops::DerefMut, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A Postgres connection string in --catalog-dsn is required."))]
    ConnectionStringRequired,

    #[snafu(display("A catalog error occurred: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },
}

/// CLI config for catalog DSN.
#[derive(Debug, Clone, clap::Parser)]
pub struct CatalogDsnConfig {
    /// The type of catalog to use. "memory" is only useful for testing purposes.
    #[clap(
        arg_enum,
        long = "--catalog",
        env = "INFLUXDB_IOX_CATALOG_TYPE",
        default_value = "postgres"
    )]
    pub(crate) catalog_type_: CatalogType,

    /// Postgres connection string. Required if catalog is set to postgres.
    #[clap(long = "--catalog-dsn", env = "INFLUXDB_IOX_CATALOG_DSN")]
    pub dsn: Option<String>,

    /// Maximum number of connections allowed to the catalog at any one time.
    #[clap(
        long = "--catalog-max-connections",
        env = "INFLUXDB_IOX_CATALOG_MAX_CONNECTIONS",
        default_value = "10"
    )]
    pub max_catalog_connections: u32,

    /// Schema name for PostgreSQL-based catalogs.
    #[clap(
        long = "--catalog-postgres-schema-name",
        env = "INFLUXDB_IOX_CATALOG_POSTGRES_SCHEMA_NAME",
        default_value = iox_catalog::postgres::SCHEMA_NAME,
    )]
    pub postgres_schema_name: String,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ArgEnum)]
pub enum CatalogType {
    Postgres,
    Memory,
}

impl CatalogDsnConfig {
    /// Create a new memory instance for all-in-one mode if a catalog DSN is not specified, setting
    /// the default for arguments that are irrelevant
    pub fn new_memory() -> Self {
        info!("Catalog: In-memory");

        Self {
            catalog_type_: CatalogType::Memory,
            dsn: None,
            max_catalog_connections: 10,
            postgres_schema_name: iox_catalog::postgres::SCHEMA_NAME.to_string(),
        }
    }

    /// Create a new Postgres instance for all-in-one mode if a catalog DSN is specified
    pub fn new_postgres(
        dsn: String,
        max_catalog_connections: u32,
        postgres_schema_name: String,
    ) -> Self {
        info!("Catalog: Postgres at `{}`", dsn);

        Self {
            catalog_type_: CatalogType::Postgres,
            dsn: Some(dsn),
            max_catalog_connections,
            postgres_schema_name,
        }
    }

    pub async fn get_catalog(
        &self,
        app_name: &'static str,
        metrics: Arc<metric::Registry>,
    ) -> Result<Arc<dyn Catalog>, Error> {
        let catalog = match self.catalog_type_ {
            CatalogType::Postgres => Arc::new(
                PostgresCatalog::connect(
                    app_name,
                    &self.postgres_schema_name,
                    self.dsn.as_ref().context(ConnectionStringRequiredSnafu)?,
                    self.max_catalog_connections,
                    metrics,
                )
                .await
                .context(CatalogSnafu)?,
            ) as Arc<dyn Catalog>,
            CatalogType::Memory => {
                let mem = MemCatalog::new(metrics);

                let mut txn = mem.start_transaction().await.context(CatalogSnafu)?;
                create_or_get_default_records(2, txn.deref_mut())
                    .await
                    .context(CatalogSnafu)?;
                txn.commit().await.context(CatalogSnafu)?;

                Arc::new(mem) as Arc<dyn Catalog>
            }
        };

        Ok(catalog)
    }
}
