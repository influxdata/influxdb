use iox_catalog::{
    create_or_get_default_records, interface::Catalog, mem::MemCatalog, postgres::PostgresCatalog,
};
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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ArgEnum)]
pub enum CatalogType {
    Postgres,
    Memory,
}

impl CatalogDsnConfig {
    pub async fn get_catalog(&self, app_name: &'static str) -> Result<Arc<dyn Catalog>, Error> {
        let catalog = match self.catalog_type_ {
            CatalogType::Postgres => Arc::new(
                PostgresCatalog::connect(
                    app_name,
                    iox_catalog::postgres::SCHEMA_NAME,
                    self.dsn.as_ref().context(ConnectionStringRequiredSnafu)?,
                )
                .await
                .context(CatalogSnafu)?,
            ) as Arc<dyn Catalog>,
            CatalogType::Memory => {
                let mem = MemCatalog::new();

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
