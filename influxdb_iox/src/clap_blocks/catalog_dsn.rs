use std::sync::Arc;

use iox_catalog::{
    interface::{Catalog, Error},
    postgres::PostgresCatalog,
};

/// CLI config for catalog DSN.
#[derive(Debug, Clone, clap::Parser)]
pub struct CatalogDsnConfig {
    /// Postgres connection string
    #[clap(long = "--catalog-dsn", env = "INFLUXDB_IOX_CATALOG_DSN")]
    pub dsn: String,
}

impl CatalogDsnConfig {
    pub async fn get_catalog(&self, app_name: &'static str) -> Result<Arc<dyn Catalog>, Error> {
        let catalog = Arc::new(
            PostgresCatalog::connect(app_name, iox_catalog::postgres::SCHEMA_NAME, &self.dsn)
                .await?,
        );

        Ok(catalog)
    }
}
