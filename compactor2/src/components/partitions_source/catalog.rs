use std::{fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;

use super::PartitionsSource;

#[derive(Debug)]
pub struct CatalogPartitionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
    threshold: Duration,
    time_provider: Arc<dyn TimeProvider>,
}

impl CatalogPartitionsSource {
    pub fn new(
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        threshold: Duration,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            backoff_config,
            catalog,
            threshold,
            time_provider,
        }
    }
}

impl Display for CatalogPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl PartitionsSource for CatalogPartitionsSource {
    async fn fetch(&self) -> Vec<PartitionId> {
        let cutoff = self.time_provider.now() - self.threshold;

        Backoff::new(&self.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .partitions_to_compact(cutoff.into())
                    .await
            })
            .await
            .expect("retry forever")
    }
}
