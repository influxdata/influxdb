use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Partition, PartitionId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;

use super::PartitionsSource;

#[derive(Debug)]
pub struct CatalogPartitionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
    minute_threshold: u64,
    time_provider: Arc<dyn TimeProvider>,
}

impl CatalogPartitionsSource {
    pub fn new(
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        minute_threshold: u64,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            backoff_config,
            catalog,
            minute_threshold,
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
        let time_minutes_ago = self.time_provider.minutes_ago(self.minute_threshold);

        Backoff::new(&self.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .partitions_to_compact(time_minutes_ago.into())
                    .await
            })
            .await
            .expect("retry forever")
    }

    async fn fetch_by_id(&self, partition_id: PartitionId) -> Option<Partition> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("partition_by_id", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .get_by_id(partition_id)
                    .await
            })
            .await
            .expect("retry forever")
    }
}
