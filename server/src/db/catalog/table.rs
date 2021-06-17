use super::partition::Partition;
use crate::db::catalog::metrics::TableMetrics;
use data_types::partition_metadata::PartitionSummary;
use hashbrown::HashMap;
use std::sync::Arc;
use tracker::RwLock;

/// A `Table` is a collection of `Partition` each of which is a collection of `Chunk`
#[derive(Debug)]
pub struct Table {
    /// Database name
    db_name: Arc<str>,
    /// Table name
    table_name: Arc<str>,
    /// key is partition key
    partitions: HashMap<Arc<str>, Arc<RwLock<Partition>>>,
    /// Table metrics
    metrics: TableMetrics,
}

impl Table {
    /// Create a new table catalog object.
    ///
    /// This function is not pub because `Table`s should be
    /// created using the interfaces on [`Catalog`](crate::db::catalog::Catalog) and not
    /// instantiated directly.
    pub(super) fn new(db_name: Arc<str>, table_name: Arc<str>, metrics: TableMetrics) -> Self {
        Self {
            db_name,
            table_name,
            partitions: Default::default(),
            metrics,
        }
    }

    pub fn partition(&self, partition_key: impl AsRef<str>) -> Option<&Arc<RwLock<Partition>>> {
        self.partitions.get(partition_key.as_ref())
    }

    pub fn partitions(&self) -> impl Iterator<Item = &Arc<RwLock<Partition>>> + '_ {
        self.partitions.values()
    }

    pub fn get_or_create_partition(
        &mut self,
        partition_key: impl AsRef<str>,
    ) -> &Arc<RwLock<Partition>> {
        let metrics = &self.metrics;
        let db_name = &self.db_name;
        let table_name = &self.table_name;
        let (_, partition) = self
            .partitions
            .raw_entry_mut()
            .from_key(partition_key.as_ref())
            .or_insert_with(|| {
                let partition_key = Arc::from(partition_key.as_ref());
                let partition_metrics = metrics.new_partition_metrics();
                let partition = Partition::new(
                    Arc::clone(&db_name),
                    Arc::clone(&partition_key),
                    Arc::clone(&table_name),
                    partition_metrics,
                );
                let partition = Arc::new(metrics.new_partition_lock(partition));
                (partition_key, partition)
            });
        partition
    }

    pub fn partition_keys(&self) -> impl Iterator<Item = &Arc<str>> + '_ {
        self.partitions.keys()
    }

    pub fn partition_summaries(&self) -> impl Iterator<Item = PartitionSummary> + '_ {
        self.partitions.values().map(|x| x.read().summary())
    }
}
