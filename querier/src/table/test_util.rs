use super::{PruneMetrics, QuerierTable, QuerierTableArgs};
use crate::{
    cache::CatalogCache, create_ingester_connection_for_testing, parquet::ChunkAdapter,
    IngesterPartition,
};
use arrow::record_batch::RecordBatch;
use data_types::{ChunkId, SequenceNumber, ShardIndex};
use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::{TestCatalog, TestPartition, TestShard, TestTable};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use schema::{sort::SortKey, Projection, Schema};
use sharder::JumpHash;
use std::{sync::Arc, time::Duration};
use tokio::runtime::Handle;

/// Create a [`QuerierTable`] for testing.
pub async fn querier_table(catalog: &Arc<TestCatalog>, table: &Arc<TestTable>) -> QuerierTable {
    let catalog_cache = Arc::new(CatalogCache::new_testing(
        catalog.catalog(),
        catalog.time_provider(),
        catalog.metric_registry(),
        catalog.object_store(),
        &Handle::current(),
    ));
    let chunk_adapter = Arc::new(ChunkAdapter::new(catalog_cache, catalog.metric_registry()));

    let mut repos = catalog.catalog.repositories().await;
    let mut catalog_schema = get_schema_by_name(&table.namespace.namespace.name, repos.as_mut())
        .await
        .unwrap();
    let schema = catalog_schema.tables.remove(&table.table.name).unwrap();
    let schema = Arc::new(Schema::try_from(schema).unwrap());

    let namespace_name = Arc::from(table.namespace.namespace.name.as_str());

    let namespace_retention_period = table
        .namespace
        .namespace
        .retention_period_ns
        .map(|retention| Duration::from_nanos(retention as u64));
    QuerierTable::new(QuerierTableArgs {
        sharder: Some(Arc::new(JumpHash::new(
            (0..1).map(ShardIndex::new).map(Arc::new),
        ))),
        namespace_id: table.namespace.namespace.id,
        namespace_name,
        namespace_retention_period,
        table_id: table.table.id,
        table_name: table.table.name.clone().into(),
        schema,
        ingester_connection: Some(create_ingester_connection_for_testing()),
        chunk_adapter,
        exec: catalog.exec(),
        prune_metrics: Arc::new(PruneMetrics::new(&catalog.metric_registry())),
    })
}

/// Convert the line protocol in `lp `to a RecordBatch
pub(crate) fn lp_to_record_batch(lp: &str) -> RecordBatch {
    lp_to_mutable_batch(lp).1.to_arrow(Projection::All).unwrap()
}

/// Helper for creating IngesterPartitions
#[derive(Debug, Clone)]
pub(crate) struct IngesterPartitionBuilder {
    schema: Arc<Schema>,
    shard: Arc<TestShard>,
    partition: Arc<TestPartition>,
    ingester_name: Arc<str>,
    ingester_chunk_id: u128,

    partition_sort_key: Option<Arc<SortKey>>,

    /// Data returned from the partition, in line protocol format
    lp: Vec<String>,
}

impl IngesterPartitionBuilder {
    pub(crate) fn new(
        schema: &Arc<Schema>,
        shard: &Arc<TestShard>,
        partition: &Arc<TestPartition>,
    ) -> Self {
        Self {
            schema: Arc::clone(schema),
            shard: Arc::clone(shard),
            partition: Arc::clone(partition),
            ingester_name: Arc::from("ingester1"),
            partition_sort_key: None,
            ingester_chunk_id: 1,
            lp: Vec::new(),
        }
    }

    /// set the partition chunk id to use when creating partitons
    pub(crate) fn with_ingester_chunk_id(mut self, ingester_chunk_id: u128) -> Self {
        self.ingester_chunk_id = ingester_chunk_id;
        self
    }

    /// Set the line protocol that will be present in this partition
    /// with an interator of `AsRef<str>`s
    pub(crate) fn with_lp(mut self, lp: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.lp = lp.into_iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Create a ingester partition with the specified max parquet sequence number
    pub(crate) fn build_with_max_parquet_sequence_number(
        &self,
        parquet_max_sequence_number: Option<SequenceNumber>,
    ) -> IngesterPartition {
        let tombstone_max_sequence_number = None;

        self.build(parquet_max_sequence_number, tombstone_max_sequence_number)
    }

    /// Create an ingester partition with the specified field values
    pub(crate) fn build(
        &self,
        parquet_max_sequence_number: Option<SequenceNumber>,
        tombstone_max_sequence_number: Option<SequenceNumber>,
    ) -> IngesterPartition {
        let data = self.lp.iter().map(|lp| lp_to_record_batch(lp)).collect();

        IngesterPartition::new(
            Arc::clone(&self.ingester_name),
            None,
            self.partition.partition.id,
            self.shard.shard.id,
            0,
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            self.partition_sort_key.clone(),
        )
        .try_add_chunk(
            ChunkId::new_test(self.ingester_chunk_id),
            Arc::clone(&self.schema),
            data,
        )
        .unwrap()
    }
}
