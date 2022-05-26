use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use data_types::{ChunkId, SequenceNumber};
use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::{TestCatalog, TestNamespace, TestPartition, TestSequencer, TestTable};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use parquet_file::storage::ParquetStorage;
use schema::{selection::Selection, sort::SortKey, Schema};

use crate::{
    cache::CatalogCache, chunk::ParquetChunkAdapter, create_ingester_connection_for_testing,
    IngesterPartition,
};

use super::QuerierTable;

/// Create a [`QuerierTable`] for testing.
pub async fn querier_table(catalog: &Arc<TestCatalog>, table: &Arc<TestTable>) -> QuerierTable {
    let catalog_cache = Arc::new(CatalogCache::new(
        catalog.catalog(),
        catalog.time_provider(),
        catalog.metric_registry(),
        usize::MAX,
    ));
    let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
        catalog_cache,
        ParquetStorage::new(catalog.object_store()),
        catalog.metric_registry(),
        catalog.time_provider(),
    ));

    let mut repos = catalog.catalog.repositories().await;
    let mut catalog_schema = get_schema_by_name(&table.namespace.namespace.name, repos.as_mut())
        .await
        .unwrap();
    let schema = catalog_schema.tables.remove(&table.table.name).unwrap();
    let schema = Arc::new(Schema::try_from(schema).unwrap());

    let namespace_name = Arc::from(table.namespace.namespace.name.as_str());

    QuerierTable::new(
        namespace_name,
        table.table.id,
        table.table.name.clone().into(),
        schema,
        create_ingester_connection_for_testing(),
        chunk_adapter,
    )
}

/// Convert the line protocol in `lp `to a RecordBatch
pub(crate) fn lp_to_record_batch(lp: &str) -> RecordBatch {
    lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
}

/// Helper for creating IngesterPartitions
#[derive(Debug, Clone)]
pub(crate) struct IngesterPartitionBuilder {
    ns: Arc<TestNamespace>,
    table: Arc<TestTable>,
    schema: Arc<Schema>,
    sequencer: Arc<TestSequencer>,
    partition: Arc<TestPartition>,
    ingester_name: Arc<str>,
    ingester_chunk_id: u128,

    partition_sort_key: Arc<Option<SortKey>>,

    /// Data returned from the partition, in line protocol format
    lp: String,
}

impl IngesterPartitionBuilder {
    pub(crate) fn new(
        ns: Arc<TestNamespace>,
        table: Arc<TestTable>,
        schema: Arc<Schema>,
        sequencer: Arc<TestSequencer>,
        partition: Arc<TestPartition>,
    ) -> Self {
        Self {
            ns,
            table,
            schema,
            sequencer,
            partition,
            ingester_name: Arc::from("ingester1"),
            partition_sort_key: Arc::new(None),
            ingester_chunk_id: 1,
            lp: "table foo=1i 1".to_string(),
        }
    }

    /// Create a ingester partition with the specified max parquet sequence number
    pub(crate) fn build_with_max_parquet_sequence_number(
        &self,
        parquet_max_sequence_number: Option<SequenceNumber>,
    ) -> Arc<IngesterPartition> {
        let tombstone_max_sequence_number = None;

        self.build(parquet_max_sequence_number, tombstone_max_sequence_number)
    }

    /// Create an ingester partition with the specified field values
    pub(crate) fn build(
        &self,
        parquet_max_sequence_number: Option<SequenceNumber>,
        tombstone_max_sequence_number: Option<SequenceNumber>,
    ) -> Arc<IngesterPartition> {
        Arc::new(
            IngesterPartition::try_new(
                Arc::clone(&self.ingester_name),
                ChunkId::new_test(self.ingester_chunk_id),
                Arc::from(self.ns.namespace.name.as_str()),
                Arc::from(self.table.table.name.as_str()),
                self.partition.partition.id,
                self.sequencer.sequencer.id,
                Arc::clone(&self.schema),
                parquet_max_sequence_number,
                tombstone_max_sequence_number,
                Arc::clone(&self.partition_sort_key),
                vec![lp_to_record_batch(&self.lp)],
            )
            .unwrap(),
        )
    }
}
