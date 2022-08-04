use super::{query_access::PruneMetrics, QuerierTable, QuerierTableArgs};
use crate::{
    cache::CatalogCache, chunk::ChunkAdapter, create_ingester_connection_for_testing,
    IngesterPartition, QuerierChunkLoadSetting,
};
use arrow::record_batch::RecordBatch;
use data_types::{ChunkId, KafkaPartition, ParquetFileId, SequenceNumber};
use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::{TestCatalog, TestPartition, TestSequencer, TestTable};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use parquet_file::storage::ParquetStorage;
use schema::{selection::Selection, sort::SortKey, Schema};
use sharder::JumpHash;
use std::{collections::HashMap, sync::Arc};

/// Create a [`QuerierTable`] for testing.
pub async fn querier_table(
    catalog: &Arc<TestCatalog>,
    table: &Arc<TestTable>,
    load_settings: HashMap<ParquetFileId, QuerierChunkLoadSetting>,
) -> QuerierTable {
    let catalog_cache = Arc::new(CatalogCache::new_testing(
        catalog.catalog(),
        catalog.time_provider(),
        catalog.metric_registry(),
    ));
    let chunk_adapter = Arc::new(ChunkAdapter::new(
        catalog_cache,
        ParquetStorage::new(catalog.object_store()),
        catalog.metric_registry(),
        load_settings,
    ));

    let mut repos = catalog.catalog.repositories().await;
    let mut catalog_schema = get_schema_by_name(&table.namespace.namespace.name, repos.as_mut())
        .await
        .unwrap();
    let schema = catalog_schema.tables.remove(&table.table.name).unwrap();
    let schema = Arc::new(Schema::try_from(schema).unwrap());

    let namespace_name = Arc::from(table.namespace.namespace.name.as_str());

    QuerierTable::new(QuerierTableArgs {
        sharder: Arc::new(JumpHash::new((0..1).map(KafkaPartition::new).map(Arc::new)).unwrap()),
        namespace_name,
        id: table.table.id,
        table_name: table.table.name.clone().into(),
        schema,
        ingester_connection: Some(create_ingester_connection_for_testing()),
        chunk_adapter,
        exec: catalog.exec(),
        max_query_bytes: usize::MAX,
        prune_metrics: Arc::new(PruneMetrics::new(&catalog.metric_registry())),
    })
}

/// Convert the line protocol in `lp `to a RecordBatch
pub(crate) fn lp_to_record_batch(lp: &str) -> RecordBatch {
    lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
}

/// Helper for creating IngesterPartitions
#[derive(Debug, Clone)]
pub(crate) struct IngesterPartitionBuilder {
    table: Arc<TestTable>,
    schema: Arc<Schema>,
    sequencer: Arc<TestSequencer>,
    partition: Arc<TestPartition>,
    ingester_name: Arc<str>,
    ingester_chunk_id: u128,

    partition_sort_key: Arc<Option<SortKey>>,

    /// Data returned from the partition, in line protocol format
    lp: Vec<String>,
}

impl IngesterPartitionBuilder {
    pub(crate) fn new(
        table: &Arc<TestTable>,
        schema: &Arc<Schema>,
        sequencer: &Arc<TestSequencer>,
        partition: &Arc<TestPartition>,
    ) -> Self {
        Self {
            table: Arc::clone(table),
            schema: Arc::clone(schema),
            sequencer: Arc::clone(sequencer),
            partition: Arc::clone(partition),
            ingester_name: Arc::from("ingester1"),
            partition_sort_key: Arc::new(None),
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
            Arc::from(self.table.table.name.as_str()),
            self.partition.partition.id,
            self.sequencer.sequencer.id,
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            Arc::clone(&self.partition_sort_key),
        )
        .try_add_chunk(
            ChunkId::new_test(self.ingester_chunk_id),
            Arc::clone(&self.schema),
            data,
        )
        .unwrap()
    }
}
