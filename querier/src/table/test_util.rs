use super::{PruneMetrics, QuerierTable, QuerierTableArgs};
use crate::{
    cache::CatalogCache, create_ingester_connection_for_testing, parquet::ChunkAdapter,
    IngesterPartition,
};
use arrow::record_batch::RecordBatch;
use data_types::ChunkId;
use iox_catalog::interface::{get_schema_by_name, SoftDeletedRows};
use iox_query::chunk_statistics::ColumnRanges;
use iox_tests::{TestCatalog, TestPartition, TestTable};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use schema::{Projection, Schema};
use std::{sync::Arc, time::Duration};
use tokio::runtime::Handle;
use uuid::Uuid;

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
    let mut catalog_schema = get_schema_by_name(
        &table.namespace.namespace.name,
        repos.as_mut(),
        SoftDeletedRows::ExcludeDeleted,
    )
    .await
    .unwrap();
    let table_info = catalog_schema.tables.remove(&table.table.name).unwrap();
    let schema = Schema::try_from(table_info.columns).unwrap();

    let namespace_name = Arc::from(table.namespace.namespace.name.as_str());

    let namespace_retention_period = table
        .namespace
        .namespace
        .retention_period_ns
        .map(|retention| Duration::from_nanos(retention as u64));
    QuerierTable::new(QuerierTableArgs {
        namespace_id: table.namespace.namespace.id,
        namespace_name,
        namespace_retention_period,
        table_id: table.table.id,
        table_name: table.table.name.clone().into(),
        schema,
        ingester_connection: Some(create_ingester_connection_for_testing()),
        chunk_adapter,
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
    schema: Schema,
    partition: Arc<TestPartition>,
    ingester_chunk_id: u128,

    partition_column_ranges: ColumnRanges,

    /// Data returned from the partition, in line protocol format
    lp: Vec<String>,
}

impl IngesterPartitionBuilder {
    pub(crate) fn new(schema: Schema, partition: &Arc<TestPartition>) -> Self {
        Self {
            schema,
            partition: Arc::clone(partition),
            partition_column_ranges: Default::default(),
            ingester_chunk_id: 1,
            lp: Vec::new(),
        }
    }

    /// Set the line protocol that will be present in this partition
    /// with an interator of `AsRef<str>`s
    pub(crate) fn with_lp(mut self, lp: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.lp = lp.into_iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Set column ranges.
    pub(crate) fn with_colum_ranges(mut self, column_ranges: ColumnRanges) -> Self {
        self.partition_column_ranges = column_ranges;
        self
    }

    /// Create an ingester partition with the specified field values
    pub(crate) fn build(&self) -> IngesterPartition {
        let data = self.lp.iter().map(|lp| lp_to_record_batch(lp)).collect();

        let mut part = IngesterPartition::new(
            Uuid::new_v4(),
            self.partition.partition.transition_partition_id(),
            0,
        )
        .try_add_chunk(
            ChunkId::new_test(self.ingester_chunk_id),
            self.schema.clone(),
            data,
        )
        .unwrap();

        part.set_partition_column_ranges(&self.partition_column_ranges);

        part
    }
}
