use self::query_access::QuerierTableChunkPruner;
use self::state_reconciler::Reconciler;
use crate::{
    chunk::ChunkAdapter,
    ingester::{self, IngesterPartition},
    IngesterConnection,
};
use data_types::{KafkaPartition, PartitionId, TableId};
use futures::join;
use iox_query::{provider::ChunkPruner, QueryChunk};
use observability_deps::tracing::debug;
use predicate::Predicate;
use schema::Schema;
use snafu::{ResultExt, Snafu};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

mod query_access;
mod state_reconciler;

#[cfg(test)]
mod test_util;

#[derive(Debug, Snafu)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    #[snafu(display("Error getting partitions from ingester: {}", source))]
    GettingIngesterPartitions { source: ingester::Error },

    #[snafu(display(
        "Ingester '{}' and '{}' both provide data for partition {}",
        ingester1,
        ingester2,
        partition
    ))]
    IngestersOverlap {
        ingester1: Arc<str>,
        ingester2: Arc<str>,
        partition: PartitionId,
    },

    #[snafu(display("Cannot combine ingester data with catalog/cache: {}", source))]
    StateFusion {
        source: state_reconciler::ReconcileError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Table representation for the querier.
#[derive(Debug)]
pub struct QuerierTable {
    /// Sequencers responsible for the table's data that need to be queried for unpersisted data
    sequencer_ids: Vec<KafkaPartition>,

    /// Namespace the table is in
    namespace_name: Arc<str>,

    /// Table name.
    table_name: Arc<str>,

    /// Table ID.
    id: TableId,

    /// Table schema.
    schema: Arc<Schema>,

    /// Connection to ingester
    ingester_connection: Option<Arc<dyn IngesterConnection>>,

    /// Interface to create chunks for this table.
    chunk_adapter: Arc<ChunkAdapter>,

    /// Handle reconciling ingester and catalog data
    reconciler: Reconciler,
}

impl QuerierTable {
    /// Create new table.
    pub fn new(
        sequencer_ids: Vec<KafkaPartition>,
        namespace_name: Arc<str>,
        id: TableId,
        table_name: Arc<str>,
        schema: Arc<Schema>,
        ingester_connection: Option<Arc<dyn IngesterConnection>>,
        chunk_adapter: Arc<ChunkAdapter>,
    ) -> Self {
        let reconciler = Reconciler::new(
            Arc::clone(&table_name),
            Arc::clone(&namespace_name),
            Arc::clone(&chunk_adapter),
        );

        Self {
            sequencer_ids,
            namespace_name,
            table_name,
            id,
            schema,
            ingester_connection,
            chunk_adapter,
            reconciler,
        }
    }

    /// Table name.
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }

    /// Table ID.
    #[allow(dead_code)]
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Schema.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Query all chunks within this table.
    ///
    /// This currently contains all parquet files linked to their unprocessed tombstones.
    pub async fn chunks(&self, predicate: &Predicate) -> Result<Vec<Arc<dyn QueryChunk>>> {
        debug!(
            ?predicate,
            namespace=%self.namespace_name,
            table_name=%self.table_name(),
            "Fetching all chunks"
        );

        let catalog_cache = self.chunk_adapter.catalog_cache();

        // ask ingesters for data, also optimistically fetching catalog
        // contents at the same time to pre-warm cache
        let (partitions, _parquet_files, _tombstones) = join!(
            self.ingester_partitions(predicate),
            catalog_cache.parquet_file().get(self.id()),
            catalog_cache.tombstone().get(self.id()),
        );

        // handle errors / cache refresh
        let partitions = partitions?;

        // figure out if the ingester has created new parquet files or
        // tombstones the querier doens't yet know about
        self.validate_caches(&partitions);

        debug!(
            namespace=%self.namespace_name,
            table_name=%self.table_name(),
            num_ingester_partitions=%partitions.len(),
            "Ingester partitions fetched"
        );

        // Now fetch the actual contents of the catalog we need
        let (parquet_files, tombstones) = join!(
            catalog_cache.parquet_file().get(self.id()),
            catalog_cache.tombstone().get(self.id())
        );

        self.reconciler
            .reconcile(partitions, tombstones.to_vec(), parquet_files)
            .await
            .context(StateFusionSnafu)
    }

    /// Get a chunk pruner that can be used to prune chunks retrieved via [`chunks`](Self::chunks)
    pub fn chunk_pruner(&self) -> Arc<dyn ChunkPruner> {
        Arc::new(QuerierTableChunkPruner {})
    }

    /// Get partitions from ingesters.
    async fn ingester_partitions(&self, predicate: &Predicate) -> Result<Vec<IngesterPartition>> {
        if let Some(ingester_connection) = &self.ingester_connection {
            // For now, ask for *all* columns in the table from the ingester (need
            // at least all pk (time, tag) columns for
            // deduplication.
            //
            // As a future optimization, might be able to fetch only
            // fields that are needed in query
            let columns: Vec<String> = self
                .schema
                .iter()
                .map(|(_, f)| f.name().to_string())
                .collect();

            // get any chunks from the ingester
            let partitions_result = ingester_connection
                .partitions(
                    &self.sequencer_ids,
                    Arc::clone(&self.namespace_name),
                    Arc::clone(&self.table_name),
                    columns,
                    predicate,
                    Arc::clone(&self.schema),
                )
                .await
                .context(GettingIngesterPartitionsSnafu);

            let partitions = partitions_result?;

            // check that partitions from ingesters don't overlap
            let mut seen = HashMap::with_capacity(partitions.len());
            for partition in &partitions {
                match seen.entry(partition.partition_id()) {
                    Entry::Occupied(o) => {
                        return Err(Error::IngestersOverlap {
                            ingester1: Arc::clone(o.get()),
                            ingester2: Arc::clone(partition.ingester()),
                            partition: partition.partition_id(),
                        })
                    }
                    Entry::Vacant(v) => {
                        v.insert(Arc::clone(partition.ingester()));
                    }
                }
            }

            Ok(partitions)
        } else {
            // No ingesters are configured
            Ok(vec![])
        }
    }

    /// Handles invalidating parquet and tombstone caches if the
    /// responses from the ingesters refer to newer parquet data or
    /// tombstone data than is in the cache.
    fn validate_caches(&self, partitions: &[IngesterPartition]) {
        // figure out if the ingester has created new parquet files or
        // tombstones the querier doens't yet know about
        let catalog_cache = self.chunk_adapter.catalog_cache();

        let max_parquet_sequence_number = partitions
            .iter()
            .flat_map(|p| p.parquet_max_sequence_number())
            .max();

        let parquet_cache_outdated = catalog_cache
            .parquet_file()
            .expire_on_newly_persisted_files(self.id, max_parquet_sequence_number);

        let max_tombstone_sequence_number = partitions
            .iter()
            .flat_map(|p| p.tombstone_max_sequence_number())
            .max();

        let tombstone_cache_outdated = catalog_cache
            .tombstone()
            .expire_on_newly_persisted_files(self.id, max_tombstone_sequence_number);

        debug!(
            namespace=%self.namespace_name,
            table_name=%self.table_name(),
            parquet_cache_outdated,
            tombstone_cache_outdated,
            ?max_parquet_sequence_number,
            ?max_tombstone_sequence_number,
            "Ingester partitions fetched"
        );
    }

    /// clear the parquet file cache
    #[cfg(test)]
    fn clear_parquet_cache(&self) {
        self.chunk_adapter
            .catalog_cache()
            .parquet_file()
            .expire(self.id)
    }

    /// clear the tombstone cache
    #[cfg(test)]
    fn clear_tombstone_cache(&self) {
        self.chunk_adapter
            .catalog_cache()
            .tombstone()
            .expire(self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ingester::{test_util::MockIngesterConnection, IngesterPartition},
        table::test_util::{querier_table, IngesterPartitionBuilder},
    };
    use assert_matches::assert_matches;
    use data_types::{ChunkId, ColumnType, SequenceNumber};
    use iox_tests::util::{now, TestCatalog, TestTable};
    use predicate::Predicate;
    use schema::{builder::SchemaBuilder, InfluxFieldType};
    use std::sync::Arc;
    use test_helpers::maybe_start_logging;

    #[tokio::test]
    async fn test_parquet_chunks() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;

        let table1 = ns.create_table("table1").await;
        let table2 = ns.create_table("table2").await;

        let sequencer1 = ns.create_sequencer(1).await;
        let sequencer2 = ns.create_sequencer(2).await;

        let partition11 = table1
            .with_sequencer(&sequencer1)
            .create_partition("k")
            .await;
        let partition12 = table1
            .with_sequencer(&sequencer2)
            .create_partition("k")
            .await;
        let partition21 = table2
            .with_sequencer(&sequencer1)
            .create_partition("k")
            .await;

        table1.create_column("time", ColumnType::Time).await;
        table1.create_column("foo", ColumnType::F64).await;
        table2.create_column("time", ColumnType::Time).await;
        table2.create_column("foo", ColumnType::F64).await;

        let querier_table = TestQuerierTable::new(&catalog, &table1).await;

        // no parquet files yet
        assert!(querier_table.chunks().await.unwrap().is_empty());

        let file111 = partition11
            .create_parquet_file_with_min_max("table1 foo=1 11", 1, 2, now_nanos(), now_nanos())
            .await;
        let file112 = partition11
            .create_parquet_file_with_min_max("table1 foo=2 22", 3, 4, now_nanos(), now_nanos())
            .await;
        let file113 = partition11
            .create_parquet_file_with_min_max("table1 foo=3 33", 5, 6, now_nanos(), now_nanos())
            .await;
        let file114 = partition11
            .create_parquet_file_with_min_max("table1 foo=4 44", 7, 8, now_nanos(), now_nanos())
            .await;
        let file115 = partition11
            .create_parquet_file_with_min_max("table1 foo=5 55", 9, 10, now_nanos(), now_nanos())
            .await;
        let file121 = partition12
            .create_parquet_file_with_min_max("table1 foo=5 55", 1, 2, now_nanos(), now_nanos())
            .await;
        let _file211 = partition21
            .create_parquet_file_with_min_max("table2 foo=6 66", 1, 2, now_nanos(), now_nanos())
            .await;

        file111.flag_for_delete().await;

        let tombstone1 = table1
            .with_sequencer(&sequencer1)
            .create_tombstone(7, 1, 100, "foo=1")
            .await;
        tombstone1.mark_processed(&file112).await;
        let tombstone2 = table1
            .with_sequencer(&sequencer1)
            .create_tombstone(8, 1, 100, "foo=1")
            .await;
        tombstone2.mark_processed(&file112).await;

        // As we have now made new parquet files, force a cache refresh
        querier_table.inner().clear_parquet_cache();
        querier_table.inner().clear_tombstone_cache();

        // now we have some files
        // this contains all files except for:
        // - file111: marked for delete
        // - file221: wrong table
        let mut chunks = querier_table.chunks().await.unwrap();
        chunks.sort_by_key(|c| c.id());
        assert_eq!(chunks.len(), 5);

        // check IDs
        assert_eq!(
            chunks[0].id(),
            ChunkId::new_test(file112.parquet_file.id.get() as u128),
        );
        assert_eq!(
            chunks[1].id(),
            ChunkId::new_test(file113.parquet_file.id.get() as u128),
        );
        assert_eq!(
            chunks[2].id(),
            ChunkId::new_test(file114.parquet_file.id.get() as u128),
        );
        assert_eq!(
            chunks[3].id(),
            ChunkId::new_test(file115.parquet_file.id.get() as u128),
        );
        assert_eq!(
            chunks[4].id(),
            ChunkId::new_test(file121.parquet_file.id.get() as u128),
        );

        // check delete predicates
        // file112: marked as processed
        assert_eq!(chunks[0].delete_predicates().len(), 0);
        // file113: has delete predicate
        assert_eq!(chunks[1].delete_predicates().len(), 2);
        // file114: predicates are directly within the chunk range => assume they are materialized
        assert_eq!(chunks[2].delete_predicates().len(), 0);
        // file115: came after in sequencer
        assert_eq!(chunks[3].delete_predicates().len(), 0);
        // file121: wrong sequencer
        assert_eq!(chunks[4].delete_predicates().len(), 0);
    }

    #[tokio::test]
    async fn test_compactor_collision() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;
        let sequencer = ns.create_sequencer(1).await;
        let partition = table.with_sequencer(&sequencer).create_partition("k").await;
        let schema = make_schema(&table).await;

        // create a parquet file that cannot be processed by the querier:
        //
        //
        // --------------------------- sequence number ----------------------------->
        // |           0           |           1           |           2           |
        //
        //
        //                          Available Information:
        // (        ingester reports as "persited"         )
        //                                                 ( ingester in-mem data  )
        //                         (                  parquet file                 )
        //
        //
        //                        Desired Information:
        //                         (  wanted parquet data  )
        //                                                 ( ignored parquet data  )
        //                                                 ( ingester in-mem data  )
        //
        //
        // However there is no way to split the parquet data into the "wanted" and "ignored" part because we don't have
        // row-level sequence numbers.

        partition
            .create_parquet_file_with_min_max("table foo=1 11", 1, 2, now_nanos(), now_nanos())
            .await;

        let builder = IngesterPartitionBuilder::new(&table, &schema, &sequencer, &partition);
        let ingester_partition =
            builder.build_with_max_parquet_sequence_number(Some(SequenceNumber::new(1)));

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(ingester_partition);

        let err = querier_table.chunks().await.unwrap_err();
        assert_matches!(err, Error::StateFusion { .. });
    }

    #[tokio::test]
    async fn test_state_reconcile() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;
        let sequencer = ns.create_sequencer(1).await;
        let partition1 = table
            .with_sequencer(&sequencer)
            .create_partition("k1")
            .await;
        let partition2 = table
            .with_sequencer(&sequencer)
            .create_partition("k2")
            .await;
        table.create_column("time", ColumnType::Time).await;
        table.create_column("foo", ColumnType::F64).await;

        // kept because max sequence number <= 2
        let file1 = partition1
            .create_parquet_file_with_min_max("table foo=1 11", 1, 2, now_nanos(), now_nanos())
            .await;

        // pruned because min sequence number > 2
        partition1
            .create_parquet_file_with_min_max("table foo=2 22", 3, 3, now_nanos(), now_nanos())
            .await;

        // kept because max sequence number <= 3
        let file2 = partition2
            .create_parquet_file_with_min_max("table foo=1 11", 1, 3, now_nanos(), now_nanos())
            .await;

        // pruned because min sequence number > 3
        partition2
            .create_parquet_file_with_min_max("table foo=2 22", 4, 4, now_nanos(), now_nanos())
            .await;

        // partition1: kept because sequence number <= 10
        // partition2: kept because sequence number <= 11
        table
            .with_sequencer(&sequencer)
            .create_tombstone(10, 1, 100, "foo=1")
            .await;

        // partition1: pruned because sequence number > 10
        // partition2: kept because sequence number <= 11
        table
            .with_sequencer(&sequencer)
            .create_tombstone(11, 1, 100, "foo=2")
            .await;

        // partition1: pruned because sequence number > 10
        // partition2: pruned because sequence number > 11
        table
            .with_sequencer(&sequencer)
            .create_tombstone(12, 1, 100, "foo=3")
            .await;

        let schema = Arc::new(
            SchemaBuilder::new()
                .influx_field("foo", InfluxFieldType::Integer)
                .timestamp()
                .build()
                .unwrap(),
        );

        let ingester_chunk_id1 = u128::MAX - 1;

        let builder1 = IngesterPartitionBuilder::new(&table, &schema, &sequencer, &partition1);
        let builder2 = IngesterPartitionBuilder::new(&table, &schema, &sequencer, &partition2);

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(
                // this chunk is kept
                builder1
                    .with_ingester_chunk_id(ingester_chunk_id1)
                    .with_lp(["table foo=3i 33"])
                    .build(
                        // parquet max persisted sequence number
                        Some(SequenceNumber::new(2)),
                        // tombstone max persisted sequence number
                        Some(SequenceNumber::new(10)),
                    ),
            )
            .with_ingester_partition(
                // this chunk is filtered out because it has no record batches but the reconciling still takes place
                builder2.with_ingester_chunk_id(u128::MAX).build(
                    // parquet max persisted sequence number
                    Some(SequenceNumber::new(3)),
                    // tombstone max persisted sequence number
                    Some(SequenceNumber::new(11)),
                ),
            );

        let mut chunks = querier_table.chunks().await.unwrap();

        chunks.sort_by_key(|c| c.id());

        // three chunks (two parquet files and one for the in-mem ingester data)
        assert_eq!(chunks.len(), 3);

        // check IDs
        assert_eq!(
            chunks[0].id(),
            ChunkId::new_test(file1.parquet_file.id.get() as u128),
        );
        assert_eq!(
            chunks[1].id(),
            ChunkId::new_test(file2.parquet_file.id.get() as u128),
        );
        assert_eq!(chunks[2].id(), ChunkId::new_test(ingester_chunk_id1));

        // check delete predicates
        // parquet chunks have predicate attached
        assert_eq!(chunks[0].delete_predicates().len(), 1);
        assert_eq!(chunks[1].delete_predicates().len(), 2);
        // ingester in-mem chunk doesn't need predicates, because the ingester has already materialized them for us
        assert_eq!(chunks[2].delete_predicates().len(), 0);
    }

    #[tokio::test]
    async fn test_ingester_overlap_detection() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;
        let sequencer = ns.create_sequencer(1).await;
        let partition1 = table
            .with_sequencer(&sequencer)
            .create_partition("k1")
            .await;
        let partition2 = table
            .with_sequencer(&sequencer)
            .create_partition("k2")
            .await;

        let schema = Arc::new(
            SchemaBuilder::new()
                .influx_field("foo", InfluxFieldType::Integer)
                .timestamp()
                .build()
                .unwrap(),
        );

        let builder1 = IngesterPartitionBuilder::new(&table, &schema, &sequencer, &partition1);
        let builder2 = IngesterPartitionBuilder::new(&table, &schema, &sequencer, &partition2);

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(
                builder1
                    .clone()
                    .with_ingester_chunk_id(1)
                    .with_lp(vec!["table foo=1i 1"])
                    .build(
                        // parquet max persisted sequence number
                        None, // tombstone max persisted sequence number
                        None,
                    ),
            )
            .with_ingester_partition(
                builder2
                    .with_ingester_chunk_id(2)
                    .with_lp(vec!["table foo=2i 2"])
                    .build(
                        // parquet max persisted sequence number
                        None, // tombstone max persisted sequence number
                        None,
                    ),
            )
            .with_ingester_partition(
                builder1
                    .with_ingester_chunk_id(3)
                    .with_lp(vec!["table foo=3i 3"])
                    .build(
                        // parquet max persisted sequence number
                        None, // tombstone max persisted sequence number
                        None,
                    ),
            );

        let err = querier_table.chunks().await.unwrap_err();

        assert_matches!(err, Error::IngestersOverlap { .. });
    }

    #[tokio::test]
    async fn test_parquet_cache_refresh() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table1").await;
        let sequencer = ns.create_sequencer(1).await;
        let partition = table.with_sequencer(&sequencer).create_partition("k").await;
        let schema = make_schema(&table).await;

        let builder = IngesterPartitionBuilder::new(&table, &schema, &sequencer, &partition)
            .with_lp(["table foo=1 1"]);

        // Parquet file between with max sequence number 2
        partition
            .create_parquet_file_with_min_max("table1 foo=1 11", 1, 2, now_nanos(), now_nanos())
            .await;

        let ingester_partition =
            builder.build_with_max_parquet_sequence_number(Some(SequenceNumber::new(2)));

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(ingester_partition);

        // Expect 2 chunks: one for ingester, and one from parquet file
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 2);

        // Now, make a second chunk with max sequence number 3
        partition
            .create_parquet_file_with_min_max("table1 foo=1 22", 2, 3, now_nanos(), now_nanos())
            .await;

        // With the same ingester response, still expect 2 chunks: one
        // for ingester, and one from parquet file
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 2);

        // update the ingester response to return a new max parquet
        // sequence number that includes the new file (3)
        let ingester_partition =
            builder.build_with_max_parquet_sequence_number(Some(SequenceNumber::new(3)));

        let querier_table = querier_table
            .clear_ingester_partitions()
            .with_ingester_partition(ingester_partition);

        // expect the second file is found, resulting in three chunks
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn test_tombstone_cache_refresh() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table1").await;
        let sequencer = ns.create_sequencer(1).await;
        let partition = table.with_sequencer(&sequencer).create_partition("k").await;
        let schema = make_schema(&table).await;
        // Expect 1 chunk with with one delete predicate
        let querier_table = TestQuerierTable::new(&catalog, &table).await;

        let builder = IngesterPartitionBuilder::new(&table, &schema, &sequencer, &partition)
            .with_lp(["table foo=1 1"]);

        // parquet file with max sequence number 1
        partition
            .create_parquet_file_with_min_max("table1 foo=1 11", 1, 1, now_nanos(), now_nanos())
            .await;

        // tombstone with max sequence number 2
        table
            .with_sequencer(&sequencer)
            .create_tombstone(2, 1, 100, "foo=1")
            .await;

        let max_parquet_sequence_number = Some(SequenceNumber::new(1));
        let max_tombstone_sequence_number = Some(SequenceNumber::new(2));
        let ingester_partition =
            builder.build(max_parquet_sequence_number, max_tombstone_sequence_number);

        let querier_table = querier_table.with_ingester_partition(ingester_partition);

        let deletes = num_deletes(querier_table.chunks().await.unwrap());
        assert_eq!(&deletes, &[1, 0]);

        // Now, make a second tombstone with max sequence number 3
        table
            .with_sequencer(&sequencer)
            .create_tombstone(3, 1, 100, "foo=1")
            .await;

        // With the same ingester response, still expect 1 delete
        // (because cache is not cleared)
        let deletes = num_deletes(querier_table.chunks().await.unwrap());
        assert_eq!(&deletes, &[1, 0]);

        // update the ingester response to return a new max delete sequence number
        let max_tombstone_sequence_number = Some(SequenceNumber::new(3));
        let ingester_partition =
            builder.build(max_parquet_sequence_number, max_tombstone_sequence_number);
        let querier_table = querier_table
            .clear_ingester_partitions()
            .with_ingester_partition(ingester_partition);

        // second tombstone should be found
        let deletes = num_deletes(querier_table.chunks().await.unwrap());
        assert_eq!(&deletes, &[2, 0]);
    }

    /// Adds a "foo" column to the table and returns the created schema
    async fn make_schema(table: &Arc<TestTable>) -> Arc<Schema> {
        table.create_column("foo", ColumnType::F64).await;
        table.create_column("time", ColumnType::Time).await;
        // create corresponding schema
        Arc::new(
            SchemaBuilder::new()
                .influx_field("foo", InfluxFieldType::Float)
                .timestamp()
                .build()
                .unwrap(),
        )
    }

    /// A `QuerierTable` and some number of `IngesterPartitions` that
    /// are fed to the ingester connection on the next call to
    /// `chunks()`
    struct TestQuerierTable {
        // The underling table
        querier_table: QuerierTable,
        /// Ingester partitions
        ingester_partitions: Vec<IngesterPartition>,
    }

    impl TestQuerierTable {
        /// Create a new wrapped [`QuerierTable`]
        async fn new(catalog: &Arc<TestCatalog>, table: &Arc<TestTable>) -> Self {
            Self {
                querier_table: querier_table(catalog, table).await,
                ingester_partitions: vec![],
            }
        }

        /// Return a reference to the inner table
        fn inner(&self) -> &QuerierTable {
            &self.querier_table
        }

        /// add the `ingester_partition` to the ingester response processed by the table
        fn with_ingester_partition(mut self, ingester_partition: IngesterPartition) -> Self {
            self.ingester_partitions.push(ingester_partition);
            self
        }

        /// Clears ingester partitions for next response from ingester
        fn clear_ingester_partitions(mut self) -> Self {
            self.ingester_partitions.clear();
            self
        }

        /// Invokes querier_table.chunks modeling the ingester sending the partitions in this table
        async fn chunks(&self) -> Result<Vec<Arc<dyn QueryChunk>>> {
            let pred = Predicate::default();

            self.querier_table
                .ingester_connection
                .as_ref()
                .unwrap()
                .as_any()
                .downcast_ref::<MockIngesterConnection>()
                .unwrap()
                .next_response(Ok(self.ingester_partitions.clone()));

            self.querier_table.chunks(&pred).await
        }
    }

    /// returns the number of deletes in each chunk
    fn num_deletes(mut chunks: Vec<Arc<dyn QueryChunk>>) -> Vec<usize> {
        chunks.sort_by_key(|c| c.id());
        chunks
            .iter()
            .map(|chunk| chunk.delete_predicates().len())
            .collect()
    }

    /// returns the value of now as nanoseconds since the epoch
    fn now_nanos() -> i64 {
        now().timestamp_nanos()
    }
}
