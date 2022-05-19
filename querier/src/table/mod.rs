use self::query_access::QuerierTableChunkPruner;
use self::state_reconciler::Reconciler;
use crate::{
    chunk::ParquetChunkAdapter,
    ingester::{self, IngesterPartition},
    IngesterConnection,
};
use backoff::{Backoff, BackoffConfig};
use data_types::{PartitionId, TableId};
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
    /// Namespace the table is in
    namespace_name: Arc<str>,

    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// Table name.
    table_name: Arc<str>,

    /// Table ID.
    id: TableId,

    /// Table schema.
    schema: Arc<Schema>,

    /// Connection to ingester
    ingester_connection: Arc<dyn IngesterConnection>,

    /// Interface to create chunks for this table.
    chunk_adapter: Arc<ParquetChunkAdapter>,

    /// Handle reconciling ingester and catalog data
    reconciler: Reconciler,
}

impl QuerierTable {
    /// Create new table.
    pub fn new(
        namespace_name: Arc<str>,
        backoff_config: BackoffConfig,
        id: TableId,
        table_name: Arc<str>,
        schema: Arc<Schema>,
        ingester_connection: Arc<dyn IngesterConnection>,
        chunk_adapter: Arc<ParquetChunkAdapter>,
    ) -> Self {
        let reconciler = Reconciler::new(
            Arc::clone(&table_name),
            Arc::clone(&namespace_name),
            Arc::clone(&chunk_adapter),
        );

        Self {
            namespace_name,
            backoff_config,
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
        debug!(?predicate, namespace=%self.namespace_name, table_name=%self.table_name(), "Fetching all chunks");

        // ask ingesters for data
        let ingester_partitions = self.ingester_partitions(predicate).await?;

        debug!(
            namespace=%self.namespace_name,
            table_name=%self.table_name(),
            num_ingester_partitions=%ingester_partitions.len(),
            "Ingester partitions fetched"
        );

        // get parquet files and tombstones in a single catalog transaction
        // IMPORTANT: this needs to happen AFTER gathering data from the ingesters
        // TODO: figure out some form of caching
        let (parquet_files, tombstones) = Backoff::new(&self.backoff_config)
            .retry_all_errors::<_, _, _, iox_catalog::interface::Error>(
                "get parquet files and tombstones for table",
                || async {
                    let mut txn = self.chunk_adapter.catalog().start_transaction().await?;

                    let parquet_files = txn
                        .parquet_files()
                        .list_by_table_not_to_delete_with_metadata(self.id)
                        .await?;

                    debug!(
                        ?parquet_files,
                        namespace=%self.namespace_name,
                        table_name=%self.table_name(),
                        "Parquet files from catalog"
                    );

                    let tombstones = txn.tombstones().list_by_table(self.id).await?;

                    txn.commit().await?;

                    Ok((parquet_files, tombstones))
                },
            )
            .await
            .expect("retry forever");

        self.reconciler
            .reconcile(ingester_partitions, tombstones, parquet_files)
            .await
            .context(StateFusionSnafu)
    }

    /// Get a chunk pruner that can be used to prune chunks retrieved via [`chunks`](Self::chunks)
    pub fn chunk_pruner(&self) -> Arc<dyn ChunkPruner> {
        Arc::new(QuerierTableChunkPruner {})
    }

    /// Get partitions from ingesters.
    async fn ingester_partitions(
        &self,
        predicate: &Predicate,
    ) -> Result<Vec<Arc<IngesterPartition>>> {
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

        // get any chunks from the ingster
        let partitions = self
            .ingester_connection
            .partitions(
                Arc::clone(&self.namespace_name),
                Arc::clone(&self.table_name),
                columns,
                predicate,
                Arc::clone(&self.schema),
            )
            .await
            .context(GettingIngesterPartitionsSnafu)?;

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
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use assert_matches::assert_matches;
    use data_types::{ChunkId, ColumnType, SequenceNumber};
    use iox_tests::util::{now, TestCatalog};
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use predicate::Predicate;
    use schema::{builder::SchemaBuilder, selection::Selection, InfluxFieldType};

    use super::*;
    use crate::{
        ingester::{test_util::MockIngesterConnection, IngesterPartition},
        table::test_util::querier_table,
    };

    #[tokio::test]
    async fn test_parquet_chunks() {
        let pred = Predicate::default();
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

        table1.create_column("foo", ColumnType::I64).await;
        table2.create_column("foo", ColumnType::I64).await;

        let querier_table = querier_table(&catalog, &table1).await;

        // no parquet files yet
        assert!(querier_table.chunks(&pred).await.unwrap().is_empty());

        let file111 = partition11
            .create_parquet_file_with_min_max(
                "table1 foo=1 11",
                1,
                2,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;
        let file112 = partition11
            .create_parquet_file_with_min_max(
                "table1 foo=2 22",
                3,
                4,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;
        let file113 = partition11
            .create_parquet_file_with_min_max(
                "table1 foo=3 33",
                5,
                6,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;
        let file114 = partition11
            .create_parquet_file_with_min_max(
                "table1 foo=4 44",
                7,
                8,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;
        let file115 = partition11
            .create_parquet_file_with_min_max(
                "table1 foo=5 55",
                9,
                10,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;
        let file121 = partition12
            .create_parquet_file_with_min_max(
                "table1 foo=5 55",
                1,
                2,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;
        let _file211 = partition21
            .create_parquet_file_with_min_max(
                "table2 foo=6 66",
                1,
                2,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
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

        // now we have some files
        // this contains all files except for:
        // - file111: marked for delete
        // - file221: wrong table
        let mut chunks = querier_table.chunks(&pred).await.unwrap();
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
        let pred = Predicate::default();
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;
        let sequencer = ns.create_sequencer(1).await;
        let partition = table.with_sequencer(&sequencer).create_partition("k").await;
        table.create_column("foo", ColumnType::I64).await;

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
            .create_parquet_file_with_min_max(
                "table foo=1 11",
                1,
                2,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;

        let querier_table = querier_table(&catalog, &table).await;

        querier_table
            .ingester_connection
            .as_any()
            .downcast_ref::<MockIngesterConnection>()
            .unwrap()
            .next_response(Ok(vec![Arc::new(
                IngesterPartition::try_new(
                    Arc::from("ingester"),
                    ChunkId::new(),
                    Arc::from(ns.namespace.name.clone()),
                    Arc::from(table.table.name.clone()),
                    partition.partition.id,
                    sequencer.sequencer.id,
                    Arc::new(SchemaBuilder::new().build().unwrap()),
                    Some(SequenceNumber::new(1)),
                    None,
                    vec![],
                )
                .unwrap(),
            )]));

        let err = querier_table.chunks(&pred).await.unwrap_err();
        assert_matches!(err, Error::StateFusion { .. });
    }

    #[tokio::test]
    async fn test_state_reconcile() {
        let pred = Predicate::default();
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

        // kept because max sequence number <= 2
        let file1 = partition1
            .create_parquet_file_with_min_max(
                "table foo=1 11",
                1,
                2,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;

        // pruned because min sequence number > 2
        partition1
            .create_parquet_file_with_min_max(
                "table foo=2 22",
                3,
                3,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;

        // kept because max sequence number <= 3
        let file2 = partition2
            .create_parquet_file_with_min_max(
                "table foo=1 11",
                1,
                3,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
            .await;

        // pruned because min sequence number > 3
        partition2
            .create_parquet_file_with_min_max(
                "table foo=2 22",
                4,
                4,
                now().timestamp_nanos(),
                now().timestamp_nanos(),
            )
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

        let querier_table = querier_table(&catalog, &table).await;

        let ingester_chunk_id1 = ChunkId::new_test(u128::MAX - 1);
        let ingester_chunk_id2 = ChunkId::new_test(u128::MAX);
        querier_table
            .ingester_connection
            .as_any()
            .downcast_ref::<MockIngesterConnection>()
            .unwrap()
            .next_response(Ok(vec![
                // this chunk is kept
                Arc::new(
                    IngesterPartition::try_new(
                        Arc::from("ingester"),
                        ingester_chunk_id1,
                        Arc::from(ns.namespace.name.clone()),
                        Arc::from(table.table.name.clone()),
                        partition1.partition.id,
                        sequencer.sequencer.id,
                        Arc::new(
                            SchemaBuilder::new()
                                .influx_field("foo", InfluxFieldType::Integer)
                                .timestamp()
                                .build()
                                .unwrap(),
                        ),
                        // parquet max persisted sequence number
                        Some(SequenceNumber::new(2)),
                        // tombstone max persisted sequence number
                        Some(SequenceNumber::new(10)),
                        vec![lp_to_record_batch("table foo=3i 33")],
                    )
                    .unwrap(),
                ),
                // this chunk is filtered out because it has no record batches but the reconciling still takes place
                Arc::new(
                    IngesterPartition::try_new(
                        Arc::from("ingester"),
                        ingester_chunk_id2,
                        Arc::from(ns.namespace.name.clone()),
                        Arc::from(table.table.name.clone()),
                        partition2.partition.id,
                        sequencer.sequencer.id,
                        Arc::new(
                            SchemaBuilder::new()
                                .influx_field("foo", InfluxFieldType::Integer)
                                .timestamp()
                                .build()
                                .unwrap(),
                        ),
                        // parquet max persisted sequence number
                        Some(SequenceNumber::new(3)),
                        // tombstone max persisted sequence number
                        Some(SequenceNumber::new(11)),
                        vec![],
                    )
                    .unwrap(),
                ),
            ]));

        let mut chunks = querier_table.chunks(&pred).await.unwrap();
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
        assert_eq!(chunks[2].id(), ingester_chunk_id1);

        // check delete predicates
        // parquet chunks have predicate attached
        assert_eq!(chunks[0].delete_predicates().len(), 1);
        assert_eq!(chunks[1].delete_predicates().len(), 2);
        // ingester in-mem chunk doesn't need predicates, because the ingester has already materialized them for us
        assert_eq!(chunks[2].delete_predicates().len(), 0);
    }

    #[tokio::test]
    async fn test_ingester_overlap_detection() {
        let pred = Predicate::default();
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

        let querier_table = querier_table(&catalog, &table).await;

        let ingester_chunk_id1 = ChunkId::new_test(1);
        let ingester_chunk_id2 = ChunkId::new_test(2);
        let ingester_chunk_id3 = ChunkId::new_test(3);
        querier_table
            .ingester_connection
            .as_any()
            .downcast_ref::<MockIngesterConnection>()
            .unwrap()
            .next_response(Ok(vec![
                Arc::new(
                    IngesterPartition::try_new(
                        Arc::from("ingester1"),
                        ingester_chunk_id1,
                        Arc::from(ns.namespace.name.clone()),
                        Arc::from(table.table.name.clone()),
                        partition1.partition.id,
                        sequencer.sequencer.id,
                        Arc::new(
                            SchemaBuilder::new()
                                .influx_field("foo", InfluxFieldType::Integer)
                                .timestamp()
                                .build()
                                .unwrap(),
                        ),
                        // parquet max persisted sequence number
                        None,
                        // tombstone max persisted sequence number
                        None,
                        vec![lp_to_record_batch("table foo=1i 1")],
                    )
                    .unwrap(),
                ),
                Arc::new(
                    IngesterPartition::try_new(
                        Arc::from("ingester1"),
                        ingester_chunk_id2,
                        Arc::from(ns.namespace.name.clone()),
                        Arc::from(table.table.name.clone()),
                        partition2.partition.id,
                        sequencer.sequencer.id,
                        Arc::new(
                            SchemaBuilder::new()
                                .influx_field("foo", InfluxFieldType::Integer)
                                .timestamp()
                                .build()
                                .unwrap(),
                        ),
                        // parquet max persisted sequence number
                        None,
                        // tombstone max persisted sequence number
                        None,
                        vec![lp_to_record_batch("table foo=2i 2")],
                    )
                    .unwrap(),
                ),
                Arc::new(
                    IngesterPartition::try_new(
                        Arc::from("ingester2"),
                        ingester_chunk_id3,
                        Arc::from(ns.namespace.name.clone()),
                        Arc::from(table.table.name.clone()),
                        partition1.partition.id,
                        sequencer.sequencer.id,
                        Arc::new(
                            SchemaBuilder::new()
                                .influx_field("foo", InfluxFieldType::Integer)
                                .timestamp()
                                .build()
                                .unwrap(),
                        ),
                        // parquet max persisted sequence number
                        None,
                        // tombstone max persisted sequence number
                        None,
                        vec![lp_to_record_batch("table foo=3i 3")],
                    )
                    .unwrap(),
                ),
            ]));

        let err = querier_table.chunks(&pred).await.unwrap_err();
        assert_matches!(err, Error::IngestersOverlap { .. });
    }

    fn lp_to_record_batch(lp: &str) -> RecordBatch {
        lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
    }
}
