use self::query_access::QuerierTableChunkPruner;
use self::state_reconciler::Reconciler;
use crate::{
    ingester::{self, IngesterPartition},
    parquet::ChunkAdapter,
    IngesterConnection,
};
use data_types::{ColumnId, DeletePredicate, NamespaceId, PartitionId, ShardIndex, TableId};
use datafusion::error::DataFusionError;
use futures::join;
use iox_query::{exec::Executor, provider, provider::ChunkPruner, QueryChunk};
use observability_deps::tracing::{debug, trace};
use predicate::Predicate;
use schema::Schema;
use sharder::JumpHash;
use snafu::{ResultExt, Snafu};
use std::borrow::Cow;
use std::collections::HashSet;
use std::time::Duration;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use trace::span::{Span, SpanRecorder};
use uuid::Uuid;

pub use self::query_access::metrics::PruneMetrics;
pub(crate) use self::query_access::MetricPruningObserver;

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

    #[snafu(display("Chunk pruning failed: {}", source))]
    ChunkPruning { source: provider::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DataFusionError {
    fn from(err: Error) -> Self {
        Self::External(Box::new(err) as _)
    }
}

/// Args to create a [`QuerierTable`].
pub struct QuerierTableArgs {
    pub sharder: Option<Arc<JumpHash<Arc<ShardIndex>>>>,
    pub namespace_id: NamespaceId,
    pub namespace_name: Arc<str>,
    pub namespace_retention_period: Option<Duration>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub schema: Schema,
    pub ingester_connection: Option<Arc<dyn IngesterConnection>>,
    pub chunk_adapter: Arc<ChunkAdapter>,
    pub exec: Arc<Executor>,
    pub prune_metrics: Arc<PruneMetrics>,
}

/// Table representation for the querier.
#[derive(Debug)]
pub struct QuerierTable {
    /// Sharder to query for which shards are responsible for the table's data. If not specified,
    /// query all ingesters because we're using the RPC write path.
    sharder: Option<Arc<JumpHash<Arc<ShardIndex>>>>,

    /// Namespace the table is in
    namespace_name: Arc<str>,

    /// Namespace ID for this table.
    namespace_id: NamespaceId,

    /// Namespace retenion
    namespace_retention_period: Option<Duration>,

    /// Table name.
    table_name: Arc<str>,

    /// Table ID.
    table_id: TableId,

    /// Table schema.
    schema: Schema,

    /// Connection to ingester
    ingester_connection: Option<Arc<dyn IngesterConnection>>,

    /// Interface to create chunks for this table.
    chunk_adapter: Arc<ChunkAdapter>,

    /// Executor for queries.
    exec: Arc<Executor>,

    /// Metrics for chunk pruning.
    prune_metrics: Arc<PruneMetrics>,
}

impl QuerierTable {
    /// Create new table.
    pub fn new(args: QuerierTableArgs) -> Self {
        let QuerierTableArgs {
            sharder,
            namespace_id,
            namespace_name,
            namespace_retention_period,
            table_id,
            table_name,
            schema,
            ingester_connection,
            chunk_adapter,
            exec,
            prune_metrics,
        } = args;

        Self {
            sharder,
            namespace_name,
            namespace_id,
            namespace_retention_period,
            table_name,
            table_id,
            schema,
            ingester_connection,
            chunk_adapter,
            exec,
            prune_metrics,
        }
    }

    /// Table name.
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }

    /// Table ID.
    #[allow(dead_code)]
    pub fn id(&self) -> TableId {
        self.table_id
    }

    /// Schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Query all chunks within this table.
    ///
    /// This currently contains all parquet files linked to their unprocessed tombstones.
    pub async fn chunks(
        &self,
        predicate: &Predicate,
        span: Option<Span>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let mut span_recorder = SpanRecorder::new(span);
        match self
            .chunks_inner(predicate, &span_recorder, projection)
            .await
        {
            Ok(chunks) => {
                span_recorder.ok("got chunks");
                Ok(chunks)
            }
            Err(e) => {
                span_recorder.error("failed to get chunks");
                Err(e)
            }
        }
    }

    async fn chunks_inner(
        &self,
        predicate: &Predicate,
        span_recorder: &SpanRecorder,
        projection: Option<&Vec<usize>>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        debug!(
            ?predicate,
            namespace=%self.namespace_name,
            table_name=%self.table_name(),
            "Fetching all chunks"
        );

        let (predicate, retention_delete_pred) = match self.namespace_retention_period {
            // The retention is not fininte, add predicate to filter out data outside retention
            // period
            Some(retention_period) => {
                let retention_time_ns = self
                    .chunk_adapter
                    .catalog_cache()
                    .time_provider()
                    .now()
                    .timestamp_nanos()
                    - retention_period.as_nanos() as i64;

                // Add predicate to only keep chunks inside the retention period: time >=
                // retention_period
                let predicate = predicate.clone().with_retention(retention_time_ns);

                // Expression used to add to delete predicate to delete data older than retention
                // period time < retention_time
                let retention_delete_pred = Some(DeletePredicate::retention_delete_predicate(
                    retention_time_ns,
                ));

                (Cow::Owned(predicate), retention_delete_pred)
            }
            // inifite retention, no need to add predicate
            None => (Cow::Borrowed(predicate), None),
        };

        let catalog_cache = self.chunk_adapter.catalog_cache();

        // ask ingesters for data, also optimistically fetching catalog
        // contents at the same time to pre-warm cache
        let (partitions, _parquet_files, _tombstones) = join!(
            self.ingester_partitions(
                &predicate,
                span_recorder.child_span("ingester partitions"),
                projection,
            ),
            catalog_cache.parquet_file().get(
                self.id(),
                None,
                None,
                span_recorder.child_span("cache GET parquet_file (pre-warm")
            ),
            catalog_cache.tombstone().get(
                self.id(),
                None,
                span_recorder.child_span("cache GET tombstone (pre-warm)")
            ),
        );

        // handle errors / cache refresh
        let partitions = partitions?;

        // determine max parquet sequence number for cache invalidation, if using the write buffer
        // path.
        let max_parquet_sequence_number = if self.rpc_write() {
            None
        } else {
            partitions
                .iter()
                .flat_map(|p| p.parquet_max_sequence_number())
                .max()
        };
        let max_tombstone_sequence_number = partitions
            .iter()
            .flat_map(|p| p.tombstone_max_sequence_number())
            .max();

        // If using the RPC write path, determine number of persisted parquet files per ingester
        // UUID seen in the ingester query responses for cache invalidation. If this is an empty
        // HashMap, then there are no results from the ingesters.
        let persisted_file_counts_by_ingester_uuid = if self.rpc_write() {
            Some(collect_persisted_file_counts(
                partitions.len(),
                partitions
                    .iter()
                    .map(|p| (p.ingester_uuid(), p.completed_persistence_count())),
            ))
        } else {
            None
        };

        debug!(
            namespace=%self.namespace_name,
            table_name=%self.table_name(),
            num_ingester_partitions=%partitions.len(),
            "Ingester partitions fetched"
        );

        // Now fetch the actual contents of the catalog we need
        // NB: Pass max parquet/tombstone sequence numbers to `get`
        //     to ensure cache is refreshed if we learned about new files/tombstones.
        let (parquet_files, tombstones) = join!(
            catalog_cache.parquet_file().get(
                self.id(),
                max_parquet_sequence_number,
                persisted_file_counts_by_ingester_uuid,
                span_recorder.child_span("cache GET parquet_file"),
            ),
            catalog_cache.tombstone().get(
                self.id(),
                max_tombstone_sequence_number,
                span_recorder.child_span("cache GET tombstone")
            )
        );

        let columns: HashSet<ColumnId> = parquet_files
            .files
            .iter()
            .flat_map(|cached_file| cached_file.column_set.iter().copied())
            .collect();
        let cached_namespace = self
            .chunk_adapter
            .catalog_cache()
            .namespace()
            .get(
                Arc::clone(&self.namespace_name),
                &[(&self.table_name, &columns)],
                span_recorder.child_span("cache GET namespace schema"),
            )
            .await;
        let Some(cached_table) = cached_namespace
            .as_ref()
            .and_then(|ns| ns.tables.get(self.table_name.as_ref())) else {
                return Ok(vec![]);
            };

        let reconciler = Reconciler::new(
            Arc::clone(&self.table_name),
            Arc::clone(&self.namespace_name),
            Arc::clone(self.chunk_adapter.catalog_cache()),
            self.rpc_write(),
        );

        // create parquet files
        let parquet_files = self
            .chunk_adapter
            .new_chunks(
                Arc::clone(cached_table),
                Arc::clone(&parquet_files.files),
                &predicate,
                MetricPruningObserver::new(Arc::clone(&self.prune_metrics)),
                span_recorder.child_span("new_chunks"),
            )
            .await;

        let chunks = reconciler
            .reconcile(
                partitions,
                tombstones.to_vec(),
                retention_delete_pred,
                parquet_files,
                span_recorder.child_span("reconcile"),
            )
            .await
            .context(StateFusionSnafu)?;
        trace!("Fetched chunks");

        let num_initial_chunks = chunks.len();
        let chunks = self
            .chunk_pruner()
            .prune_chunks(
                self.table_name(),
                // use up-to-date schema
                &cached_table.schema,
                chunks,
                &predicate,
            )
            .context(ChunkPruningSnafu)?;
        debug!(
            %predicate,
            num_initial_chunks,
            num_final_chunks=chunks.len(),
            "pruned with pushed down predicates"
        );
        Ok(chunks)
    }

    /// Get a chunk pruner that can be used to prune chunks retrieved via [`chunks`](Self::chunks)
    pub fn chunk_pruner(&self) -> Arc<dyn ChunkPruner> {
        Arc::new(QuerierTableChunkPruner::new(Arc::clone(
            &self.prune_metrics,
        )))
    }

    /// Get partitions from ingesters.
    async fn ingester_partitions(
        &self,
        predicate: &Predicate,
        span: Option<Span>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Vec<IngesterPartition>> {
        let mut span_recorder = SpanRecorder::new(span);

        if let Some(ingester_connection) = &self.ingester_connection {
            match self
                .ingester_partitions_inner(
                    Arc::clone(ingester_connection),
                    predicate,
                    &span_recorder,
                    projection,
                )
                .await
            {
                Ok(partitions) => {
                    span_recorder.ok("Got partitions");
                    Ok(partitions)
                }
                Err(e) => {
                    span_recorder.error("failed");
                    Err(e)
                }
            }
        } else {
            // No ingesters are configured
            span_recorder.ok("No ingesters configured");
            Ok(vec![])
        }
    }

    async fn ingester_partitions_inner(
        &self,
        ingester_connection: Arc<dyn IngesterConnection>,
        predicate: &Predicate,
        span_recorder: &SpanRecorder,
        projection: Option<&Vec<usize>>,
    ) -> Result<Vec<IngesterPartition>> {
        // If the projection is provided, use it. Otherwise, use all columns of the table
        // The provided projection should include all columns needed by the query
        let columns = self.schema.select_given_and_pk_columns(projection);

        // Get the shard indexes responsible for this table's data from the sharder to
        // determine which ingester(s) to query.
        // Currently, the sharder will only return one shard index per table, but in the
        // near future, the sharder might return more than one shard index for one table.
        let shard_indexes = self
            .sharder
            .as_ref()
            .map(|sharder| vec![**sharder.shard_for_query(&self.table_name, &self.namespace_name)]);

        // get cached table w/o any must-coverage information
        let Some(cached_table) = self.chunk_adapter
            .catalog_cache()
            .namespace()
            .get(
                Arc::clone(&self.namespace_name),
                &[],
                span_recorder.child_span("get namespace")
            )
            .await
            .and_then(|ns| ns.tables.get(&self.table_name).cloned())
        else {
            return Ok(vec![])
        };

        // get any chunks from the ingester(s)
        let partitions_result = ingester_connection
            .partitions(
                shard_indexes,
                self.namespace_id,
                cached_table,
                columns,
                predicate,
                span_recorder.child_span("IngesterConnection partitions"),
            )
            .await
            .context(GettingIngesterPartitionsSnafu);

        let partitions = partitions_result?;

        if !self.rpc_write() {
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
        }

        Ok(partitions)
    }

    /// Whether we're using the RPC write path or not. Write buffer mode will always specify a
    /// sharder; the RPC write path never will.
    fn rpc_write(&self) -> bool {
        self.sharder.is_none()
    }

    /// clear the parquet file cache
    #[cfg(test)]
    fn clear_parquet_cache(&self) {
        self.chunk_adapter
            .catalog_cache()
            .parquet_file()
            .expire(self.table_id)
    }

    /// clear the tombstone cache
    #[cfg(test)]
    fn clear_tombstone_cache(&self) {
        self.chunk_adapter
            .catalog_cache()
            .tombstone()
            .expire(self.table_id)
    }
}

// Given metadata from a list of ingester request [`PartitionData`]s, sum the total completed
// persistence counts for each ingester UUID so that the Parquet file cache can see if it knows
// about a different set of ingester UUIDs or a different number of persisted Parquet files and
// therefore needs to refresh its view of the catalog.
fn collect_persisted_file_counts(
    capacity: usize,
    partitions: impl Iterator<Item = (Option<Uuid>, u64)>,
) -> HashMap<Uuid, u64> {
    partitions.fold(
        HashMap::with_capacity(capacity),
        |mut map, (uuid, count)| {
            if let Some(uuid) = uuid {
                let sum = map.entry(uuid).or_default();
                *sum += count;
            }
            map
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ingester::{test_util::MockIngesterConnection, IngesterPartition},
        table::test_util::{querier_table, IngesterPartitionBuilder},
    };
    use arrow_util::assert_batches_eq;
    use assert_matches::assert_matches;
    use data_types::{ChunkId, ColumnType, CompactionLevel, SequenceNumber};
    use iox_query::exec::IOxSessionContext;
    use iox_tests::{TestCatalog, TestParquetFileBuilder, TestTable};
    use iox_time::TimeProvider;
    use predicate::Predicate;
    use schema::{builder::SchemaBuilder, InfluxFieldType};
    use std::sync::Arc;
    use test_helpers::maybe_start_logging;
    use trace::{span::SpanStatus, RingBufferTraceCollector};

    #[test]
    fn sum_up_persisted_file_counts() {
        let output = collect_persisted_file_counts(0, std::iter::empty());
        assert!(
            output.is_empty(),
            "Expected output to be empty, instead was: {output:?}"
        );

        // If there's no UUIDs, don't count anything
        let input = [(None, 10)];
        let output = collect_persisted_file_counts(input.len(), input.into_iter());
        assert!(
            output.is_empty(),
            "Expected output to be empty, instead was: {output:?}"
        );

        let uuid = Uuid::new_v4();
        let input = [(Some(uuid), 20), (Some(uuid), 22), (None, 10)];
        let output = collect_persisted_file_counts(input.len(), input.into_iter());
        assert_eq!(output.len(), 1);
        assert_eq!(*output.get(&uuid).unwrap(), 42);
    }

    #[tokio::test]
    async fn test_prune_parquet_chunks_outside_retention() {
        test_helpers::maybe_start_logging();

        let catalog = TestCatalog::new();

        // namespace with 1-hour retention policy
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let inside_retention = catalog.time_provider.now().timestamp_nanos(); // now
        let outside_retention =
            inside_retention - Duration::from_secs(2 * 60 * 60).as_nanos() as i64; // 2 hours ago

        let shard = ns.create_shard(1).await;
        let table = ns.create_table("cpu").await;

        table.create_column("host", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        table.create_column("load", ColumnType::F64).await;

        let partition = table.with_shard(&shard).create_partition("a").await;

        let querier_table = TestQuerierTable::new(&catalog, &table).await;

        // no parquet files yet
        assert!(querier_table.chunks().await.unwrap().is_empty());

        // C1: partially inside retention
        let lp = format!(
            "
                cpu,host=a load=1 {inside_retention}\n
                cpu,host=aa load=11 {outside_retention}\n
            "
        );
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(1)
            .with_min_time(outside_retention)
            .with_max_time(inside_retention);
        let file_partially_inside = partition.create_parquet_file(builder).await;

        // C2: fully inside retention
        let lp = format!(
            "
            cpu,host=b load=2 {inside_retention}\n
            cpu,host=bb load=21 {inside_retention}\n
            "
        );
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(2)
            .with_min_time(inside_retention)
            .with_max_time(inside_retention);
        let file_fully_inside = partition.create_parquet_file(builder).await;

        // C3: fully outside retention
        let lp = format!(
            "
            cpu,host=z load=0 {outside_retention}\n
            cpu,host=zz load=01 {outside_retention}\n
            "
        );
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(3)
            .with_min_time(outside_retention)
            .with_max_time(outside_retention);
        let _file_fully_outside = partition.create_parquet_file(builder).await;

        // As we have now made new parquet files, force a cache refresh
        querier_table.inner().clear_parquet_cache();
        querier_table.inner().clear_tombstone_cache();

        // Invoke chunks that will prune chunks fully outside retention C3
        let mut chunks = querier_table.chunks().await.unwrap();
        chunks.sort_by_key(|c| c.id());
        assert_eq!(chunks.len(), 2);

        // check IDs
        assert_eq!(
            chunks[0].id(),
            ChunkId::new_test(file_partially_inside.parquet_file.id.get() as u128),
        );
        assert_eq!(
            chunks[1].id(),
            ChunkId::new_test(file_fully_inside.parquet_file.id.get() as u128),
        );
    }

    #[tokio::test]
    async fn test_parquet_chunks() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        // Namespace with infinite retention policy
        let ns = catalog.create_namespace_with_retention("ns", None).await;

        let table1 = ns.create_table("table1").await;
        let table2 = ns.create_table("table2").await;

        let shard1 = ns.create_shard(1).await;
        let shard2 = ns.create_shard(2).await;

        let partition11 = table1.with_shard(&shard1).create_partition("k").await;
        let partition12 = table1.with_shard(&shard2).create_partition("k").await;
        let partition21 = table2.with_shard(&shard1).create_partition("k").await;

        table1.create_column("time", ColumnType::Time).await;
        table1.create_column("foo", ColumnType::F64).await;
        table2.create_column("time", ColumnType::Time).await;
        table2.create_column("foo", ColumnType::F64).await;

        let querier_table = TestQuerierTable::new(&catalog, &table1).await;

        // no parquet files yet
        assert!(querier_table.chunks().await.unwrap().is_empty());

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=1 11")
            .with_max_seq(2)
            .with_min_time(11)
            .with_max_time(11);
        let file111 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=2 22")
            .with_max_seq(4)
            .with_min_time(22)
            .with_max_time(22);
        let file112 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=3 33")
            .with_max_seq(6)
            .with_min_time(33)
            .with_max_time(33);
        let file113 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=4 44")
            .with_max_seq(8)
            .with_min_time(44)
            .with_max_time(44);
        let file114 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=5 55")
            .with_max_seq(10)
            .with_min_time(55)
            .with_max_time(55);
        let file115 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=5 55")
            .with_max_seq(2)
            .with_min_time(55)
            .with_max_time(55);
        let file121 = partition12.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=10 100")
            .with_max_seq(2)
            .with_min_time(99)
            .with_max_time(99);
        let file122 = partition12.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=10 100")
            .with_max_seq(2)
            .with_min_time(100)
            .with_max_time(100);
        let _file123 = partition12.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table2 foo=6 66")
            .with_max_seq(2)
            .with_min_time(66)
            .with_max_time(66);
        let _file211 = partition21.create_parquet_file(builder).await;

        file111.flag_for_delete().await;

        let tombstone1 = table1
            .with_shard(&shard1)
            .create_tombstone(7, 1, 100, "foo=1")
            .await;
        tombstone1.mark_processed(&file112).await;
        let tombstone2 = table1
            .with_shard(&shard1)
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
        // - file123: filtered by predicate
        let pred = Predicate::new().with_range(0, 100);
        let mut chunks = querier_table.chunks_with_predicate(&pred).await.unwrap();
        chunks.sort_by_key(|c| c.id());
        assert_eq!(chunks.len(), 6);

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
        assert_eq!(
            chunks[5].id(),
            ChunkId::new_test(file122.parquet_file.id.get() as u128),
        );

        // check delete predicates
        // file112: marked as processed
        assert_eq!(chunks[0].delete_predicates().len(), 0);
        // file113: has delete predicate
        assert_eq!(chunks[1].delete_predicates().len(), 2);
        // file114: predicates are directly within the chunk range => assume they are materialized
        assert_eq!(chunks[2].delete_predicates().len(), 0);
        // file115: came after in sequence
        assert_eq!(chunks[3].delete_predicates().len(), 0);
        // file121: wrong shard
        assert_eq!(chunks[4].delete_predicates().len(), 0);
        // file122: wrong shard
        assert_eq!(chunks[5].delete_predicates().len(), 0);
    }

    #[tokio::test]
    async fn test_parquet_with_projection_pushdown_to_ingester() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table").await;
        let shard = ns.create_shard(1).await;
        let partition = table.with_shard(&shard).create_partition("k").await;
        let schema = make_schema_two_fields_two_tags(&table).await;

        // let add a partion from the ingester
        let builder = IngesterPartitionBuilder::new(schema, &shard, &partition)
            .with_lp(["table,tag1=val1,tag2=val2 foo=3,bar=4 11"]);

        let ingester_partition =
            builder.build_with_max_parquet_sequence_number(Some(SequenceNumber::new(1)));

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(ingester_partition);

        // Expect one chunk from the ingester
        let pred = Predicate::new().with_range(0, 100);
        let chunks = querier_table
            .chunks_with_predicate_and_projection(&pred, Some(&vec![1])) // only select `foo` column
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        let chunk = &chunks[0];
        assert_eq!(chunk.chunk_type(), "IngesterPartition");

        // verify chunk schema
        let schema = chunk.schema();
        let fields = schema
            .fields_iter()
            .map(|i| i.name().to_string())
            .collect::<Vec<_>>();
        // only foo column. No bar
        assert_eq!(fields, vec!["foo"]);
        // all tags should be present
        let tags = schema
            .tags_iter()
            .map(|i| i.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"tag1".to_string()));
        assert!(tags.contains(&"tag2".to_string()));
        //
        let times = schema
            .time_iter()
            .map(|i| i.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(times, vec!["time"]);

        // verify chunk data
        let batches = chunk
            .data()
            .read_to_batches(chunk.schema(), IOxSessionContext::with_testing().inner())
            .await;
        let expected = vec![
            "+-----+------+------+--------------------------------+",
            "| foo | tag1 | tag2 | time                           |",
            "+-----+------+------+--------------------------------+",
            "| 3   | val1 | val2 | 1970-01-01T00:00:00.000000011Z |",
            "+-----+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_compactor_collision() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table").await;
        let shard = ns.create_shard(1).await;
        let partition = table.with_shard(&shard).create_partition("k").await;
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
        // However there is no way to split the parquet data into the "wanted" and "ignored" part
        // because we don't have row-level sequence numbers.

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table foo=1 11")
            .with_max_seq(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        partition.create_parquet_file(builder).await;

        let builder = IngesterPartitionBuilder::new(schema, &shard, &partition);
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
        // infinite retention
        let ns = catalog.create_namespace_with_retention("ns", None).await;
        let table = ns.create_table("table").await;
        let shard = ns.create_shard(1).await;
        let partition1 = table.with_shard(&shard).create_partition("k1").await;
        let partition2 = table.with_shard(&shard).create_partition("k2").await;
        table.create_column("time", ColumnType::Time).await;
        table.create_column("foo", ColumnType::F64).await;

        // kept because max sequence number <= 2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table foo=1 11")
            .with_max_seq(2);
        let file1 = partition1.create_parquet_file(builder).await;

        // pruned because min sequence number > 2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table foo=2 22")
            .with_max_seq(3);
        partition1.create_parquet_file(builder).await;

        // kept because max sequence number <= 3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table foo=1 11")
            .with_max_seq(3);
        let file2 = partition2.create_parquet_file(builder).await;

        // pruned because min sequence number > 3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table foo=2 22")
            .with_max_seq(4);
        partition2.create_parquet_file(builder).await;

        // partition1: kept because sequence number <= 10
        // partition2: kept because sequence number <= 11
        table
            .with_shard(&shard)
            .create_tombstone(10, 1, 100, "foo=1")
            .await;

        // partition1: pruned because sequence number > 10
        // partition2: kept because sequence number <= 11
        table
            .with_shard(&shard)
            .create_tombstone(11, 1, 100, "foo=2")
            .await;

        // partition1: pruned because sequence number > 10
        // partition2: pruned because sequence number > 11
        table
            .with_shard(&shard)
            .create_tombstone(12, 1, 100, "foo=3")
            .await;

        let schema = SchemaBuilder::new()
            .influx_field("foo", InfluxFieldType::Integer)
            .timestamp()
            .build()
            .unwrap();

        let ingester_chunk_id1 = u128::MAX - 1;

        let builder1 = IngesterPartitionBuilder::new(schema.clone(), &shard, &partition1);
        let builder2 = IngesterPartitionBuilder::new(schema, &shard, &partition2);
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
                // this chunk is filtered out because it has no record batches but the reconciling
                // still takes place
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

        // check types
        assert_eq!(chunks[0].chunk_type(), "parquet");
        assert_eq!(chunks[1].chunk_type(), "parquet");
        assert_eq!(chunks[2].chunk_type(), "IngesterPartition");

        // check delete predicates
        // parquet chunks have predicate attached
        assert_eq!(chunks[0].delete_predicates().len(), 1);
        assert_eq!(chunks[1].delete_predicates().len(), 2);
        // ingester in-mem chunk doesn't need predicates, because the ingester has already
        // materialized them for us
        assert_eq!(chunks[2].delete_predicates().len(), 0);

        // check spans
        let root_span = querier_table
            .traces
            .spans()
            .into_iter()
            .find(|s| s.name == "root")
            .expect("root span not found");
        assert_eq!(root_span.status, SpanStatus::Ok);
        let ip_span = querier_table
            .traces
            .spans()
            .into_iter()
            .find(|s| s.name == "ingester partitions")
            .expect("ingester partitions span not found");
        assert_eq!(ip_span.status, SpanStatus::Ok);
    }

    #[tokio::test]
    async fn test_ingester_overlap_detection() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table").await;
        let shard = ns.create_shard(1).await;
        let partition1 = table.with_shard(&shard).create_partition("k1").await;
        let partition2 = table.with_shard(&shard).create_partition("k2").await;

        let schema = SchemaBuilder::new()
            .influx_field("foo", InfluxFieldType::Integer)
            .timestamp()
            .build()
            .unwrap();

        let builder1 = IngesterPartitionBuilder::new(schema.clone(), &shard, &partition1);
        let builder2 = IngesterPartitionBuilder::new(schema, &shard, &partition2);

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
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table1").await;
        let shard = ns.create_shard(1).await;
        let partition = table.with_shard(&shard).create_partition("k").await;
        let schema = make_schema(&table).await;

        let builder =
            IngesterPartitionBuilder::new(schema, &shard, &partition).with_lp(["table foo=1 1"]);

        // Parquet file between with max sequence number 2
        let pf_builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=1 11")
            .with_max_seq(2);
        partition.create_parquet_file(pf_builder).await;

        let ingester_partition =
            builder.build_with_max_parquet_sequence_number(Some(SequenceNumber::new(2)));

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(ingester_partition);

        // Expect 2 chunks: one for ingester, and one from parquet file
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 2);

        // Now, make a second chunk with max sequence number 3
        let pf_builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=1 22")
            .with_max_seq(3);
        partition.create_parquet_file(pf_builder).await;

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
        // infinite retention
        let ns = catalog.create_namespace_with_retention("ns", None).await;
        let table = ns.create_table("table1").await;
        let shard = ns.create_shard(1).await;
        let partition = table.with_shard(&shard).create_partition("k").await;
        let schema = make_schema(&table).await;
        // Expect 1 chunk with with one delete predicate
        let querier_table = TestQuerierTable::new(&catalog, &table).await;

        let builder =
            IngesterPartitionBuilder::new(schema, &shard, &partition).with_lp(["table foo=1 1"]);

        // parquet file with max sequence number 1
        let pf_builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=1 11")
            .with_max_seq(1);
        partition.create_parquet_file(pf_builder).await;

        // tombstone with max sequence number 2
        table
            .with_shard(&shard)
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
            .with_shard(&shard)
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

    #[tokio::test]
    async fn test_tombstone_cache_refresh_with_retention() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        // 1-hour retention
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table1").await;
        let shard = ns.create_shard(1).await;
        let partition = table.with_shard(&shard).create_partition("k").await;
        let schema = make_schema(&table).await;
        // Expect 1 chunk with with one delete predicate
        let querier_table = TestQuerierTable::new(&catalog, &table).await;

        let builder =
            IngesterPartitionBuilder::new(schema, &shard, &partition).with_lp(["table foo=1 1"]);

        // parquet file with max sequence number 1
        let pf_builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=1 11")
            .with_max_seq(1);
        partition.create_parquet_file(pf_builder).await;

        // tombstone with max sequence number 2
        table
            .with_shard(&shard)
            .create_tombstone(2, 1, 100, "foo=1")
            .await;

        let max_parquet_sequence_number = Some(SequenceNumber::new(1));
        let max_tombstone_sequence_number = Some(SequenceNumber::new(2));
        let ingester_partition =
            builder.build(max_parquet_sequence_number, max_tombstone_sequence_number);

        let querier_table = querier_table.with_ingester_partition(ingester_partition);

        // There must be two delete predicates: one from the tombstone, one from retention
        let deletes = num_deletes(querier_table.chunks().await.unwrap());
        assert_eq!(&deletes, &[2, 0]);
    }

    /// Adds a "foo" column to the table and returns the created schema
    async fn make_schema(table: &Arc<TestTable>) -> Schema {
        table.create_column("foo", ColumnType::F64).await;
        table.create_column("time", ColumnType::Time).await;
        // create corresponding schema
        SchemaBuilder::new()
            .influx_field("foo", InfluxFieldType::Float)
            .timestamp()
            .build()
            .unwrap()
    }

    async fn make_schema_two_fields_two_tags(table: &Arc<TestTable>) -> Schema {
        table.create_column("time", ColumnType::Time).await;
        table.create_column("foo", ColumnType::F64).await;
        table.create_column("bar", ColumnType::F64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;

        // create corresponding schema
        SchemaBuilder::new()
            .influx_field("foo", InfluxFieldType::Float)
            .influx_field("bar", InfluxFieldType::Float)
            .tag("tag1")
            .tag("tag2")
            .timestamp()
            .build()
            .unwrap()
    }

    /// A `QuerierTable` and some number of `IngesterPartitions` that
    /// are fed to the ingester connection on the next call to
    /// `chunks()`
    struct TestQuerierTable {
        /// The underling table
        querier_table: QuerierTable,

        /// Ingester partitions
        ingester_partitions: Vec<IngesterPartition>,

        /// Trace collector
        traces: Arc<RingBufferTraceCollector>,
    }

    impl TestQuerierTable {
        /// Create a new wrapped [`QuerierTable`].
        ///
        /// Uses default chunk load settings.
        async fn new(catalog: &Arc<TestCatalog>, table: &Arc<TestTable>) -> Self {
            Self {
                querier_table: querier_table(catalog, table).await,
                ingester_partitions: vec![],
                traces: Arc::new(RingBufferTraceCollector::new(100)),
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
            self.chunks_with_predicate(&pred).await
        }

        /// Invokes querier_table.chunks modeling the ingester sending the partitions in this table
        async fn chunks_with_predicate(
            &self,
            pred: &Predicate,
        ) -> Result<Vec<Arc<dyn QueryChunk>>> {
            self.chunks_with_predicate_and_projection(pred, None).await
        }

        /// Invokes querier_table.chunks modeling the ingester sending the partitions in this table
        async fn chunks_with_predicate_and_projection(
            &self,
            pred: &Predicate,
            projection: Option<&Vec<usize>>,
        ) -> Result<Vec<Arc<dyn QueryChunk>>> {
            self.querier_table
                .ingester_connection
                .as_ref()
                .unwrap()
                .as_any()
                .downcast_ref::<MockIngesterConnection>()
                .unwrap()
                .next_response(Ok(self.ingester_partitions.clone()));

            let span = Some(Span::root("root", Arc::clone(&self.traces) as _));
            self.querier_table.chunks(pred, span, projection).await
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
}
