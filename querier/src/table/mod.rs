use self::query_access::QuerierTableChunkPruner;
use crate::{
    cache::{
        namespace::CachedTable,
        partition::{CachedPartition, PartitionRequest},
    },
    ingester::{self, IngesterPartition},
    parquet::ChunkAdapter,
    IngesterConnection,
};
use data_types::{ColumnId, NamespaceId, ParquetFile, TableId, TransitionPartitionId};
use datafusion::error::DataFusionError;
use futures::join;
use iox_query::{provider, provider::ChunkPruner, QueryChunk};
use observability_deps::tracing::{debug, trace};
use predicate::Predicate;
use schema::Schema;
use snafu::{ResultExt, Snafu};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use trace::span::{Span, SpanRecorder};
use uuid::Uuid;

pub use self::query_access::metrics::PruneMetrics;

mod query_access;

#[cfg(test)]
mod test_util;

#[derive(Debug, Snafu)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    #[snafu(display("Error getting partitions from ingester: {}", source))]
    GettingIngesterPartitions { source: ingester::Error },

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
    pub namespace_id: NamespaceId,
    pub namespace_name: Arc<str>,
    pub namespace_retention_period: Option<Duration>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub schema: Schema,
    pub ingester_connection: Option<Arc<dyn IngesterConnection>>,
    pub chunk_adapter: Arc<ChunkAdapter>,
    pub prune_metrics: Arc<PruneMetrics>,
}

/// Table representation for the querier.
#[derive(Debug)]
pub struct QuerierTable {
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

    /// Metrics for chunk pruning.
    prune_metrics: Arc<PruneMetrics>,
}

impl QuerierTable {
    /// Create new table.
    pub fn new(args: QuerierTableArgs) -> Self {
        let QuerierTableArgs {
            namespace_id,
            namespace_name,
            namespace_retention_period,
            table_id,
            table_name,
            schema,
            ingester_connection,
            chunk_adapter,
            prune_metrics,
        } = args;

        Self {
            namespace_name,
            namespace_id,
            namespace_retention_period,
            table_name,
            table_id,
            schema,
            ingester_connection,
            chunk_adapter,
            prune_metrics,
        }
    }

    /// Table name.
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }

    /// Table ID.
    pub fn id(&self) -> TableId {
        self.table_id
    }

    /// Schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Query all chunks within this table.
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

        let catalog_cache = self.chunk_adapter.catalog_cache();

        // Ask ingesters for data, also optimistically fetching catalog
        // contents at the same time to pre-warm cache.
        //
        // We don't wanna wait for the cache though because we have a the actual cache request later anyways that might
        // even invalidate what we did during warm-up. The cache system keeps requests running in the background
        // anyways, so if the warm-up fetched up-to-date data, we'll get that from the cache later.
        let ingester_ready = CancellationToken::new();
        let (partitions, _) = join!(
            async {
                let partitions = self
                    .ingester_partitions(
                        predicate,
                        span_recorder.child_span("ingester partitions"),
                        projection,
                    )
                    .await;
                ingester_ready.cancel();
                partitions
            },
            async {
                tokio::select! {
                    _ = catalog_cache.parquet_file().get(
                        self.id(),
                        None,
                        span_recorder.child_span("cache GET parquet_file (pre-warm)")
                    ) => {},
                    _ = ingester_ready.cancelled() => {},
                }
            },
        );

        // handle errors / cache refresh
        let partitions = partitions?;

        // Determine number of persisted parquet files per ingester UUID seen in the ingester query
        // responses for cache invalidation. If `persisted_file_counts_by_ingester_uuid` is empty,
        // then there are no results from the ingesters.
        let persisted_file_counts_by_ingester_uuid = collect_persisted_file_counts(
            partitions.len(),
            partitions
                .iter()
                .map(|p| (p.ingester_uuid(), p.completed_persistence_count())),
        );

        debug!(
            namespace=%self.namespace_name,
            table_name=%self.table_name(),
            num_ingester_partitions=%partitions.len(),
            "Ingester partitions fetched"
        );

        // Now fetch the actual contents of the catalog we need
        // NB: Pass max parquet sequence numbers to `get`
        //     to ensure cache is refreshed if we learned about new files.
        let parquet_files = catalog_cache
            .parquet_file()
            .get(
                self.id(),
                Some(persisted_file_counts_by_ingester_uuid),
                span_recorder.child_span("cache GET parquet_file"),
            )
            .await;

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
        let cached_partitions = self
            .fetch_cached_partitions(
                cached_table,
                &partitions,
                &parquet_files.files,
                span_recorder.child_span("fetch cached partitions"),
            )
            .await;

        // create parquet files
        let parquet_files = self
            .chunk_adapter
            .new_chunks(
                Arc::clone(cached_table),
                Arc::clone(&parquet_files.files),
                &cached_partitions,
                span_recorder.child_span("new_chunks"),
            )
            .await;

        // build final chunk list
        let chunks = partitions
            .into_iter()
            .filter_map(|mut c| {
                let cached_partition = cached_partitions.get(&c.partition_id())?;
                c.set_partition_column_ranges(&cached_partition.column_ranges);
                Some(c)
            })
            .flat_map(|c| c.into_chunks().into_iter())
            .map(|c| Arc::new(c) as Arc<dyn QueryChunk>)
            .chain(
                parquet_files
                    .into_iter()
                    .map(|c| Arc::new(c) as Arc<dyn QueryChunk>),
            )
            .collect::<Vec<_>>();
        trace!("Fetched chunks");

        let num_initial_chunks = chunks.len();
        let chunks = self
            .chunk_pruner()
            .prune_chunks(
                self.table_name(),
                // use up-to-date schema
                &cached_table.schema,
                chunks,
                predicate,
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

    async fn fetch_cached_partitions(
        &self,
        cached_table: &Arc<CachedTable>,
        ingester_partitions: &[IngesterPartition],
        parquet_files: &[Arc<ParquetFile>],
        span: Option<Span>,
    ) -> HashMap<TransitionPartitionId, CachedPartition> {
        let span_recorder = SpanRecorder::new(span);

        let mut should_cover: HashMap<TransitionPartitionId, HashSet<ColumnId>> =
            HashMap::with_capacity(ingester_partitions.len());

        // For ingester partitions we only need the column ranges -- which are static -- not the
        // sort key. So it is sufficient to collect the partition IDs.
        for p in ingester_partitions {
            should_cover.entry(p.partition_id()).or_default();
        }

        // For parquet files we must ensure that the -- potentially evolving -- sort key coveres
        // the primary key.
        let pk = cached_table
            .primary_key_column_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        for f in parquet_files {
            should_cover
                .entry(f.partition_id.clone())
                .or_default()
                .extend(f.column_set.iter().copied().filter(|id| pk.contains(id)));
        }

        // batch request all partitions
        let requests = should_cover
            .into_iter()
            .map(|(id, cover)| PartitionRequest {
                partition_id: id,
                sort_key_should_cover: cover.into_iter().collect(),
            })
            .collect();
        let partitions = self
            .chunk_adapter
            .catalog_cache()
            .partition()
            .get(
                Arc::clone(cached_table),
                requests,
                span_recorder.child_span("fetch partitions"),
            )
            .await;

        partitions.into_iter().map(|p| (p.id.clone(), p)).collect()
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
                self.namespace_id,
                cached_table,
                columns,
                predicate,
                span_recorder.child_span("IngesterConnection partitions"),
            )
            .await
            .context(GettingIngesterPartitionsSnafu);

        let partitions = partitions_result?;

        Ok(partitions)
    }

    /// clear the parquet file cache
    #[cfg(test)]
    fn clear_parquet_cache(&self) {
        self.chunk_adapter
            .catalog_cache()
            .parquet_file()
            .expire(self.table_id)
    }
}

// Given metadata from a list of ingester request [`PartitionData`]s, sum the total completed
// persistence counts for each ingester UUID so that the Parquet file cache can see if it knows
// about a different set of ingester UUIDs or a different number of persisted Parquet files and
// therefore needs to refresh its view of the catalog.
fn collect_persisted_file_counts(
    capacity: usize,
    partitions: impl Iterator<Item = (Uuid, u64)>,
) -> HashMap<Uuid, u64> {
    partitions.fold(
        HashMap::with_capacity(capacity),
        |mut map, (uuid, count)| {
            let sum = map.entry(uuid).or_default();
            *sum += count;
            map
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::test_util::{assert_cache_access_metric_count, assert_catalog_access_metric_count},
        ingester::{test_util::MockIngesterConnection, IngesterPartition},
        table::test_util::{querier_table, IngesterPartitionBuilder},
    };
    use arrow::datatypes::DataType;
    use arrow_util::assert_batches_eq;
    use data_types::{ChunkId, ColumnType};
    use datafusion::{
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use generated_types::influxdata::iox::partition_template::v1::{
        template_part::Part, PartitionTemplate, TemplatePart,
    };
    use iox_query::{chunk_statistics::ColumnRange, exec::IOxSessionContext};
    use iox_tests::{TestCatalog, TestParquetFileBuilder, TestTable};
    use predicate::Predicate;
    use schema::{builder::SchemaBuilder, InfluxFieldType, TIME_COLUMN_NAME};
    use std::sync::Arc;
    use test_helpers::maybe_start_logging;
    use trace::RingBufferTraceCollector;

    #[test]
    fn sum_up_persisted_file_counts() {
        let output = collect_persisted_file_counts(0, std::iter::empty());
        assert!(
            output.is_empty(),
            "Expected output to be empty, instead was: {output:?}"
        );

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let input = [(uuid1, 20), (uuid1, 22), (uuid2, 30)];
        let output = collect_persisted_file_counts(input.len(), input.into_iter());
        assert_eq!(output.len(), 2);
        assert_eq!(*output.get(&uuid1).unwrap(), 42);
        assert_eq!(*output.get(&uuid2).unwrap(), 30);
    }

    #[tokio::test]
    async fn test_parquet_chunks() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        // Namespace with infinite retention policy
        let ns = catalog.create_namespace_with_retention("ns", None).await;

        let table1 = ns.create_table("table1").await;
        let table2 = ns.create_table("table2").await;

        let partition11 = table1.create_partition("k").await;
        let partition12 = table1.create_partition("k").await;
        let partition21 = table2.create_partition("k").await;

        table1.create_column("time", ColumnType::Time).await;
        table1.create_column("foo", ColumnType::F64).await;
        table2.create_column("time", ColumnType::Time).await;
        table2.create_column("foo", ColumnType::F64).await;

        let querier_table = TestQuerierTable::new(&catalog, &table1).await;

        // no parquet files yet
        assert!(querier_table.chunks().await.unwrap().is_empty());

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=1 11")
            .with_min_time(11)
            .with_max_time(11);
        let file111 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=2 22")
            .with_min_time(22)
            .with_max_time(22);
        let file112 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=3 33")
            .with_min_time(33)
            .with_max_time(33);
        let file113 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=4 44")
            .with_min_time(44)
            .with_max_time(44);
        let file114 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=5 55")
            .with_min_time(55)
            .with_max_time(55);
        let file115 = partition11.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=5 55")
            .with_min_time(55)
            .with_max_time(55);
        let file121 = partition12.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=10 100")
            .with_min_time(99)
            .with_max_time(99);
        let file122 = partition12.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table1 foo=10 100")
            .with_min_time(100)
            .with_max_time(100);
        let _file123 = partition12.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("table2 foo=6 66")
            .with_min_time(66)
            .with_max_time(66);
        let _file211 = partition21.create_parquet_file(builder).await;

        file111.flag_for_delete().await;

        // As we have now made new parquet files, force a cache refresh
        querier_table.inner().clear_parquet_cache();

        // now we have some files
        // this contains all files except for:
        // - file111: marked for delete
        // - file221: wrong table
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
    }

    #[tokio::test]
    async fn test_parquet_with_projection_pushdown_to_ingester() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table").await;
        let partition = table.create_partition("k").await;
        let schema = make_schema_two_fields_two_tags(&table).await;

        // let add a partion from the ingester
        let builder = IngesterPartitionBuilder::new(schema, &partition)
            .with_lp(["table,tag1=val1,tag2=val2 foo=3,bar=4 11"]);

        let ingester_partition = builder.build();

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
            "| 3.0 | val1 | val2 | 1970-01-01T00:00:00.000000011Z |",
            "+-----+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_parquet_cache_refresh() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table1").await;
        let partition = table.create_partition("k").await;
        let schema = make_schema(&table).await;

        let builder = IngesterPartitionBuilder::new(schema, &partition).with_lp(["table foo=1 1"]);

        // Parquet file between with max sequence number 2
        let pf_builder = TestParquetFileBuilder::default().with_line_protocol("table1 foo=1 11");
        partition.create_parquet_file(pf_builder).await;

        let ingester_partition = builder.build();

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(ingester_partition);

        // Expect 2 chunks: one for ingester, and one from parquet file
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 2);

        // Now, make a second chunk with max sequence number 3
        let pf_builder = TestParquetFileBuilder::default().with_line_protocol("table1 foo=1 22");
        partition.create_parquet_file(pf_builder).await;

        // With the same ingester response, still expect 2 chunks: one
        // for ingester, and one from parquet file
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 2);

        // update the ingester response
        let ingester_partition = builder.build();

        let querier_table = querier_table
            .clear_ingester_partitions()
            .with_ingester_partition(ingester_partition);

        // expect the second file is found, resulting in three chunks
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn test_custom_partitioning() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns
            .create_table_with_partition_template(
                "table",
                Some(PartitionTemplate {
                    parts: vec![TemplatePart {
                        part: Some(Part::TagValue(String::from("tag1"))),
                    }],
                }),
            )
            .await;
        let partition_a = table.create_partition("val1a").await;
        let partition_b = table.create_partition("val1b").await;
        let schema = make_schema_two_fields_two_tags(&table).await;

        let file1 = partition_a
            .create_parquet_file(
                TestParquetFileBuilder::default()
                    .with_line_protocol("table,tag1=val1a,tag2=val2a foo=3,bar=4 10")
                    .with_min_time(10)
                    .with_max_time(10),
            )
            .await;
        let _file2 = partition_b
            .create_parquet_file(
                TestParquetFileBuilder::default()
                    .with_line_protocol("table,tag1=val1b,tag2=val2b foo=3,bar=4 10")
                    .with_min_time(10)
                    .with_max_time(10),
            )
            .await;

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(
                IngesterPartitionBuilder::new(schema.clone(), &partition_a)
                    .with_lp(["table,tag1=val1a,tag2=val2a foo=3,bar=4 11"])
                    .with_colum_ranges(Arc::new(HashMap::from([(
                        Arc::from("tag1"),
                        ColumnRange {
                            min_value: Arc::new(ScalarValue::from("val1a")),
                            max_value: Arc::new(ScalarValue::from("val1a")),
                        },
                    )])))
                    .build(),
            )
            .with_ingester_partition(
                IngesterPartitionBuilder::new(schema, &partition_b)
                    .with_lp(["table,tag1=val1b,tag2=val2b foo=5,bar=6 11"])
                    .with_colum_ranges(Arc::new(HashMap::from([(
                        Arc::from("tag1"),
                        ColumnRange {
                            min_value: Arc::new(ScalarValue::from("val1b")),
                            max_value: Arc::new(ScalarValue::from("val1b")),
                        },
                    )])))
                    .build(),
            );

        // Expect one chunk from the ingester
        let pred = Predicate::new().with_expr(col("tag1").eq(lit(ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::from("val1a")),
        ))));
        let mut chunks = querier_table
            .chunks_with_predicate_and_projection(&pred, None)
            .await
            .unwrap();
        chunks.sort_by_key(|c| c.chunk_type().to_owned());
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk_type(), "IngesterPartition");
        assert_eq!(chunks[1].chunk_type(), "parquet");
        assert_eq!(
            chunks[1].id().get().as_u128(),
            file1.parquet_file.id.get() as u128
        );
    }

    #[tokio::test]
    async fn test_partition_caching() {
        maybe_start_logging();
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table").await;
        let partition_1 = table.create_partition("p1").await;
        let partition_2 = table.create_partition("p2").await;
        let schema = make_schema_two_fields_two_tags(&table).await;

        partition_1
            .create_parquet_file(
                TestParquetFileBuilder::default().with_line_protocol("table foo=1,bar=1 11"),
            )
            .await;
        partition_1
            .create_parquet_file(
                TestParquetFileBuilder::default().with_line_protocol("table foo=2,bar=2 22"),
            )
            .await;
        partition_1
            .create_parquet_file(
                TestParquetFileBuilder::default().with_line_protocol("table foo=3,bar=3 33"),
            )
            .await;
        partition_2
            .create_parquet_file(
                TestParquetFileBuilder::default().with_line_protocol("table foo=1,bar=1 11"),
            )
            .await;

        let ingester_partition_builder = IngesterPartitionBuilder::new(
            schema.select_by_names(&["foo", TIME_COLUMN_NAME]).unwrap(),
            &partition_1,
        )
        .with_lp(["table foo=1 1"]);

        // set up performs a few lookups
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_hash_id", 4);

        let querier_table = TestQuerierTable::new(&catalog, &table)
            .await
            .with_ingester_partition(ingester_partition_builder.build());

        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 5);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_hash_id", 4);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );
        assert_cache_access_metric_count(&catalog.metric_registry, "partition", 2);

        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 5);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_hash_id", 4);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );
        assert_cache_access_metric_count(&catalog.metric_registry, "partition", 4);

        partition_2
            .create_parquet_file(
                TestParquetFileBuilder::default().with_line_protocol("table,tag1=a foo=1,bar=1 11"),
            )
            .await;
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_hash_id", 5);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );

        // file not visible yet
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 5);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_hash_id", 5);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );
        assert_cache_access_metric_count(&catalog.metric_registry, "partition", 6);

        // change inster ID => invalidates cache
        let querier_table = querier_table
            .clear_ingester_partitions()
            .with_ingester_partition(ingester_partition_builder.build());
        let chunks = querier_table.chunks().await.unwrap();
        assert_eq!(chunks.len(), 6);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_hash_id", 5);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            2,
        );
        assert_cache_access_metric_count(&catalog.metric_registry, "partition", 8);
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
}
