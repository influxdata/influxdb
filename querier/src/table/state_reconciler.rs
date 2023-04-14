//! Logic to reconcile the catalog and ingester state

mod interface;

use data_types::{DeletePredicate, PartitionId, ShardId, Tombstone, TombstoneId};
use iox_query::QueryChunk;
use observability_deps::tracing::debug;
use schema::sort::SortKey;
use snafu::Snafu;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};
use trace::span::{Span, SpanRecorder};

use crate::{
    cache::CatalogCache, ingester::IngesterChunk, parquet::QuerierParquetChunk,
    tombstone::QuerierTombstone, IngesterPartition,
};

use self::interface::{IngesterPartitionInfo, TombstoneInfo};

#[derive(Snafu, Debug)]
#[allow(missing_copy_implementations)]
pub enum ReconcileError {
    #[snafu(display("Compactor processed file that the querier would need to split apart which is not yet implemented"))]
    CompactorConflict,
}

/// Handles reconciling catalog and ingester state.
#[derive(Debug)]
pub struct Reconciler {
    table_name: Arc<str>,
    namespace_name: Arc<str>,
    catalog_cache: Arc<CatalogCache>,
}

impl Reconciler {
    pub(crate) fn new(
        table_name: Arc<str>,
        namespace_name: Arc<str>,
        catalog_cache: Arc<CatalogCache>,
    ) -> Self {
        Self {
            table_name,
            namespace_name,
            catalog_cache,
        }
    }

    /// Reconciles ingester state (ingester_partitions) and catalog state (parquet_files and
    /// tombstones), producing a list of chunks to query
    pub(crate) async fn reconcile(
        &self,
        ingester_partitions: Vec<IngesterPartition>,
        tombstones: Vec<Arc<Tombstone>>,
        retention_delete_pred: Option<Arc<DeletePredicate>>,
        parquet_files: Vec<QuerierParquetChunk>,
        span: Option<Span>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, ReconcileError> {
        let span_recorder = SpanRecorder::new(span);
        let mut chunks = self
            .build_chunks_from_parquet(
                &ingester_partitions,
                tombstones,
                retention_delete_pred.clone(),
                parquet_files,
                span_recorder.child_span("build_chunks_from_parquet"),
            )
            .await?;
        chunks.extend(self.build_ingester_chunks(ingester_partitions, retention_delete_pred));
        debug!(num_chunks=%chunks.len(), "Final chunk count after reconcilation");

        let chunks = self.sync_partition_sort_keys(chunks);

        let chunks: Vec<Arc<dyn QueryChunk>> = chunks
            .into_iter()
            .map(|c| c.upcast_to_querier_chunk().into())
            .collect();

        Ok(chunks)
    }

    async fn build_chunks_from_parquet(
        &self,
        ingester_partitions: &[IngesterPartition],
        tombstones: Vec<Arc<Tombstone>>,
        retention_delete_pred: Option<Arc<DeletePredicate>>,
        parquet_files: Vec<QuerierParquetChunk>,
        span: Option<Span>,
    ) -> Result<Vec<Box<dyn UpdatableQuerierChunk>>, ReconcileError> {
        let span_recorder = SpanRecorder::new(span);
        debug!(
            namespace=%self.namespace_name(),
            table_name=%self.table_name(),
            ?tombstones,
            num_parquet_files=parquet_files.len(),
            "Reconciling "
        );

        let tombstone_exclusion = tombstone_exclude_list(ingester_partitions, &tombstones);

        let querier_tombstones: Vec<_> =
            tombstones.into_iter().map(QuerierTombstone::from).collect();

        // match chunks and tombstones
        let mut tombstones_by_shard: HashMap<ShardId, Vec<QuerierTombstone>> = HashMap::new();

        for tombstone in querier_tombstones {
            tombstones_by_shard
                .entry(tombstone.shard_id())
                .or_default()
                .push(tombstone);
        }

        debug!(
            namespace=%self.namespace_name(),
            table_name=%self.table_name(),
            n=parquet_files.len(),
            parquet_ids=?parquet_files
                .iter()
                .map(|f| f.meta().parquet_file_id().get())
                .collect::<Vec<_>>(),
            "Parquet files after filtering"
        );

        debug!(num_chunks=%parquet_files.len(), "Created chunks from parquet files");

        let mut chunks: Vec<Box<dyn UpdatableQuerierChunk>> =
            Vec::with_capacity(parquet_files.len() + ingester_partitions.len());

        let retention_expr_len = usize::from(retention_delete_pred.is_some());
        for chunk in parquet_files.into_iter() {
            let tombstones = tombstones_by_shard.get(&chunk.meta().shard_id());

            let tombstones_len = if let Some(tombstones) = tombstones {
                tombstones.len()
            } else {
                0
            };
            let mut delete_predicates = Vec::with_capacity(tombstones_len + retention_expr_len);

            if let Some(tombstones) = tombstones {
                for tombstone in tombstones {
                    // check conditions that don't need catalog access first to avoid unnecessary
                    // catalog load

                    // Check if tombstone should be excluded based on the ingester response
                    if tombstone_exclusion
                        .contains(&(chunk.meta().partition_id(), tombstone.tombstone_id()))
                    {
                        continue;
                    }

                    // Check if tombstone even applies to the sequence number range within the
                    // parquet file. There
                    // are the following cases here:
                    //
                    // 1. Tombstone comes before chunk min sequence number:
                    //    There is no way the tombstone can affect the chunk.
                    // 2. Tombstone comes after chunk max sequence number:
                    //    Tombstone affects whole chunk (it might be marked as processed though,
                    //    we'll check that further down).
                    // 3. Tombstone is in the min-max sequence number range of the chunk:
                    //    Technically the querier has NO way to determine the rows that are
                    //    affected by the tombstone since we have no row-level sequence numbers.
                    //    Such a file can be created by two sources -- the ingester and the
                    //    compactor. The ingester must have materialized the tombstone while
                    //    creating the parquet file, so the querier can skip it. The compactor also
                    //    materialized the tombstones, so we can skip it as well. In the compactor
                    //    case the tombstone will even be marked as processed.
                    //
                    // So the querier only needs to consider the tombstone in case 2.
                    if tombstone.sequence_number() <= chunk.meta().max_sequence_number() {
                        continue;
                    }

                    // TODO: also consider time ranges
                    // (https://github.com/influxdata/influxdb_iox/issues/4086)

                    // check if tombstone is marked as processed
                    if self
                        .catalog_cache
                        .processed_tombstones()
                        .exists(
                            chunk.meta().parquet_file_id(),
                            tombstone.tombstone_id(),
                            span_recorder.child_span("cache GET exists processed_tombstone"),
                        )
                        .await
                    {
                        continue;
                    }

                    delete_predicates.push(Arc::clone(tombstone.delete_predicate()));
                }
            }

            if let Some(retention_delete_pred) = retention_delete_pred.clone() {
                delete_predicates.push(retention_delete_pred);
            }

            let chunk = chunk.with_delete_predicates(delete_predicates);

            chunks.push(Box::new(chunk) as Box<dyn UpdatableQuerierChunk>);
        }

        Ok(chunks)
    }

    fn build_ingester_chunks(
        &self,
        ingester_partitions: Vec<IngesterPartition>,
        retention_delete_pred: Option<Arc<DeletePredicate>>,
    ) -> impl Iterator<Item = Box<dyn UpdatableQuerierChunk>> {
        // Add ingester chunks to the overall chunk list.
        // - filter out chunks that don't have any record batches
        // - tombstones don't need to be applied since they were already materialized by the
        //   ingester
        ingester_partitions
            .into_iter()
            .flat_map(move |c| {
                let c = match &retention_delete_pred {
                    Some(pred) => c.with_delete_predicates(vec![Arc::clone(pred)]),
                    None => c,
                };
                c.into_chunks().into_iter()
            })
            .map(|c| Box::new(c) as Box<dyn UpdatableQuerierChunk>)
    }

    fn sync_partition_sort_keys(
        &self,
        chunks: Vec<Box<dyn UpdatableQuerierChunk>>,
    ) -> Vec<Box<dyn UpdatableQuerierChunk>> {
        // collect latest (= longest) sort key
        // Note that the partition sort key may stale (only a subset of the most recent partition
        // sort key) because newer chunks have new columns.
        // However,  since the querier doesn't (yet) know about these chunks in the `chunks` list above
        // using the most up to date sort key from the chunks it does know about is sufficient.
        let mut sort_keys = HashMap::<PartitionId, Arc<SortKey>>::new();
        for c in &chunks {
            if let Some(sort_key) = c.partition_sort_key_arc() {
                match sort_keys.entry(c.partition_id()) {
                    Entry::Occupied(mut o) => {
                        if sort_key.len() > o.get().len() {
                            *o.get_mut() = sort_key;
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(sort_key);
                    }
                }
            }
        }

        // write partition sort keys to chunks
        chunks
            .into_iter()
            .map(|chunk| {
                let partition_id = chunk.partition_id();
                let sort_key = sort_keys.get(&partition_id);
                chunk.update_partition_sort_key(sort_key.cloned())
            })
            .collect()
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    #[must_use]
    pub fn namespace_name(&self) -> &str {
        self.namespace_name.as_ref()
    }
}

trait UpdatableQuerierChunk: QueryChunk {
    fn partition_sort_key_arc(&self) -> Option<Arc<SortKey>>;

    fn update_partition_sort_key(
        self: Box<Self>,
        sort_key: Option<Arc<SortKey>>,
    ) -> Box<dyn UpdatableQuerierChunk>;

    fn upcast_to_querier_chunk(self: Box<Self>) -> Box<dyn QueryChunk>;
}

impl UpdatableQuerierChunk for QuerierParquetChunk {
    fn partition_sort_key_arc(&self) -> Option<Arc<SortKey>> {
        self.partition_sort_key_arc()
    }

    fn update_partition_sort_key(
        self: Box<Self>,
        sort_key: Option<Arc<SortKey>>,
    ) -> Box<dyn UpdatableQuerierChunk> {
        Box::new(self.with_partition_sort_key(sort_key))
    }

    fn upcast_to_querier_chunk(self: Box<Self>) -> Box<dyn QueryChunk> {
        self as _
    }
}

impl UpdatableQuerierChunk for IngesterChunk {
    fn partition_sort_key_arc(&self) -> Option<Arc<SortKey>> {
        self.partition_sort_key_arc()
    }

    fn update_partition_sort_key(
        self: Box<Self>,
        sort_key: Option<Arc<SortKey>>,
    ) -> Box<dyn UpdatableQuerierChunk> {
        Box::new(self.with_partition_sort_key(sort_key))
    }

    fn upcast_to_querier_chunk(self: Box<Self>) -> Box<dyn QueryChunk> {
        self as _
    }
}

/// Generates "exclude" filter for tombstones.
///
/// Since tombstones are shard-wide but data persistence is partition-based (which are
/// sub-units of shards), we cannot just remove tombstones entirely but need to decide on a
/// per-partition basis. This function generates a lookup table of partition-tombstone tuples that
/// later need to be EXCLUDED/IGNORED when pairing tombstones with chunks.
fn tombstone_exclude_list<I, T>(
    ingester_partitions: &[I],
    tombstones: &[T],
) -> HashSet<(PartitionId, TombstoneId)>
where
    I: IngesterPartitionInfo,
    T: TombstoneInfo,
{
    // Build shard-based lookup table.
    let mut lookup_table: HashMap<ShardId, Vec<&I>> = HashMap::default();
    for partition in ingester_partitions {
        lookup_table
            .entry(partition.shard_id())
            .or_default()
            .push(partition);
    }

    let mut exclude = HashSet::new();
    for t in tombstones {
        if let Some(partitions) = lookup_table.get(&t.shard_id()) {
            for p in partitions {
                if let Some(persisted_max) = p.tombstone_max_sequence_number() {
                    if t.sequence_number() > persisted_max {
                        // newer than persisted => exclude
                        exclude.insert((p.partition_id(), t.id()));
                    } else {
                        // in persisted range => keep
                    }
                } else {
                    // partition has no persisted data at all => need to exclude tombstone which is
                    // too new
                    exclude.insert((p.partition_id(), t.id()));
                }
            }
        }
    }

    exclude
}

#[cfg(test)]
mod tests {
    use super::{interface::ParquetFileInfo, *};
    use data_types::{CompactionLevel, SequenceNumber};

    #[test]
    fn test_filter_tombstones_empty() {
        let actual =
            tombstone_exclude_list::<MockIngesterPartitionInfo, MockTombstoneInfo>(&[], &[]);
        assert!(actual.is_empty());
    }

    #[test]
    fn test_filter_tombstones_many() {
        let ingester_partitions = &[
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(1),
                shard_id: ShardId::new(1),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: Some(SequenceNumber::new(10)),
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(2),
                shard_id: ShardId::new(1),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: None,
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(3),
                shard_id: ShardId::new(1),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: Some(SequenceNumber::new(3)),
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(4),
                shard_id: ShardId::new(2),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: Some(SequenceNumber::new(7)),
            },
        ];
        let tombstones = &[
            MockTombstoneInfo {
                id: TombstoneId::new(1),
                shard_id: ShardId::new(1),
                sequence_number: SequenceNumber::new(2),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(2),
                shard_id: ShardId::new(1),
                sequence_number: SequenceNumber::new(3),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(3),
                shard_id: ShardId::new(1),
                sequence_number: SequenceNumber::new(4),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(4),
                shard_id: ShardId::new(1),
                sequence_number: SequenceNumber::new(9),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(5),
                shard_id: ShardId::new(1),
                sequence_number: SequenceNumber::new(10),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(6),
                shard_id: ShardId::new(1),
                sequence_number: SequenceNumber::new(11),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(7),
                shard_id: ShardId::new(2),
                sequence_number: SequenceNumber::new(6),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(8),
                shard_id: ShardId::new(2),
                sequence_number: SequenceNumber::new(7),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(9),
                shard_id: ShardId::new(2),
                sequence_number: SequenceNumber::new(8),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(10),
                shard_id: ShardId::new(3),
                sequence_number: SequenceNumber::new(10),
            },
        ];

        let actual = tombstone_exclude_list(ingester_partitions, tombstones);
        let expected = HashSet::from([
            (PartitionId::new(1), TombstoneId::new(6)),
            (PartitionId::new(2), TombstoneId::new(1)),
            (PartitionId::new(2), TombstoneId::new(2)),
            (PartitionId::new(2), TombstoneId::new(3)),
            (PartitionId::new(2), TombstoneId::new(4)),
            (PartitionId::new(2), TombstoneId::new(5)),
            (PartitionId::new(2), TombstoneId::new(6)),
            (PartitionId::new(3), TombstoneId::new(3)),
            (PartitionId::new(3), TombstoneId::new(4)),
            (PartitionId::new(3), TombstoneId::new(5)),
            (PartitionId::new(3), TombstoneId::new(6)),
            (PartitionId::new(4), TombstoneId::new(9)),
        ]);
        assert_eq!(actual, expected);
    }

    #[derive(Debug)]
    struct MockIngesterPartitionInfo {
        partition_id: PartitionId,
        shard_id: ShardId,
        parquet_max_sequence_number: Option<SequenceNumber>,
        tombstone_max_sequence_number: Option<SequenceNumber>,
    }

    impl IngesterPartitionInfo for MockIngesterPartitionInfo {
        fn partition_id(&self) -> PartitionId {
            self.partition_id
        }

        fn shard_id(&self) -> ShardId {
            self.shard_id
        }

        fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
            self.parquet_max_sequence_number
        }

        fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
            self.tombstone_max_sequence_number
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockParquetFileInfo {
        partition_id: PartitionId,
        max_sequence_number: SequenceNumber,
        compaction_level: CompactionLevel,
    }

    impl ParquetFileInfo for MockParquetFileInfo {
        fn partition_id(&self) -> PartitionId {
            self.partition_id
        }

        fn max_sequence_number(&self) -> SequenceNumber {
            self.max_sequence_number
        }

        fn compaction_level(&self) -> CompactionLevel {
            self.compaction_level
        }
    }

    #[derive(Debug)]
    struct MockTombstoneInfo {
        id: TombstoneId,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    }

    impl TombstoneInfo for MockTombstoneInfo {
        fn id(&self) -> TombstoneId {
            self.id
        }

        fn shard_id(&self) -> ShardId {
            self.shard_id
        }

        fn sequence_number(&self) -> SequenceNumber {
            self.sequence_number
        }
    }
}
