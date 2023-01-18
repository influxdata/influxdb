//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::{
    provider::record_batch_exec::RecordBatchesExec, util::arrow_sort_key_exprs, QueryChunk,
    QueryChunkData,
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use data_types::TableSummary;
use datafusion::{
    datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl},
    execution::context::TaskContext,
    physical_plan::{
        empty::EmptyExec,
        file_format::{FileScanConfig, ParquetExec},
        union::UnionExec,
        ExecutionPlan, Statistics,
    },
};
use object_store::ObjectMeta;
use predicate::Predicate;
use schema::{sort::SortKey, Schema};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

/// Holds a list of chunks that all have the same "URL" and
/// will be scanned using the same ParquetExec.
///
/// Also tracks the overall sort key which is provided to DataFusion
/// plans
#[derive(Debug)]
struct ParquetChunkList {
    object_store_url: ObjectStoreUrl,
    object_metas: Vec<ObjectMeta>,
    /// Sort key to place on the ParquetExec, validated to be
    /// compatible with all chunk sort keys
    sort_key: Option<SortKey>,
}

impl ParquetChunkList {
    /// Create a new chunk list with the specified chunk and overall
    /// sort order. If the desired output sort key is specified
    /// (e.g. the partition sort key) also computes compatibility with
    /// with the chunk order.
    fn new(
        object_store_url: ObjectStoreUrl,
        chunk: &dyn QueryChunk,
        meta: ObjectMeta,
        output_sort_key: Option<&SortKey>,
    ) -> Self {
        let sort_key = combine_sort_key(output_sort_key.cloned(), chunk.sort_key());

        Self {
            object_store_url,
            object_metas: vec![meta],
            sort_key,
        }
    }

    /// Add the parquet file the list of files to be scanned, updating
    /// the sort key as necessary.
    fn add_parquet_file(&mut self, chunk: &dyn QueryChunk, meta: ObjectMeta) {
        self.object_metas.push(meta);

        self.sort_key = combine_sort_key(self.sort_key.take(), chunk.sort_key());
    }
}

/// Combines the existing sort key with the sort key of the chunk,
/// returning the new combined compatible sort key that describes both
/// chunks.
///
/// If it is not possible to find a compatible sort key, None is
/// returned signifying "unknown sort order"
fn combine_sort_key(
    existing_sort_key: Option<SortKey>,
    chunk_sort_key: Option<&SortKey>,
) -> Option<SortKey> {
    if let (Some(existing_sort_key), Some(chunk_sort_key)) = (existing_sort_key, chunk_sort_key) {
        let combined_sort_key = SortKey::try_merge_key(&existing_sort_key, chunk_sort_key);

        // Avoid cloning the sort key when possible, as the sort key
        // is likely to commonly be the same
        match combined_sort_key {
            Some(combined_sort_key) if combined_sort_key == &existing_sort_key => {
                Some(existing_sort_key)
            }
            Some(combined_sort_key) => Some(combined_sort_key.clone()),
            None => None,
        }
    } else {
        // no existing sort key means the data wasn't consistently sorted so leave it alone
        None
    }
}

/// Place [chunk](QueryChunk)s into physical nodes.
///
/// This will group chunks into [record batch](QueryChunkData::RecordBatches) and [parquet
/// file](QueryChunkData::Parquet) chunks. The latter will also be grouped by store.
///
/// Record batch chunks will be turned into a single [`RecordBatchesExec`].
///
/// Parquet chunks will be turned into a [`ParquetExec`] per store, each of them with
/// [`target_partitions`](datafusion::execution::context::SessionConfig::target_partitions) file groups.
///
/// If this function creates more than one physical node, they will be combined using an [`UnionExec`]. Otherwise, a
/// single node will be returned directly.
///
/// If output_sort_key is specified, the ParquetExec will be marked
/// with that sort key, otherwise it will be computed from the input chunks. TODO check if this is helpful or not
///
/// # Empty Inputs
/// For empty inputs (i.e. no chunks), this will create a single [`EmptyExec`] node with appropriate schema.
///
/// # Predicates
/// The give `predicate` will only be applied to [`ParquetExec`] nodes since they are the only node type benifiting from
/// pushdown ([`RecordBatchesExec`] has NO builtin filter function). Delete predicates are NOT applied at all. The
/// caller is responsible for wrapping the output node into appropriate filter nodes.
pub fn chunks_to_physical_nodes(
    iox_schema: &Schema,
    output_sort_key: Option<&SortKey>,
    chunks: Vec<Arc<dyn QueryChunk>>,
    predicate: Predicate,
    context: Arc<TaskContext>,
) -> Arc<dyn ExecutionPlan> {
    if chunks.is_empty() {
        return Arc::new(EmptyExec::new(false, iox_schema.as_arrow()));
    }

    let mut record_batch_chunks: Vec<(SchemaRef, Vec<RecordBatch>, Arc<TableSummary>)> = vec![];
    let mut parquet_chunks: HashMap<String, ParquetChunkList> = HashMap::new();

    for chunk in &chunks {
        match chunk.data() {
            QueryChunkData::RecordBatches(batches) => {
                record_batch_chunks.push((chunk.schema().as_arrow(), batches, chunk.summary()));
            }
            QueryChunkData::Parquet(parquet_input) => {
                let url_str = parquet_input.object_store_url.as_str().to_owned();
                match parquet_chunks.entry(url_str) {
                    Entry::Occupied(mut o) => {
                        o.get_mut()
                            .add_parquet_file(chunk.as_ref(), parquet_input.object_meta);
                    }
                    Entry::Vacant(v) => {
                        v.insert(ParquetChunkList::new(
                            parquet_input.object_store_url,
                            chunk.as_ref(),
                            parquet_input.object_meta,
                            output_sort_key,
                        ));
                    }
                }
            }
        }
    }

    let mut output_nodes: Vec<Arc<dyn ExecutionPlan>> = vec![];
    if !record_batch_chunks.is_empty() {
        output_nodes.push(Arc::new(RecordBatchesExec::new(
            record_batch_chunks,
            iox_schema.as_arrow(),
        )));
    }
    let mut parquet_chunks: Vec<_> = parquet_chunks.into_iter().collect();
    parquet_chunks.sort_by_key(|(url_str, _)| url_str.clone());
    let target_partitions = context.session_config().target_partitions();
    for (_url_str, chunk_list) in parquet_chunks {
        let ParquetChunkList {
            object_store_url,
            object_metas,
            sort_key,
        } = chunk_list;

        let file_groups = distribute(
            object_metas.into_iter().map(|object_meta| PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            }),
            target_partitions,
        );

        // Tell datafusion about the sort key, if any
        let file_schema = iox_schema.as_arrow();
        let output_ordering =
            sort_key.map(|sort_key| arrow_sort_key_exprs(&sort_key, &file_schema));

        let base_config = FileScanConfig {
            object_store_url,
            file_schema,
            file_groups,
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering,
            infinite_source: false,
        };
        let meta_size_hint = None;
        let parquet_exec = ParquetExec::new(base_config, predicate.filter_expr(), meta_size_hint);
        output_nodes.push(Arc::new(parquet_exec));
    }

    assert!(!output_nodes.is_empty());
    if output_nodes.len() == 1 {
        output_nodes.pop().expect("checked length")
    } else {
        Arc::new(UnionExec::new(output_nodes))
    }
}

/// Distribute items from the given iterator into `n` containers.
///
/// This will produce less than `n` containers if the input has less than `n` elements.
///
/// # Panic
/// Panics if `n` is 0.
fn distribute<I, T>(it: I, n: usize) -> Vec<Vec<T>>
where
    I: IntoIterator<Item = T>,
{
    assert!(n > 0);

    let mut outputs: Vec<_> = (0..n).map(|_| vec![]).collect();
    let mut pos = 0usize;
    for x in it {
        outputs[pos].push(x);
        pos = (pos + 1) % n;
    }
    outputs.into_iter().filter(|o| !o.is_empty()).collect()
}

#[cfg(test)]
mod tests {
    use schema::sort::SortKeyBuilder;

    use super::*;

    #[test]
    fn test_distribute() {
        assert_eq!(distribute(0..0u8, 1), Vec::<Vec<u8>>::new(),);

        assert_eq!(distribute(0..3u8, 1), vec![vec![0, 1, 2]],);

        assert_eq!(distribute(0..3u8, 2), vec![vec![0, 2], vec![1]],);

        assert_eq!(distribute(0..3u8, 10), vec![vec![0], vec![1], vec![2]],);
    }

    #[test]
    fn test_combine_sort_key() {
        let skey_t1 = SortKeyBuilder::new()
            .with_col("t1")
            .with_col("time")
            .build();

        let skey_t1_t2 = SortKeyBuilder::new()
            .with_col("t1")
            .with_col("t2")
            .with_col("time")
            .build();

        let skey_t2_t1 = SortKeyBuilder::new()
            .with_col("t2")
            .with_col("t1")
            .with_col("time")
            .build();

        assert_eq!(combine_sort_key(None, None), None);
        assert_eq!(combine_sort_key(Some(skey_t1.clone()), None), None);
        assert_eq!(combine_sort_key(None, Some(&skey_t1)), None);

        assert_eq!(
            combine_sort_key(Some(skey_t1.clone()), Some(&skey_t1)),
            Some(skey_t1.clone())
        );

        assert_eq!(
            combine_sort_key(Some(skey_t1.clone()), Some(&skey_t1_t2)),
            Some(skey_t1_t2.clone())
        );

        assert_eq!(
            combine_sort_key(Some(skey_t1_t2.clone()), Some(&skey_t1)),
            Some(skey_t1_t2.clone())
        );

        assert_eq!(
            combine_sort_key(Some(skey_t2_t1.clone()), Some(&skey_t1)),
            Some(skey_t2_t1.clone())
        );

        assert_eq!(combine_sort_key(Some(skey_t2_t1), Some(&skey_t1_t2)), None);
    }
}
