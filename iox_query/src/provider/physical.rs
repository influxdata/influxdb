//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::{provider::record_batch_exec::RecordBatchesExec, QueryChunk, QueryChunkData};
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
use schema::Schema;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

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
/// # Empty Inputs
/// For empty inputs (i.e. no chunks), this will create a single [`EmptyExec`] node with appropriate schema.
///
/// # Predicates
/// The give `predicate` will only be applied to [`ParquetExec`] nodes since they are the only node type benifiting from
/// pushdown ([`RecordBatchesExec`] has NO builtin filter function). Delete predicates are NOT applied at all. The
/// caller is responsible for wrapping the output node into appropriate filter nodes.
pub fn chunks_to_physical_nodes(
    iox_schema: Arc<Schema>,
    chunks: Vec<Arc<dyn QueryChunk>>,
    predicate: Predicate,
    context: Arc<TaskContext>,
) -> Arc<dyn ExecutionPlan> {
    if chunks.is_empty() {
        return Arc::new(EmptyExec::new(false, iox_schema.as_arrow()));
    }

    let mut record_batch_chunks: Vec<(SchemaRef, Vec<RecordBatch>, Arc<TableSummary>)> = vec![];
    let mut parquet_chunks: HashMap<String, (ObjectStoreUrl, Vec<ObjectMeta>)> = HashMap::new();

    for chunk in &chunks {
        match chunk.data() {
            QueryChunkData::RecordBatches(batches) => {
                record_batch_chunks.push((chunk.schema().as_arrow(), batches, chunk.summary()));
            }
            QueryChunkData::Parquet(parquet_input) => {
                let url_str = parquet_input.object_store_url.as_str().to_owned();
                match parquet_chunks.entry(url_str) {
                    Entry::Occupied(mut o) => {
                        o.get_mut().1.push(parquet_input.object_meta);
                    }
                    Entry::Vacant(v) => {
                        v.insert((
                            parquet_input.object_store_url,
                            vec![parquet_input.object_meta],
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
    let target_partitions = context.session_config().target_partitions;
    for (_url_str, (url, chunks)) in parquet_chunks {
        let file_groups = distribute(
            chunks.into_iter().map(|object_meta| PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            }),
            target_partitions,
        );
        let base_config = FileScanConfig {
            object_store_url: url,
            file_schema: iox_schema.as_arrow(),
            file_groups,
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            config_options: context.session_config().config_options(),
        };
        output_nodes.push(Arc::new(ParquetExec::new(
            base_config,
            predicate.filter_expr(),
            None,
        )));
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
    use super::*;

    #[test]
    fn test_distribute() {
        assert_eq!(distribute(0..0u8, 1), Vec::<Vec<u8>>::new(),);

        assert_eq!(distribute(0..3u8, 1), vec![vec![0, 1, 2]],);

        assert_eq!(distribute(0..3u8, 2), vec![vec![0, 2], vec![1]],);

        assert_eq!(distribute(0..3u8, 10), vec![vec![0], vec![1], vec![2]],);
    }
}
