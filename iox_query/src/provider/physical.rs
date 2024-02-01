//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::statistics::build_statistics_for_chunks;
use crate::{
    provider::record_batch_exec::RecordBatchesExec, util::arrow_sort_key_exprs, QueryChunk,
    QueryChunkData, CHUNK_ORDER_COLUMN_NAME,
};
use arrow::datatypes::{Fields, Schema as ArrowSchema, SchemaRef};
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::{FileScanConfig, ParquetExec},
    },
    physical_expr::PhysicalSortExpr,
    physical_plan::{empty::EmptyExec, expressions::Column, union::UnionExec, ExecutionPlan},
    scalar::ScalarValue,
};
use object_store::ObjectMeta;
use schema::{sort::SortKey, Schema};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

/// Extension for [`PartitionedFile`] to hold the original [`QueryChunk`] and the [`SortKey`] that was passed to [`chunks_to_physical_nodes`].
pub struct PartitionedFileExt {
    pub chunk: Arc<dyn QueryChunk>,
    pub output_sort_key_memo: Option<SortKey>,
}

/// Holds a list of chunks that all have the same "URL" and
/// will be scanned using the same ParquetExec.
///
/// Also tracks the overall sort key which is provided to DataFusion
/// plans
#[derive(Debug)]
struct ParquetChunkList {
    object_store_url: ObjectStoreUrl,
    chunks: Vec<(ObjectMeta, Arc<dyn QueryChunk>)>,
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
        chunk: &Arc<dyn QueryChunk>,
        meta: ObjectMeta,
        output_sort_key: Option<&SortKey>,
    ) -> Self {
        let sort_key = combine_sort_key(output_sort_key.cloned(), chunk.sort_key(), chunk.schema());

        Self {
            object_store_url,
            chunks: vec![(meta, Arc::clone(chunk))],
            sort_key,
        }
    }

    /// Add the parquet file the list of files to be scanned, updating
    /// the sort key as necessary.
    fn add_parquet_file(&mut self, chunk: &Arc<dyn QueryChunk>, meta: ObjectMeta) {
        self.chunks.push((meta, Arc::clone(chunk)));

        self.sort_key = combine_sort_key(self.sort_key.take(), chunk.sort_key(), chunk.schema());
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
    chunk_schema: &Schema,
) -> Option<SortKey> {
    if let (Some(existing_sort_key), Some(chunk_sort_key)) = (existing_sort_key, chunk_sort_key) {
        let combined_sort_key = SortKey::try_merge_key(&existing_sort_key, chunk_sort_key);

        if let Some(combined_sort_key) = combined_sort_key {
            let chunk_cols = chunk_schema
                .iter()
                .map(|(_t, field)| field.name().as_str())
                .collect::<HashSet<_>>();
            for (col, _opts) in combined_sort_key.iter() {
                if !chunk_sort_key.contains(col.as_ref()) && chunk_cols.contains(col.as_ref()) {
                    return None;
                }
            }
        }

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
    schema: &SchemaRef,
    output_sort_key: Option<&SortKey>,
    chunks: Vec<Arc<dyn QueryChunk>>,
    target_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    if chunks.is_empty() {
        return Arc::new(EmptyExec::new(Arc::clone(schema)));
    }

    let mut record_batch_chunks: Vec<Arc<dyn QueryChunk>> = vec![];
    let mut parquet_chunks: HashMap<String, ParquetChunkList> = HashMap::new();

    for chunk in &chunks {
        match chunk.data() {
            QueryChunkData::RecordBatches(_) => {
                record_batch_chunks.push(Arc::clone(chunk));
            }
            QueryChunkData::Parquet(parquet_input) => {
                let url_str = parquet_input.object_store_url.as_str().to_owned();
                match parquet_chunks.entry(url_str) {
                    Entry::Occupied(mut o) => {
                        o.get_mut()
                            .add_parquet_file(chunk, parquet_input.object_meta);
                    }
                    Entry::Vacant(v) => {
                        // better have some instead of no sort information at all
                        let output_sort_key = output_sort_key.or_else(|| chunk.sort_key());
                        v.insert(ParquetChunkList::new(
                            parquet_input.object_store_url,
                            chunk,
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
            Arc::clone(schema),
            output_sort_key.cloned(),
        )));
    }
    let mut parquet_chunks: Vec<_> = parquet_chunks.into_iter().collect();
    parquet_chunks.sort_by_key(|(url_str, _)| url_str.clone());
    let has_chunk_order_col = schema.field_with_name(CHUNK_ORDER_COLUMN_NAME).is_ok();
    for (_url_str, chunk_list) in parquet_chunks {
        let ParquetChunkList {
            object_store_url,
            mut chunks,
            sort_key,
        } = chunk_list;

        // ensure that chunks are actually ordered by chunk order
        chunks.sort_by_key(|(_meta, c)| c.order());

        // Compute statistics for the chunks
        let query_chunks = chunks
            .iter()
            .map(|(_meta, chunk)| Arc::clone(chunk))
            .collect::<Vec<_>>();
        let statistics = build_statistics_for_chunks(&query_chunks, Arc::clone(schema));

        let file_groups = distribute(
            chunks.into_iter().map(|(object_meta, chunk)| {
                let partition_values = if has_chunk_order_col {
                    vec![ScalarValue::from(chunk.order().get())]
                } else {
                    vec![]
                };
                PartitionedFile {
                    object_meta,
                    partition_values,
                    range: None,
                    extensions: Some(Arc::new(PartitionedFileExt {
                        chunk,
                        output_sort_key_memo: output_sort_key.cloned(),
                    })),
                }
            }),
            target_partitions,
        );

        // Tell datafusion about the sort key, if any
        let output_ordering = sort_key.map(|sort_key| arrow_sort_key_exprs(&sort_key, schema));

        let (table_partition_cols, file_schema, output_ordering) = if has_chunk_order_col {
            let table_partition_cols = vec![schema
                .field_with_name(CHUNK_ORDER_COLUMN_NAME)
                .unwrap()
                .clone()];
            let file_schema = Arc::new(ArrowSchema::new(
                schema
                    .fields
                    .iter()
                    .filter(|f| f.name() != CHUNK_ORDER_COLUMN_NAME)
                    .map(Arc::clone)
                    .collect::<Fields>(),
            ));
            let output_ordering = Some(
                output_ordering
                    .unwrap_or_default()
                    .into_iter()
                    .chain(std::iter::once(PhysicalSortExpr {
                        expr: Arc::new(
                            Column::new_with_schema(CHUNK_ORDER_COLUMN_NAME, schema)
                                .expect("just added col"),
                        ),
                        options: Default::default(),
                    }))
                    .collect::<Vec<_>>(),
            );
            (table_partition_cols, file_schema, output_ordering)
        } else {
            (vec![], Arc::clone(schema), output_ordering)
        };

        // No sort order is represented by an empty Vec
        let output_ordering = vec![output_ordering.unwrap_or_default()];

        let base_config = FileScanConfig {
            object_store_url,
            file_schema,
            file_groups,
            statistics,
            projection: None,
            limit: None,
            table_partition_cols,
            output_ordering,
        };
        let meta_size_hint = None;

        let parquet_exec = ParquetExec::new(base_config, None, meta_size_hint);
        output_nodes.push(Arc::new(parquet_exec));
    }

    assert!(!output_nodes.is_empty());
    Arc::new(UnionExec::new(output_nodes))
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
    use datafusion::{
        common::stats::Precision,
        physical_plan::{ColumnStatistics, Statistics},
    };
    use schema::{sort::SortKeyBuilder, InfluxFieldType, SchemaBuilder, TIME_COLUMN_NAME};

    use crate::{
        chunk_order_field,
        statistics::build_statistics_for_chunks,
        test::{format_execution_plan, TestChunk},
    };

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
        let schema_t1 = SchemaBuilder::new().tag("t1").timestamp().build().unwrap();
        let skey_t1 = SortKeyBuilder::new()
            .with_col("t1")
            .with_col(TIME_COLUMN_NAME)
            .build();

        let schema_t1_t2 = SchemaBuilder::new()
            .tag("t1")
            .tag("t2")
            .timestamp()
            .build()
            .unwrap();
        let skey_t1_t2 = SortKeyBuilder::new()
            .with_col("t1")
            .with_col("t2")
            .with_col(TIME_COLUMN_NAME)
            .build();

        let skey_t2_t1 = SortKeyBuilder::new()
            .with_col("t2")
            .with_col("t1")
            .with_col(TIME_COLUMN_NAME)
            .build();

        // output is None if any of the parameters is None (either no sort key requested or chunk is unsorted)
        assert_eq!(combine_sort_key(None, None, &schema_t1), None);
        assert_eq!(
            combine_sort_key(Some(skey_t1.clone()), None, &schema_t1),
            None
        );
        assert_eq!(combine_sort_key(None, Some(&skey_t1), &schema_t1), None);

        // keeping sort key identical works
        assert_eq!(
            combine_sort_key(Some(skey_t1.clone()), Some(&skey_t1), &schema_t1),
            Some(skey_t1.clone())
        );
        assert_eq!(
            combine_sort_key(Some(skey_t1.clone()), Some(&skey_t1), &schema_t1_t2),
            Some(skey_t1.clone())
        );

        // extending sort key works (chunk has more columns than existing key)
        assert_eq!(
            combine_sort_key(Some(skey_t1.clone()), Some(&skey_t1_t2), &schema_t1_t2),
            Some(skey_t1_t2.clone())
        );

        // extending sort key works (quorum has more columns than this chunk)
        assert_eq!(
            combine_sort_key(Some(skey_t1_t2.clone()), Some(&skey_t1), &schema_t1),
            Some(skey_t1_t2.clone())
        );
        assert_eq!(
            combine_sort_key(Some(skey_t2_t1.clone()), Some(&skey_t1), &schema_t1),
            Some(skey_t2_t1.clone())
        );

        // extending does not work if quorum covers columns that the chunk has but that are NOT sorted for that chunk
        assert_eq!(
            combine_sort_key(Some(skey_t1_t2.clone()), Some(&skey_t1), &schema_t1_t2),
            None
        );
        assert_eq!(
            combine_sort_key(Some(skey_t2_t1.clone()), Some(&skey_t1), &schema_t1_t2),
            None
        );

        // column order conflicts are detected
        assert_eq!(
            combine_sort_key(Some(skey_t2_t1), Some(&skey_t1_t2), &schema_t1_t2),
            None
        );
    }

    #[test]
    fn test_chunks_to_physical_nodes_empty() {
        let schema = TestChunk::new("table").schema().as_arrow();
        let plan = chunks_to_physical_nodes(&schema, None, vec![], 2);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " EmptyExec"
        "###
        );
    }

    #[test]
    fn test_chunks_to_physical_nodes_recordbatch() {
        let chunk = TestChunk::new("table");
        let schema = chunk.schema().as_arrow();
        let plan = chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk)], 2);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " UnionExec"
        - "   RecordBatchesExec: chunks=1"
        "###
        );
    }

    #[test]
    fn test_chunks_to_physical_nodes_parquet_one_file() {
        let chunk = TestChunk::new("table").with_dummy_parquet_file();
        let schema = chunk.schema().as_arrow();
        let plan = chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk)], 2);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " UnionExec"
        - "   ParquetExec: file_groups={1 group: [[0.parquet]]}"
        "###
        );
    }

    #[test]
    fn test_chunks_to_physical_nodes_parquet_many_files() {
        let chunk1 = TestChunk::new("table").with_id(0).with_dummy_parquet_file();
        let chunk2 = TestChunk::new("table").with_id(1).with_dummy_parquet_file();
        let chunk3 = TestChunk::new("table").with_id(2).with_dummy_parquet_file();
        let schema = chunk1.schema().as_arrow();
        let plan = chunks_to_physical_nodes(
            &schema,
            None,
            vec![Arc::new(chunk1), Arc::new(chunk2), Arc::new(chunk3)],
            2,
        );
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " UnionExec"
        - "   ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}"
        "###
        );
    }

    #[test]
    fn test_chunks_to_physical_nodes_parquet_many_store() {
        let chunk1 = TestChunk::new("table")
            .with_id(0)
            .with_dummy_parquet_file_and_store("iox1://");
        let chunk2 = TestChunk::new("table")
            .with_id(1)
            .with_dummy_parquet_file_and_store("iox2://");
        let schema = chunk1.schema().as_arrow();
        let plan =
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1), Arc::new(chunk2)], 2);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " UnionExec"
        - "   ParquetExec: file_groups={1 group: [[0.parquet]]}"
        - "   ParquetExec: file_groups={1 group: [[1.parquet]]}"
        "###
        );
    }

    #[test]
    fn test_chunks_to_physical_nodes_mixed() {
        let chunk1 = TestChunk::new("table").with_dummy_parquet_file();
        let chunk2 = TestChunk::new("table");
        let schema = chunk1.schema().as_arrow();
        let plan =
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1), Arc::new(chunk2)], 2);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " UnionExec"
        - "   RecordBatchesExec: chunks=1"
        - "   ParquetExec: file_groups={1 group: [[0.parquet]]}"
        "###
        );
    }

    #[test]
    fn test_chunks_to_physical_nodes_mixed_with_chunk_order() {
        let chunk1 = TestChunk::new("table")
            .with_tag_column("tag")
            .with_dummy_parquet_file();
        let chunk2 = TestChunk::new("table").with_tag_column("tag");
        let schema = Arc::new(ArrowSchema::new(
            chunk1
                .schema()
                .as_arrow()
                .fields
                .iter()
                .cloned()
                .chain(std::iter::once(chunk_order_field()))
                .collect::<Fields>(),
        ));
        let plan =
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1), Arc::new(chunk2)], 2);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " UnionExec"
        - "   RecordBatchesExec: chunks=1, projection=[tag, __chunk_order]"
        - "   ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag, __chunk_order], output_ordering=[__chunk_order@1 ASC]"
        "###
        );
    }

    // reproducer of https://github.com/influxdata/idpe/issues/18287
    #[test]
    fn reproduce_schema_bug_in_parquet_exec() {
        // schema with one tag, one filed, time and CHUNK_ORDER_COLUMN_NAME
        let schema: SchemaRef = SchemaBuilder::new()
            .tag("tag")
            .influx_field("field", InfluxFieldType::Float)
            .timestamp()
            .influx_field(CHUNK_ORDER_COLUMN_NAME, InfluxFieldType::Integer)
            .build()
            .unwrap()
            .into();

        // create a test chunk with one tag, one filed, time and CHUNK_ORDER_COLUMN_NAME
        let record_batch_chunk = Arc::new(
            TestChunk::new("t")
                .with_tag_column_with_stats("tag", Some("AL"), Some("MT"))
                .with_time_column_with_stats(Some(10), Some(20))
                .with_i64_field_column_with_stats("field", Some(0), Some(100))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(5), Some(6)),
        );

        // create them same test chunk but with a parquet file
        let parquet_chunk = Arc::new(
            TestChunk::new("t")
                .with_tag_column_with_stats("tag", Some("AL"), Some("MT"))
                .with_i64_field_column_with_stats("field", Some(0), Some(100))
                .with_time_column_with_stats(Some(10), Some(20))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(5), Some(6))
                .with_dummy_parquet_file(),
        );

        // Build a RecordBatchsExec for record_batch_chunk
        //
        // Use chunks_to_physical_nodes to build a plan with UnionExec on top of RecordBatchesExec
        // Note: I purposely use chunks_to_physical_node to create plan for both record_batch_chunk and parquet_chunk to
        //       consistently create their plan. Also chunks_to_physical_node is used to do create plan in optimization
        //       passes that I will need
        let plan = chunks_to_physical_nodes(
            &schema,
            None,
            vec![Arc::clone(&record_batch_chunk) as Arc<dyn QueryChunk>],
            1,
        );
        // remove union
        let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() else {
            panic!("plan is not a UnionExec");
        };
        let plan_record_batches_exec = Arc::clone(&union_exec.inputs()[0]);
        // verify this is a RecordBatchesExec
        assert!(plan_record_batches_exec
            .as_any()
            .downcast_ref::<RecordBatchesExec>()
            .is_some());

        // Build a ParquetExec for parquet_chunk
        //
        // Use chunks_to_physical_nodes to build a plan with UnionExec on top of ParquetExec
        let plan = chunks_to_physical_nodes(
            &schema,
            None,
            vec![Arc::clone(&parquet_chunk) as Arc<dyn QueryChunk>],
            1,
        );
        // remove union
        let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() else {
            panic!("plan is not a UnionExec");
        };
        let plan_parquet_exec = Arc::clone(&union_exec.inputs()[0]);
        // verify this is a ParquetExec
        assert!(plan_parquet_exec
            .as_any()
            .downcast_ref::<ParquetExec>()
            .is_some());

        // Schema of 2 chunks are the same
        assert_eq!(record_batch_chunk.schema(), parquet_chunk.schema());

        // Schema of the corresponding plans are also the same
        assert_eq!(
            plan_record_batches_exec.schema(),
            plan_parquet_exec.schema()
        );

        // Statistics of 2 chunks are the same
        let record_batch_stats =
            build_statistics_for_chunks(&[record_batch_chunk], Arc::clone(&schema));
        let parquet_stats = build_statistics_for_chunks(&[parquet_chunk], schema);
        assert_eq!(record_batch_stats, parquet_stats);

        // Statistics of the corresponding plans should also be the same except the CHUNK_ORDER_COLUMN_NAME
        // Notes:
        //  1. We do compute stats for CHUNK_ORDER_COLUMN_NAME and store it as in FileScanConfig.statistics
        //     See: https://github.com/influxdata/influxdb_iox/blob/0e5b97d9e913111641f65b9af31e3b3f45f3b14b/iox_query/src/provider/physical.rs#L311C24-L311C24
        //     So, if we get statistics there, we have everything
        //  2. However, if we get statistics through the DF plan's statistics() method, we will not get stats for CHUNK_ORDER_COLUMN_NAME
        //     The reason is we store CHUNK_ORDER_COLUMN_NAME as table_partition_cols in DF and DF has not computed stats for it yet.
        //     See: https://github.com/apache/arrow-datafusion/blob/a9d66e2b492843c2fb335a7dfe27fed073629b09/datafusion/core/src/datasource/physical_plan/file_scan_config.rs#L139
        // When we get the plan's statistics, we won't care about CHUNK_ORDER_COLUMN_NAME becasue it is not a real column.
        // Thus, we are good for now. In the future, if we want a 100% consistent for CHUNK_ORDER_COLUMN_NAME, we need
        // to modify DF to compute stats for table_partition_cols
        //
        // Here both parquet's plan stats and FileScanConfig stats
        //
        // Cast to ParquetExec to get statistics
        let plan_parquet_exec = plan_parquet_exec
            .as_any()
            .downcast_ref::<ParquetExec>()
            .unwrap();
        // stats of the parquet plan generally computed from propagating stats from input plans/chunks/columns
        let parquet_plan_stats = plan_parquet_exec.statistics().unwrap();
        // stats stored in FileScanConfig
        let parqet_file_stats = &plan_parquet_exec.base_config().statistics;

        // stats of IOx specific recod batch plan
        let record_batch_plan_stats = plan_record_batches_exec.statistics().unwrap();

        // Record batch plan stats is the same as parquet file stats and includes everything
        assert_eq!(record_batch_plan_stats, *parqet_file_stats);

        // Verify content
        //
        // Actual columns have stats
        let col_stats = vec![
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Utf8(Some("MT".to_string()))),
                min_value: Precision::Exact(ScalarValue::Utf8(Some("AL".to_string()))),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(20), None)),
                min_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(10), None)),
                distinct_count: Precision::Absent,
            },
        ];
        //
        // Add CHUNK_ORDER_COLUMN_NAME with stats
        let mut parquet_file_col_stats = col_stats.clone();
        parquet_file_col_stats.push(ColumnStatistics {
            null_count: Precision::Absent,
            max_value: Precision::Exact(ScalarValue::Int64(Some(6))),
            min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
            distinct_count: Precision::Absent,
        });
        //
        // Add CHUNK_ORDER_COLUMN_NAME without stats
        let mut parquet_plan_stats_col_stats = col_stats;
        parquet_plan_stats_col_stats.push(ColumnStatistics {
            null_count: Precision::Absent,
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            distinct_count: Precision::Absent,
        });
        //
        let expected_parquet_plan_stats = Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Absent,
            column_statistics: parquet_plan_stats_col_stats,
        };
        //
        let expected_parquet_file_stats = Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Absent,
            column_statistics: parquet_file_col_stats,
        };

        // Content of Record batch plan stats that include stats of CHUNK_ORDER_COLUMN_NAME
        assert_eq!(record_batch_plan_stats, expected_parquet_file_stats);
        // Content of parquet file stats that also include stats of CHUNK_ORDER_COLUMN_NAME
        assert_eq!(*parqet_file_stats, expected_parquet_file_stats);
        //
        // Content of parquet plan stats that does not include stats of CHUNK_ORDER_COLUMN_NAME
        assert_eq!(parquet_plan_stats, expected_parquet_plan_stats);
    }
}
