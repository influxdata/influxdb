//! This module is responsible for compacting Ingester's data

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder},
    delete_predicate::DeletePredicate,
    partition_metadata::TableSummary,
};
use datafusion::{
    error::DataFusionError,
    physical_plan::{common::SizedRecordBatchStream, SendableRecordBatchStream},
};
use predicate::{
    delete_predicate::parse_delete_predicate,
    predicate::{Predicate, PredicateMatch},
};
use query::{
    exec::{stringset::StringSet, Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk, QueryChunkMeta,
};
use schema::{merge::SchemaMerger, selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};

use crate::data::PersistingBatch;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error while building logical plan for Ingester's compaction"))]
    LogicalPlan {
        source: query::frontend::reorg::Error,
    },

    #[snafu(display("Error while building physical plan for Ingester's compaction"))]
    PhysicalPlan { source: DataFusionError },

    #[snafu(display("Error while executing Ingester's compaction"))]
    ExecutePlan { source: DataFusionError },

    #[snafu(display("Error while building delete predicate from start time, {}, stop time, {}, and serialized predicate, {}", min, max, predicate))]
    DeletePredicate {
        source: predicate::delete_predicate::Error,
        min: String,
        max: String,
        predicate: String,
    },
}

/// A specialized `Error` for Ingester's Compact errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Compact the given Ingester's data
/// Note: the given `executor` should be created  with the IngesterServer
pub async fn compact(
    executor: &Executor,
    data: Arc<PersistingBatch>,
) -> Result<SendableRecordBatchStream> {
    let chunk = CompactingChunk::new(data);

    // Build logical plan for compaction
    let ctx = executor.new_context(ExecutorType::Reorg);
    let logical_plan = ReorgPlanner::new()
        .scan_single_chunk_plan(chunk.schema(), Arc::new(chunk))
        .context(LogicalPlanSnafu {})?;

    // Build physical plan
    let physical_plan = ctx
        .prepare_plan(&logical_plan)
        .await
        .context(PhysicalPlanSnafu {})?;

    // Execute the plan and return the compacted stream
    let output_stream = ctx
        .execute_stream(physical_plan)
        .await
        .context(ExecutePlanSnafu {})?;
    Ok(output_stream)
}

// todo: move this function to a more appropriate crate
/// Return the merged schema for RecordBatches
///
/// This is infallable because the schemas of chunks within a
/// partition are assumed to be compatible because that schema was
/// enforced as part of writing into the partition
pub fn merge_record_batch_schemas(batches: &[Arc<RecordBatch>]) -> Arc<Schema> {
    let mut merger = SchemaMerger::new();
    for batch in batches {
        let schema = Schema::try_from(batch.schema()).expect("Schema conversion error");
        merger = merger.merge(&schema).expect("schemas compatible");
    }
    Arc::new(merger.build())
}

// ------------------------------------------------------------
/// CompactingChunk is a wrapper of a PersistingBatch that implements
/// QueryChunk and QueryChunkMeta needed to build a query plan to compact its data
#[derive(Debug)]
pub struct CompactingChunk {
    /// Provided ingest data
    pub data: Arc<PersistingBatch>,

    /// Statistic of this data which is currently empty as it may not needed
    pub summary: TableSummary,

    /// Delete predicates to be applied while copacting this
    pub delete_predicates: Vec<Arc<DeletePredicate>>,
}

impl<'a> CompactingChunk {
    /// Create a PersistingChunk for a given PesistingBatch
    pub fn new(data: Arc<PersistingBatch>) -> Self {
        // Empty table summary because PersistingBatch does not keep stats
        // Todo - note to self: the TableSummary is used to get stastistics
        // while planning the scan. This empty column stats may
        // cause some issues during planning. Will verify if it is the case.
        let summary = TableSummary {
            // todo: do not have table name to provide here.
            //   Either accept this ID as the name (used mostly for debug/log) while
            //   running compaction or store table name with the PersistingBatch to
            //   avoid reading the catalog for it
            name: data.table_id.get().to_string(),
            columns: vec![],
        };

        let mut delete_predicates = vec![];
        for delete in &data.deletes {
            let delete_predicate = Arc::new(
                parse_delete_predicate(
                    &delete.min_time.get().to_string(),
                    &delete.max_time.get().to_string(),
                    &delete.serialized_predicate,
                )
                .expect("Error building delete predicate"),
            );

            delete_predicates.push(delete_predicate);
        }

        Self {
            data,
            summary,
            delete_predicates,
        }
    }
}

impl QueryChunkMeta for CompactingChunk<'_> {
    fn summary(&self) -> &TableSummary {
        &self.summary
    }

    fn schema(&self) -> Arc<Schema> {
        // Merge schema of all RecordBatches of the PerstingBatch
        let batches: Vec<Arc<RecordBatch>> =
            self.data.data.iter().map(|s| Arc::clone(&s.data)).collect();
        merge_record_batch_schemas(&batches)
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        self.delete_predicates.as_ref()
    }
}

impl QueryChunk for CompactingChunk<'_> {
    type Error = Error;

    // This function should not be used in PersistingBatch context
    fn id(&self) -> ChunkId {
        unimplemented!()
    }

    // This function should not be used in PersistingBatch context
    fn addr(&self) -> ChunkAddr {
        unimplemented!()
    }

    /// Returns the name of the table stored in this chunk
    fn table_name(&self) -> &str {
        &self.summary.name
    }

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // always true because they are not deduplicated yet
        true
    }

    /// Returns the result of applying the `predicate` to the chunk
    /// using an efficient, but inexact method, based on metadata.
    ///
    /// NOTE: This method is suitable for calling during planning, and
    /// may return PredicateMatch::Unknown for certain types of
    /// predicates.
    fn apply_predicate_to_metadata(
        &self,
        _predicate: &Predicate,
    ) -> Result<PredicateMatch, Self::Error> {
        Ok(PredicateMatch::Unknown)
    }

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        _predicate: &Predicate,
        _columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
        Ok(None)
    }

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error> {
        Ok(None)
    }

    /// Provides access to raw `QueryChunk` data as an
    /// asynchronous stream of `RecordBatch`es filtered by a *required*
    /// predicate. Note that not all chunks can evaluate all types of
    /// predicates and this function will return an error
    /// if requested to evaluate with a predicate that is not supported
    ///
    /// This is the analog of the `TableProvider` in DataFusion
    ///
    /// The reason we can't simply use the `TableProvider` trait
    /// directly is that the data for a particular Table lives in
    /// several chunks within a partition, so there needs to be an
    /// implementation of `TableProvider` that stitches together the
    /// streams from several different `QueryChunk`s.
    fn read_filter(
        &self,
        _predicate: &Predicate, // no needs because all data will be read for compaction
        _selection: Selection<'_>,  // no needs because all columns will be read and compact
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        let batches: Vec<_> = self.data.data.iter().map(|s| Arc::clone(&s.data)).collect();
        let stream = SizedRecordBatchStream::new(self.schema().as_arrow(), batches);
        Ok(Box::pin(stream))
    }

    /// Returns true if data of this chunk is sorted
    fn is_sorted_on_pk(&self) -> bool {
        false
    }

    /// Returns the sort key of the chunk if any
    fn sort_key(&self) -> Option<SortKey<'_>> {
        None
    }

    /// Returns chunk type
    fn chunk_type(&self) -> &str {
        "PersistingBatch"
    }

    // This function should not be used in PersistingBatch context
    fn order(&self) -> ChunkOrder {
        unimplemented!()
    }
}



#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::data::SnapshotBatch;

    use super::*;

    use arrow::{array::{ArrayRef, DictionaryArray, Int64Array, StringArray, BooleanArray, TimestampNanosecondArray, UInt64Array, Float64Array}, datatypes::{Int32Type, DataType, TimeUnit}};
    use iox_catalog::interface::{SequenceNumber, SequencerId, TableId, PartitionId};
    use query::test::{TestChunk, raw_data};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_merge_batch_schema() {

        // Merge schema of the batches
        // The fileds in the schema are sorted by column name
        let batches = create_batches();
        let merged_schema = (&*merge_record_batch_schemas(&batches)).clone();

        // Expected Arrow schema
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("dict", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
            arrow::datatypes::Field::new("int64", DataType::Int64, true),
            arrow::datatypes::Field::new("string", DataType::Utf8, true),
            arrow::datatypes::Field::new("bool", DataType::Boolean, true),
            arrow::datatypes::Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            arrow::datatypes::Field::new("uint64", DataType::UInt64, false),
            arrow::datatypes::Field::new("float64", DataType::Float64, true),
        ]));
        let expected_schema = Schema::try_from(arrow_schema).unwrap().sort_fields_by_name();
        
        assert_eq!(
            expected_schema, merged_schema,
            "\nExpected:\n{:#?}\nActual:\n{:#?}",
            expected_schema, merged_schema
        );
    }

    #[tokio::test]
    async fn test_compact() {
        let batches = create_batches_with_influxtype().await;
        let persisting_batch = make_persisting_batch(batches);
        let exc = Executor::new(1);
        
        let stream = compact(&exc, persisting_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream).await.unwrap();

        println!("output_batches: {:#?}", output_batches);
        
        
        // let table = pretty_format_batches(&[batch]).unwrap();

        // let expected = vec![
        //     "+------+-------+--------+---------+-------+--------+--------------------------------+",
        //     "| dict | int64 | uint64 | float64 | bool  | string | time                           |",
        //     "+------+-------+--------+---------+-------+--------+--------------------------------+",
        //     "| a    | -1    | 1      | 1       | true  | foo    |                                |",
        //     "|      |       |        |         |       |        | 1970-01-01T00:00:00.000000100Z |",
        //     "| b    | 2     | 2      | 2       | false | bar    | 2021-07-20T23:28:50Z           |",
        //     "+------+-------+--------+---------+-------+--------+--------------------------------+",
        // ];

    }

    // ----------------------------------------------------------------------------------------------
    // Data for testing
    // Crerate pure RecordBatches without knowledge of Influx datatype
    fn create_batches() -> Vec<Arc<RecordBatch>> {
        // Batch 1: <dict, i64, str, bool, time>  & 3 rows
        let dict_array: ArrayRef = Arc::new(
            vec![Some("a"), None, Some("b")]  
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let int64_array: ArrayRef =
            Arc::new([Some(-1), None, Some(2)].iter().collect::<Int64Array>());
        let string_array: ArrayRef = Arc::new(
            vec![Some("foo"), Some("and"), Some("bar")] 
                .into_iter()
                .collect::<StringArray>(),
        );
        let bool_array: ArrayRef = Arc::new(
            [Some(true), None, Some(false)]
                .iter()
                .collect::<BooleanArray>(),
        );
        let ts_array: ArrayRef = Arc::new(
            [Some(150), Some(200), Some(1526823730000000000)]
                .iter()
                .collect::<TimestampNanosecondArray>(),
        );
        let batch1 = RecordBatch::try_from_iter_with_nullable(vec![
            ("dict", dict_array, true),
            ("int64", int64_array, true),
            ("string", string_array, true),
            ("bool", bool_array, true),
            ("time", ts_array, false),  // not null
        ])
        .unwrap();


        // Batch 2: <dict, u64, f64, str, bool, time> & 2 rows
        let dict_array: ArrayRef = Arc::new(
            vec![None, Some("d")]  
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );  
        let uint64_array: ArrayRef =
            Arc::new([Some(1), Some(2)].iter().collect::<UInt64Array>()); // not null
        let float64_array: ArrayRef = Arc::new(
            [Some(1.0), Some(2.0)]
                .iter()
                .collect::<Float64Array>(),
        );
        let string_array: ArrayRef = Arc::new(
            vec![Some("foo"), Some("bar")] 
                .into_iter()
                .collect::<StringArray>(),
        );
        let bool_array: ArrayRef = Arc::new(
            [Some(true), None] 
                .iter()
                .collect::<BooleanArray>(),
        );
        let ts_array: ArrayRef = Arc::new(
            [Some(100), Some(1626823730000000000)] // not null
                .iter()
                .collect::<TimestampNanosecondArray>(),
        );
        let batch2 = RecordBatch::try_from_iter_with_nullable (vec![
            ("dict", dict_array, true),
            ("uint64", uint64_array, false),  // not null
            ("float64", float64_array, true),
            ("string", string_array, true),
            ("bool", bool_array, true),
            ("time", ts_array, false), // not null
        ])
        .unwrap();

        vec![Arc::new(batch1), Arc::new(batch2)]
    }

    // RecordBatches with knowledge of influx metadata 
    pub async fn create_batches_with_influxtype() -> Vec<Arc<RecordBatch>> {
        // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
        let mut batches = vec![];

        // This test covers all kind of chunks: overlap, non-overlap without duplicates within, non-overlap with duplicates within
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_ten_rows_of_data_some_duplicates(),
        );
        let batch1 = raw_data(&vec![chunk1]).await[0].clone();
        //println!("BATCH1: {:#?}", batch1);
        batches.push(Arc::new(batch1));

        // chunk2 overlaps with chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );
        let batch2 = raw_data(&vec![chunk2]).await[0].clone();
        //println!("BATCH2: {:#?}", batch2);
        batches.push(Arc::new(batch2));

        // chunk3 no overlap, no duplicates within
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column_with_full_stats(
                    Some(8000),
                    Some(20000),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_three_rows_of_data(),
        );
        let batch3 = raw_data(&vec![chunk3]).await[0].clone();
        //println!("BATCH3: {:#?}", batch3);
        batches.push(Arc::new(batch3));

        // chunk3 no overlap, duplicates within
        let chunk4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column_with_full_stats(
                    Some(28000),
                    Some(220000),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_may_contain_pk_duplicates(true)
                .with_four_rows_of_data(),
        );
        let batch4 = raw_data(&vec![chunk4]).await[0].clone();
        //println!("BATCH4: {:#?}", batch4);
        batches.push(Arc::new(batch4));

        batches
    }

    pub fn make_persisting_batch<'a>(batches: Vec<Arc<RecordBatch>>) -> PersistingBatch {
        // make snapshots for the bacthes
        let mut snapshots = vec![];
        let mut seq_num = 1;
        for batch in batches {
            let seq = SequenceNumber::new(seq_num);
            snapshots.push(make_snapshot_batch(batch, seq, seq ));
            seq_num = seq_num + 1;
        }

        PersistingBatch {
            sequencer_id: SequencerId::new(1),
            table_id: TableId::new(1),
            partition_id: PartitionId::new(1),
            object_store_id: Uuid::new_v4(),
            data: snapshots,
            deletes: vec![],
        }
    }

    pub fn make_snapshot_batch(batch: Arc<RecordBatch>, min: SequenceNumber, max: SequenceNumber) -> SnapshotBatch {
        SnapshotBatch {
            min_sequencer_number: min,
            max_sequencer_number: max,
            data: batch,
        }
    }


}