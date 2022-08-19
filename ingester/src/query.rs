//! Module to handle query on Ingester's data

use crate::data::{QueryableBatch, SnapshotBatch};
use arrow::record_batch::RecordBatch;
use arrow_util::util::ensure_schema;
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, PartitionId, SequenceNumber, TableSummary,
    TimestampMinMax, Tombstone,
};
use datafusion::physical_plan::{
    common::SizedRecordBatchStream,
    metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics},
    SendableRecordBatchStream,
};
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkError, QueryChunkMeta,
};
use observability_deps::tracing::trace;
use predicate::{
    delete_predicate::{tombstones_to_delete_predicates, tombstones_to_delete_predicates_iter},
    Predicate,
};
use schema::{merge::merge_record_batch_schemas, selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};
use std::{any::Any, sync::Arc};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Internal error concatenating record batches {}", source))]
    Schema { source: schema::Error },

    #[snafu(display("Internal error concatenating record batches {}", source))]
    ConcatBatches { source: arrow::error::ArrowError },

    #[snafu(display("Internal error filtering columns from a record batch {}", source))]
    FilterColumns { source: crate::data::Error },

    #[snafu(display("Internal error filtering record batch: {}", source))]
    FilterBatch { source: arrow::error::ArrowError },
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl QueryableBatch {
    /// Initilaize a QueryableBatch
    pub fn new(table_name: &str, data: Vec<Arc<SnapshotBatch>>, deletes: Vec<Tombstone>) -> Self {
        let delete_predicates = tombstones_to_delete_predicates(&deletes);
        Self {
            data,
            delete_predicates,
            table_name: table_name.to_string(),
        }
    }

    /// Add snapshots to this batch
    pub fn with_data(mut self, mut data: Vec<Arc<SnapshotBatch>>) -> Self {
        self.data.append(&mut data);
        self
    }

    /// Add more tombstones
    pub fn add_tombstones(&mut self, deletes: &[Tombstone]) {
        let delete_predicates = tombstones_to_delete_predicates_iter(deletes);
        self.delete_predicates.extend(delete_predicates);
    }

    /// return min and max of all the snapshots
    pub fn min_max_sequence_numbers(&self) -> (SequenceNumber, SequenceNumber) {
        let min = self
            .data
            .first()
            .expect("The Queryable Batch should not empty")
            .min_sequence_number;

        let max = self
            .data
            .first()
            .expect("The Queryable Batch should not empty")
            .max_sequence_number;

        assert!(min <= max);

        (min, max)
    }

    /// return true if it has no data
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl QueryChunkMeta for QueryableBatch {
    fn summary(&self) -> Option<Arc<TableSummary>> {
        None
    }

    fn schema(&self) -> Arc<Schema> {
        // TODO: may want store this schema as a field of QueryableBatch and
        // only do this schema merge the first time it is called

        // Merge schema of all RecordBatches of the PerstingBatch
        let batches: Vec<Arc<RecordBatch>> =
            self.data.iter().map(|s| Arc::clone(&s.data)).collect();
        merge_record_batch_schemas(&batches)
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        None // Ingester data has not persisted yet and should not be attached to any partition
    }

    fn partition_id(&self) -> Option<PartitionId> {
        // Ingetser data is not officially attached to any parittion id yet.
        // Only persisting & persisted data has partition id and we want to keep it that way
        None
    }

    fn sort_key(&self) -> Option<&SortKey> {
        None // Ingester data is not sorted
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        self.delete_predicates.as_ref()
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        // Note: we need to consider which option we want to go with
        //  . Return None here and avoid taking time to compute time's min max of RecordBacthes (current choice)
        //  . Compute time's min max here and avoid compacting non-overlapped QueryableBatches in the Ingester
        None
    }
}

impl QueryChunk for QueryableBatch {
    // This function should not be used in QueryBatch context
    fn id(&self) -> ChunkId {
        // To return a value for debugging and make it consistent with ChunkId created in Compactor,
        // use Uuid for this
        ChunkId::new()
    }

    /// Returns the name of the table stored in this chunk
    fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // always true because they are not deduplicated yet
        true
    }

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        _ctx: IOxSessionContext,
        _predicate: &Predicate,
        _columns: Selection<'_>,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        Ok(None)
    }

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        _ctx: IOxSessionContext,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        Ok(None)
    }

    /// Provides access to raw `QueryChunk` data as an
    /// asynchronous stream of `RecordBatch`es
    fn read_filter(
        &self,
        mut ctx: IOxSessionContext,
        _predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, QueryChunkError> {
        ctx.set_metadata("storage", "ingester");
        ctx.set_metadata("projection", format!("{}", selection));
        trace!(?selection, "selection");

        let schema = self.schema().select(selection).context(SchemaSnafu)?;

        // Get all record batches from their snapshots
        let batches = self
            .data
            .iter()
            .filter_map(|snapshot| {
                let batch = snapshot
                    // Only return columns in the selection
                    .scan(selection)
                    .context(FilterColumnsSnafu {})
                    .transpose()?
                    // ensure batch has desired schema
                    .and_then(|batch| {
                        ensure_schema(&schema.as_arrow(), &batch).context(ConcatBatchesSnafu {})
                    })
                    .map(Arc::new);
                Some(batch)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Return stream of data
        let dummy_metrics = ExecutionPlanMetricsSet::new();
        let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
        let stream = SizedRecordBatchStream::new(schema.as_arrow(), batches, mem_metrics);
        Ok(Box::pin(stream))
    }

    /// Returns chunk type
    fn chunk_type(&self) -> &str {
        "PersistingBatch"
    }

    // This function should not be used in PersistingBatch context
    fn order(&self) -> ChunkOrder {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::create_tombstone;
    use arrow::{
        array::{
            ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
            TimestampNanosecondArray, UInt64Array,
        },
        datatypes::{DataType, Int32Type, TimeUnit},
    };
    use data_types::{DeleteExpr, Op, Scalar, TimestampRange};

    #[tokio::test]
    async fn test_merge_batch_schema() {
        // Merge schema of the batches
        // The fields in the schema are sorted by column name
        let batches = create_batches();
        let merged_schema = (&*merge_record_batch_schemas(&batches)).clone();

        // Expected Arrow schema
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            arrow::datatypes::Field::new("int64", DataType::Int64, true),
            arrow::datatypes::Field::new("string", DataType::Utf8, true),
            arrow::datatypes::Field::new("bool", DataType::Boolean, true),
            arrow::datatypes::Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            arrow::datatypes::Field::new("uint64", DataType::UInt64, false),
            arrow::datatypes::Field::new("float64", DataType::Float64, true),
        ]));
        let expected_schema = Schema::try_from(arrow_schema)
            .unwrap()
            .sort_fields_by_name();

        assert_eq!(
            expected_schema, merged_schema,
            "\nExpected:\n{:#?}\nActual:\n{:#?}",
            expected_schema, merged_schema
        );
    }

    #[tokio::test]
    async fn test_tombstones_to_delete_predicates() {
        // create tombstones
        let tombstones = vec![
            create_tombstone(1, 1, 1, 1, 100, 200, "temp=10"),
            create_tombstone(1, 1, 1, 2, 100, 350, "temp!=10 and city=Boston"),
        ];

        // This new queryable batch will convert tombstone to delete predicates
        let query_batch = QueryableBatch::new("test_table", vec![], tombstones);
        let predicates = query_batch.delete_predicates();
        let expected = vec![
            Arc::new(DeletePredicate {
                range: TimestampRange::new(100, 200),
                exprs: vec![DeleteExpr {
                    column: String::from("temp"),
                    op: Op::Eq,
                    scalar: Scalar::I64(10),
                }],
            }),
            Arc::new(DeletePredicate {
                range: TimestampRange::new(100, 350),
                exprs: vec![
                    DeleteExpr {
                        column: String::from("temp"),
                        op: Op::Ne,
                        scalar: Scalar::I64(10),
                    },
                    DeleteExpr {
                        column: String::from("city"),
                        op: Op::Eq,
                        scalar: Scalar::String(String::from(r#"Boston"#)),
                    },
                ],
            }),
        ];

        assert_eq!(expected, predicates);
    }

    // ----------------------------------------------------------------------------------------------
    // Data for testing

    // Create pure RecordBatches without knowledge of Influx datatype
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
            ("time", ts_array, false), // not null
        ])
        .unwrap();

        // Batch 2: <dict, u64, f64, str, bool, time> & 2 rows
        let dict_array: ArrayRef = Arc::new(
            vec![None, Some("d")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let uint64_array: ArrayRef = Arc::new([Some(1), Some(2)].iter().collect::<UInt64Array>()); // not null
        let float64_array: ArrayRef =
            Arc::new([Some(1.0), Some(2.0)].iter().collect::<Float64Array>());
        let string_array: ArrayRef = Arc::new(
            vec![Some("foo"), Some("bar")]
                .into_iter()
                .collect::<StringArray>(),
        );
        let bool_array: ArrayRef = Arc::new([Some(true), None].iter().collect::<BooleanArray>());
        let ts_array: ArrayRef = Arc::new(
            [Some(100), Some(1626823730000000000)] // not null
                .iter()
                .collect::<TimestampNanosecondArray>(),
        );
        let batch2 = RecordBatch::try_from_iter_with_nullable(vec![
            ("dict", dict_array, true),
            ("uint64", uint64_array, false), // not null
            ("float64", float64_array, true),
            ("string", string_array, true),
            ("bool", bool_array, true),
            ("time", ts_array, false), // not null
        ])
        .unwrap();

        vec![Arc::new(batch1), Arc::new(batch2)]
    }
}
