//! Module to handle query on Ingester's data

use std::sync::Arc;

use arrow::{
    array::BooleanArray, compute::filter_record_batch, error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use arrow_util::util::merge_record_batches;
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder},
    delete_predicate::DeletePredicate,
    partition_metadata::TableSummary,
};
use datafusion::{
    error::DataFusionError,
    logical_plan::ExprRewritable,
    physical_plan::{
        common::SizedRecordBatchStream,
        metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics},
        PhysicalExpr, SendableRecordBatchStream,
    },
};
use iox_catalog::interface::{SequenceNumber, Tombstone};
use observability_deps::tracing::debug;
use predicate::{delete_predicate::parse_delete_predicate, Predicate, PredicateMatch};
use query::{
    exec::stringset::StringSet,
    util::{df_physical_expr_from_schema_and_expr, MissingColumnsToNull},
    QueryChunk, QueryChunkMeta,
};
use schema::{merge::merge_record_batch_schemas, selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};

use crate::data::{QueryableBatch, SnapshotBatch};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Internal error concatenating record batches {}", source))]
    ConcatBatches { source: arrow::error::ArrowError },

    #[snafu(display("Internal error filtering columns from a record batch {}", source))]
    FilterColumns { source: crate::data::Error },

    #[snafu(display("Internal error rewriting predicate for QueryableBatch: {}", source))]
    RewritingFilterPredicate {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Internal error converting logical expression to physical one: {}",
        source
    ))]
    ToPhysicalExpr {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error filtering record batch: {}", source))]
    FilterBatch { source: arrow::error::ArrowError },
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl QueryableBatch {
    /// Initilaize a QueryableBatch
    pub fn new(table_name: &str, data: Vec<SnapshotBatch>, deletes: Vec<Tombstone>) -> Self {
        let mut delete_predicates = vec![];
        for delete in &deletes {
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
            deletes,
            delete_predicates,
            table_name: table_name.to_string(),
        }
    }

    /// return min and max of all the snapshots
    pub fn min_max_sequence_numbers(&self) -> (SequenceNumber, SequenceNumber) {
        let min = self
            .data
            .first()
            .expect("The Queryable Batch should not empty")
            .min_sequencer_number;

        let max = self
            .data
            .first()
            .expect("The Queryable Batch should not empty")
            .max_sequencer_number;

        assert!(min <= max);

        (min, max)
    }
}

impl QueryChunkMeta for QueryableBatch {
    fn summary(&self) -> Option<&TableSummary> {
        None
    }

    fn schema(&self) -> Arc<Schema> {
        // todo: may want store this schema as a field of QueryableBatch and
        // only do this schema merge the first time it is call

        // Merge schema of all RecordBatches of the PerstingBatch
        let batches: Vec<Arc<RecordBatch>> =
            self.data.iter().map(|s| Arc::clone(&s.data)).collect();
        merge_record_batch_schemas(&batches)
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        self.delete_predicates.as_ref()
    }
}

impl QueryChunk for QueryableBatch {
    type Error = Error;

    // This function should not be used in QueryBatch context
    fn id(&self) -> ChunkId {
        // always return id 0 for debugging mode
        // todo: need to see if the same id for all chunks will cause any panics
        ChunkId::new_test(0)
    }

    // This function should not be used in PersistingBatch context
    fn addr(&self) -> ChunkAddr {
        unimplemented!()
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
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        // Get all record batches from their snapshots
        let mut batches = vec![];
        for snapshot in &self.data {
            // Only return columns in the selection
            let batch = snapshot.scan(selection).context(FilterColumnsSnafu {})?;
            if let Some(batch) = batch {
                batches.push(batch);
            }
        }

        // Combine record batches into one batch and padding null values as needed
        // Schema of all record batches after mergeing
        let schema = merge_record_batch_schemas(&batches);
        let batch =
            merge_record_batches(schema.as_arrow(), batches).context(ConcatBatchesSnafu {})?;

        let mut stream_batches = vec![];
        if let Some(mut batch) = batch {
            // Apply predicate to filter data
            if let Some(filter_expr) = predicate.filter_expr() {
                // Since the predicate may include columns that the batches do not have,
                // we need to rewrite the predicate to replace those column names with NULL
                let mut rewriter = MissingColumnsToNull::new(&schema);
                let filter_expr = filter_expr
                    .rewrite(&mut rewriter)
                    .context(RewritingFilterPredicateSnafu {})?;
                let df_phy_expr =
                    df_physical_expr_from_schema_and_expr(schema.as_arrow(), filter_expr)
                        .context(ToPhysicalExprSnafu)?;
                let num_rows_before = batch.num_rows();
                batch = batch_filter(&batch, &df_phy_expr).context(FilterBatchSnafu)?;
                let num_rows_after = batch.num_rows();
                debug!(
                    ?num_rows_before,
                    ?num_rows_after,
                    "predicate pushdown for QueryableBatch"
                );
            }

            if batch.num_rows() > 0 {
                stream_batches.push(Arc::new(batch));
            }
        }

        // Return stream of data
        let dummy_metrics = ExecutionPlanMetricsSet::new();
        let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
        let stream =
            SizedRecordBatchStream::new(self.schema().as_arrow(), stream_batches, mem_metrics);
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

// Filter data from RecordBatch
// Borrow from DF's https://github.com/apache/arrow-datafusion/blob/ecd0081bde98e9031b81aa6e9ae2a4f309fcec12/datafusion/src/physical_plan/filter.rs#L186
// TODO: if we make DF batch_filter public, we can call that function directly
fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> ArrowResult<RecordBatch> {
    predicate
        .evaluate(batch)
        .map(|v| v.into_array(batch.num_rows()))
        .map_err(DataFusionError::into)
        .and_then(|array| {
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Filter predicate evaluated to non-boolean value".to_string(),
                    )
                    .into()
                })
                // apply filter array to record batch
                .and_then(|filter_array| filter_record_batch(batch, filter_array))
        })
}

#[cfg(test)]
mod tests {
    use crate::test_util::{
        create_batches_with_influxtype_different_columns_different_order,
        create_one_record_batch_with_influxtype_no_duplicates, create_tombstone,
        make_queryable_batch,
    };

    use super::*;

    use arrow::{
        array::{
            ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
            TimestampNanosecondArray, UInt64Array,
        },
        datatypes::{DataType, Int32Type, TimeUnit},
    };
    use arrow_util::assert_batches_eq;
    use data_types::{
        delete_predicate::{DeleteExpr, Op, Scalar},
        timestamp::TimestampRange,
    };
    use datafusion::logical_plan::{col, lit};
    use predicate::PredicateBuilder;

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

    #[tokio::test]
    async fn test_read_filter() {
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        let stream = batch
            .read_filter(&Predicate::default(), Selection::All) // return all columns
            .unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_columns() {
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        let stream = batch
            .read_filter(
                &Predicate::default(),
                Selection::Some(&["time", "field_int"]), // return 2 out of 3 columns
            )
            .unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        let expected = vec![
            "+-----------+-----------------------------+",
            "| field_int | time                        |",
            "+-----------+-----------------------------+",
            "| 1000      | 1970-01-01T00:00:00.000008Z |",
            "| 10        | 1970-01-01T00:00:00.000010Z |",
            "| 70        | 1970-01-01T00:00:00.000020Z |",
            "+-----------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_predicate() {
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        // tag1 = VT
        let expr = col("tag1").eq(lit("VT"));
        let pred = PredicateBuilder::default().add_expr(expr).build();

        let stream = batch.read_filter(&pred, Selection::All).unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_predicate_on_missing_column() {
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        // foo = VT
        let expr = col("foo").eq(lit("VT")); // `foo` column not available
        let pred = PredicateBuilder::default().add_expr(expr).build();

        let stream = batch.read_filter(&pred, Selection::All).unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // missing_column = "VT" -> return nothing
        let expected = vec!["++", "++"];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_predicate_on_missing_column_is_null() {
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        // foo is NULL
        let expr = col("foo").is_null();
        let pred = PredicateBuilder::default().add_expr(expr).build();

        let stream = batch.read_filter(&pred, Selection::All).unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // missing_column is NULL  --> return everything
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_not_exist_columns() {
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        let stream = batch
            .read_filter(
                &Predicate::default(),
                Selection::Some(&["foo"]), // column not exist
            )
            .unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        let expected = vec!["++", "++"];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_many_batches() {
        let batches = create_batches_with_influxtype_different_columns_different_order().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        let stream = batch
            .read_filter(&Predicate::default(), Selection::All) // return all columns
            .unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      |      | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        |      | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        |      | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       |      | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         |      | AL   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_many_batches_filer_columns_predicates() {
        let batches = create_batches_with_influxtype_different_columns_different_order().await;
        let batch = make_queryable_batch("test_table", 1, batches);

        // Only read 2 columns: "tag1" and "time"
        let selection = Selection::Some(&["tag1", "time"]);

        // foo is NULL AND tag1=CT
        let expr = col("foo").is_null().and(col("tag1").eq(lit("CT")));
        let pred = PredicateBuilder::default().add_expr(expr).build();

        let stream = batch.read_filter(&pred, selection).unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // missing_column is NULL AND tag1=CT --> return 2 columns with tag1=CT only
        let expected = vec![
            "+------+--------------------------------+",
            "| tag1 | time                           |",
            "+------+--------------------------------+",
            "| CT   | 1970-01-01T00:00:00.000000100Z |",
            "| CT   | 1970-01-01T00:00:00.000000500Z |",
            "+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_read_filter_many_batches_not_exist_columns() {
        let batches = create_batches_with_influxtype_different_columns_different_order().await;
        let batch = make_queryable_batch("test_table", 1, batches);
        let stream = batch
            .read_filter(
                &Predicate::default(),
                Selection::Some(&["foo", "bar"]), // column not exist
            )
            .unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        let expected = vec!["++", "++"];
        assert_batches_eq!(&expected, &batches);
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
