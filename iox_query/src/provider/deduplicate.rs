//! Implemention of DeduplicateExec operator (resolves primary key conflicts) plumbing and tests
mod algo;
mod key_ranges;

use std::{fmt, sync::Arc};

use arrow::{
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use datafusion_util::{watch::WatchedTask, AdapterStream};

pub use self::algo::RecordBatchDeduplicator;
use datafusion::{
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{
            self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
        },
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use futures::StreamExt;
use observability_deps::tracing::{debug, trace};
use tokio::sync::mpsc;

/// # DeduplicateExec
///
/// This operator takes an input stream of RecordBatches that is
/// already sorted on "sort_key" and applies IOx specific deduplication
/// logic.
///
/// The output is dependent on the order of the the input rows which
/// have the same key.
///
/// Specifically, the value chosen for each non-sort_key column is the
/// "last" non-null value. This is used to model "upserts" when new
/// rows with the same primary key are inserted a second time to update
/// existing values.
///
/// # Example
/// For example, given a sort key of (t1, t2) and the following input
/// (already sorted on t1 and t2):
///
/// ```text
/// +----+----+----+----+
/// | t1 | t2 | f1 | f2 |
/// +----+----+----+----+
/// | a  | x  | 2  |    |
/// | a  | x  | 2  | 1  |
/// | a  | x  |    | 3  |
/// | a  | y  | 3  | 1  |
/// | b  | y  | 3  |    |
/// | c  | y  | 1  | 1  |
/// +----+----+----+----+
/// ```
///
/// This operator will produce the following output (note the values
/// chosen for (a, x)):
///
/// ```text
/// +----+----+----+----+
/// | t1 | t2 | f1 | f2 |
/// +----+----+----+----+
/// | a  | x  | 2  | 3  |
/// | a  | y  | 3  | 1  |
/// | b  | y  | 3  |    |
/// | c  | y  | 1  | 1  |
/// +----+----+----+----+
/// ```
///
/// # Field Resolution (why the last non-null value?)
///
/// The choice of the latest non-null value instead of the latest value is
/// subtle and thus we try to document the rationale here. It is a
/// consequence of the LineProtocol update model.
///
/// Some observations about line protocol are:
///
/// 1. Lines are treated as "UPSERT"s (aka updating any existing
///    values, possibly adding new fields)
///
/// 2. Fields can not be removed or set to NULL via a line (So if a
///    field has a NULL value it means the user didn't provide a value
///    for that field)
///
/// For example, this data (with a NULL for `f2`):
///
/// ```text
/// t1 | f1 | f2
/// ---+----+----
///  a | 1  | 3
//   a | 2  |
/// ```
///
/// Would have come from line protocol like
/// ```text
/// m,t1=a f1=1,f2=3
/// m,t1=a f1=3
/// ```
/// (note there was no value for f2 provided in the second line, it can
/// be read as "upsert value of f1=3, the value of f2 is not modified).
///
/// Thus it would not be correct to take the latest value from f2
/// (NULL) as in the source input the field's value was not provided.
#[derive(Debug)]
pub struct DeduplicateExec {
    input: Arc<dyn ExecutionPlan>,
    sort_keys: Vec<PhysicalSortExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl DeduplicateExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, sort_keys: Vec<PhysicalSortExpr>) -> Self {
        Self {
            input,
            sort_keys,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[derive(Debug)]
struct DeduplicateMetrics {
    baseline_metrics: BaselineMetrics,
    num_dupes: metrics::Count,
}

impl DeduplicateMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            num_dupes: MetricBuilder::new(metrics).counter("num_dupes", partition),
        }
    }
}

impl ExecutionPlan for DeduplicateExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.sort_keys)
    }

    fn required_input_ordering(&self) -> Vec<Option<&[PhysicalSortExpr]>> {
        // requires the input to be sorted on the primary key
        vec![self.output_ordering()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        let input = Arc::clone(&children[0]);
        Ok(Arc::new(Self::new(input, self.sort_keys.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(partition, "Start DeduplicationExec::execute");

        if partition != 0 {
            return Err(DataFusionError::Internal(
                "DeduplicateExec only supports a single input stream".to_string(),
            ));
        }
        let deduplicate_metrics = DeduplicateMetrics::new(&self.metrics, partition);

        let input_stream = self.input.execute(0, context)?;

        // the deduplication is performed in a separate task which is
        // then sent via a channel to the output
        let (tx, rx) = mpsc::channel(1);

        let fut = deduplicate(
            input_stream,
            self.sort_keys.clone(),
            tx.clone(),
            deduplicate_metrics,
        );

        // A second task watches the output of the worker task and reports errors
        let handle = WatchedTask::new(fut, vec![tx], "deduplicate batches");

        debug!(
            partition,
            "End building stream for DeduplicationExec::execute"
        );

        Ok(AdapterStream::adapt(self.schema(), rx, handle))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // For now use a single input -- it might be helpful
        // eventually to deduplicate in parallel by hash partitioning
        // the inputs (based on sort keys)
        vec![Distribution::SinglePartition]
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.sort_keys.iter().map(|e| e.to_string()).collect();
                write!(f, "DeduplicateExec: [{}]", expr.join(","))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        // use a guess from our input but they are NOT exact
        Statistics {
            is_exact: false,
            ..self.input.statistics()
        }
    }
}

async fn deduplicate(
    mut input_stream: SendableRecordBatchStream,
    sort_keys: Vec<PhysicalSortExpr>,
    tx: mpsc::Sender<ArrowResult<RecordBatch>>,
    deduplicate_metrics: DeduplicateMetrics,
) -> ArrowResult<()> {
    let DeduplicateMetrics {
        baseline_metrics,
        num_dupes,
    } = deduplicate_metrics;

    let elapsed_compute = baseline_metrics.elapsed_compute();
    let mut deduplicator = RecordBatchDeduplicator::new(sort_keys, num_dupes, None);

    // Stream input through the indexer
    while let Some(batch) = input_stream.next().await {
        let batch = batch?;

        // First check if this batch has same sort key with its previous batch
        let timer = elapsed_compute.timer();
        if let Some(last_batch) = deduplicator
            .last_batch_with_no_same_sort_key(&batch)
            .record_output(&baseline_metrics)
        {
            timer.done();
            // No, different sort key, so send the last batch downstream first
            if last_batch.num_rows() > 0 {
                tx.send(Ok(last_batch))
                    .await
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
            }
        } else {
            timer.done()
        }

        // deduplicate data of the batch
        let timer = elapsed_compute.timer();
        let output_batch = deduplicator.push(batch).record_output(&baseline_metrics)?;
        timer.done();
        if output_batch.num_rows() > 0 {
            tx.send(Ok(output_batch))
                .await
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
        }
    }
    debug!("before sending the left over batch");

    // send any left over batch
    let timer = elapsed_compute.timer();
    if let Some(output_batch) = deduplicator.finish()?.record_output(&baseline_metrics) {
        timer.done();
        if output_batch.num_rows() > 0 {
            tx.send(Ok(output_batch))
                .await
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
        }
    } else {
        timer.done()
    }
    debug!("done sending the left over batch");

    Ok(())
}

#[cfg(test)]
mod test {
    use arrow::compute::SortOptions;
    use arrow::datatypes::{Int32Type, SchemaRef};
    use arrow::{
        array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray},
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::{expressions::col, memory::MemoryExec};
    use datafusion_util::test_collect;

    use super::*;
    use arrow::array::{DictionaryArray, Int64Array};
    use std::iter::FromIterator;

    #[tokio::test]
    async fn test_single_tag() {
        // input:
        // t1 | f1 | f2
        // ---+----+----
        //  a | 1  |
        //  a | 2  | 3
        //  a |    | 4
        //  b | 5  | 6
        //  c | 7  |
        //  c |    |
        //  c |    | 8
        //
        // expected output:
        //
        // t1 | f1 | f2
        // ---+----+----
        //  a | 2  | 4
        //  b | 5  | 6
        //  c | 7  | 8

        let t1 = StringArray::from(vec![
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("c"),
            Some("c"),
        ]);
        let f1 = Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            Some(5.0),
            Some(7.0),
            None,
            None,
        ]);
        let f2 = Float64Array::from(vec![
            None,
            Some(3.0),
            Some(4.0),
            Some(6.0),
            None,
            None,
            Some(8.0),
        ]);

        let batch = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
            ("f2", Arc::new(f2) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![PhysicalSortExpr {
            expr: col("t1", &batch.schema()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let results = dedupe(vec![batch], sort_keys).await;

        let expected = vec![
            "+----+----+----+",
            "| t1 | f1 | f2 |",
            "+----+----+----+",
            "| a  | 2  | 4  |",
            "| b  | 5  | 6  |",
            "| c  | 7  | 8  |",
            "+----+----+----+",
        ];
        assert_batches_eq!(&expected, &results.output);
    }

    #[tokio::test]
    async fn test_with_timestamp() {
        // input:
        // f1 | f2 | time
        // ---+----+------
        //  1 |    | 100
        //    | 3  | 100
        //
        // expected output:
        //
        // f1 | f2 | time
        // ---+----+-------
        //  1 | 3  | 100
        let f1 = Float64Array::from(vec![Some(1.0), None]);
        let f2 = Float64Array::from(vec![None, Some(3.0)]);

        let time = TimestampNanosecondArray::from(vec![Some(100), Some(100)]);

        let batch = RecordBatch::try_from_iter(vec![
            ("f1", Arc::new(f1) as ArrayRef),
            ("f2", Arc::new(f2) as ArrayRef),
            ("time", Arc::new(time) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![PhysicalSortExpr {
            expr: col("time", &batch.schema()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let results = dedupe(vec![batch], sort_keys).await;

        let expected = vec![
            "+----+----+--------------------------------+",
            "| f1 | f2 | time                           |",
            "+----+----+--------------------------------+",
            "| 1  | 3  | 1970-01-01T00:00:00.000000100Z |",
            "+----+----+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &results.output);
    }

    #[tokio::test]
    async fn test_multi_tag() {
        // input:
        // t1 | t2 | f1 | f2
        // ---+----+----+----
        //  a | b  | 1  |
        //  a | b  | 2  | 3
        //  a | b  |    | 4
        //  a | z  | 5  |
        //  b | b  | 6  |
        //  b | c  | 7  | 6
        //  c | c  | 8  |
        //  d | b  |    | 9
        //  e |    | 10 | 11
        //  e |    | 12 |
        //    | f  | 13 |
        //    | f  |    | 14
        //
        // expected output:
        // t1 | t2 | f1 | f2
        // ---+----+----+----
        //  a | b  | 2  | 4
        //  a | z  | 5  |
        //  b | b  | 6  |
        //  b | c  | 7  | 6
        //  c | c  | 8  |
        //  d | b  |    | 9
        //  e |    | 12 | 11
        //    | f  | 13 | 14

        let t1 = StringArray::from(vec![
            Some("a"),
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("e"),
            None,
            None,
        ]);

        let t2 = StringArray::from(vec![
            Some("b"),
            Some("b"),
            Some("b"),
            Some("z"),
            Some("b"),
            Some("c"),
            Some("c"),
            Some("b"),
            None,
            None,
            Some("f"),
            Some("f"),
        ]);

        let f1 = Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            Some(5.0),
            Some(6.0),
            Some(7.0),
            Some(8.0),
            None,
            Some(10.0),
            Some(12.0),
            Some(13.0),
            None,
        ]);

        let f2 = Float64Array::from(vec![
            None,
            Some(3.0),
            Some(4.0),
            None,
            None,
            Some(6.0),
            None,
            Some(9.0),
            Some(11.0),
            None,
            None,
            Some(14.0),
        ]);

        let batch = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
            ("f2", Arc::new(f2) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![
            PhysicalSortExpr {
                expr: col("t1", &batch.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("t2", &batch.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ];

        let results = dedupe(vec![batch], sort_keys).await;

        let expected = vec![
            "+----+----+----+----+",
            "| t1 | t2 | f1 | f2 |",
            "+----+----+----+----+",
            "| a  | b  | 2  | 4  |",
            "| a  | z  | 5  |    |",
            "| b  | b  | 6  |    |",
            "| b  | c  | 7  | 6  |",
            "| c  | c  | 8  |    |",
            "| d  | b  |    | 9  |",
            "| e  |    | 12 | 11 |",
            "|    | f  | 13 | 14 |",
            "+----+----+----+----+",
        ];
        assert_batches_eq!(&expected, &results.output);
    }

    #[tokio::test]
    async fn test_string_with_timestamp() {
        // input:
        //    s   | i  | time
        // -------+----+------
        //  "cat" |    | 100
        //        | 3  | 100
        //        | 4  | 200
        //  "dog" |    | 200
        //
        // expected output:
        //
        //    s   | i | time
        // -------+----+-------
        //  "cat" | 3  | 100
        //  "dog" | 4  | 200
        let s = StringArray::from(vec![Some("cat"), None, None, Some("dog")]);

        let i = Int64Array::from(vec![None, Some(3), Some(4), None]);

        let time = TimestampNanosecondArray::from(vec![Some(100), Some(100), Some(200), Some(200)]);

        let batch = RecordBatch::try_from_iter(vec![
            ("s", Arc::new(s) as ArrayRef),
            ("i", Arc::new(i) as ArrayRef),
            ("time", Arc::new(time) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![PhysicalSortExpr {
            expr: col("time", &batch.schema()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let results = dedupe(vec![batch], sort_keys).await;

        let expected = vec![
            "+-----+---+--------------------------------+",
            "| s   | i | time                           |",
            "+-----+---+--------------------------------+",
            "| cat | 3 | 1970-01-01T00:00:00.000000100Z |",
            "| dog | 4 | 1970-01-01T00:00:00.000000200Z |",
            "+-----+---+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &results.output);
    }

    #[tokio::test]
    async fn test_last_is_null_with_timestamp() {
        // input:
        //    s   | i  | time
        // -------+----+------
        //  "cat" |     | 1639612800000000000
        //        | 10  | 1639612800000000000
        //
        // expected output:
        //
        //    s   | i | time
        // -------+----+-------
        //  "cat" | 10  | 1639612800000000000
        let s = StringArray::from(vec![Some("cat"), None]);

        let i = Int64Array::from(vec![None, Some(10)]);

        let time = TimestampNanosecondArray::from(vec![
            Some(1639612800000000000),
            Some(1639612800000000000),
        ]);

        let batch = RecordBatch::try_from_iter(vec![
            ("s", Arc::new(s) as ArrayRef),
            ("i", Arc::new(i) as ArrayRef),
            ("time", Arc::new(time) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![PhysicalSortExpr {
            expr: col("time", &batch.schema()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];

        let results = dedupe(vec![batch], sort_keys).await;

        let expected = vec![
            "+-----+----+----------------------+",
            "| s   | i  | time                 |",
            "+-----+----+----------------------+",
            "| cat | 10 | 2021-12-16T00:00:00Z |",
            "+-----+----+----------------------+",
        ];
        assert_batches_eq!(&expected, &results.output);
    }

    #[tokio::test]
    async fn test_multi_record_batch() {
        // input:
        // t1 | t2 | f1 | f2
        // ---+----+----+----
        //  a | b  | 1  | 2
        //  a | c  | 3  |
        //  a | c  | 4  | 5
        //  ====(next batch)====
        //  a | c  |    | 6
        //  b | d  | 7  | 8

        //
        // expected output:
        // t1 | t2 | f1 | f2
        // ---+----+----+----
        //  a | b  | 1  | 2
        //  a | c  | 4  | 6
        //  b | d  | 7  | 8

        let t1 = StringArray::from(vec![Some("a"), Some("a"), Some("a")]);

        let t2 = StringArray::from(vec![Some("b"), Some("c"), Some("c")]);

        let f1 = Float64Array::from(vec![Some(1.0), Some(3.0), Some(4.0)]);

        let f2 = Float64Array::from(vec![Some(2.0), None, Some(5.0)]);

        let batch1 = RecordBatch::try_from_iter_with_nullable(vec![
            ("t1", Arc::new(t1) as ArrayRef, true),
            ("t2", Arc::new(t2) as ArrayRef, true),
            ("f1", Arc::new(f1) as ArrayRef, true),
            ("f2", Arc::new(f2) as ArrayRef, true),
        ])
        .unwrap();

        let t1 = StringArray::from(vec![Some("a"), Some("b")]);

        let t2 = StringArray::from(vec![Some("c"), Some("d")]);

        let f1 = Float64Array::from(vec![None, Some(7.0)]);

        let f2 = Float64Array::from(vec![Some(6.0), Some(8.0)]);

        let batch2 = RecordBatch::try_from_iter_with_nullable(vec![
            ("t1", Arc::new(t1) as ArrayRef, true),
            ("t2", Arc::new(t2) as ArrayRef, true),
            ("f1", Arc::new(f1) as ArrayRef, true),
            ("f2", Arc::new(f2) as ArrayRef, true),
        ])
        .unwrap();

        let sort_keys = vec![
            PhysicalSortExpr {
                expr: col("t1", &batch2.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("t2", &batch2.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ];

        let results = dedupe(vec![batch1, batch2], sort_keys).await;

        let expected = vec![
            "+----+----+----+----+",
            "| t1 | t2 | f1 | f2 |",
            "+----+----+----+----+",
            "| a  | b  | 1  | 2  |",
            "| a  | c  | 4  | 6  |",
            "| b  | d  | 7  | 8  |",
            "+----+----+----+----+",
        ];
        assert_batches_eq!(&expected, &results.output);
        // 5 rows in initial input, 3 rows in output ==> 2 dupes
        assert_eq!(results.num_dupes(), 5 - 3);
    }

    #[tokio::test]
    async fn test_no_dupes() {
        // special case test for data without duplicates (fast path)
        // input:
        // t1 | f1
        // ---+----
        //  a | 1
        //  ====(next batch)====
        //  b | 2
        //
        // expected output:
        //
        // t1 | f1
        // ---+----
        //  a | 1
        //  b | 2

        let t1 = StringArray::from(vec![Some("a")]);
        let f1 = Float64Array::from(vec![Some(1.0)]);

        let batch1 = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
        ])
        .unwrap();

        let t1 = StringArray::from(vec![Some("b")]);
        let f1 = Float64Array::from(vec![Some(2.0)]);

        let batch2 = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![PhysicalSortExpr {
            expr: col("t1", &batch2.schema()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let results = dedupe(vec![batch1, batch2], sort_keys).await;

        let expected = vec![
            "+----+----+",
            "| t1 | f1 |",
            "+----+----+",
            "| a  | 1  |",
            "| b  | 2  |",
            "+----+----+",
        ];
        assert_batches_eq!(&expected, &results.output);

        // also validate there were no dupes detected
        assert_eq!(results.num_dupes(), 0);
    }

    #[tokio::test]
    async fn test_single_pk() {
        // test boundary condition

        // input:
        // t1 | f1 | f2
        // ---+----+----
        //  a | 1  | 2
        //  a | 3  | 4
        //
        // expected output:
        //
        // t1 | f1 | f2
        // ---+----+----
        //  a | 3  | 4

        let t1 = StringArray::from(vec![Some("a"), Some("a")]);
        let f1 = Float64Array::from(vec![Some(1.0), Some(3.0)]);
        let f2 = Float64Array::from(vec![Some(2.0), Some(4.0)]);

        let batch = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
            ("f2", Arc::new(f2) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![PhysicalSortExpr {
            expr: col("t1", &batch.schema()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let results = dedupe(vec![batch], sort_keys).await;

        let expected = vec![
            "+----+----+----+",
            "| t1 | f1 | f2 |",
            "+----+----+----+",
            "| a  | 3  | 4  |",
            "+----+----+----+",
        ];
        assert_batches_eq!(&expected, &results.output);
    }

    #[tokio::test]
    async fn test_column_reorder() {
        // test if they fields come before tags and tags not in right order

        // input:
        // f1 | t2 | t1
        // ---+----+----
        //  1 | a  | a
        //  2 | a  | a
        //  3 | a  | b
        //  4 | b  | b
        //
        // expected output:
        //
        // f1 | t2 | t1
        // ---+----+----
        //  2 | a  | a
        //  3 | a  | b
        //  4 | b  | b

        let f1 = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
        let t2 = StringArray::from(vec![Some("a"), Some("a"), Some("a"), Some("b")]);
        let t1 = StringArray::from(vec![Some("a"), Some("a"), Some("b"), Some("b")]);

        let batch = RecordBatch::try_from_iter(vec![
            ("f1", Arc::new(f1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
            ("t1", Arc::new(t1) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![
            PhysicalSortExpr {
                expr: col("t1", &batch.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("t2", &batch.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ];

        let results = dedupe(vec![batch], sort_keys).await;

        let expected = vec![
            "+----+----+----+",
            "| f1 | t2 | t1 |",
            "+----+----+----+",
            "| 2  | a  | a  |",
            "| 3  | a  | b  |",
            "| 4  | b  | b  |",
            "+----+----+----+",
        ];
        assert_batches_eq!(&expected, &results.output);
    }

    #[tokio::test]
    #[should_panic(expected = "This is the error")]
    async fn test_input_error_propagated() {
        // test that an error from the input gets to the output

        // input:
        // t1 | f1
        // ---+----
        //  a | 1
        // === next batch ===
        // (error)

        let t1 = StringArray::from(vec![Some("a")]);
        let f1 = Float64Array::from(vec![Some(1.0)]);

        let batch = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
        ])
        .unwrap();

        let schema = batch.schema();
        let batches = vec![
            Ok(batch),
            Err(ArrowError::ComputeError("This is the error".to_string())),
        ];

        let input = Arc::new(DummyExec {
            schema: Arc::clone(&schema),
            batches,
        });

        let sort_keys = vec![PhysicalSortExpr {
            expr: col("t1", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let exec: Arc<dyn ExecutionPlan> = Arc::new(DeduplicateExec::new(input, sort_keys));
        test_collect(exec).await;
    }

    #[tokio::test]
    async fn test_dictionary() {
        let t1 = DictionaryArray::<Int32Type>::from_iter(vec![Some("a"), Some("a"), Some("b")]);
        let t2 = DictionaryArray::<Int32Type>::from_iter(vec![Some("b"), Some("c"), Some("c")]);
        let f1 = Float64Array::from(vec![Some(1.0), Some(3.0), Some(4.0)]);
        let f2 = Float64Array::from(vec![Some(2.0), None, Some(5.0)]);

        let batch1 = RecordBatch::try_from_iter_with_nullable(vec![
            ("t1", Arc::new(t1) as ArrayRef, true),
            ("t2", Arc::new(t2) as ArrayRef, true),
            ("f1", Arc::new(f1) as ArrayRef, true),
            ("f2", Arc::new(f2) as ArrayRef, true),
        ])
        .unwrap();

        let t1 = DictionaryArray::<Int32Type>::from_iter(vec![Some("b"), Some("c")]);
        let t2 = DictionaryArray::<Int32Type>::from_iter(vec![Some("c"), Some("d")]);
        let f1 = Float64Array::from(vec![None, Some(7.0)]);
        let f2 = Float64Array::from(vec![Some(6.0), Some(8.0)]);

        let batch2 = RecordBatch::try_from_iter_with_nullable(vec![
            ("t1", Arc::new(t1) as ArrayRef, true),
            ("t2", Arc::new(t2) as ArrayRef, true),
            ("f1", Arc::new(f1) as ArrayRef, true),
            ("f2", Arc::new(f2) as ArrayRef, true),
        ])
        .unwrap();

        let sort_keys = vec![
            PhysicalSortExpr {
                expr: col("t1", &batch1.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("t2", &batch1.schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ];

        let results = dedupe(vec![batch1, batch2], sort_keys).await;

        let cols: Vec<_> = results
            .output
            .iter()
            .map(|batch| {
                batch
                    .column(batch.schema().column_with_name("t1").unwrap().0)
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap()
            })
            .collect();

        // Should produce optimised dictionaries
        // The batching is not important
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].keys().len(), 2);
        assert_eq!(cols[0].values().len(), 1); // "a"
        assert_eq!(cols[1].keys().len(), 1);
        assert_eq!(cols[1].values().len(), 1); // "b"
        assert_eq!(cols[2].keys().len(), 1);
        assert_eq!(cols[2].values().len(), 1); // "c"

        let expected = vec![
            "+----+----+----+----+",
            "| t1 | t2 | f1 | f2 |",
            "+----+----+----+----+",
            "| a  | b  | 1  | 2  |",
            "| a  | c  | 3  |    |",
            "| b  | c  | 4  | 6  |",
            "| c  | d  | 7  | 8  |",
            "+----+----+----+----+",
        ];
        assert_batches_eq!(&expected, &results.output);
        // 5 rows in initial input, 4 rows in output ==> 1 dupes
        assert_eq!(results.num_dupes(), 5 - 4);
    }

    struct TestResults {
        output: Vec<RecordBatch>,
        exec: Arc<DeduplicateExec>,
    }

    impl TestResults {
        /// return the number of duplicates this deduplicator detected
        fn num_dupes(&self) -> usize {
            let metrics = self.exec.metrics().unwrap();

            let metrics = metrics
                .iter()
                .filter(|m| m.value().name() == "num_dupes")
                .collect::<Vec<_>>();

            assert_eq!(
                metrics.len(),
                1,
                "expected only one duplicate metric, found {:?}",
                metrics
            );
            metrics[0].value().as_usize()
        }
    }

    /// Run the input through the deduplicator and return results
    async fn dedupe(input: Vec<RecordBatch>, sort_keys: Vec<PhysicalSortExpr>) -> TestResults {
        test_helpers::maybe_start_logging();

        // Setup in memory stream
        let schema = input[0].schema();
        let projection = None;
        let input = Arc::new(MemoryExec::try_new(&[input], schema, projection).unwrap());

        // Create and run the deduplicator
        let exec = Arc::new(DeduplicateExec::new(input, sort_keys));
        let output = test_collect(Arc::clone(&exec) as Arc<dyn ExecutionPlan>).await;

        TestResults { output, exec }
    }

    /// A PhysicalPlan that sends a specific set of
    /// Result<RecordBatch> for testing.
    #[derive(Debug)]
    struct DummyExec {
        schema: SchemaRef,
        batches: Vec<ArrowResult<RecordBatch>>,
    }

    impl ExecutionPlan for DummyExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn output_partitioning(&self) -> Partitioning {
            unimplemented!();
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            unimplemented!()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            assert_eq!(partition, 0);

            debug!(partition, "Start DummyExec::execute");

            // queue them all up
            let (tx, rx) = mpsc::unbounded_channel();

            // queue up all the results
            let batches: Vec<_> = self
                .batches
                .iter()
                .map(|r| match r {
                    Ok(batch) => Ok(batch.clone()),
                    Err(e) => Err(clone_error(e)),
                })
                .collect();
            let tx_captured = tx.clone();
            let fut = async move {
                for r in batches {
                    tx_captured.send(r).unwrap();
                }

                Ok(())
            };
            let handle = WatchedTask::new(fut, vec![tx], "dummy send");

            debug!(partition, "End DummyExec::execute");
            Ok(AdapterStream::adapt_unbounded(self.schema(), rx, handle))
        }

        fn statistics(&self) -> Statistics {
            // don't know anything about the statistics
            Statistics::default()
        }
    }

    fn clone_error(e: &ArrowError) -> ArrowError {
        use ArrowError::*;
        match e {
            ComputeError(msg) => ComputeError(msg.to_string()),
            _ => unimplemented!(),
        }
    }
}
