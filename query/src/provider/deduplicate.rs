//! Implemention of DeduplicateExec operator (resolves primary key conflicts) plumbing and tests
mod algo;

use std::{fmt, sync::Arc};

use arrow::{
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use async_trait::async_trait;

use crate::exec::stream::AdapterStream;

use self::algo::RecordBatchDeduplicator;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{
        expressions::PhysicalSortExpr, DisplayFormatType, Distribution, ExecutionPlan,
        Partitioning, SQLMetric, SendableRecordBatchStream,
    },
};
use futures::StreamExt;
use hashbrown::HashMap;
use observability_deps::tracing::debug;
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
    num_dupes: Arc<SQLMetric>,
}

impl DeduplicateExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, sort_keys: Vec<PhysicalSortExpr>) -> Self {
        let num_dupes = SQLMetric::counter();
        Self {
            input,
            sort_keys,
            num_dupes,
        }
    }
}

#[async_trait]
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        let input = Arc::clone(&children[0]);
        Ok(Arc::new(Self::new(input, self.sort_keys.clone())))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "DeduplicateExec only supports a single input stream".to_string(),
            ));
        }

        let input_stream = self.input.execute(0).await?;

        // the deduplication is performed in a separate task which is
        // then sent via a channel to the output
        let (tx, rx) = mpsc::channel(1);

        let task = tokio::task::spawn(deduplicate(
            input_stream,
            self.sort_keys.clone(),
            tx.clone(),
            Arc::clone(&self.num_dupes),
        ));

        // A second task watches the output of the worker task
        tokio::task::spawn(async move {
            let task_result = task.await;

            let msg = match task_result {
                Err(join_err) => {
                    debug!(e=%join_err, "Error joining deduplicate task");
                    Some(ArrowError::ExternalError(Box::new(join_err)))
                }
                Ok(Err(e)) => {
                    debug!(%e, "Error in deduplicate task itself");
                    Some(e)
                }
                Ok(Ok(())) => {
                    // successful
                    None
                }
            };

            if let Some(e) = msg {
                // try and tell the receiver something went
                // wrong. Note we ignore errors sending this message
                // as that means the receiver has already been
                // shutdown and no one cares anymore lol
                if tx.send(Err(e)).await.is_err() {
                    debug!("deduplicate receiver hung up");
                }
            }
        });

        Ok(AdapterStream::adapt(self.schema(), rx))
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.sort_keys.iter().map(|e| e.to_string()).collect();
                write!(f, "DeduplicateExec: [{}]", expr.join(","))
            }
        }
    }

    fn metrics(&self) -> HashMap<String, SQLMetric> {
        let mut metrics = HashMap::new();
        metrics.insert("numDuplicates".to_owned(), self.num_dupes.as_ref().clone());
        metrics
    }
}

async fn deduplicate(
    mut input_stream: SendableRecordBatchStream,
    sort_keys: Vec<PhysicalSortExpr>,
    tx: mpsc::Sender<ArrowResult<RecordBatch>>,
    num_dupes: Arc<SQLMetric>,
) -> ArrowResult<()> {
    let mut deduplicator = RecordBatchDeduplicator::new(sort_keys, num_dupes);

    // Stream input through the indexer
    while let Some(batch) = input_stream.next().await {
        let batch = batch?;

        // First check if this batch has same sort key with its previous batch
        if let Some(last_batch) = deduplicator.last_batch_with_no_same_sort_key(&batch) {
            // No same sort key, so send the last batch downstream first
            tx.send(Ok(last_batch))
                .await
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
        }

        // deduplicate data of the batch
        let output_batch = deduplicator.push(batch)?;
        tx.send(Ok(output_batch))
            .await
            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
    }

    // send any left over batch
    if let Some(output_batch) = deduplicator.finish()? {
        tx.send(Ok(output_batch))
            .await
            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use arrow::compute::SortOptions;
    use arrow::datatypes::SchemaRef;
    use arrow::{
        array::{ArrayRef, Float64Array, StringArray},
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::{collect, expressions::col, memory::MemoryExec};

    use super::*;

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
            expr: col("t1"),
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
                expr: col("t1"),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("t2"),
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

        let batch1 = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
            ("f2", Arc::new(f2) as ArrayRef),
        ])
        .unwrap();

        let t1 = StringArray::from(vec![Some("a"), Some("b")]);

        let t2 = StringArray::from(vec![Some("b"), Some("d")]);

        let f1 = Float64Array::from(vec![None, Some(7.0)]);

        let f2 = Float64Array::from(vec![Some(6.0), Some(8.0)]);

        let batch2 = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
            ("f1", Arc::new(f1) as ArrayRef),
            ("f2", Arc::new(f2) as ArrayRef),
        ])
        .unwrap();

        let sort_keys = vec![
            PhysicalSortExpr {
                expr: col("t1"),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("t2"),
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
            expr: col("t1"),
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
            expr: col("t1"),
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
                expr: col("t1"),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("t2"),
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
            expr: col("t1"),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let exec = Arc::new(DeduplicateExec::new(input, sort_keys));
        let output = collect(Arc::clone(&exec) as Arc<dyn ExecutionPlan>)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            output.contains("Compute error: This is the error"),
            "actual output: {}",
            output
        );
    }

    struct TestResults {
        output: Vec<RecordBatch>,
        exec: Arc<DeduplicateExec>,
    }

    impl TestResults {
        /// return the number of duplicates this deduplicator detected
        fn num_dupes(&self) -> usize {
            self.exec
                .metrics()
                .get("numDuplicates")
                .expect("No dupe metrics found")
                .value()
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
        let output = collect(Arc::clone(&exec) as Arc<dyn ExecutionPlan>)
            .await
            .unwrap();

        TestResults { output, exec }
    }

    /// A PhysicalPlan that sends a specific set of
    /// Result<RecordBatch> for testing.
    #[derive(Debug)]
    struct DummyExec {
        schema: SchemaRef,
        batches: Vec<ArrowResult<RecordBatch>>,
    }

    #[async_trait]
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

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            &self,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
            assert_eq!(partition, 0);

            // ensure there is space to queue up the channel
            let (tx, rx) = mpsc::channel(self.batches.len());

            // queue up all the results
            for r in &self.batches {
                match r {
                    Ok(batch) => tx.send(Ok(batch.clone())).await.unwrap(),
                    Err(e) => tx.send(Err(clone_error(e))).await.unwrap(),
                }
            }

            Ok(AdapterStream::adapt(self.schema(), rx))
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
