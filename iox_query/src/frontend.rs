pub mod common;
pub mod influxql;
pub mod influxrpc;
pub mod reorg;
pub mod sql;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::{
        metrics::{self, MetricValue},
        ExecutionPlan, ExecutionPlanVisitor,
    };
    use datafusion_util::{test_collect_partition, test_execute_partition};
    use futures::StreamExt;
    use schema::{merge::SchemaMerger, sort::SortKey, Schema};

    use crate::{
        exec::{split::StreamSplitExec, Executor, ExecutorType, IOxSessionContext},
        frontend::reorg::ReorgPlanner,
        provider::{DeduplicateExec, RecordBatchesExec},
        test::TestChunk,
        QueryChunk, QueryChunkMeta, ScanPlanBuilder,
    };

    /// A macro to asserts the contents of the extracted metrics is reasonable
    ///
    macro_rules! assert_extracted_metrics {
        ($EXTRACTED: expr, $EXPECTED_OUTPUT_ROWS: expr) => {
            assert!(
                $EXTRACTED.elapsed_compute.value() > 0,
                "some elapsed compute time"
            );
            assert_eq!(
                $EXTRACTED.output_rows.value(),
                $EXPECTED_OUTPUT_ROWS,
                "expected output row count"
            );

            let start_ts = $EXTRACTED
                .start_timestamp
                .value()
                .expect("start timestamp")
                .timestamp_nanos();
            let end_ts = $EXTRACTED
                .end_timestamp
                .value()
                .expect("end timestamp")
                .timestamp_nanos();

            assert!(start_ts > 0, "start timestamp was non zero");
            assert!(end_ts > 0, "end timestamp was non zero");
            assert!(
                start_ts < end_ts,
                "start timestamp was before end timestamp"
            );
        };
    }

    #[tokio::test]
    async fn test_scan_plan_deduplication() {
        // Create 2 overlapped chunks
        let (schema, chunks) = get_test_overlapped_chunks();
        let ctx = IOxSessionContext::with_testing();

        // Build a logical plan with deduplication
        let scan_plan = ScanPlanBuilder::new(Arc::from("t"), schema, ctx.child_ctx("scan_plan"))
            .with_chunks(chunks)
            .build()
            .unwrap();
        let logical_plan = scan_plan.plan_builder.build().unwrap();

        // Build physical plan
        let executor = Executor::new_testing();
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&logical_plan)
            .await
            .unwrap();

        // Verify output data
        // Since data is merged due to deduplication, the two input chunks will be merged into one output chunk
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            1,
            "{:?}",
            physical_plan.output_partitioning()
        );
        let batches0 = test_collect_partition(Arc::clone(&physical_plan), 0).await;
        // Data is sorted on tag1 & time. One row is removed due to deduplication
        let expected = vec![
            "+-----------+------------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | time                           |",
            "+-----------+------------+------+--------------------------------+",
            "| 100       |            | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        |            | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      |            | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z    |",
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z    |", // other row with the same tag1 and time is removed
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z    |",
            "+-----------+------------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches0);
    }

    #[tokio::test]
    async fn test_scan_plan_without_deduplication() {
        // Create 2 overlapped chunks
        let (schema, chunks) = get_test_chunks();
        let ctx = IOxSessionContext::with_testing();

        // Build a logical plan without deduplication
        let scan_plan = ScanPlanBuilder::new(Arc::from("t"), schema, ctx.child_ctx("scan_plan"))
            .with_chunks(chunks)
            // force it to not deduplicate
            .enable_deduplication(false)
            .build()
            .unwrap();
        let logical_plan = scan_plan.plan_builder.build().unwrap();

        // Build physical plan
        let executor = Executor::new_testing();
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&logical_plan)
            .await
            .unwrap();

        // Verify output data: 2 input chunks are pushed out as 2 output chunks
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            2,
            "{:?}",
            physical_plan.output_partitioning()
        );
        //
        // First chunk has 5 rows
        let batches0 = test_collect_partition(Arc::clone(&physical_plan), 0).await;
        // Data is not sorted on anything
        let expected = vec![
            "+-----------+------------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | time                           |",
            "+-----------+------------+------+--------------------------------+",
            "| 1000      |            | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        |            | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       |            | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches0);
        //
        // Second chunk has 4 rows with duplicates
        let batches1 = test_collect_partition(Arc::clone(&physical_plan), 1).await;
        // Data is not sorted on anything
        let expected = vec![
            "+-----------+------------+------+-----------------------------+",
            "| field_int | field_int2 | tag1 | time                        |",
            "+-----------+------------+------+-----------------------------+",
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z |",
            "| 10        | 10         | VT   | 1970-01-01T00:00:00.000210Z |", // duplicate 1
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z |",
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z |", // duplicate 2
            "+-----------+------------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches1);
    }

    #[tokio::test]
    async fn test_scan_plan_without_deduplication_but_sort() {
        // Create 2 overlapped chunks
        let (schema, chunks) = get_test_chunks();
        let sort_key = SortKey::from_columns(vec!["time", "tag1"]);
        let ctx = IOxSessionContext::with_testing();

        // Build a logical plan without deduplication but sort
        let scan_plan = ScanPlanBuilder::new(Arc::from("t"), schema, ctx.child_ctx("scan_plan"))
            .with_chunks(chunks)
            // force it to not deduplicate
            .enable_deduplication(false)
            // force to sort on time & tag1
            .with_output_sort_key(sort_key)
            .build()
            .unwrap();
        let logical_plan = scan_plan.plan_builder.build().unwrap();

        // Build physical plan
        let executor = Executor::new_testing();
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&logical_plan)
            .await
            .unwrap();

        // Verify output data: 2 input chunks merged into one output chunk
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            1,
            "{:?}",
            physical_plan.output_partitioning()
        );
        let batches0 = test_collect_partition(Arc::clone(&physical_plan), 0).await;
        // Data is sorted on time & tag1 without deduplication
        let expected = vec![
            "+-----------+------------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | time                           |",
            "+-----------+------------+------+--------------------------------+",
            "| 100       |            | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        |            | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      |            | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z    |",
            "| 10        | 10         | VT   | 1970-01-01T00:00:00.000210Z    |", // duplicate 1
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z    |", // duplicate 2
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z    |",
            "+-----------+------------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches0);
    }

    #[tokio::test]
    async fn test_metrics() {
        let (schema, chunks) = get_test_chunks();
        let sort_key = SortKey::from_columns(vec!["time", "tag1"]);

        // Use a split plan as it has StreamSplitExec, DeduplicateExec and IOxReadFilternode
        let split_plan = ReorgPlanner::new(IOxSessionContext::with_testing())
            .split_plan(Arc::from("t"), schema, chunks, sort_key, vec![1000])
            .expect("created compact plan");

        let executor = Executor::new_testing();
        let plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&split_plan)
            .await
            .unwrap();

        let mut stream0 = test_execute_partition(Arc::clone(&plan), 0).await;
        let mut num_rows = 0;
        while let Some(batch) = stream0.next().await {
            num_rows += batch.unwrap().num_rows();
        }
        assert_eq!(num_rows, 3);

        let mut stream1 = test_execute_partition(Arc::clone(&plan), 1).await;
        let mut num_rows = 0;
        while let Some(batch) = stream1.next().await {
            num_rows += batch.unwrap().num_rows();
        }
        assert_eq!(num_rows, 5);

        // now validate metrics are good
        let extracted = extract_metrics(plan.as_ref(), |plan| {
            plan.as_any().downcast_ref::<RecordBatchesExec>().is_some()
        })
        .unwrap();

        assert_extracted_metrics!(extracted, 5);

        // now the deduplicator
        let extracted = extract_metrics(plan.as_ref(), |plan| {
            plan.as_any().downcast_ref::<DeduplicateExec>().is_some()
        })
        .unwrap();

        assert_extracted_metrics!(extracted, 3);

        // now the the split
        let extracted = extract_metrics(plan.as_ref(), |plan| {
            plan.as_any().downcast_ref::<StreamSplitExec>().is_some()
        })
        .unwrap();

        assert_extracted_metrics!(extracted, 8);
    }

    // Extracted baseline metrics for the specified operator
    #[derive(Debug)]
    struct ExtractedMetrics {
        elapsed_compute: metrics::Time,
        output_rows: metrics::Count,
        start_timestamp: metrics::Timestamp,
        end_timestamp: metrics::Timestamp,
    }

    // walks a plan tree, looking for the first plan node where a
    // predicate returns true and extracts the common metrics
    struct MetricsExtractor<P>
    where
        P: FnMut(&dyn ExecutionPlan) -> bool,
    {
        pred: P,
        inner: Option<ExtractedMetrics>,
    }

    impl<P> ExecutionPlanVisitor for MetricsExtractor<P>
    where
        P: FnMut(&dyn ExecutionPlan) -> bool,
    {
        type Error = std::convert::Infallible;

        fn pre_visit(
            &mut self,
            plan: &dyn ExecutionPlan,
        ) -> std::result::Result<bool, Self::Error> {
            // not visiting this one
            if !(self.pred)(plan) {
                return Ok(true);
            }
            let metrics = plan.metrics().unwrap().aggregate_by_name();
            let mut elapsed_compute: Option<metrics::Time> = None;
            let mut output_rows: Option<metrics::Count> = None;
            let mut start_timestamp: Option<metrics::Timestamp> = None;
            let mut end_timestamp: Option<metrics::Timestamp> = None;

            metrics.iter().for_each(|m| match m.value() {
                MetricValue::ElapsedCompute(t) => {
                    assert!(elapsed_compute.is_none());
                    elapsed_compute = Some(t.clone())
                }
                MetricValue::OutputRows(c) => {
                    assert!(output_rows.is_none());
                    output_rows = Some(c.clone())
                }
                MetricValue::StartTimestamp(ts) => {
                    assert!(start_timestamp.is_none());
                    start_timestamp = Some(ts.clone())
                }
                MetricValue::EndTimestamp(ts) => {
                    assert!(end_timestamp.is_none());
                    end_timestamp = Some(ts.clone())
                }
                _ => {}
            });

            self.inner = Some(ExtractedMetrics {
                elapsed_compute: elapsed_compute.expect("did not find metric"),
                output_rows: output_rows.expect("did not find metric"),
                start_timestamp: start_timestamp.expect("did not find metric"),
                end_timestamp: end_timestamp.expect("did not find metric"),
            });

            // found what we are looking for, no need to continue
            Ok(false)
        }
    }

    fn extract_metrics<P>(plan: &dyn ExecutionPlan, pred: P) -> Option<ExtractedMetrics>
    where
        P: FnMut(&dyn ExecutionPlan) -> bool,
    {
        let mut extractor = MetricsExtractor { pred, inner: None };

        datafusion::physical_plan::accept(plan, &mut extractor).unwrap();

        extractor.inner
    }

    fn test_chunks(overlapped: bool) -> (Arc<Schema>, Vec<Arc<dyn QueryChunk>>) {
        let max_time = if overlapped { 70000 } else { 7000 };
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_partition_id(1)
                .with_time_column_with_stats(Some(50), Some(max_time))
                .with_tag_column_with_stats("tag1", Some("AL"), Some("MT"))
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );

        // Chunk 2 has an extra field, and only 4 rows
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_partition_id(1)
                .with_time_column_with_stats(Some(28000), Some(220000))
                .with_tag_column_with_stats("tag1", Some("UT"), Some("WA"))
                .with_i64_field_column("field_int")
                .with_i64_field_column("field_int2")
                .with_may_contain_pk_duplicates(true)
                .with_four_rows_of_data(),
        );

        let schema = SchemaMerger::new()
            .merge(&chunk1.schema())
            .unwrap()
            .merge(&chunk2.schema())
            .unwrap()
            .build();

        (schema, vec![chunk1, chunk2])
    }

    fn get_test_chunks() -> (Arc<Schema>, Vec<Arc<dyn QueryChunk>>) {
        test_chunks(false)
    }

    fn get_test_overlapped_chunks() -> (Arc<Schema>, Vec<Arc<dyn QueryChunk>>) {
        test_chunks(true)
    }
}
