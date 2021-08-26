pub mod influxrpc;
pub mod reorg;
pub mod sql;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::compute::SortOptions;
    use datafusion::physical_plan::{
        metrics::{self, MetricValue},
        ExecutionPlan, ExecutionPlanVisitor,
    };
    use futures::StreamExt;
    use internal_types::schema::{merge::SchemaMerger, sort::SortKey, Schema};

    use crate::{
        exec::{split::StreamSplitExec, Executor, ExecutorType},
        frontend::reorg::ReorgPlanner,
        provider::{DeduplicateExec, IOxReadFilterNode},
        test::TestChunk,
        QueryChunkMeta,
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
    async fn test_metrics() {
        let (schema, chunks) = get_test_chunks();

        let mut sort_key = SortKey::with_capacity(1);
        sort_key.push(
            "time",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );

        // Use a split plan as it has StreamSplitExec, DeduplicateExec and IOxReadFilternode
        let (_, split_plan) = ReorgPlanner::new()
            .split_plan(schema, chunks, sort_key, 1000)
            .expect("created compact plan");

        let executor = Executor::new(1);
        let plan = executor
            .new_context(ExecutorType::Reorg)
            .prepare_plan(&split_plan)
            .unwrap();

        let mut stream0 = plan.execute(0).await.expect("ran the plan");
        let mut num_rows = 0;
        while let Some(batch) = stream0.next().await {
            num_rows += batch.unwrap().num_rows();
        }
        assert_eq!(num_rows, 3);

        let mut stream1 = plan.execute(1).await.expect("ran the plan");
        let mut num_rows = 0;
        while let Some(batch) = stream1.next().await {
            num_rows += batch.unwrap().num_rows();
        }
        assert_eq!(num_rows, 5);

        // now validate metrics are good
        let extracted = extract_metrics(plan.as_ref(), |plan| {
            plan.as_any()
                .downcast_ref::<IOxReadFilterNode<TestChunk>>()
                .is_some()
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
            let metrics = plan.metrics().unwrap().aggregate_by_partition();
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

    fn get_test_chunks() -> (Arc<Schema>, Vec<Arc<TestChunk>>) {
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(50), Some(7000))
                .with_tag_column_with_stats("tag1", Some("AL"), Some("MT"))
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );

        // Chunk 2 has an extra field, and only 4 fields
        let chunk2 = Arc::new(
            TestChunk::new("t")
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

        (Arc::new(schema), vec![chunk1, chunk2])
    }
}
