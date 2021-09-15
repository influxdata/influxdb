//! This module contains the code to map DataFusion metrics to `Span`s
//! for use in distributed tracing (e.g. Jaeger)

use std::{borrow::Cow, fmt};

use chrono::{DateTime, Utc};
use datafusion::physical_plan::{
    metrics::{MetricValue, MetricsSet},
    DisplayFormatType, ExecutionPlan,
};
use observability_deps::tracing::debug;
use trace::span::Span;

/// This function translates data in DataFusion `MetricSets` into IOx
/// [`Span`]s. It records a snapshot of the current state of the
/// DataFusion metrics, so it should only be invoked *after* a plan is
/// fully `collect`ed.
///
/// Each `ExecutionPlan` in the plan gets its own new [`Span`]
///
/// The start and end time of the span are taken from the
/// ExecutionPlan's metrics, falling back to the parent span's
/// timestamps if there are no metrics
///
/// Span metadata is used to record:
/// 1. If the ExecutionPlan had no metrics
/// 2. The total number of rows produced by the ExecutionPlan (if available)
/// 3. The elapsed compute time taken by the ExecutionPlan
pub(crate) fn send_metrics_to_tracing(parent_span: &Span, physical_plan: &dyn ExecutionPlan) {
    // The parent span may be open, but since the physical_plan is
    // assumed to be fully collected, using `now()` is a conservative
    // estimate of the end time
    let default_end_time = Utc::now();
    send_metrics_to_tracing_inner(default_end_time, parent_span, physical_plan)
}

fn send_metrics_to_tracing_inner(
    default_end_time: DateTime<Utc>,
    parent_span: &Span,
    physical_plan: &dyn ExecutionPlan,
) {
    // Somthing like this when one_line is contributed back upstream
    //let plan_name = physical_plan.displayable().one_line().to_string();

    // create a child span for this physical plan node. Truncate the
    // name first 20 characters of the display representation to avoid
    // making massive span names
    let plan_name = one_line(physical_plan).to_string();

    let plan_name = if plan_name.len() > 20 {
        Cow::Owned((&plan_name[0..20]).to_string())
    } else {
        Cow::Owned(plan_name)
    };
    let mut span = parent_span.child(plan_name);

    span.start = parent_span.start;

    // parent span may not have completed yet
    let span_end = parent_span.end.unwrap_or(default_end_time);
    span.end = Some(span_end);

    match physical_plan.metrics() {
        None => {
            // this DataFusion node had no metrics, so record that in
            // metadata and use the start/stop time of the parent span
            span.metadata
                .insert("missing_statistics".into(), "true".into());
        }
        Some(metrics) => {
            // this DataFusion node had metrics, translate them into
            // span information

            // Aggregate metrics from all DataFusion partitions
            // together (maybe in the future it would be neat to
            // expose per partition traces)
            let metrics = metrics.aggregate_by_partition();

            let (start_ts, end_ts) = get_timestamps(&metrics);

            if start_ts.is_some() {
                span.start = start_ts
            }

            if end_ts.is_some() {
                span.end = end_ts
            }

            if let Some(output_rows) = metrics.output_rows() {
                let output_rows = output_rows as i64;
                span.metadata
                    .insert("output_rows".into(), output_rows.into());
            }
            if let Some(elapsed_compute) = metrics.elapsed_compute() {
                let elapsed_compute = elapsed_compute as i64;
                span.metadata
                    .insert("elapsed_compute_nanos".into(), elapsed_compute.into());
            }
        }
    }

    // recurse
    for child in physical_plan.children() {
        send_metrics_to_tracing_inner(span_end, &span, child.as_ref())
    }

    span.export()
}

// todo contribute this back upstream to datafusion (add to `DisplayableExecutionPlan`)

/// Return a `Display`able structure that produces a single line, for
/// this node only (does not recurse to children)
pub fn one_line(plan: &dyn ExecutionPlan) -> impl fmt::Display + '_ {
    struct Wrapper<'a> {
        plan: &'a dyn ExecutionPlan,
    }
    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let t = DisplayFormatType::Default;
            self.plan.fmt_as(t, f)
        }
    }

    Wrapper { plan }
}

// TODO maybe also contribute these back upstream to datafusion (make
// as a method on MetricsSet)

/// Return the start, and end timestamps of the metrics set, if any
fn get_timestamps(metrics: &MetricsSet) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let mut start_ts = None;
    let mut end_ts = None;

    for metric in metrics.iter() {
        if metric.labels().is_empty() {
            match metric.value() {
                MetricValue::StartTimestamp(ts) => {
                    if ts.value().is_some() && start_ts.is_some() {
                        debug!(
                            ?metric,
                            ?start_ts,
                            "WARNING: more than one StartTimestamp metric found"
                        )
                    }
                    start_ts = ts.value()
                }
                MetricValue::EndTimestamp(ts) => {
                    if ts.value().is_some() && end_ts.is_some() {
                        debug!(
                            ?metric,
                            ?end_ts,
                            "WARNING: more than one EndTimestamp metric found"
                        )
                    }
                    end_ts = ts.value()
                }
                _ => {}
            }
        }
    }

    (start_ts, end_ts)
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use chrono::TimeZone;
    use datafusion::physical_plan::{
        metrics::{Count, Time, Timestamp},
        Metric,
    };

    use std::{
        num::{NonZeroU128, NonZeroU64},
        sync::Arc,
        time::Duration,
    };

    use trace::{
        ctx::{SpanContext, SpanId, TraceId},
        span::{MetaValue, SpanStatus},
        RingBufferTraceCollector,
    };

    use super::*;

    #[test]
    fn name_truncation() {
        let name = "This is a really super duper long node name";
        let exec = TestExec::new(name, Default::default());

        let traces = TraceBuilder::new();
        send_metrics_to_tracing(&traces.make_span(), &exec);

        let spans = traces.spans();
        assert_eq!(spans.len(), 1);
        // name is truncated to 20 cahracters
        assert_eq!(spans[0].name, "TestExec: This is a ", "span: {:#?}", spans);
    }

    // children and time propagation
    #[test]
    fn children_and_timestamps() {
        let ts1 = Utc.timestamp(1, 0);
        let ts2 = Utc.timestamp(2, 0);
        let ts3 = Utc.timestamp(3, 0);
        let ts4 = Utc.timestamp(4, 0);
        let ts5 = Utc.timestamp(5, 0);

        // build this timestamp tree:
        //
        // exec:   [ ts1 -------- ts4]   <-- both start and end timestamps
        // child1:   [ ts2 - ]      <-- only start timestamp
        // child2:   [ ts2 --- ts3] <-- both start and end timestamps
        // child3:   [     --- ts3] <-- only end timestamps (e.g. bad data)
        // child4:   [     ]        <-- no timestamps
        let mut exec = TestExec::new("exec", make_time_metricset(Some(ts1), Some(ts4)));
        exec.new_child("child1", make_time_metricset(Some(ts2), None));
        exec.new_child("child2", make_time_metricset(Some(ts2), Some(ts3)));
        exec.new_child("child3", make_time_metricset(None, Some(ts3)));
        exec.new_child("child4", make_time_metricset(None, None));

        let traces = TraceBuilder::new();
        send_metrics_to_tracing_inner(ts5, &traces.make_span(), &exec);

        let spans = traces.spans();
        println!("Spans: \n\n{:#?}", spans);
        assert_eq!(spans.len(), 5);

        let check_span = |span: &Span, expected_name, expected_start, expected_end| {
            assert_eq!(span.name, expected_name, "name; {:?}", span);
            assert_eq!(span.start, expected_start, "expected start; {:?}", span);
            assert_eq!(span.end, expected_end, "expected end; {:?}", span);
        };

        check_span(&spans[0], "TestExec: child1", Some(ts2), Some(ts4));
        check_span(&spans[1], "TestExec: child2", Some(ts2), Some(ts3));
        check_span(&spans[2], "TestExec: child3", Some(ts1), Some(ts3));
        check_span(&spans[3], "TestExec: child4", Some(ts1), Some(ts4));
        check_span(&spans[4], "TestExec: exec", Some(ts1), Some(ts4));
    }

    #[test]
    fn no_metrics() {
        // given execution plan with no metrics, should add notation on metadata
        let mut exec = TestExec::new("exec", Default::default());
        exec.metrics = None;

        let traces = TraceBuilder::new();
        send_metrics_to_tracing(&traces.make_span(), &exec);

        let spans = traces.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].metadata.get("missing_statistics"),
            Some(&MetaValue::String("true".into())),
            "spans: {:#?}",
            spans
        );
    }

    // row count and elapsed compute
    #[test]
    fn metrics() {
        // given execution plan with execution time and compute spread across two partitions (1, and 2)
        let mut exec = TestExec::new("exec", Default::default());
        add_output_rows(exec.metrics_mut(), 100, 1);
        add_output_rows(exec.metrics_mut(), 200, 2);

        add_elapsed_compute(exec.metrics_mut(), 1000, 1);
        add_elapsed_compute(exec.metrics_mut(), 2000, 2);

        let traces = TraceBuilder::new();
        send_metrics_to_tracing(&traces.make_span(), &exec);

        // aggregated metrics should be reported
        let spans = traces.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].metadata.get("output_rows"),
            Some(&MetaValue::Int(300)),
            "spans: {:#?}",
            spans
        );
        assert_eq!(
            spans[0].metadata.get("elapsed_compute_nanos"),
            Some(&MetaValue::Int(3000)),
            "spans: {:#?}",
            spans
        );
    }

    fn add_output_rows(metrics: &mut MetricsSet, output_rows: usize, partition: usize) {
        let value = Count::new();
        value.add(output_rows);

        let partition = Some(partition);
        metrics.push(Arc::new(Metric::new(
            MetricValue::OutputRows(value),
            partition,
        )));
    }

    fn add_elapsed_compute(metrics: &mut MetricsSet, elapsed_compute: u64, partition: usize) {
        let value = Time::new();
        value.add_duration(Duration::from_nanos(elapsed_compute));

        let partition = Some(partition);
        metrics.push(Arc::new(Metric::new(
            MetricValue::ElapsedCompute(value),
            partition,
        )));
    }

    fn make_time_metricset(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> MetricsSet {
        let mut metrics = MetricsSet::new();
        if let Some(start) = start {
            let value = make_metrics_timestamp(start);
            let partition = None;
            metrics.push(Arc::new(Metric::new(
                MetricValue::StartTimestamp(value),
                partition,
            )));
        }

        if let Some(end) = end {
            let value = make_metrics_timestamp(end);
            let partition = None;
            metrics.push(Arc::new(Metric::new(
                MetricValue::EndTimestamp(value),
                partition,
            )));
        }

        metrics
    }

    fn make_metrics_timestamp(t: DateTime<Utc>) -> Timestamp {
        let timestamp = Timestamp::new();
        timestamp.set(t);
        timestamp
    }

    /// Encapsulates creating and capturing spans for tests
    struct TraceBuilder {
        collector: Arc<RingBufferTraceCollector>,
    }

    impl TraceBuilder {
        fn new() -> Self {
            Self {
                collector: Arc::new(RingBufferTraceCollector::new(10)),
            }
        }

        // create a new span connected to the collector
        fn make_span(&self) -> Span {
            let collector = Arc::clone(&self.collector);

            // lifted from make_span in trace/src/span.rs
            Span {
                name: "foo".into(),
                ctx: SpanContext {
                    trace_id: TraceId(NonZeroU128::new(23948923).unwrap()),
                    parent_span_id: None,
                    span_id: SpanId(NonZeroU64::new(3498394).unwrap()),
                    collector: Some(collector),
                },
                start: None,
                end: None,
                status: SpanStatus::Unknown,
                metadata: Default::default(),
                events: vec![],
            }
        }

        /// return all collected spans
        fn spans(&self) -> Vec<Span> {
            self.collector.spans()
        }
    }

    /// mocked out execution plan we can control metrics
    #[derive(Debug)]
    struct TestExec {
        name: String,
        metrics: Option<MetricsSet>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    }

    impl TestExec {
        fn new(name: impl Into<String>, metrics: MetricsSet) -> Self {
            Self {
                name: name.into(),
                metrics: Some(metrics),
                children: vec![],
            }
        }

        fn new_child(&mut self, name: impl Into<String>, metrics: MetricsSet) {
            self.children.push(Arc::new(Self::new(name, metrics)));
        }

        fn metrics_mut(&mut self) -> &mut MetricsSet {
            self.metrics.as_mut().unwrap()
        }
    }

    #[async_trait]
    impl ExecutionPlan for TestExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            unimplemented!()
        }

        fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
            unimplemented!()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            self.children.clone()
        }

        fn with_new_children(
            &self,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        async fn execute(
            &self,
            _partition: usize,
        ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            unimplemented!()
        }

        fn statistics(&self) -> datafusion::physical_plan::Statistics {
            unimplemented!()
        }

        fn metrics(&self) -> Option<MetricsSet> {
            self.metrics.clone()
        }

        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestExec: {}", self.name)
        }
    }
}
