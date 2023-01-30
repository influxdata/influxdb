//! This module contains the code to map DataFusion metrics to `Span`s
//! for use in distributed tracing (e.g. Jaeger)

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::physical_plan::{
    metrics::{MetricValue, MetricsSet},
    DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use futures::StreamExt;
use hashbrown::HashMap;
use observability_deps::tracing::debug;
use std::{fmt, sync::Arc};
use trace::span::{Span, SpanRecorder};

const PER_PARTITION_TRACING_ENABLE_ENV: &str = "INFLUXDB_IOX_PER_PARTITION_TRACING";
fn per_partition_tracing() -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    static TRACING_ENABLED: AtomicU8 = AtomicU8::new(u8::MAX);

    match TRACING_ENABLED.load(Ordering::Relaxed) {
        u8::MAX => {
            let val = std::env::var(PER_PARTITION_TRACING_ENABLE_ENV)
                .ok()
                .and_then(|x| x.parse::<BooleanFlag>().ok())
                .map(Into::into)
                .unwrap_or(false);

            TRACING_ENABLED.store(val as u8, Ordering::Relaxed);
            val
        }
        x => x != 0,
    }
}

/// Stream wrapper that records DataFusion `MetricSets` into IOx
/// [`Span`]s when it is dropped.
pub(crate) struct TracedStream {
    inner: SendableRecordBatchStream,
    span_recorder: SpanRecorder,
    physical_plan: Arc<dyn ExecutionPlan>,
}

impl TracedStream {
    /// Return a stream that records DataFusion `MetricSets` from
    /// `physical_plan` into `span` when dropped.
    pub(crate) fn new(
        inner: SendableRecordBatchStream,
        span: Option<trace::span::Span>,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            inner,
            span_recorder: SpanRecorder::new(span),
            physical_plan,
        }
    }
}

impl RecordBatchStream for TracedStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for TracedStream {
    type Item = arrow::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl Drop for TracedStream {
    fn drop(&mut self) {
        if let Some(span) = self.span_recorder.span() {
            let default_end_time = Utc::now();
            let per_partition_tracing = per_partition_tracing();
            send_metrics_to_tracing(
                default_end_time,
                span,
                self.physical_plan.as_ref(),
                per_partition_tracing,
            );
        }
    }
}

/// This function translates data in DataFusion `MetricSets` into IOx
/// [`Span`]s. It records a snapshot of the current state of the
/// DataFusion metrics, so it should only be invoked *after* a plan is
/// fully `collect`ed.
///
/// Each `ExecutionPlan` in the plan gets its own new [`Span`] that covers
/// the time spent executing its partitions and its children
///
/// Each `ExecutionPlan` also has a new [`Span`] for each of its
/// partitions that collected metrics
///
/// The start and end time of the span are taken from the
/// ExecutionPlan's metrics, falling back to the parent span's
/// timestamps if there are no metrics
///
/// Span metadata is used to record:
/// 1. If the ExecutionPlan had no metrics
/// 2. The total number of rows produced by the ExecutionPlan (if available)
/// 3. The elapsed compute time taken by the ExecutionPlan
fn send_metrics_to_tracing(
    default_end_time: DateTime<Utc>,
    parent_span: &Span,
    physical_plan: &dyn ExecutionPlan,
    per_partition_tracing: bool,
) {
    // Something like this when one_line is contributed back upstream
    //let plan_name = physical_plan.displayable().one_line().to_string();
    let desc = one_line(physical_plan).to_string();
    let operator_name: String = desc.chars().take_while(|x| *x != ':').collect();

    // Get the timings of the parent operator
    let parent_start_time = parent_span.start.unwrap_or(default_end_time);
    let parent_end_time = parent_span.end.unwrap_or(default_end_time);

    // A span for the operation, this is the aggregate of all the partition spans
    let mut operator_span = parent_span.child(operator_name.clone());
    operator_span.metadata.insert("desc".into(), desc.into());

    let mut operator_metrics = SpanMetrics {
        output_rows: None,
        elapsed_compute_nanos: None,
    };

    // The total duration for this span and all its children and partitions
    let mut operator_start_time = DateTime::<Utc>::MAX_UTC;
    let mut operator_end_time = DateTime::<Utc>::MIN_UTC;

    match physical_plan.metrics() {
        None => {
            // this DataFusion node had no metrics, so record that in
            // metadata and use the start/stop time of the parent span
            operator_span
                .metadata
                .insert("missing_statistics".into(), "true".into());
        }
        Some(metrics) => {
            // Create a separate span for each partition in the operator
            for (partition, metrics) in partition_metrics(metrics) {
                let (start_ts, end_ts) = get_timestamps(&metrics);

                let partition_start_time = start_ts.unwrap_or(parent_start_time);
                let partition_end_time = end_ts.unwrap_or(parent_end_time);

                let partition_metrics = SpanMetrics {
                    output_rows: metrics.output_rows(),
                    elapsed_compute_nanos: metrics.elapsed_compute(),
                };

                operator_start_time = operator_start_time.min(partition_start_time);
                operator_end_time = operator_end_time.max(partition_end_time);

                // Update the aggregate totals in the operator span
                operator_metrics.aggregate_child(&partition_metrics);

                // Generate a span for the partition if
                // - these metrics correspond to a partition
                // - per partition tracing is enabled
                if per_partition_tracing {
                    if let Some(partition) = partition {
                        let mut partition_span =
                            operator_span.child(format!("{} ({})", operator_name, partition));

                        partition_span.start = Some(partition_start_time);
                        partition_span.end = Some(partition_end_time);

                        partition_metrics.add_to_span(&mut partition_span);

                        partition_span.export();
                    }
                }
            }
        }
    }

    // If we've not encountered any metrics to determine the operator's start
    // and end time, use those of the parent
    if operator_start_time == DateTime::<Utc>::MAX_UTC {
        operator_start_time = parent_span.start.unwrap_or(default_end_time);
    }

    if operator_end_time == DateTime::<Utc>::MIN_UTC {
        operator_end_time = parent_span.end.unwrap_or(default_end_time);
    }

    operator_span.start = Some(operator_start_time);
    operator_span.end = Some(operator_end_time);

    // recurse
    for child in physical_plan.children() {
        send_metrics_to_tracing(
            operator_end_time,
            &operator_span,
            child.as_ref(),
            per_partition_tracing,
        );
    }

    operator_metrics.add_to_span(&mut operator_span);
    operator_span.export();
}

#[derive(Debug)]
struct SpanMetrics {
    output_rows: Option<usize>,
    elapsed_compute_nanos: Option<usize>,
}

impl SpanMetrics {
    fn aggregate_child(&mut self, child: &Self) {
        if let Some(rows) = child.output_rows {
            *self.output_rows.get_or_insert(0) += rows;
        }

        if let Some(nanos) = child.elapsed_compute_nanos {
            *self.elapsed_compute_nanos.get_or_insert(0) += nanos;
        }
    }

    fn add_to_span(&self, span: &mut Span) {
        if let Some(rows) = self.output_rows {
            span.metadata
                .insert("output_rows".into(), (rows as i64).into());
        }

        if let Some(nanos) = self.elapsed_compute_nanos {
            span.metadata
                .insert("elapsed_compute_nanos".into(), (nanos as i64).into());
        }
    }
}

fn partition_metrics(metrics: MetricsSet) -> HashMap<Option<usize>, MetricsSet> {
    let mut hashmap = HashMap::<_, MetricsSet>::new();
    for metric in metrics.iter() {
        hashmap
            .entry(metric.partition())
            .or_default()
            .push(Arc::clone(metric))
    }
    hashmap
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

/// Boolean flag that works with environment variables.
#[derive(Debug, Clone, Copy)]
pub enum BooleanFlag {
    True,
    False,
}

impl std::str::FromStr for BooleanFlag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "yes" | "y" | "true" | "t" | "1" => Ok(Self::True),
            "no" | "n" | "false" | "f" | "0" => Ok(Self::False),
            _ => Err(format!(
                "Invalid boolean flag '{}'. Valid options: yes, no, y, n, true, false, t, f, 1, 0",
                s
            )),
        }
    }
}

impl From<BooleanFlag> for bool {
    fn from(yes_no: BooleanFlag) -> Self {
        matches!(yes_no, BooleanFlag::True)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use datafusion::{
        execution::context::TaskContext,
        physical_plan::{
            expressions::PhysicalSortExpr,
            metrics::{Count, Time, Timestamp},
            Metric,
        },
    };
    use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
    use trace::{ctx::SpanContext, span::MetaValue, RingBufferTraceCollector};

    #[test]
    fn name_truncation() {
        let name = "Foo: expr nonsense";
        let exec = TestExec::new(name, Default::default());

        let traces = TraceBuilder::new();
        send_metrics_to_tracing(Utc::now(), &traces.make_span(), &exec, true);

        let spans = traces.spans();
        assert_eq!(spans.len(), 1);

        // name is truncated to the operator name
        assert_eq!(spans[0].name, "TestExec - Foo", "span: {:#?}", spans);
    }

    // children and time propagation
    #[test]
    fn children_and_timestamps() {
        let ts1 = Utc.timestamp_opt(1, 0).unwrap();
        let ts2 = Utc.timestamp_opt(2, 0).unwrap();
        let ts3 = Utc.timestamp_opt(3, 0).unwrap();
        let ts4 = Utc.timestamp_opt(4, 0).unwrap();
        let ts5 = Utc.timestamp_opt(5, 0).unwrap();

        let mut many_partition = MetricsSet::new();
        add_time_metrics(&mut many_partition, None, Some(ts2), Some(1));
        add_time_metrics(&mut many_partition, Some(ts2), Some(ts3), Some(2));
        add_time_metrics(&mut many_partition, Some(ts1), None, Some(3));

        // build this timestamp tree:
        //
        // exec:   [ ts1 -------- ts4]   <-- both start and end timestamps
        // child1:   [ ts2 - ]      <-- only start timestamp
        // child2:   [ ts2 --- ts3] <-- both start and end timestamps
        // child3:   [     --- ts3] <-- only end timestamps (e.g. bad data)
        // child4:   [     ]        <-- no timestamps
        // child5 (1): [   --- ts2]
        // child5 (2): [ ts2 --- ts3]
        // child5 (4): [ ts1 ---  ]
        let mut exec = TestExec::new("exec", make_time_metric_set(Some(ts1), Some(ts4), Some(1)));
        exec.new_child(
            "child1: foo",
            make_time_metric_set(Some(ts2), None, Some(1)),
        );
        exec.new_child(
            "child2: bar",
            make_time_metric_set(Some(ts2), Some(ts3), None),
        );
        exec.new_child(
            "child3: baz",
            make_time_metric_set(None, Some(ts3), Some(1)),
        );
        exec.new_child("child4: bingo", make_time_metric_set(None, None, Some(1)));
        exec.new_child("child5: bongo", many_partition);

        let traces = TraceBuilder::new();
        send_metrics_to_tracing(ts5, &traces.make_span(), &exec, true);

        let spans = traces.spans();
        let spans: BTreeMap<_, _> = spans.iter().map(|s| (s.name.as_ref(), s)).collect();

        println!("Spans: \n\n{:#?}", spans);
        assert_eq!(spans.len(), 12);

        let check_span = |span: &Span, expected_start, expected_end, desc: Option<&str>| {
            assert_eq!(span.start, expected_start, "expected start; {:?}", span);
            assert_eq!(span.end, expected_end, "expected end; {:?}", span);
            assert_eq!(span.metadata.get("desc").map(|x| x.string().unwrap()), desc);
        };

        check_span(
            spans["TestExec - exec"],
            Some(ts1),
            Some(ts4),
            Some("TestExec - exec"),
        );
        check_span(spans["TestExec - exec (1)"], Some(ts1), Some(ts4), None);

        check_span(
            spans["TestExec - child1"],
            Some(ts2),
            Some(ts4),
            Some("TestExec - child1: foo"),
        );
        check_span(spans["TestExec - child1 (1)"], Some(ts2), Some(ts4), None);

        check_span(
            spans["TestExec - child2"],
            Some(ts2),
            Some(ts3),
            Some("TestExec - child2: bar"),
        );

        check_span(
            spans["TestExec - child3"],
            Some(ts1),
            Some(ts3),
            Some("TestExec - child3: baz"),
        );
        check_span(spans["TestExec - child3 (1)"], Some(ts1), Some(ts3), None);

        check_span(
            spans["TestExec - child4"],
            Some(ts1),
            Some(ts4),
            Some("TestExec - child4: bingo"),
        );

        check_span(
            spans["TestExec - child5"],
            Some(ts1),
            Some(ts4),
            Some("TestExec - child5: bongo"),
        );
        check_span(spans["TestExec - child5 (1)"], Some(ts1), Some(ts2), None);
        check_span(spans["TestExec - child5 (2)"], Some(ts2), Some(ts3), None);
        check_span(spans["TestExec - child5 (3)"], Some(ts1), Some(ts4), None);
    }

    #[test]
    fn no_metrics() {
        // given execution plan with no metrics, should add notation on metadata
        let mut exec = TestExec::new("exec", Default::default());
        exec.metrics = None;

        let traces = TraceBuilder::new();
        send_metrics_to_tracing(Utc::now(), &traces.make_span(), &exec, true);

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
        send_metrics_to_tracing(Utc::now(), &traces.make_span(), &exec, true);

        // aggregated metrics should be reported
        let spans = traces.spans();
        let spans: BTreeMap<_, _> = spans.iter().map(|s| (s.name.as_ref(), s)).collect();

        assert_eq!(spans.len(), 3);

        let check_span = |span: &Span, output_row: i64, nanos: i64| {
            assert_eq!(
                span.metadata.get("output_rows"),
                Some(&MetaValue::Int(output_row)),
                "span: {:#?}",
                span
            );

            assert_eq!(
                span.metadata.get("elapsed_compute_nanos"),
                Some(&MetaValue::Int(nanos)),
                "spans: {:#?}",
                span
            );
        };

        check_span(spans["TestExec - exec"], 300, 3000);
        check_span(spans["TestExec - exec (1)"], 100, 1000);
        check_span(spans["TestExec - exec (2)"], 200, 2000);
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

    fn make_time_metric_set(
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        partition: Option<usize>,
    ) -> MetricsSet {
        let mut metrics = MetricsSet::new();
        add_time_metrics(&mut metrics, start, end, partition);
        metrics
    }

    fn add_time_metrics(
        metrics: &mut MetricsSet,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        partition: Option<usize>,
    ) {
        if let Some(start) = start {
            let value = make_metrics_timestamp(start);
            metrics.push(Arc::new(Metric::new(
                MetricValue::StartTimestamp(value),
                partition,
            )));
        }

        if let Some(end) = end {
            let value = make_metrics_timestamp(end);
            metrics.push(Arc::new(Metric::new(
                MetricValue::EndTimestamp(value),
                partition,
            )));
        }
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
            SpanContext::new(Arc::clone(&self.collector) as _).child("foo")
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

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            unimplemented!()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            self.children.clone()
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
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
            write!(f, "TestExec - {}", self.name)
        }
    }

    #[test]
    fn test_parsing() {
        assert!(bool::from(BooleanFlag::from_str("yes").unwrap()));
        assert!(bool::from(BooleanFlag::from_str("Yes").unwrap()));
        assert!(bool::from(BooleanFlag::from_str("YES").unwrap()));

        assert!(!bool::from(BooleanFlag::from_str("No").unwrap()));
        assert!(!bool::from(BooleanFlag::from_str("FaLse").unwrap()));

        BooleanFlag::from_str("foo").unwrap_err();
    }
}
