//! Ring buffer of queries that have been run with some brief information

use crate::exec::IOxSessionContext;
use crate::memory_pool::Monitor;
use crate::physical_optimizer::ParquetFileMetrics;
use data_types::NamespaceId;
use datafusion::physical_plan::{
    ExecutionPlan,
    metrics::{MetricValue, MetricsSet},
};
use influxdb_iox_client::batched_write::MaybeBatchedWriteClient as WriteClient;
use influxdb_line_protocol::LineProtocolBuilder;
use iox_query_params::StatementParams;
use iox_time::{Time, TimeProvider};
use metric::{
    DurationHistogram, Metric, MetricObserver, U64Counter, U64Gauge, U64Histogram,
    U64HistogramOptions,
};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    fmt::Debug,
    ops::DerefMut,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use trace::ctx::TraceId;
use tracing::{info, warn};
use tracker::InstrumentedAsyncOwnedSemaphorePermit;
use uuid::Uuid;

/// Phase of a query entry.
///
/// ```text
///         +---------------------------------+---> fail
///         |                                 |
///         |                                 |
/// ---> received ---> planned ---> permit ---+
///         |             |           |       |
///         |             |           |       |
///         |             |           |       +---> success
///         |             |           |
///         |             |           |
///         +-------------+-----------+-----------> cancel
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum QueryPhase {
    /// Query was received but not processed.
    ///
    /// This is the initial state.
    ///
    /// # Done
    /// - The query has been received (and potentially authenticated) by the server.
    ///
    /// # To Do
    /// - The query is not planned.
    /// - The concurrency-limiting semaphore has NOT yet issued a permit.
    /// - The query has not been executed.
    Received,

    /// Query was planned and is waiting for a semaphore permit.
    ///
    /// # Done
    /// - The query has been received (and potentially authenticated) by the server.
    /// - The query was planned.
    ///
    /// # To Do
    /// - The concurrency-limiting semaphore has NOT yet issued a permit.
    /// - The query has not been executed.
    Planned,

    /// Query has the permit to be executed and is likely being executed.
    ///
    /// # Done
    /// - The query has been received (and potentially authenticated) by the server.
    /// - The query was planned.
    /// - The concurrency-limiting semaphore has issued a permit.
    ///
    /// # To Do
    /// - The query has not been executed.
    Permit,

    /// Query was cancelled (likely by the user or a downstream component).
    ///
    /// This is a terminal state.
    Cancel,

    /// Query was fully executed successfully.
    ///
    /// This is a terminal state.
    Success,

    /// Query failed due to an error, e.g. during planning or during execution.
    ///
    /// This is a terminal state.
    Fail,
}

impl QueryPhase {
    /// Name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Planned => "planned",
            Self::Permit => "permit",
            Self::Cancel => "cancel",
            Self::Success => "success",
            Self::Fail => "fail",
        }
    }
}

impl std::fmt::Debug for QueryPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::fmt::Display for QueryPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub struct IngesterMetrics {
    /// when the querier has enough information from all ingesters so that it can proceed with query planning
    pub latency_to_plan: Duration,

    /// measured from the initial request, when the querier has all the data from all ingesters
    pub latency_to_full_data: Duration,

    /// ingester response rows
    pub response_rows: u64,

    /// ingester partition count
    pub partition_count: u64,

    /// ingester record batch size in bytes
    pub response_size: u64,
}

/// State of a [`QueryLogEntry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogEntryState {
    /// Unique ID.
    pub id: Uuid,

    /// Namespace ID.
    pub namespace_id: NamespaceId,

    /// Namespace name.
    pub namespace_name: Arc<str>,

    /// The type of query
    pub query_type: &'static str,

    /// The text of the query (SQL for sql queries, pbjson for storage rpc queries)
    pub query_text: QueryTextWrapper,

    /// key-value parameters associated with this query
    pub query_params: StatementParams,

    /// An identifier for the authentication that was used to run the
    /// query. This is output in various logs so MUST NOT be a secret
    /// value.
    pub auth_id: Option<String>,

    /// The trace ID if any
    pub trace_id: Option<TraceId>,

    /// Time at which the query was run
    pub issue_time: Time,

    /// Number of partitions processed by the query.
    pub partitions: Option<u64>,

    /// Number of rows returned by this query.
    pub num_rows: Option<u64>,

    /// Number of parquet files processed by the query.
    pub parquet_files: Option<u64>,

    /// Duration it took to acquire a semaphore permit.
    pub permit_duration: Option<Duration>,

    /// Duration it took to plan the query.
    pub plan_duration: Option<Duration>,

    /// Duration it took to execute the query.
    pub execute_duration: Option<Duration>,

    /// Duration from [`issue_time`](Self::issue_time) til the query ended somehow.
    pub end2end_duration: Option<Duration>,

    /// CPU duration spend for computation.
    pub compute_duration: Option<Duration>,

    /// Peak memory allocated for processing the query.
    pub max_memory: Option<i64>,

    /// If the query completed successfully
    pub success: bool,

    /// If the query is currently running (in any state).
    pub running: bool,

    /// Phase.
    pub phase: QueryPhase,

    pub ingester_metrics: Option<IngesterMetrics>,

    // The number of files that are held under a
    // [`DeduplicateExec`](crate::provider::deduplicate::DeduplicateExec)
    pub deduplicated_parquet_files: Option<u64>,

    // The number of partitions that are held under a
    // [`DeduplicateExec`](crate::provider::deduplicate::DeduplicateExec)
    pub deduplicated_partitions: Option<u64>,
}

impl QueryLogEntryState {
    /// Convert a query log entry state into the InfluxDB line protocol format.
    pub fn to_line_protocol(
        &self,
        builder: LineProtocolBuilder<Vec<u8>>,
        measurement_name: &str,
    ) -> LineProtocolBuilder<Vec<u8>> {
        // Tags

        let mut lp = builder
            .measurement(measurement_name)
            .tag("id", &self.id.to_string())
            .tag("namespace_id", &self.namespace_id.get().to_string())
            .tag("namespace_name", &self.namespace_name)
            .tag("query_type", self.query_type)
            .tag("phase", self.phase.name());

        lp = match &self.auth_id {
            Some(auth_id) => lp.tag("auth_id", auth_id.as_str()),
            None => lp,
        };

        lp = match &self.trace_id {
            Some(trace_id) => lp.tag("trace_id", trace_id.get().to_string().as_str()),
            None => lp,
        };

        // Fields

        let mut lp = lp
            .field("running", self.running.to_string().as_str())
            .field("success", self.success.to_string().as_str())
            .field("query_text", self.query_text.to_string().as_str())
            .field("query_params", self.query_params.to_string().as_str())
            .field("query_issue_time_ns", self.issue_time.timestamp_nanos());

        lp = match self.partitions {
            Some(partitions) => lp.field("partition_count", partitions),
            None => lp,
        };

        lp = match self.parquet_files {
            Some(parquet_files) => lp.field("parquet_file_count", parquet_files),
            None => lp,
        };

        lp = match &self.permit_duration {
            Some(permit_duration) => {
                lp.field("permit_duration_ns", permit_duration.as_nanos() as u64)
            }
            None => lp,
        };

        lp = match &self.plan_duration {
            Some(plan_duration) => lp.field("plan_duration_ns", plan_duration.as_nanos() as u64),
            None => lp,
        };

        lp = match &self.execute_duration {
            Some(execute_duration) => {
                lp.field("execute_duration_ns", execute_duration.as_nanos() as u64)
            }
            None => lp,
        };

        lp = match &self.end2end_duration {
            Some(end2end_duration) => {
                lp.field("end_to_end_duration_ns", end2end_duration.as_nanos() as u64)
            }
            None => lp,
        };

        lp = match &self.compute_duration {
            Some(compute_duration) => {
                lp.field("compute_duration_ns", compute_duration.as_nanos() as u64)
            }
            None => lp,
        };

        lp = match self.max_memory {
            Some(max_memory) => lp.field("max_memory_bytes", max_memory),
            None => lp,
        };

        lp = match self.ingester_metrics {
            Some(im) => lp
                .field(
                    "ingester_latency_to_plan_ns",
                    im.latency_to_plan.as_nanos() as u64,
                )
                .field(
                    "ingester_latency_to_full_data_ns",
                    im.latency_to_full_data.as_nanos() as u64,
                )
                .field("ingester_response_row_count", im.response_rows)
                .field("ingester_response_size_bytes", im.response_size)
                .field("ingester_partition_count", im.partition_count),
            None => lp,
        };

        match chrono::Utc::now().timestamp_nanos_opt() {
            Some(ns) => lp.timestamp(ns).close_line(),
            None => lp.close_line(),
        }
    }
}

/// Unpack an option of a struct into a individual members.
///
/// # Example
/// ```
/// # use iox_query::optional_struct;
/// struct S {
///     a: u8,
///     b: String,
/// }
///
/// let s_some: Option<S> = Some(S {
///     a: 1,
///     b: "foo".to_owned(),
/// });
///
/// optional_struct! {
///     S {
///         a,
///         b,
///     } = s_some
/// };
///
/// assert_eq!(
///     format!("{a:?}"),
///     "Some(1)",
/// );
/// assert_eq!(
///     format!("{b:?}"),
///     "Some(\"foo\")",
/// );
/// ```
///
/// This however will fail to compile because you forgot a field:
///
/// ```compile_fail
/// # use iox_query::optional_struct;
/// struct S {
///     a: u8,
///     b: String,
/// }
///
/// let s_some: Option<S> = Some(S {
///     a: 1,
///     b: "foo".to_owned(),
/// });
///
/// optional_struct! {
///     S {
///         a,
///         // b is missing
///     } = s_some
/// };
/// ```
#[macro_export]
macro_rules! optional_struct {
    ($sname:ident {$($fname:ident),*$(,)?} = $s:ident) => {
        let ($($fname),*) = match $s {
            Some($sname {$($fname),*}) => ($(Some($fname)),*),
            None => Default::default(),
        };
    }
}

/// Write `variable` to `metric` (using [`RecordMetricByRef`]) if it is `Some` but was previously `None`.
macro_rules! record_metric_if_now_set {
    ($variable:ident, $prev_state:ident, $metric:ident) => {
        if let (Some(variable), None) = ($variable, $prev_state.and_then(|s| s.$variable.as_ref()))
        {
            $metric.$variable.record_ref(variable);
        }
    };
}

/// Helper trait for [`record_metric_if_now_set`].
trait RecordMetricByRef<V> {
    fn record_ref(&self, v: &V);
}

impl RecordMetricByRef<u64> for U64Histogram {
    fn record_ref(&self, v: &u64) {
        self.record(*v);
    }
}

impl RecordMetricByRef<i64> for U64Histogram {
    fn record_ref(&self, v: &i64) {
        self.record(*v as u64);
    }
}

impl RecordMetricByRef<Duration> for DurationHistogram {
    fn record_ref(&self, v: &Duration) {
        self.record(*v);
    }
}

impl RecordMetricByRef<IngesterMetrics> for MetricsIngesterMetrics {
    fn record_ref(&self, v: &IngesterMetrics) {
        let IngesterMetrics {
            latency_to_plan,
            latency_to_full_data,
            response_rows,
            partition_count,
            response_size,
        } = v;
        self.latency_to_plan.record(*latency_to_plan);
        self.latency_to_full_data.record(*latency_to_full_data);
        self.response_rows.record(*response_rows);
        self.partition_count.record(*partition_count);
        self.response_size.record(*response_size);
    }
}

/// Information about a single query that was executed
#[derive(Debug)]
pub struct QueryLogEntry {
    /// State.
    state: Mutex<Arc<QueryLogEntryState>>,
}

impl QueryLogEntry {
    /// Get current state.
    pub fn state(&self) -> Arc<QueryLogEntryState> {
        Arc::clone(&self.state.lock())
    }

    /// Sets new state and call [`emit`](Self::emit).
    fn set_and_emit(
        &self,
        state: QueryLogEntryState,
        metrics: &Metrics,
        query_log_write_client: Option<Arc<WriteClient>>,
    ) {
        let mut state = Arc::new(state);
        std::mem::swap(self.state.lock().deref_mut(), &mut state);
        self.emit(metrics, Some(state), query_log_write_client);
    }

    /// Emit entry to various systems.
    ///
    /// You should usually call [`set_and_emit`](Self::set_and_emit), but directly calling this method is OK for new
    /// entries (in which case the previous state is `None`).
    fn emit(
        &self,
        metrics: &Metrics,
        prev_state: Option<Arc<QueryLogEntryState>>,
        query_log_write_client: Option<Arc<WriteClient>>,
    ) {
        let state = self.state();

        if let Some(prev_state) = &prev_state {
            assert_ne!(
                state.phase, prev_state.phase,
                "must NOT emit the same phase twice!",
            );
        }

        Self::emit_log(&state);
        Self::emit_metrics(metrics, &state, prev_state.as_deref());

        if let Some(query_log_write_client) = query_log_write_client {
            self.emit_line_protocol(&query_log_write_client, &state, "query_log");
        }
    }

    /// Log entry.
    fn emit_log(state: &QueryLogEntryState) {
        let QueryLogEntryState {
            id,
            namespace_id,
            namespace_name,
            query_type,
            query_text,
            query_params,
            auth_id,
            trace_id,
            issue_time,
            partitions,
            parquet_files,
            permit_duration,
            plan_duration,
            execute_duration,
            end2end_duration,
            compute_duration,
            max_memory,
            success,
            running,
            phase,
            ingester_metrics,
            deduplicated_parquet_files,
            deduplicated_partitions,
            num_rows,
        } = state;

        optional_struct!(
            IngesterMetrics {
                latency_to_plan,
                latency_to_full_data,
                response_rows,
                partition_count,
                response_size,
            } = ingester_metrics
        );

        info!(
            when=phase.name(),
            id=%id,
            namespace_id=namespace_id.get(),
            namespace_name=namespace_name.as_ref(),
            query_type=query_type,
            query_text=%query_text,
            query_params=%query_params,
            auth_id,
            trace_id=trace_id.map(|id| format!("{:x}", id.get())),
            issue_time=%issue_time,
            partitions,
            num_rows,
            parquet_files,
            deduplicated_partitions,
            deduplicated_parquet_files,
            plan_duration_secs=plan_duration.map(|d| d.as_secs_f64()),
            permit_duration_secs=permit_duration.map(|d| d.as_secs_f64()),
            execute_duration_secs=execute_duration.map(|d| d.as_secs_f64()),
            end2end_duration_secs=end2end_duration.map(|d| d.as_secs_f64()),
            compute_duration_secs=compute_duration.map(|d| d.as_secs_f64()),
            max_memory=max_memory,
            ingester_metrics.latency_to_plan_secs=latency_to_plan.map(|d| d.as_secs_f64()),
            ingester_metrics.latency_to_full_data_secs=latency_to_full_data.map(|d| d.as_secs_f64()),
            ingester_metrics.response_rows=response_rows,
            ingester_metrics.partition_count=partition_count,
            ingester_metrics.response_size=response_size,
            success=success,
            running=running,
            cancelled=(*phase == QueryPhase::Cancel),
            "query",
        )
    }

    /// Emit entry to metrics.
    ///
    /// This only emits attributes that have changed from the previous state.
    fn emit_metrics(
        metrics: &Metrics,
        state: &QueryLogEntryState,
        prev_state: Option<&QueryLogEntryState>,
    ) {
        let QueryLogEntryState {
            id: _,
            namespace_id: _,
            namespace_name: _,
            query_type: _,
            query_text: _,
            query_params: _,
            auth_id: _,
            trace_id: _,
            issue_time: _,
            partitions,
            parquet_files,
            permit_duration,
            plan_duration,
            execute_duration,
            end2end_duration,
            compute_duration,
            max_memory,
            success: _,
            running: _,
            phase,
            ingester_metrics,
            deduplicated_parquet_files,
            deduplicated_partitions,
            num_rows,
        } = state;

        metrics.phase_entered.get(*phase).inc(1);
        metrics.phase_current.get(*phase).inc(1);
        if let Some(prev_state) = prev_state {
            metrics.phase_current.get(prev_state.phase).dec(1);
        }

        record_metric_if_now_set!(partitions, prev_state, metrics);
        record_metric_if_now_set!(parquet_files, prev_state, metrics);
        record_metric_if_now_set!(permit_duration, prev_state, metrics);
        record_metric_if_now_set!(plan_duration, prev_state, metrics);
        record_metric_if_now_set!(execute_duration, prev_state, metrics);
        record_metric_if_now_set!(end2end_duration, prev_state, metrics);
        record_metric_if_now_set!(compute_duration, prev_state, metrics);
        record_metric_if_now_set!(max_memory, prev_state, metrics);
        record_metric_if_now_set!(ingester_metrics, prev_state, metrics);
        record_metric_if_now_set!(deduplicated_parquet_files, prev_state, metrics);
        record_metric_if_now_set!(deduplicated_partitions, prev_state, metrics);
        record_metric_if_now_set!(num_rows, prev_state, metrics);
    }

    pub fn emit_line_protocol(
        &self,
        query_log_write_client: &Arc<WriteClient>,
        state: &QueryLogEntryState,
        measurement_name: &str,
    ) {
        let lp: Vec<u8> = state
            .to_line_protocol(LineProtocolBuilder::new(), measurement_name)
            .build();
        let mut query_log_write_client = query_log_write_client.as_ref().clone();

        // Using tokio::spawn so all the callers don't have to be async
        tokio::spawn(async move {
            // The query logs are auto generated by the querier, so it should
            // always contain valid UTF-8.
            // If there are invalid UTF-8 characters, String::from_utf8_lossy()
            // will replace them with the replacement character to get most of
            // the information in the query logs across.
            let lp_string = String::from_utf8_lossy(&lp).into_owned();

            query_log_write_client
                .write_lp("_internal", lp_string)
                .await
                .inspect_err(|e| {
                    warn!(error=%e, "Failed to write query log entry to line protocol client");
                })
        });
    }
}

/// Snapshot of the entries the [`QueryLog`].
#[derive(Debug, Default)]
pub struct QueryLogEntries {
    /// Entries.
    pub entries: VecDeque<Arc<QueryLogEntry>>,

    /// Maximum number of entries
    pub max_size: usize,

    /// Number of evicted entries due to the "max size" constraint.
    pub evicted: usize,
}

/// Stores a fixed number `QueryExecutions` -- handles locking
/// internally so can be shared across multiple
pub struct QueryLog {
    log: Mutex<VecDeque<Arc<QueryLogEntry>>>,
    max_size: usize,
    evicted: AtomicUsize,
    time_provider: Arc<dyn TimeProvider>,
    metrics: Arc<Metrics>,
    id_gen: IDGen,
    /// Optional client to write query logs as line protocol to an external system.
    query_log_write_client: Option<Arc<WriteClient>>,
}

impl QueryLog {
    /// Create a new QueryLog that can hold at most `size` items.
    /// When the `size+1` item is added, item `0` is evicted.
    pub fn new(
        max_size: usize,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        query_log_write_client: Option<Arc<WriteClient>>,
    ) -> Self {
        Self::new_with_id_gen(
            max_size,
            time_provider,
            metric_registry,
            Box::new(Uuid::new_v4),
            query_log_write_client,
        )
    }

    pub fn new_with_id_gen(
        max_size: usize,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        id_gen: IDGen,
        query_log_write_client: Option<Arc<WriteClient>>,
    ) -> Self {
        Self {
            log: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
            evicted: AtomicUsize::new(0),
            time_provider,
            metrics: Arc::new(Metrics::new(metric_registry)),
            id_gen,
            query_log_write_client,
        }
    }

    /// Push a query log entry with an externally-provided query ID.
    ///
    /// This is used by the enterprise query executor to pass through
    /// a query ID from an external source (e.g., a flight ticket).
    #[cfg(feature = "external-query-id")]
    #[expect(clippy::too_many_arguments)]
    pub fn push_with_query_id(
        &self,
        query_id: Uuid,
        namespace_id: NamespaceId,
        namespace_name: Arc<str>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
        auth_id: Option<String>,
        trace_id: Option<TraceId>,
    ) -> QueryCompletedToken<StateReceived> {
        self.push_entry(
            query_id,
            namespace_id,
            namespace_name,
            query_type,
            query_text,
            query_params,
            auth_id,
            trace_id,
        )
    }

    #[expect(clippy::too_many_arguments)]
    pub fn push(
        &self,
        namespace_id: NamespaceId,
        namespace_name: Arc<str>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
        auth_id: Option<String>,
        trace_id: Option<TraceId>,
    ) -> QueryCompletedToken<StateReceived> {
        self.push_entry(
            (self.id_gen)(),
            namespace_id,
            namespace_name,
            query_type,
            query_text,
            query_params,
            auth_id,
            trace_id,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn push_entry(
        &self,
        query_id: Uuid,
        namespace_id: NamespaceId,
        namespace_name: Arc<str>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
        auth_id: Option<String>,
        trace_id: Option<TraceId>,
    ) -> QueryCompletedToken<StateReceived> {
        let entry = Arc::new(QueryLogEntry {
            state: Mutex::new(Arc::new(QueryLogEntryState {
                id: query_id,
                namespace_id,
                namespace_name,
                query_type,
                query_text: QueryTextWrapper(query_text.into()),
                query_params,
                auth_id,
                trace_id,
                issue_time: self.time_provider.now(),
                partitions: Default::default(),
                parquet_files: Default::default(),
                permit_duration: Default::default(),
                plan_duration: Default::default(),
                execute_duration: Default::default(),
                end2end_duration: Default::default(),
                compute_duration: Default::default(),
                max_memory: Default::default(),
                success: false,
                running: true,
                phase: QueryPhase::Received,
                ingester_metrics: None,
                deduplicated_parquet_files: Default::default(),
                deduplicated_partitions: Default::default(),
                num_rows: Default::default(),
            })),
        });
        entry.emit(&self.metrics, None, self.query_log_write_client.clone());

        let token = QueryCompletedToken {
            entry: Some(Arc::clone(&entry)),
            time_provider: Arc::clone(&self.time_provider),
            metrics: Arc::clone(&self.metrics),
            state: Default::default(),
            query_log_write_client: self.query_log_write_client.clone(),
        };

        if self.max_size == 0 {
            return token;
        }

        let mut log = self.log.lock();

        // enforce limit
        while log.len() > self.max_size {
            log.pop_front();
            self.evicted.fetch_add(1, Ordering::SeqCst);
        }

        log.push_back(Arc::clone(&entry));
        token
    }

    pub fn entries(&self) -> QueryLogEntries {
        let log = self.log.lock();
        QueryLogEntries {
            entries: log.clone(),
            max_size: self.max_size,
            evicted: self.evicted.load(Ordering::SeqCst),
        }
    }
}

impl Debug for QueryLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryLog")
            .field("log", &self.log)
            .field("max_size", &self.max_size)
            .field("evicted", &self.evicted)
            .field("time_provider", &self.time_provider)
            .field("id_gen", &"<ID_GEN>")
            .finish()
    }
}

trait State {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>>;

    fn memory_monitor(&self) -> Option<&Arc<Monitor>>;
}

/// State of [`QueryCompletedToken`], equivalent to [`QueryPhase::Received`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StateReceived;

impl State for StateReceived {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        None
    }

    fn memory_monitor(&self) -> Option<&Arc<Monitor>> {
        None
    }
}

/// State of [`QueryCompletedToken`], equivalent to [`QueryPhase::Planned`].
#[derive(Debug)]
pub struct StatePlanned {
    /// Physical execution plan.
    plan: Arc<dyn ExecutionPlan>,

    /// Memory usage monitor.
    memory_monitor: Arc<Monitor>,
}

impl State for StatePlanned {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        Some(&self.plan)
    }

    fn memory_monitor(&self) -> Option<&Arc<Monitor>> {
        Some(&self.memory_monitor)
    }
}

/// State of [`QueryCompletedToken`], equivalent to [`QueryPhase::Permit`].
#[derive(Debug)]
pub struct StatePermit {
    /// Physical execution plan.
    plan: Arc<dyn ExecutionPlan>,

    /// Memory usage monitor.
    memory_monitor: Arc<Monitor>,
}

impl State for StatePermit {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        Some(&self.plan)
    }

    fn memory_monitor(&self) -> Option<&Arc<Monitor>> {
        Some(&self.memory_monitor)
    }
}

/// A `QueryCompletedToken` is returned by `record_query` implementations of
/// a `QueryNamespace`. It is used to trigger side-effects (such as query timing)
/// on query completion.
#[derive(Debug)]
#[expect(private_bounds)]
pub struct QueryCompletedToken<S>
where
    S: State,
{
    /// Entry.
    ///
    /// This is optional so we can implement type state and [`Drop`] at the same time.
    entry: Option<Arc<QueryLogEntry>>,

    /// Time provider
    time_provider: Arc<dyn TimeProvider>,

    /// Metrics
    metrics: Arc<Metrics>,

    /// Current state.
    state: S,

    /// Optional client to write query logs as line protocol to an external system.
    query_log_write_client: Option<Arc<WriteClient>>,
}

#[expect(private_bounds)]
impl<S> QueryCompletedToken<S>
where
    S: State,
{
    /// Underlying entry.
    pub fn entry(&self) -> &Arc<QueryLogEntry> {
        self.entry.as_ref().expect("valid state")
    }

    fn collect_execution_data(&self, state: &mut QueryLogEntryState) {
        self.collect_compute_time(state);
        self.collect_memory_usage(state);
        self.collect_ingester_metrics(state);
        self.collect_num_rows(state);
    }

    fn collect_compute_time(&self, state: &mut QueryLogEntryState) {
        let Some(plan) = self.state.plan() else {
            return;
        };

        state.compute_duration = Some(collect_compute_duration(plan.as_ref()));
    }

    fn collect_memory_usage(&self, state: &mut QueryLogEntryState) {
        if let Some(memory_monitor) = self.state.memory_monitor() {
            state.max_memory = Some(memory_monitor.max() as i64);
        }
    }

    fn collect_ingester_metrics(&self, state: &mut QueryLogEntryState) {
        if let Some(plan) = self.state.plan() {
            state.ingester_metrics = Some(collect_ingester_metrics(plan.as_ref()));
        }
    }

    fn collect_num_rows(&self, state: &mut QueryLogEntryState) {
        if let Some(plan) = self.state.plan() {
            state.num_rows = collect_num_rows(plan.as_ref());
        }
    }

    /// Called when a terminal/final state ([fail](QueryPhase::Fail), [success](QueryPhase::Success),
    /// [cancel](QueryPhase::Cancel)) occurred. The query will NOT be used anymore afterwards.
    fn done(&self, entry: Arc<QueryLogEntry>, mut state: QueryLogEntryState) {
        if state.phase != QueryPhase::Fail && state.execute_duration.is_none() {
            state.phase = QueryPhase::Cancel;

            if state.permit_duration.is_some() {
                // started computation, collect partial stats
                self.collect_execution_data(&mut state);
            }
        }

        let now = self.time_provider.now();
        set_relative(state.issue_time, now, &mut state.end2end_duration);
        state.running = false;

        entry.set_and_emit(state, &self.metrics, self.query_log_write_client.clone());
    }
}

impl QueryCompletedToken<StateReceived> {
    /// Record that this query got planned.
    pub fn planned(
        mut self,
        ctx: &IOxSessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> QueryCompletedToken<StatePlanned> {
        let entry = self.entry.take().expect("valid state");
        let mut state = entry.state().as_ref().clone();
        self.set_time(&mut state);
        state.phase = QueryPhase::Planned;
        let ParquetFileMetrics {
            partitions,
            parquet_files,
            deduplicated_parquet_files,
            deduplicated_partitions,
        } = ParquetFileMetrics::plan_metrics(plan.as_ref());

        state.partitions = Some(u64::try_from(partitions).expect("Is this computer 128-bit??"));
        state.parquet_files =
            Some(u64::try_from(parquet_files).expect("Is this computer 128-bit??"));
        state.deduplicated_parquet_files =
            Some(u64::try_from(deduplicated_parquet_files).expect("Is this computer 128-bit??"));
        state.deduplicated_partitions =
            Some(u64::try_from(deduplicated_partitions).expect("Is this computer 128-bit??"));
        entry.set_and_emit(state, &self.metrics, self.query_log_write_client.clone());

        QueryCompletedToken {
            entry: Some(entry),
            time_provider: Arc::clone(&self.time_provider),
            metrics: Arc::clone(&self.metrics),
            state: StatePlanned {
                plan,
                memory_monitor: Arc::clone(ctx.memory_monitor()),
            },
            query_log_write_client: self.query_log_write_client.clone(),
        }
    }

    /// Record that this query failed during planning.
    pub fn fail(mut self) {
        let entry = self.entry.take().expect("valid state");
        let mut state = entry.state().as_ref().clone();
        self.set_time(&mut state);
        state.phase = QueryPhase::Fail;
        self.done(entry, state);
    }

    fn set_time(&self, state: &mut QueryLogEntryState) {
        let now = self.time_provider.now();
        let origin = state.issue_time;
        set_relative(origin, now, &mut state.plan_duration);
    }
}

impl QueryCompletedToken<StatePlanned> {
    /// Record that this query got a semaphore permit.
    pub fn permit(mut self) -> QueryCompletedToken<StatePermit> {
        let entry = self.entry.take().expect("valid state");
        let mut state = entry.state().as_ref().clone();

        if let Some(plan_duration) = state.plan_duration {
            let now = self.time_provider.now();
            let origin = state.issue_time + plan_duration;
            set_relative(origin, now, &mut state.permit_duration);
        }
        state.phase = QueryPhase::Permit;
        entry.set_and_emit(state, &self.metrics, self.query_log_write_client.clone());

        QueryCompletedToken {
            entry: Some(entry),
            time_provider: Arc::clone(&self.time_provider),
            metrics: Arc::clone(&self.metrics),
            state: StatePermit {
                plan: Arc::clone(&self.state.plan),
                memory_monitor: Arc::clone(&self.state.memory_monitor),
            },
            query_log_write_client: self.query_log_write_client.clone(),
        }
    }
}

impl QueryCompletedToken<StatePermit> {
    /// Record that this query completed successfully
    pub fn success(mut self) {
        let entry = self.entry.take().expect("valid state");
        let mut state = entry.state().as_ref().clone();

        state.success = true;
        state.phase = QueryPhase::Success;

        self.finish(entry, state)
    }

    /// Record that the query finished execution with an error.
    pub fn fail(mut self) {
        let entry = self.entry.take().expect("valid state");
        let mut state = entry.state().as_ref().clone();

        state.phase = QueryPhase::Fail;

        self.finish(entry, state)
    }

    fn finish(&self, entry: Arc<QueryLogEntry>, mut state: QueryLogEntryState) {
        if let (Some(permit_duration), Some(plan_duration)) =
            (state.permit_duration, state.plan_duration)
        {
            let now = self.time_provider.now();
            let origin = state.issue_time + permit_duration + plan_duration;
            set_relative(origin, now, &mut state.execute_duration);
        }

        self.collect_execution_data(&mut state);
        self.done(entry, state);
    }
}

impl<S> Drop for QueryCompletedToken<S>
where
    S: State,
{
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            let state = entry.state().as_ref().clone();
            self.done(entry, state);
        }
    }
}

/// Boxed description of a query that knows how to render to a string
///
/// This avoids storing potentially large strings
pub type QueryText = Box<dyn std::fmt::Display + Send + Sync>;

/// Wrapper for QueryText that also implements [`Debug`].
#[derive(Clone)]
pub struct QueryTextWrapper(Arc<dyn std::fmt::Display + Send + Sync>);

impl QueryTextWrapper {
    #[cfg(test)]
    fn from_static(s: &'static str) -> Self {
        Self(Arc::new(s.to_owned()))
    }
}

impl Debug for QueryTextWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for QueryTextWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq for QueryTextWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_string() == other.0.to_string()
    }
}

impl Eq for QueryTextWrapper {}

/// Method that generates [`Uuid`]s.
pub type IDGen = Box<dyn Fn() -> Uuid + Send + Sync>;

fn set_relative(origin: Time, now: Time, target: &mut Option<Duration>) {
    match now.checked_duration_since(origin) {
        Some(dur) => {
            *target = Some(dur);
        }
        None => {
            warn!("Clock went backwards, not query duration")
        }
    }
}

/// Collect compute duration from [`ExecutionPlan`].
fn collect_compute_duration(plan: &dyn ExecutionPlan) -> Duration {
    let mut total = Duration::ZERO;

    if let Some(nanos) = plan
        .metrics()
        .as_ref()
        .and_then(MetricsSet::elapsed_compute)
    {
        total += Duration::from_nanos(nanos as u64);
    }

    for child in plan.children() {
        total += collect_compute_duration(child.as_ref());
    }

    total
}

fn collect_ingester_metrics(plan: &dyn ExecutionPlan) -> IngesterMetrics {
    let mut latency_to_plan = Duration::ZERO;
    let mut latency_to_full_data = Duration::ZERO;
    let mut response_rows = 0;
    let mut partition_count = 0;
    let mut response_size = 0;

    if let Some(metrics) = plan.metrics() {
        for m in metrics.iter() {
            let value = m.value();

            match value.name() {
                "latency_to_plan" => {
                    if let MetricValue::Time { time, .. } = value {
                        // only update to a greater time, we run query ingesters in parallel so only care
                        // about the max time of those requests.
                        latency_to_plan =
                            Duration::from_nanos(time.value() as u64).max(latency_to_plan);
                    }
                }
                "latency_to_full_data" => {
                    if let MetricValue::Time { time, .. } = value {
                        // only update to a greater time, we run query ingesters in parallel so only care
                        // about the max time of those requests.
                        latency_to_full_data =
                            Duration::from_nanos(time.value() as u64).max(latency_to_full_data);
                    }
                }
                "response_rows" => {
                    if let MetricValue::Count { count, .. } = value {
                        response_rows += count.value() as u64;
                    }
                }
                "partition_count" => {
                    if let MetricValue::Count { count, .. } = value {
                        partition_count += count.value() as u64;
                    }
                }
                "response_size" => {
                    if let MetricValue::Count { count, .. } = value {
                        response_size += count.value() as u64;
                    }
                }
                _ => {}
            }
        }
    }
    for child in plan.children() {
        let IngesterMetrics {
            latency_to_plan: lp,
            latency_to_full_data: ld,
            response_rows: rr,
            partition_count: pc,
            response_size: rs,
        } = collect_ingester_metrics(child.as_ref());
        // only update to a greater time, we run query ingesters in parallel so only care
        // about the max time of those requests.
        latency_to_plan = lp.max(latency_to_plan);
        latency_to_full_data = ld.max(latency_to_full_data);
        response_rows += rr;
        partition_count += pc;
        response_size += rs;
    }

    IngesterMetrics {
        latency_to_plan,
        latency_to_full_data,
        response_rows,
        partition_count,
        response_size,
    }
}

/// Collect the number of rows from an [`ExecutionPlan`].
///
/// ## Note
/// This function returns returns an [`Option`] to distinguish between
/// no metrics being available and no rows being returned by a query.
///
/// Specifically:
/// - A return value of [`None`] means that no metrics were available.
/// - A return value of `Some(0)` would indicate that a query produced no results.
fn collect_num_rows(plan: &dyn ExecutionPlan) -> Option<u64> {
    let mut num_rows: Option<u64> = None;
    if let Some(metrics) = plan.metrics() {
        if let Some(rows) = metrics.output_rows() {
            *num_rows.get_or_insert(0) += rows as u64;
        }
        for child in plan.children() {
            if let Some(child_rows) = collect_num_rows(child.as_ref()) {
                *num_rows.get_or_insert(0) += child_rows;
            }
        }
    }

    num_rows
}
#[derive(Debug)]
pub struct PermitAndToken {
    pub permit: InstrumentedAsyncOwnedSemaphorePermit,
    pub query_completed_token: QueryCompletedToken<StatePermit>,
}

/// A metric keyed by [`QueryPhase`].
#[derive(Debug)]
struct PhaseMetric<T>
where
    T: MetricObserver<Recorder = T>,
{
    received: T,
    planned: T,
    permit: T,
    cancel: T,
    success: T,
    fail: T,
}

impl<T> PhaseMetric<T>
where
    T: MetricObserver<Recorder = T>,
{
    fn new(metric: Metric<T>) -> Self {
        Self {
            received: metric.recorder(&[("phase", "received")]),
            planned: metric.recorder(&[("phase", "planned")]),
            permit: metric.recorder(&[("phase", "permit")]),
            cancel: metric.recorder(&[("phase", "cancel")]),
            success: metric.recorder(&[("phase", "success")]),
            fail: metric.recorder(&[("phase", "fail")]),
        }
    }

    fn get(&self, phase: QueryPhase) -> &T {
        match phase {
            QueryPhase::Received => &self.received,
            QueryPhase::Planned => &self.planned,
            QueryPhase::Permit => &self.permit,
            QueryPhase::Cancel => &self.cancel,
            QueryPhase::Success => &self.success,
            QueryPhase::Fail => &self.fail,
        }
    }
}

#[derive(Debug)]
struct Metrics {
    phase_entered: PhaseMetric<U64Counter>,
    phase_current: PhaseMetric<U64Gauge>,
    partitions: U64Histogram,
    parquet_files: U64Histogram,
    num_rows: U64Histogram,
    permit_duration: DurationHistogram,
    plan_duration: DurationHistogram,
    execute_duration: DurationHistogram,
    end2end_duration: DurationHistogram,
    compute_duration: DurationHistogram,
    max_memory: U64Histogram,
    ingester_metrics: MetricsIngesterMetrics,
    deduplicated_parquet_files: U64Histogram,
    deduplicated_partitions: U64Histogram,
}

impl Metrics {
    fn new(registry: &metric::Registry) -> Self {
        Self {
            phase_entered: PhaseMetric::new(registry.register_metric(
                "influxdb_iox_query_log_phase_entered",
                "Number of queries that entered the given phase",
            )),
            phase_current: PhaseMetric::new(registry.register_metric(
                "influxdb_iox_query_log_phase_current",
                "Number of queries that currently in the given phase",
            )),
            partitions: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_partitions",
                    "Number of partitions processed by the query",
                    || U64HistogramOptions::new([0, 1, 10, 100, 1_000, 10_000, u64::MAX]),
                )
                .recorder([]),
            parquet_files: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_parquet_files",
                    "Number of parquet files processed by the query",
                    || U64HistogramOptions::new([0, 1, 10, 100, 1_000, 10_000, u64::MAX]),
                )
                .recorder([]),
            permit_duration: registry
                .register_metric::<DurationHistogram>(
                    "influxdb_iox_query_log_permit_duration",
                    "Duration it took to acquire a semaphore permit",
                )
                .recorder([]),
            plan_duration: registry
                .register_metric::<DurationHistogram>(
                    "influxdb_iox_query_log_plan_duration",
                    "Duration it took to plan the query",
                )
                .recorder([]),
            execute_duration: registry
                .register_metric::<DurationHistogram>(
                    "influxdb_iox_query_log_execute_duration",
                    "Duration it took to execute the query",
                )
                .recorder([]),
            end2end_duration: registry
                .register_metric::<DurationHistogram>(
                    "influxdb_iox_query_log_end2end_duration",
                    "Duration from `issue_time` til the query ended somehow",
                )
                .recorder([]),
            compute_duration: registry
                .register_metric::<DurationHistogram>(
                    "influxdb_iox_query_log_compute_duration",
                    "CPU duration spend for computation",
                )
                .recorder([]),
            max_memory: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_max_memory",
                    "Peak memory allocated for processing the query",
                    || {
                        U64HistogramOptions::new([
                            // bytes
                            0,
                            1,
                            10,
                            100,
                            // kilobytes
                            1_000,
                            10_000,
                            100_000,
                            // megabytes
                            1_000_000,
                            10_000_000,
                            100_000_000,
                            // gigabytes
                            1_000_000_000,
                            10_000_000_000,
                            100_000_000_000,
                            // inf
                            u64::MAX,
                        ])
                    },
                )
                .recorder([]),
            ingester_metrics: MetricsIngesterMetrics::new(registry),
            deduplicated_parquet_files: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_deduplicated_parquet_files",
                    "The number of files that are held under a `DeduplicateExec`",
                    || U64HistogramOptions::new([0, 1, 10, 100, 1_000, 10_000, u64::MAX]),
                )
                .recorder([]),
            deduplicated_partitions: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_deduplicated_partitions",
                    "The number of partitions that are held under a `DeduplicateExec`",
                    || U64HistogramOptions::new([0, 1, 10, 100, 1_000, 10_000, u64::MAX]),
                )
                .recorder([]),
            num_rows: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_num_rows",
                    "The number of rows returned per query",
                    || {
                        U64HistogramOptions::new([
                            0,
                            1,
                            10,
                            100,
                            200,
                            500,
                            1_000,
                            2_000,
                            5_000,
                            10_000,
                            20_000,
                            100_000,
                            u64::MAX,
                        ])
                    },
                )
                .recorder([]),
        }
    }
}

#[derive(Debug)]
struct MetricsIngesterMetrics {
    latency_to_plan: DurationHistogram,
    latency_to_full_data: DurationHistogram,
    response_rows: U64Histogram,
    partition_count: U64Histogram,
    response_size: U64Histogram,
}

impl MetricsIngesterMetrics {
    fn new(registry: &metric::Registry) -> Self {
        Self {
            latency_to_plan: registry
                .register_metric::<DurationHistogram>(
                    "influxdb_iox_query_log_ingester_latency_to_plan",
                    "when the querier has enough information from all ingesters so that it can proceed with query planning",
                )
                .recorder([]),
            latency_to_full_data: registry
                .register_metric::<DurationHistogram>(
                    "influxdb_iox_query_log_ingester_latency_to_full_data",
                    "measured from the initial request, when the querier has all the data from all ingesters",
                )
                .recorder([]),
            response_rows: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_ingester_response_rows",
                    "ingester response rows",
                    || U64HistogramOptions::new([0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, u64::MAX]),
                )
                .recorder([]),
            partition_count: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_ingester_partition_count",
                    "ingester partition count",
                    || U64HistogramOptions::new([0, 1, 10, 100, 1_000, 10_000, 100_000, u64::MAX]),
                )
                .recorder([]),
            response_size: registry
                .register_metric_with_options::<U64Histogram, _>(
                    "influxdb_iox_query_log_ingester_response_size",
                    "ingester record batch size in bytes",
                    || U64HistogramOptions::new([
                        // bytes
                        0,
                        1,
                        10,
                        100,
                        // kilobytes
                        1_000,
                        10_000,
                        100_000,
                        // megabytes
                        1_000_000,
                        10_000_000,
                        100_000_000,
                        // gigabytes
                        1_000_000_000,
                        // inf
                        u64::MAX,
                    ]),
                )
                .recorder([]),
        }
    }
}

#[cfg(test)]
mod test_super {
    use datafusion::error::DataFusionError;
    use iox_query_params::params;
    use itertools::Itertools;
    use std::sync::atomic::AtomicU64;

    use datafusion::physical_plan::{
        DisplayAs, Metric,
        metrics::{MetricValue, MetricsSet},
    };
    use iox_time::MockProvider;
    use test_helpers::tracing::TracingCapture;

    use super::*;

    #[test]
    fn test_to_line_protocol_basic() {
        let Test {
            time_provider: _,
            metric_registry: _,
            token: _,
            entry,
            start_state: _,
        } = Test::default();

        // Test basic line protocol output
        let lp_builder = entry
            .state()
            .as_ref()
            .to_line_protocol(LineProtocolBuilder::new(), "query_log_test");
        let lp = lp_builder.build();

        insta::assert_snapshot!(
            format_line_protocol(&lp),
            @r#"query_log_test,id=00000000-0000-0000-0000-000000000001,namespace_id=1,namespace_name=ns,query_type=sql,phase=cancel running="false",success="false",query_text="SELECT 1",query_params="Params { }",query_issue_time_ns=100000000i,end_to_end_duration_ns=0u 1000000000000000000"#
        );
    }

    #[test]
    fn test_to_line_protocol_with_optional_fields() {
        let Test {
            time_provider,
            metric_registry: _,
            token,
            entry,
            start_state: _,
        } = Test::default();

        // Advance through phases to populate optional fields
        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());

        time_provider.inc(Duration::from_millis(2));
        let token = token.permit();

        time_provider.inc(Duration::from_millis(5));
        token.success();

        // Test line protocol with populated fields
        let lp_builder = entry
            .state()
            .as_ref()
            .to_line_protocol(LineProtocolBuilder::new(), "query_log_test");
        let lp = lp_builder.build();
        insta::assert_snapshot!(
            format_line_protocol(&lp),
            @r#"query_log_test,id=00000000-0000-0000-0000-000000000001,namespace_id=1,namespace_name=ns,query_type=sql,phase=success running="false",success="true",query_text="SELECT 1",query_params="Params { }",query_issue_time_ns=100000000i,partition_count=0u,parquet_file_count=0u,permit_duration_ns=2000000u,plan_duration_ns=1000000u,execute_duration_ns=5000000u,end_to_end_duration_ns=8000000u,compute_duration_ns=1337000000u,max_memory_bytes=0i,ingester_latency_to_plan_ns=0u,ingester_latency_to_full_data_ns=0u,ingester_response_row_count=0u,ingester_response_size_bytes=0u,ingester_partition_count=0u 1000000000000000000"#);
    }

    #[test]
    fn test_to_line_protocol_with_auth_and_trace() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(100).unwrap()));
        let metric_registry = metric::Registry::default();
        let log = QueryLog::new_with_id_gen(
            10,
            time_provider,
            &metric_registry,
            Box::new(|| Uuid::from_u128(1)),
            None,
        );

        // Create entry with auth_id and trace_id
        let namespace_id = NamespaceId::new(1);
        let namespace_name = Arc::from("ns");
        let auth_id = Some("user123".to_string());
        let trace_id = TraceId::new(42);

        let token = log.push(
            namespace_id,
            namespace_name,
            "sql",
            Box::new("SELECT 1"),
            params! {},
            auth_id,
            trace_id,
        );

        let lp_builder = token
            .entry()
            .state()
            .as_ref()
            .to_line_protocol(LineProtocolBuilder::new(), "query_log_test");
        let lp = lp_builder.build();

        insta::assert_snapshot!(
            format_line_protocol(&lp),
            @r#"query_log_test,id=00000000-0000-0000-0000-000000000001,namespace_id=1,namespace_name=ns,query_type=sql,phase=received,auth_id=user123,trace_id=42 running="true",success="false",query_text="SELECT 1",query_params="Params { }",query_issue_time_ns=100000000i 1000000000000000000"#
        );
    }

    #[test]
    fn test_token_end2end_success() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = Test::default();

        let expected = start_state;
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 0
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 0
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 0
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 0
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 1
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 0
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_plan_duration_seconds_sum 0
        influxdb_iox_query_log_plan_duration_seconds_count 0
        "##);

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            deduplicated_partitions: Some(0),
            deduplicated_parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            num_rows: None,
            phase: QueryPhase::Planned,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 1
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();

        let expected = QueryLogEntryState {
            permit_duration: Some(Duration::from_millis(10)),
            phase: QueryPhase::Permit,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 1
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        time_provider.inc(Duration::from_millis(100));
        token.success();

        let expected = QueryLogEntryState {
            execute_duration: Some(Duration::from_millis(100)),
            end2end_duration: Some(Duration::from_millis(111)),
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            num_rows: Some(10),
            success: true,
            running: false,
            phase: QueryPhase::Success,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_compute_duration_seconds_sum 1.337
        influxdb_iox_query_log_compute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.111
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_execute_duration_seconds_sum 0.1
        influxdb_iox_query_log_execute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 1
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 1
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 1
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 1
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 1
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100"} 1
        influxdb_iox_query_log_num_rows_bucket{le="200"} 1
        influxdb_iox_query_log_num_rows_bucket{le="500"} 1
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_num_rows_sum 10
        influxdb_iox_query_log_num_rows_count 1
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 1
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 1
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "success"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; num_rows = 10; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; execute_duration_secs = 0.1; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics.latency_to_plan_secs = 0.0; ingester_metrics.latency_to_full_data_secs = 0.0; ingester_metrics.response_rows = 0; ingester_metrics.partition_count = 0; ingester_metrics.response_size = 0; success = true; running = false; cancelled = false;
        "#);
    }

    #[test]
    fn test_params_end2end_success() {
        let params: StatementParams = params!(
            "a" => true,
        );
        let capture = TracingCapture::new();
        let test = Test::with_log_entry(
            NamespaceId::new(1),
            Arc::from("ns"),
            "sql",
            "SELECT $a;",
            params.clone(),
            None,
            None,
        );
        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = test;

        let expected = start_state;
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 0
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 0
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 0
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 0
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 1
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 0
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_plan_duration_seconds_sum 0
        influxdb_iox_query_log_plan_duration_seconds_count 0
        "##);

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            deduplicated_partitions: Some(0),
            deduplicated_parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            phase: QueryPhase::Planned,
            ingester_metrics: None,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 1
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();

        let expected = QueryLogEntryState {
            permit_duration: Some(Duration::from_millis(10)),
            phase: QueryPhase::Permit,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 1
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        time_provider.inc(Duration::from_millis(100));
        token.success();

        let expected = QueryLogEntryState {
            execute_duration: Some(Duration::from_millis(100)),
            end2end_duration: Some(Duration::from_millis(111)),
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            num_rows: Some(10),
            success: true,
            running: false,
            phase: QueryPhase::Success,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_compute_duration_seconds_sum 1.337
        influxdb_iox_query_log_compute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.111
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_execute_duration_seconds_sum 0.1
        influxdb_iox_query_log_execute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 1
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 1
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 1
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 1
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 1
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100"} 1
        influxdb_iox_query_log_num_rows_bucket{le="200"} 1
        influxdb_iox_query_log_num_rows_bucket{le="500"} 1
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_num_rows_sum 10
        influxdb_iox_query_log_num_rows_count 1
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 1
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 1
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "success"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; num_rows = 10; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; execute_duration_secs = 0.1; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics.latency_to_plan_secs = 0.0; ingester_metrics.latency_to_full_data_secs = 0.0; ingester_metrics.response_rows = 0; ingester_metrics.partition_count = 0; ingester_metrics.response_size = 0; success = true; running = false; cancelled = false;
        "#);
    }

    #[test]
    fn test_auth_id_end2end_success() {
        let capture = TracingCapture::new();

        let test = Test::with_log_entry(
            NamespaceId::new(1),
            Arc::from("ns"),
            "sql",
            "SELECT 1",
            StatementParams::new(),
            Some("auth-token"),
            None,
        );
        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = test;

        let expected = start_state;
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 0
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 0
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 0
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 0
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 1
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 0
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_plan_duration_seconds_sum 0
        influxdb_iox_query_log_plan_duration_seconds_count 0
        "##);

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            deduplicated_partitions: Some(0),
            deduplicated_parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            phase: QueryPhase::Planned,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 1
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();

        let expected = QueryLogEntryState {
            permit_duration: Some(Duration::from_millis(10)),
            phase: QueryPhase::Permit,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_end2end_duration_seconds_sum 0
        influxdb_iox_query_log_end2end_duration_seconds_count 0
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 1
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        time_provider.inc(Duration::from_millis(100));
        token.success();

        let expected = QueryLogEntryState {
            execute_duration: Some(Duration::from_millis(100)),
            end2end_duration: Some(Duration::from_millis(111)),
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            num_rows: Some(10),
            success: true,
            running: false,
            phase: QueryPhase::Success,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_compute_duration_seconds_sum 1.337
        influxdb_iox_query_log_compute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.111
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_execute_duration_seconds_sum 0.1
        influxdb_iox_query_log_execute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 1
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 1
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 1
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 1
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 1
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100"} 1
        influxdb_iox_query_log_num_rows_bucket{le="200"} 1
        influxdb_iox_query_log_num_rows_bucket{le="500"} 1
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_num_rows_sum 10
        influxdb_iox_query_log_num_rows_count 1
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 1
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 1
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; auth_id = "auth-token"; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; auth_id = "auth-token"; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; auth_id = "auth-token"; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "success"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; auth_id = "auth-token"; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; num_rows = 10; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; execute_duration_secs = 0.1; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics.latency_to_plan_secs = 0.0; ingester_metrics.latency_to_full_data_secs = 0.0; ingester_metrics.response_rows = 0; ingester_metrics.partition_count = 0; ingester_metrics.response_size = 0; success = true; running = false; cancelled = false;
        "#);
    }

    #[test]
    fn test_token_planning_fail() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        token.fail();

        let expected = QueryLogEntryState {
            plan_duration: Some(Duration::from_millis(1)),
            end2end_duration: Some(Duration::from_millis(1)),
            running: false,
            phase: QueryPhase::Fail,
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 0
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 0
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.001
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 0
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 0
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 1
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 1
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 0
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "fail"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; plan_duration_secs = 0.001; end2end_duration_secs = 0.001; success = false; running = false; cancelled = false;
        "#);
    }

    #[test]
    fn test_token_execution_fail() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());
        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();
        time_provider.inc(Duration::from_millis(100));
        token.fail();

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            deduplicated_partitions: Some(0),
            deduplicated_parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            permit_duration: Some(Duration::from_millis(10)),
            execute_duration: Some(Duration::from_millis(100)),
            end2end_duration: Some(Duration::from_millis(111)),
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            num_rows: Some(10),
            running: false,
            phase: QueryPhase::Fail,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_compute_duration_seconds_sum 1.337
        influxdb_iox_query_log_compute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.111
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_execute_duration_seconds_sum 0.1
        influxdb_iox_query_log_execute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 1
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 1
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 1
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 1
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 1
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100"} 1
        influxdb_iox_query_log_num_rows_bucket{le="200"} 1
        influxdb_iox_query_log_num_rows_bucket{le="500"} 1
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_num_rows_sum 10
        influxdb_iox_query_log_num_rows_count 1
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 0
        influxdb_iox_query_log_phase_current{phase="fail"} 1
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 0
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 1
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "fail"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; num_rows = 10; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; execute_duration_secs = 0.1; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics.latency_to_plan_secs = 0.0; ingester_metrics.latency_to_full_data_secs = 0.0; ingester_metrics.response_rows = 0; ingester_metrics.partition_count = 0; ingester_metrics.response_size = 0; success = false; running = false; cancelled = false;
        "#);
    }

    #[test]
    fn test_token_drop_before_planned() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        drop(token);

        let expected = QueryLogEntryState {
            end2end_duration: Some(Duration::from_millis(1)),
            running: false,
            phase: QueryPhase::Cancel,
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 0
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 0
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.001
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 0
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 0
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 0
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 0
        influxdb_iox_query_log_partitions_bucket{le="1"} 0
        influxdb_iox_query_log_partitions_bucket{le="10"} 0
        influxdb_iox_query_log_partitions_bucket{le="100"} 0
        influxdb_iox_query_log_partitions_bucket{le="1000"} 0
        influxdb_iox_query_log_partitions_bucket{le="10000"} 0
        influxdb_iox_query_log_partitions_bucket{le="inf"} 0
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 0
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 1
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 1
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 0
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_plan_duration_seconds_sum 0
        influxdb_iox_query_log_plan_duration_seconds_count 0
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "cancel"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; end2end_duration_secs = 0.001; success = false; running = false; cancelled = true;
        "#);
    }

    #[test]
    fn test_token_drop_before_acquire() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());
        time_provider.inc(Duration::from_millis(10));
        drop(token);

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            deduplicated_partitions: Some(0),
            deduplicated_parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            end2end_duration: Some(Duration::from_millis(11)),
            running: false,
            phase: QueryPhase::Cancel,
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_compute_duration_seconds_sum 0
        influxdb_iox_query_log_compute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.011
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 0
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 0
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 0
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 0
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 0
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 0
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 0
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 0
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100"} 0
        influxdb_iox_query_log_num_rows_bucket{le="200"} 0
        influxdb_iox_query_log_num_rows_bucket{le="500"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 0
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 0
        influxdb_iox_query_log_num_rows_sum 0
        influxdb_iox_query_log_num_rows_count 0
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_permit_duration_seconds_sum 0
        influxdb_iox_query_log_permit_duration_seconds_count 0
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 1
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 1
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 0
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "cancel"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; end2end_duration_secs = 0.011; success = false; running = false; cancelled = true;
        "#);
    }

    #[test]
    fn test_token_drop_before_finish() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            metric_registry,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());
        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();
        time_provider.inc(Duration::from_millis(100));
        drop(token);

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            deduplicated_partitions: Some(0),
            deduplicated_parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            permit_duration: Some(Duration::from_millis(10)),
            end2end_duration: Some(Duration::from_millis(111)),
            // partial stats collected
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            num_rows: Some(10),
            running: false,
            phase: QueryPhase::Cancel,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);
        insta::assert_snapshot!(
            format_metrics(&metric_registry),
            @r##"
        # HELP influxdb_iox_query_log_compute_duration_seconds CPU duration spend for computation
        # TYPE influxdb_iox_query_log_compute_duration_seconds histogram
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_compute_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_compute_duration_seconds_sum 1.337
        influxdb_iox_query_log_compute_duration_seconds_count 1
        # HELP influxdb_iox_query_log_deduplicated_parquet_files The number of files that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_parquet_files histogram
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_parquet_files_sum 0
        influxdb_iox_query_log_deduplicated_parquet_files_count 1
        # HELP influxdb_iox_query_log_deduplicated_partitions The number of partitions that are held under a `DeduplicateExec`
        # TYPE influxdb_iox_query_log_deduplicated_partitions histogram
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_deduplicated_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_deduplicated_partitions_sum 0
        influxdb_iox_query_log_deduplicated_partitions_count 1
        # HELP influxdb_iox_query_log_end2end_duration_seconds Duration from `issue_time` til the query ended somehow
        # TYPE influxdb_iox_query_log_end2end_duration_seconds histogram
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_end2end_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_end2end_duration_seconds_sum 0.111
        influxdb_iox_query_log_end2end_duration_seconds_count 1
        # HELP influxdb_iox_query_log_execute_duration_seconds Duration it took to execute the query
        # TYPE influxdb_iox_query_log_execute_duration_seconds histogram
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.01"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.025"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.05"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.25"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="0.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="1"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="2.5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="5"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="10"} 0
        influxdb_iox_query_log_execute_duration_seconds_bucket{le="inf"} 0
        influxdb_iox_query_log_execute_duration_seconds_sum 0
        influxdb_iox_query_log_execute_duration_seconds_count 0
        # HELP influxdb_iox_query_log_ingester_latency_to_full_data_seconds measured from the initial request, when the querier has all the data from all ingesters
        # TYPE influxdb_iox_query_log_ingester_latency_to_full_data_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_full_data_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_latency_to_plan_seconds when the querier has enough information from all ingesters so that it can proceed with query planning
        # TYPE influxdb_iox_query_log_ingester_latency_to_plan_seconds histogram
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_sum 0
        influxdb_iox_query_log_ingester_latency_to_plan_seconds_count 1
        # HELP influxdb_iox_query_log_ingester_partition_count ingester partition count
        # TYPE influxdb_iox_query_log_ingester_partition_count histogram
        influxdb_iox_query_log_ingester_partition_count_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_partition_count_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_partition_count_sum 0
        influxdb_iox_query_log_ingester_partition_count_count 1
        # HELP influxdb_iox_query_log_ingester_response_rows ingester response rows
        # TYPE influxdb_iox_query_log_ingester_response_rows histogram
        influxdb_iox_query_log_ingester_response_rows_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_rows_sum 0
        influxdb_iox_query_log_ingester_response_rows_count 1
        # HELP influxdb_iox_query_log_ingester_response_size ingester record batch size in bytes
        # TYPE influxdb_iox_query_log_ingester_response_size histogram
        influxdb_iox_query_log_ingester_response_size_bucket{le="0"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="10000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="100000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="1000000000"} 1
        influxdb_iox_query_log_ingester_response_size_bucket{le="inf"} 1
        influxdb_iox_query_log_ingester_response_size_sum 0
        influxdb_iox_query_log_ingester_response_size_count 1
        # HELP influxdb_iox_query_log_max_memory Peak memory allocated for processing the query
        # TYPE influxdb_iox_query_log_max_memory histogram
        influxdb_iox_query_log_max_memory_bucket{le="0"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="1000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="10000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="100000000000"} 1
        influxdb_iox_query_log_max_memory_bucket{le="inf"} 1
        influxdb_iox_query_log_max_memory_sum 0
        influxdb_iox_query_log_max_memory_count 1
        # HELP influxdb_iox_query_log_num_rows The number of rows returned per query
        # TYPE influxdb_iox_query_log_num_rows histogram
        influxdb_iox_query_log_num_rows_bucket{le="0"} 0
        influxdb_iox_query_log_num_rows_bucket{le="1"} 0
        influxdb_iox_query_log_num_rows_bucket{le="10"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100"} 1
        influxdb_iox_query_log_num_rows_bucket{le="200"} 1
        influxdb_iox_query_log_num_rows_bucket{le="500"} 1
        influxdb_iox_query_log_num_rows_bucket{le="1000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="2000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="5000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="10000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="20000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="100000"} 1
        influxdb_iox_query_log_num_rows_bucket{le="inf"} 1
        influxdb_iox_query_log_num_rows_sum 10
        influxdb_iox_query_log_num_rows_count 1
        # HELP influxdb_iox_query_log_parquet_files Number of parquet files processed by the query
        # TYPE influxdb_iox_query_log_parquet_files histogram
        influxdb_iox_query_log_parquet_files_bucket{le="0"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="100"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="1000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="10000"} 1
        influxdb_iox_query_log_parquet_files_bucket{le="inf"} 1
        influxdb_iox_query_log_parquet_files_sum 0
        influxdb_iox_query_log_parquet_files_count 1
        # HELP influxdb_iox_query_log_partitions Number of partitions processed by the query
        # TYPE influxdb_iox_query_log_partitions histogram
        influxdb_iox_query_log_partitions_bucket{le="0"} 1
        influxdb_iox_query_log_partitions_bucket{le="1"} 1
        influxdb_iox_query_log_partitions_bucket{le="10"} 1
        influxdb_iox_query_log_partitions_bucket{le="100"} 1
        influxdb_iox_query_log_partitions_bucket{le="1000"} 1
        influxdb_iox_query_log_partitions_bucket{le="10000"} 1
        influxdb_iox_query_log_partitions_bucket{le="inf"} 1
        influxdb_iox_query_log_partitions_sum 0
        influxdb_iox_query_log_partitions_count 1
        # HELP influxdb_iox_query_log_permit_duration_seconds Duration it took to acquire a semaphore permit
        # TYPE influxdb_iox_query_log_permit_duration_seconds histogram
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.001"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.0025"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.005"} 0
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_permit_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_permit_duration_seconds_sum 0.01
        influxdb_iox_query_log_permit_duration_seconds_count 1
        # HELP influxdb_iox_query_log_phase_current Number of queries that currently in the given phase
        # TYPE influxdb_iox_query_log_phase_current gauge
        influxdb_iox_query_log_phase_current{phase="cancel"} 1
        influxdb_iox_query_log_phase_current{phase="fail"} 0
        influxdb_iox_query_log_phase_current{phase="permit"} 0
        influxdb_iox_query_log_phase_current{phase="planned"} 0
        influxdb_iox_query_log_phase_current{phase="received"} 0
        influxdb_iox_query_log_phase_current{phase="success"} 0
        # HELP influxdb_iox_query_log_phase_entered_total Number of queries that entered the given phase
        # TYPE influxdb_iox_query_log_phase_entered_total counter
        influxdb_iox_query_log_phase_entered_total{phase="cancel"} 1
        influxdb_iox_query_log_phase_entered_total{phase="fail"} 0
        influxdb_iox_query_log_phase_entered_total{phase="permit"} 1
        influxdb_iox_query_log_phase_entered_total{phase="planned"} 1
        influxdb_iox_query_log_phase_entered_total{phase="received"} 1
        influxdb_iox_query_log_phase_entered_total{phase="success"} 0
        # HELP influxdb_iox_query_log_plan_duration_seconds Duration it took to plan the query
        # TYPE influxdb_iox_query_log_plan_duration_seconds histogram
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.001"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.0025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.005"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.01"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.025"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.05"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.25"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="0.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="1"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="2.5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="5"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="10"} 1
        influxdb_iox_query_log_plan_duration_seconds_bucket{le="inf"} 1
        influxdb_iox_query_log_plan_duration_seconds_sum 0.001
        influxdb_iox_query_log_plan_duration_seconds_count 1
        "##);

        insta::assert_snapshot!(
            format_logs(capture),
            @r#"
        level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; success = false; running = true; cancelled = false;
        level = INFO; message = query; when = "cancel"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; num_rows = 10; parquet_files = 0; deduplicated_partitions = 0; deduplicated_parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics.latency_to_plan_secs = 0.0; ingester_metrics.latency_to_full_data_secs = 0.0; ingester_metrics.response_rows = 0; ingester_metrics.partition_count = 0; ingester_metrics.response_size = 0; success = false; running = false; cancelled = true;
        "#);
    }

    struct Test {
        time_provider: Arc<MockProvider>,
        metric_registry: metric::Registry,
        token: QueryCompletedToken<StateReceived>,
        entry: Arc<QueryLogEntry>,
        start_state: QueryLogEntryState,
    }

    impl Test {
        fn with_log_entry(
            namespace_id: NamespaceId,
            namespace_name: Arc<str>,
            query_type: &'static str,
            query_text: &'static str,
            query_params: StatementParams,
            auth_id: Option<&str>,
            trace_id: Option<TraceId>,
        ) -> Self {
            let time_provider =
                Arc::new(MockProvider::new(Time::from_timestamp_millis(100).unwrap()));
            let metric_registry = metric::Registry::new();
            let id_counter = AtomicU64::new(1);
            let log = QueryLog::new_with_id_gen(
                1_000,
                Arc::clone(&time_provider) as _,
                &metric_registry,
                Box::new(move || Uuid::from_u128(id_counter.fetch_add(1, Ordering::SeqCst) as _)),
                None,
            );
            let auth_id = auth_id.map(|s| s.into());

            let token = log.push(
                namespace_id,
                Arc::clone(&namespace_name),
                query_type,
                Box::new(query_text),
                query_params.clone(),
                auth_id.clone(),
                trace_id,
            );

            let entry = Arc::clone(token.entry());

            let start_state = QueryLogEntryState {
                id: Uuid::from_u128(1),
                namespace_id,
                namespace_name,
                query_type,
                query_text: QueryTextWrapper::from_static(query_text),
                query_params,
                auth_id,
                trace_id,
                issue_time: Time::from_timestamp_millis(100).unwrap(),
                partitions: None,
                parquet_files: None,
                permit_duration: None,
                plan_duration: None,
                execute_duration: None,
                end2end_duration: None,
                compute_duration: None,
                num_rows: None,
                max_memory: None,
                success: false,
                running: true,
                phase: QueryPhase::Received,
                ingester_metrics: None,
                deduplicated_parquet_files: None,
                deduplicated_partitions: None,
            };

            Self {
                time_provider,
                metric_registry,
                token,
                entry,
                start_state,
            }
        }
    }

    impl Default for Test {
        fn default() -> Self {
            Self::with_log_entry(
                NamespaceId::new(1),
                Arc::from("ns"),
                "sql",
                "SELECT 1",
                Default::default(),
                None,
                None,
            )
        }
    }

    fn plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(TestExec)
    }

    #[derive(Debug)]
    struct TestExec;

    impl DisplayAs for TestExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            _f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            unimplemented!()
        }

        fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
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
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            unimplemented!()
        }

        fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
            unimplemented!()
        }

        fn metrics(&self) -> Option<MetricsSet> {
            let mut metrics = MetricsSet::default();

            let t = datafusion::physical_plan::metrics::Time::default();
            t.add_duration(Duration::from_millis(1_337));
            metrics.push(Arc::new(Metric::new(MetricValue::ElapsedCompute(t), None)));

            let rows = datafusion::physical_plan::metrics::Count::new();
            rows.add(10);
            metrics.push(Arc::new(Metric::new(MetricValue::OutputRows(rows), None)));

            Some(metrics)
        }
    }

    fn format_logs(capture: TracingCapture) -> String {
        let logs = capture.to_string();
        logs.split('\n')
            .map(|s| s.trim())
            .filter(|s| s.starts_with("level = INFO;"))
            .map(|s| s.to_owned())
            .join("\n")
    }

    fn format_metrics(registry: &metric::Registry) -> String {
        let mut out = Vec::<u8>::new();
        let mut reporter = metric_exporters::PrometheusTextEncoder::new(&mut out);
        registry.report(&mut reporter);
        String::from_utf8(out).expect("valid utf8")
    }

    /// Converts the line protocol to human readable format,
    /// and ensures the timestamp is deterministic.
    fn format_line_protocol(lp: &[u8]) -> String {
        // Convert to string and fix the timestamp to be deterministic
        let lp_str = String::from_utf8_lossy(lp);

        // Find the last space (which separates the timestamp from the rest)
        if let Some(last_space_idx) = lp_str.rfind(' ') {
            let (prefix, _timestamp) = lp_str.split_at(last_space_idx);
            format!("{prefix} 1000000000000000000")
        } else {
            lp_str.to_string()
        }
    }
}
