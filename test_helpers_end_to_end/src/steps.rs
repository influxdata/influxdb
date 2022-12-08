use crate::{
    check_flight_error, get_write_token, run_sql, token_is_persisted, try_run_influxql,
    try_run_sql, wait_for_persisted, wait_for_readable, MiniCluster,
};
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use futures::future::BoxFuture;
use http::StatusCode;
use observability_deps::tracing::info;

/// Test harness for end to end tests that are comprised of several steps
pub struct StepTest<'a> {
    cluster: &'a mut MiniCluster,

    /// The test steps to perform
    steps: Vec<Step>,
}

/// The test state that is passed to custom steps
pub struct StepTestState<'a> {
    /// The mini cluster
    cluster: &'a mut MiniCluster,

    /// Tokens for all data written in WriteLineProtocol steps
    write_tokens: Vec<String>,
}

impl<'a> StepTestState<'a> {
    /// Get a reference to the step test state's cluster.
    #[must_use]
    pub fn cluster(&self) -> &&'a mut MiniCluster {
        &self.cluster
    }

    /// Get a reference to the step test state's cluster.
    #[must_use]
    pub fn cluster_mut(&mut self) -> &mut &'a mut MiniCluster {
        &mut self.cluster
    }

    /// Get a reference to the step test state's write tokens.
    #[must_use]
    pub fn write_tokens(&self) -> &[String] {
        self.write_tokens.as_ref()
    }
}

/// Function used for custom [`Step`]s.
///
/// It is an async function that receives a mutable reference to [`StepTestState`].
///
/// Example of creating one (note the `boxed()` call):
/// ```
/// use futures::FutureExt;
/// use futures::future::BoxFuture;
/// use test_helpers_end_to_end::{FCustom, StepTestState};
///
/// let custom_function: FCustom = Box::new(|state: &mut StepTestState| {
///   async move {
///     // access the cluster:
///     let cluster = state.cluster();
///     // Your potentially async code here
///   }.boxed()
/// });
/// ```
pub type FCustom = Box<dyn for<'b> FnOnce(&'b mut StepTestState) -> BoxFuture<'b, ()>>;

/// Function to do custom validation on metrics. Expected to panic on validation failure.
pub type MetricsValidationFn = Box<dyn Fn(&mut StepTestState, String)>;

/// Possible test steps that a test can perform
pub enum Step {
    /// Writes the specified line protocol to the `/api/v2/write`
    /// endpoint, assert the data was written successfully
    WriteLineProtocol(String),

    /// Wait for all previously written data to be readable
    WaitForReadable,

    /// Assert that all previously written data is NOT persisted yet
    AssertNotPersisted,

    /// Assert that last previously written data is NOT persisted yet.
    AssertLastNotPersisted,

    /// Wait for all previously written data to be persisted
    WaitForPersisted,

    /// Ask the ingester if it has persisted the data. For use in tests where the querier doesn't
    /// know about the ingester, so the test needs to ask the ingester directly.
    WaitForPersistedAccordingToIngester,

    /// Run one hot and one cold compaction operation and wait for it to finish.
    Compact,

    /// Run a SQL query using the FlightSQL interface and verify that the
    /// results match the expected results using the
    /// `assert_batches_eq!` macro
    Query {
        sql: String,
        expected: Vec<&'static str>,
    },

    /// Run a SQL query that's expected to fail using the FlightSQL interface and verify that the
    /// request returns the expected error code and message
    QueryExpectingError {
        sql: String,
        expected_error_code: tonic::Code,
        expected_message: String,
    },

    /// Run a SQL query using the FlightSQL interface, and then verifies
    /// the results using the provided validation function on the
    /// results.
    ///
    /// The validation function is expected to panic on validation
    /// failure.
    VerifiedQuery {
        sql: String,
        verify: Box<dyn Fn(Vec<RecordBatch>)>,
    },

    /// Run an InfluxQL query that's expected to fail using the FlightSQL interface and verify that the
    /// request returns the expected error code and message
    InfluxQLExpectingError {
        sql: String,
        expected_error_code: tonic::Code,
        expected_message: String,
    },

    /// Retrieve the metrics and verify the results using the provided
    /// validation function.
    ///
    /// The validation function is expected to panic on validation
    /// failure.
    VerifiedMetrics(MetricsValidationFn),

    /// A custom step that can be used to implement special cases that
    /// are only used once.
    Custom(FCustom),
}

impl<'a> StepTest<'a> {
    /// Create a new test that runs each `step`, in sequence, against
    /// `cluster` panic'ing if any step fails
    pub fn new(cluster: &'a mut MiniCluster, steps: Vec<Step>) -> Self {
        Self { cluster, steps }
    }

    /// run the test.
    pub async fn run(self) {
        let Self { cluster, steps } = self;

        let mut state = StepTestState {
            cluster,
            write_tokens: vec![],
        };

        for (i, step) in steps.into_iter().enumerate() {
            info!("**** Begin step {} *****", i);
            match step {
                Step::WriteLineProtocol(line_protocol) => {
                    info!(
                        "====Begin writing line protocol to v2 HTTP API:\n{}",
                        line_protocol
                    );
                    let response = state.cluster.write_to_router(line_protocol).await;
                    assert_eq!(response.status(), StatusCode::NO_CONTENT);
                    let write_token = get_write_token(&response);
                    info!("====Done writing line protocol, got token {}", write_token);
                    state.write_tokens.push(write_token);
                }
                Step::WaitForReadable => {
                    info!("====Begin waiting for all write tokens to be readable");
                    let querier_grpc_connection =
                        state.cluster().querier().querier_grpc_connection();
                    for write_token in &state.write_tokens {
                        wait_for_readable(write_token, querier_grpc_connection.clone()).await;
                    }
                    info!("====Done waiting for all write tokens to be readable");
                }
                Step::WaitForPersisted => {
                    info!("====Begin waiting for all write tokens to be persisted");
                    let querier_grpc_connection =
                        state.cluster().querier().querier_grpc_connection();
                    for write_token in &state.write_tokens {
                        wait_for_persisted(write_token, querier_grpc_connection.clone()).await;
                    }
                    info!("====Done waiting for all write tokens to be persisted");
                }
                // Specifically for cases when the querier doesn't know about the ingester so the
                // test needs to ask the ingester directly.
                Step::WaitForPersistedAccordingToIngester => {
                    info!("====Begin waiting for all write tokens to be persisted");
                    let ingester_grpc_connection =
                        state.cluster().ingester().ingester_grpc_connection();
                    for write_token in &state.write_tokens {
                        wait_for_persisted(write_token, ingester_grpc_connection.clone()).await;
                    }
                    info!("====Done waiting for all write tokens to be persisted");
                }
                Step::AssertNotPersisted => {
                    info!("====Begin checking all tokens not persisted");
                    let querier_grpc_connection =
                        state.cluster().querier().querier_grpc_connection();
                    for write_token in &state.write_tokens {
                        let persisted =
                            token_is_persisted(write_token, querier_grpc_connection.clone()).await;
                        assert!(!persisted);
                    }
                    info!("====Done checking all tokens not persisted");
                }
                Step::AssertLastNotPersisted => {
                    info!("====Begin checking last tokens not persisted");
                    let querier_grpc_connection =
                        state.cluster().querier().querier_grpc_connection();
                    let write_token = state.write_tokens.last().expect("No data written yet");
                    let persisted =
                        token_is_persisted(write_token, querier_grpc_connection.clone()).await;
                    assert!(!persisted);
                    info!("====Done checking last tokens not persisted");
                }
                Step::Compact => {
                    info!("====Begin running compaction");
                    state.cluster.run_compaction();
                    info!("====Done running compaction");
                }
                Step::Query { sql, expected } => {
                    info!("====Begin running SQL query: {}", sql);
                    // run query
                    let batches = run_sql(
                        sql,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                    )
                    .await;
                    assert_batches_sorted_eq!(&expected, &batches);
                    info!("====Done running");
                }
                Step::QueryExpectingError {
                    sql,
                    expected_error_code,
                    expected_message,
                } => {
                    info!("====Begin running SQL query expected to error: {}", sql);

                    let err = try_run_sql(
                        sql,
                        state.cluster().namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                    )
                    .await
                    .unwrap_err();

                    check_flight_error(err, expected_error_code, Some(&expected_message));

                    info!("====Done running");
                }
                Step::VerifiedQuery { sql, verify } => {
                    info!("====Begin running SQL verified query: {}", sql);
                    // run query
                    let batches = run_sql(
                        sql,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                    )
                    .await;
                    verify(batches);
                    info!("====Done running");
                }
                Step::InfluxQLExpectingError {
                    sql,
                    expected_error_code,
                    expected_message,
                } => {
                    info!(
                        "====Begin running InfluxQL query expected to error: {}",
                        sql
                    );

                    let err = try_run_influxql(
                        sql,
                        state.cluster().namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                    )
                    .await
                    .unwrap_err();

                    check_flight_error(err, expected_error_code, Some(&expected_message));

                    info!("====Done running");
                }
                Step::VerifiedMetrics(verify) => {
                    info!("====Begin validating metrics");

                    let cluster = state.cluster();
                    let http_base = cluster.router().router_http_base();
                    let url = format!("{http_base}/metrics");

                    let client = reqwest::Client::new();
                    let metrics = client.get(&url).send().await.unwrap().text().await.unwrap();

                    verify(&mut state, metrics);

                    info!("====Done validating metrics");
                }
                Step::Custom(f) => {
                    info!("====Begin custom step");
                    f(&mut state).await;
                    info!("====Done custom step");
                }
            }
        }
    }
}
