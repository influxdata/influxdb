use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use http::StatusCode;

use arrow_util::assert_batches_sorted_eq;

use crate::{
    get_write_token, run_query, token_is_persisted, wait_for_persisted, wait_for_readable,
    MiniCluster,
};

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
/// It is an async function that receives a mutable reference to [`MiniCluster`].
///
/// Example of creating one (note the `boxed()` call):
/// ```
/// use futures::FutureExt;
/// use futures::future::BoxFuture;
/// use test_helpers_end_to_end_ng::{FCustom, StepTestState};
///
/// let custom_function: FCustom = Box::new(|state: &mut StepTestState| {
///   async move {
///     // access the cluster:
///     let cluster = state.cluster();
///     // Your potentially async code here
///   }.boxed()
/// });
/// ```
pub type FCustom = Box<dyn for<'b> Fn(&'b mut StepTestState) -> BoxFuture<'b, ()>>;

/// Possible test steps that a test can perform
pub enum Step {
    /// Writes the specified line protocol to the `/api/v2/write`
    /// endpoint, assert the data was written successfully
    WriteLineProtocol(String),

    /// Wait for all previously written data to be readable
    WaitForReadable,

    /// Assert that all previously written data is NOT persisted yet
    AssertNotPersisted,

    /// Wait for all previously written data to be persisted
    WaitForPersisted,

    /// Run a query using the FlightSQL interface and verify that the
    /// results match the expected results using the
    /// `assert_batches_eq!` macro
    Query {
        sql: String,
        expected: Vec<&'static str>,
    },

    /// Run a query using the FlightSQL interface, and then verifies
    /// the results using the provided validation function on the
    /// results.
    ///
    /// The validation function is expected to panic on validation
    /// failure.
    VerifiedQuery {
        sql: String,
        verify: Box<dyn Fn(Vec<RecordBatch>)>,
    },

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
            println!("**** Begin step {} *****", i);
            match step {
                Step::WriteLineProtocol(line_protocol) => {
                    println!(
                        "====Begin writing line protocol to v2 HTTP API:\n{}",
                        line_protocol
                    );
                    let response = state.cluster.write_to_router(line_protocol).await;
                    assert_eq!(response.status(), StatusCode::NO_CONTENT);
                    let write_token = get_write_token(&response);
                    println!("====Done writing line protocol, got token {}", write_token);
                    state.write_tokens.push(write_token);
                }
                Step::WaitForReadable => {
                    println!("====Begin waiting for all write tokens to be readable");
                    let ingester_grpc_connection =
                        state.cluster().ingester().ingester_grpc_connection();
                    for write_token in &state.write_tokens {
                        wait_for_readable(write_token, ingester_grpc_connection.clone()).await;
                    }
                    println!("====Done waiting for all write tokens to be readable");
                }
                Step::WaitForPersisted => {
                    println!("====Begin waiting for all write tokens to be persisted");
                    let ingester_grpc_connection =
                        state.cluster().ingester().ingester_grpc_connection();
                    for write_token in &state.write_tokens {
                        wait_for_persisted(write_token, ingester_grpc_connection.clone()).await;
                    }
                    println!("====Done waiting for all write tokens to be persisted");
                }
                Step::AssertNotPersisted => {
                    println!("====Begin checking all tokens not persisted");
                    let ingester_grpc_connection =
                        state.cluster().ingester().ingester_grpc_connection();
                    for write_token in &state.write_tokens {
                        let persisted =
                            token_is_persisted(write_token, ingester_grpc_connection.clone()).await;
                        assert!(!persisted);
                    }
                    println!("====Done checking all tokens not persisted");
                }
                Step::Query { sql, expected } => {
                    println!("====Begin running query: {}", sql);
                    // run query
                    let batches = run_query(
                        sql,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                    )
                    .await;
                    assert_batches_sorted_eq!(&expected, &batches);
                    println!("====Done running");
                }
                Step::VerifiedQuery { sql, verify } => {
                    println!("====Begin running verified query: {}", sql);
                    // run query
                    let batches = run_query(
                        sql,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                    )
                    .await;
                    verify(batches);
                    println!("====Done running");
                }
                Step::Custom(f) => {
                    println!("====Begin custom step");
                    f(&mut state).await;
                    println!("====Done custom step");
                }
            }
        }
    }
}
