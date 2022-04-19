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

/// Function used for custom [`Step`]s.
///
/// It is an async function that receives a mutable reference to [`MiniCluster`].
pub type FCustom = Box<dyn for<'b> Fn(&'b mut MiniCluster) -> BoxFuture<'b, ()>>;

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

    /// Run a query and verify that the results are as expected
    Query {
        sql: String,
        expected: Vec<&'static str>,
    },

    /// A custom step that can be used to implement special cases that are only used once.
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

        // Tokens for all writes performed in this test
        let mut write_tokens = vec![];

        for step in steps {
            match step {
                Step::WriteLineProtocol(line_protocol) => {
                    println!(
                        "====Begin writing line protocol to v2 HTTP API:\n{}",
                        line_protocol
                    );
                    let response = cluster.write_to_router(line_protocol).await;
                    assert_eq!(response.status(), StatusCode::NO_CONTENT);
                    let write_token = get_write_token(&response);
                    println!("====Done writing line protocol, got token {}", write_token);
                    write_tokens.push(write_token);
                }
                Step::WaitForReadable => {
                    println!("====Begin waiting for all write tokens to be readable");
                    let ingester_grpc_connection = cluster.ingester().ingester_grpc_connection();
                    for write_token in &write_tokens {
                        wait_for_readable(write_token, ingester_grpc_connection.clone()).await;
                    }
                    println!("====Done waiting for all write tokens to be readable");
                }
                Step::WaitForPersisted => {
                    println!("====Begin waiting for all write tokens to be persisted");
                    let ingester_grpc_connection = cluster.ingester().ingester_grpc_connection();
                    for write_token in &write_tokens {
                        wait_for_persisted(write_token, ingester_grpc_connection.clone()).await;
                    }
                    println!("====Done waiting for all write tokens to be persisted");
                }
                Step::AssertNotPersisted => {
                    println!("====Begin checking all tokens not persisted");
                    let ingester_grpc_connection = cluster.ingester().ingester_grpc_connection();
                    for write_token in &write_tokens {
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
                        cluster.namespace(),
                        cluster.querier().querier_grpc_connection(),
                    )
                    .await;
                    // convert String --> str
                    assert_batches_sorted_eq!(&expected, &batches);
                    println!("====Done running");
                }
                Step::Custom(f) => {
                    println!("====Begin custom step");
                    f(cluster).await;
                    println!("====Done custom step");
                }
            }
        }
    }
}
