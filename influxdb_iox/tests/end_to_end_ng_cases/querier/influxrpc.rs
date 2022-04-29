//! Tests for influxrpc / Storage gRPC endpoints

mod dump;
mod metadata;
mod read_filter;

use std::sync::Arc;
use test_helpers_end_to_end_ng::{
    maybe_skip_integration, DataGenerator, FCustom, MiniCluster, Step, StepTest,
};

/// Runs the specified custom function on a cluster with no data
pub(crate) async fn run_no_data_test(custom: FCustom) {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(&mut cluster, vec![Step::Custom(custom)])
        .run()
        .await
}

/// Run the custom test function with a cluster that has had the data from `generator` loaded
pub(crate) async fn run_data_test(generator: Arc<DataGenerator>, custom: FCustom) {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(generator.line_protocol().to_string()),
            Step::WaitForReadable,
            Step::Custom(custom),
        ],
    )
    .run()
    .await
}
