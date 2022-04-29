//! Tests for influxrpc / Storage gRPC endpoints

mod dump;
mod metadata;
mod read_filter;
mod read_group;
mod read_window_aggregate;

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

pub(crate) fn read_group_data() -> Vec<&'static str> {
    vec![
        "cpu,cpu=cpu1,host=foo  usage_user=71.0,usage_system=10.0 1000",
        "cpu,cpu=cpu1,host=foo  usage_user=72.0,usage_system=11.0 2000",
        "cpu,cpu=cpu1,host=bar  usage_user=81.0,usage_system=20.0 1000",
        "cpu,cpu=cpu1,host=bar  usage_user=82.0,usage_system=21.0 2000",
        "cpu,cpu=cpu2,host=foo  usage_user=61.0,usage_system=30.0 1000",
        "cpu,cpu=cpu2,host=foo  usage_user=62.0,usage_system=31.0 2000",
        "cpu,cpu=cpu2,host=bar  usage_user=51.0,usage_system=40.0 1000",
        "cpu,cpu=cpu2,host=bar  usage_user=52.0,usage_system=41.0 2000",
    ]
}
