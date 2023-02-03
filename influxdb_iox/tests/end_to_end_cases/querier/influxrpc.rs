//! Tests for influxrpc / Storage gRPC endpoints

mod dump;
mod metadata;
mod read_filter;
mod read_group;
mod read_window_aggregate;

use crate::query_tests2::setups::SETUPS;
use async_trait::async_trait;
use futures::FutureExt;
use observability_deps::tracing::*;
use std::sync::Arc;
use test_helpers_end_to_end::{
    maybe_skip_integration, DataGenerator, FCustom, MiniCluster, Step, StepTest, StepTestState,
};

/// Runs the specified custom function on a cluster with no data
pub(crate) async fn run_no_data_test(custom: FCustom) {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(&mut cluster, vec![Step::Custom(custom)])
        .run()
        .await
}

/// Run the custom test function with a cluster that has had the data from `generator` loaded
pub(crate) async fn run_data_test(generator: Arc<DataGenerator>, custom: FCustom) {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(generator.line_protocol().to_string()),
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

/// Perform an InfluxRPC test that creates a [`MiniCluster`] appropriate for the architecture(s)
/// under test, runs a setup defined in [`SETUPS`] and specified by the implementation of
/// `setup_name`, then performs actions and assertions defined by the implementation of
/// `request_and_assert` with the [`MiniCluster`].
#[async_trait]
trait InfluxRpcTest: Send + Sync + 'static {
    /// The name of the setup in [`SETUPS`] that should be run on the cluster before running
    /// `request_and_assert`.
    fn setup_name(&self) -> &'static str;

    /// Any requests and/or assertions that should be performed on the set up [`MiniCluster`].
    async fn request_and_assert(&self, cluster: &MiniCluster);

    /// Run the test on the appropriate architecture(s), using the setup specified by `setup_name`,
    /// and calling `request_and_assert` in a custom step after the setup steps.
    ///
    /// Note that this is defined on `Arc<Self>`, so a test using a type that implements this trait
    /// will need to call:
    ///
    /// ```ignore
    /// Arc::new(ImplementingType {}).run().await;
    /// ```
    async fn run(self: Arc<Self>) {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();
        let setup_name = self.setup_name();

        info!("Using setup {setup_name}");

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared2_never_persist(database_url.clone()).await;

        let setup_steps = SETUPS
            .get(setup_name)
            .unwrap_or_else(|| panic!("Could not find setup with key `{setup_name}`"))
            .iter();

        let cloned_self = Arc::clone(&self);

        let test_step = Step::Custom(Box::new(move |state: &mut StepTestState| {
            let cloned_self = Arc::clone(&cloned_self);
            async move {
                cloned_self.request_and_assert(state.cluster()).await;
            }
            .boxed()
        }));
        StepTest::new(&mut cluster, setup_steps.chain(std::iter::once(&test_step)))
            .run()
            .await;
    }
}
