//! Tests of SQL queries that are expected to return particular errors.

use crate::query_tests2::{framework::IoxArchitecture, setups::SETUPS};
use observability_deps::tracing::*;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest};

#[tokio::test]
async fn schema_merge_nonexistent_column() {
    SqlErrorTest {
        setup_name: "MultiChunkSchemaMerge",
        sql: "SELECT * from cpu where foo = 8",
        expected_error_code: tonic::Code::InvalidArgument,
        expected_message: "Error while planning query: Schema error: No field named 'foo'. \
        Valid fields are 'cpu'.'host', 'cpu'.'region', 'cpu'.'system', 'cpu'.'time', 'cpu'.'user'.",
    }
    .run()
    .await;
}

#[tokio::test]
async fn create_external_table() {
    // Datafusion supports `CREATE EXTERNAL TABLE`, but IOx should not (as that would be a security
    // hole)
    SqlErrorTest {
        // This test doesn't actually depend on any particular data, but to get to the error, the
        // namespace needs to exist.
        setup_name: "OneMeasurementWithTags",
        sql: "CREATE EXTERNAL TABLE foo(ts TIMESTAMP) STORED AS CSV LOCATION '/tmp/foo.csv'",
        expected_error_code: tonic::Code::InvalidArgument,
        expected_message: "Error while planning query: This feature is not implemented: \
                           Unsupported logical plan: CreateExternalTable",
    }
    .run()
    .await;
}

#[tokio::test]
async fn create_schema() {
    // Datafusion supports `CREATE SCHEMA`, but IOx should not (as that would be a security
    // hole)
    SqlErrorTest {
        // This test doesn't actually depend on any particular data, but to get to the error, the
        // namespace needs to exist.
        setup_name: "OneMeasurementWithTags",
        sql: "CREATE SCHEMA foo",
        expected_error_code: tonic::Code::InvalidArgument,
        expected_message: "Error while planning query: This feature is not implemented: \
        CreateCatalogSchema",
    }
    .run()
    .await;
}

#[tokio::test]
async fn bad_selector_num_args() {
    SqlErrorTest {
        setup_name: "OneMeasurementWithTags",
        sql: "SELECT selector_last(time)['bar'] FROM cpu",
        expected_error_code: tonic::Code::InvalidArgument,
        expected_message: "Error while planning query: Error during planning: selector_last \
        requires exactly 2 arguments, got 1",
    }
    .run()
    .await;
}

#[tokio::test]
async fn bad_selector_arg_types() {
    SqlErrorTest {
        setup_name: "OneMeasurementWithTags",
        sql: "SELECT selector_last(time, bar)['value'] FROM cpu",
        expected_error_code: tonic::Code::InvalidArgument,
        expected_message:
            "Error while planning query: Error during planning: selector_last second \
        argument must be a timestamp, but got Float64",
    }
    .run()
    .await;
}

struct SqlErrorTest {
    setup_name: &'static str,
    sql: &'static str,
    expected_error_code: tonic::Code,
    expected_message: &'static str,
}

impl SqlErrorTest {
    async fn run(&self) {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();
        let setup_name = self.setup_name;

        for arch in [IoxArchitecture::Kafkaful, IoxArchitecture::Kafkaless] {
            info!("Using IoxArchitecture::{arch:?} and setup {setup_name}");

            // Set up the cluster  ====================================
            let mut cluster = match arch {
                IoxArchitecture::Kafkaful => {
                    MiniCluster::create_non_shared_standard_never_persist(database_url.clone())
                        .await
                }
                IoxArchitecture::Kafkaless => {
                    MiniCluster::create_shared2_never_persist(database_url.clone()).await
                }
            };

            let setup_steps = SETUPS
                .get(setup_name)
                .unwrap_or_else(|| panic!("Could not find setup with key `{setup_name}`"))
                .iter()
                // When we've switched over to the Kafkaless architecture, this map can be
                // removed.
                .flat_map(|step| match (arch, step) {
                    // If we're using the old architecture and the test steps include
                    // `WaitForPersist2`, swap it with `WaitForPersist` instead.
                    (IoxArchitecture::Kafkaful, Step::WaitForPersisted2 { .. }) => {
                        vec![&Step::WaitForPersisted]
                    }
                    // If we're using the old architecture and the test steps include
                    // `WriteLineProtocol`, wait for the data to be readable after writing.
                    (IoxArchitecture::Kafkaful, Step::WriteLineProtocol { .. }) => {
                        vec![step, &Step::WaitForReadable]
                    }
                    (_, other) => vec![other],
                });

            let test_step = Step::QueryExpectingError {
                sql: self.sql.into(),
                expected_error_code: self.expected_error_code,
                expected_message: self.expected_message.into(),
            };
            StepTest::new(&mut cluster, setup_steps.chain(std::iter::once(&test_step)))
                .run()
                .await;
        }
    }
}
