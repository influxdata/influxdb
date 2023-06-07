//! Tests of SQL queries that are expected to return particular errors.

use crate::query_tests::setups::SETUPS;
use observability_deps::tracing::*;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest};

// This is a reproducer of https://github.com/influxdata/idpe/issues/17644
#[tokio::test]
async fn date_bin_interval_0() {
    SqlErrorTest {
        setup_name: "OneMeasurementTwoSeries",
        sql:  "SELECT date_bin(INTERVAL '0 second', time) as month, count(cpu.user) from cpu where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z' group by month;",
        expected_error_code: tonic::Code::InvalidArgument,
        expected_message: "External error: Execution error: DATE_BIN stride must be non-zero",
    }
    .run()
    .await;
}

#[tokio::test]
async fn schema_merge_nonexistent_column() {
    SqlErrorTest {
        setup_name: "MultiChunkSchemaMerge",
        sql: "SELECT * from cpu where foo = 8",
        expected_error_code: tonic::Code::InvalidArgument,
        expected_message: r#"Error while planning query: Schema error: No field named foo. Valid fields are cpu.host, cpu.region, cpu.system, cpu.time, cpu.user."#,
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
        expected_message: "Error while planning query: \
                           This feature is not implemented: \
                           Unsupported logical plan: CreateCatalogSchema",
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
        requires at least 2 arguments, got 1",
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

        info!("Using setup {setup_name}");

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared_never_persist(database_url.clone()).await;

        let setup_steps = SETUPS
            .get(setup_name)
            .unwrap_or_else(|| panic!("Could not find setup with key `{setup_name}`"))
            .iter();

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
