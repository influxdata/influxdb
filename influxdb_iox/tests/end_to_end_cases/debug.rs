//! Tests the `influxdb_iox debug` commands
use std::path::Path;

use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use assert_cmd::Command;
use futures::FutureExt;
use influxdb_iox_client::flight::Error as FlightError;
use predicates::prelude::*;
use tempfile::TempDir;
use test_helpers_end_to_end::{
    maybe_skip_integration, try_run_sql, MiniCluster, ServerFixture, Step, StepTest, StepTestState,
    TestConfig,
};

#[tokio::test]
async fn test_git_version() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("UNKNOWN")
                .not()
                .and(predicate::str::is_match("revision [0-9a-f]{40}").unwrap()),
        );
}

#[tokio::test]
async fn test_print_cpu() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("debug")
        .arg("print-cpu")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "rustc is using the following target options",
        ));
}

/// Tests that we can
///
/// 1. export a table from one IOx instance into a directory of files
/// 2. build a catalog from that directory of that files
/// 3. Start a all-in-one instance from that rebuilt catalog
/// 4. Can run a query successfully
#[tokio::test]
async fn build_catalog() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let table_name = "my_awesome_table";

    let mut cluster = MiniCluster::create_shared(database_url).await;

    let sql = "select tag1, tag2, val from my_awesome_table";
    let expected = [
        "+------+------+-----+",
        "| tag1 | tag2 | val |",
        "+------+------+-----+",
        "| C    | D    | 43  |",
        "+------+------+-----+",
    ];

    StepTest::new(
        &mut cluster,
        vec![
            // Persist some data
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(format!("{table_name},tag1=C,tag2=D val=43i 123456")),
            Step::WaitForPersisted {
                expected_increase: 1,
            },
            Step::Query {
                sql: sql.to_string(),
                expected: expected.to_vec(),
            },
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let router_addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = state.cluster().namespace().to_string();

                    // directory to export files to
                    let export_dir =
                        tempfile::tempdir().expect("could not get temporary directory");

                    // call `influxdb_iox remote store get-table <namespace> <table_name>`
                    // to the table to a temporary directory
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .current_dir(export_dir.as_ref())
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg(&namespace)
                        .arg(table_name)
                        .assert()
                        .success();

                    // Data is exported in <export_dir>/table_name
                    let table_dir = export_dir.path().join(table_name);

                    // We can build a catalog and start up the server and run a query
                    rebuild_and_query(&table_dir, &namespace, sql, &expected).await;

                    // We can also rebuild a catalog from just the parquet files
                    let only_parquet_dir = copy_only_parquet_files(&table_dir);
                    rebuild_and_query(only_parquet_dir.path(), &namespace, sql, &expected).await;
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Rebuilds a catalog from an export directory, starts up a server
/// and verifies the running `sql` in `namespace` produces `expected`
async fn rebuild_and_query(table_dir: &Path, namespace: &str, sql: &str, expected: &[&str]) {
    // Very occassionally, something goes wrong with the sqlite based
    // catalog and it doesn't get the new files. Thus try a few times
    //
    // See https://github.com/influxdata/influxdb_iox/issues/8287
    let mut retries = 5;

    while retries > 0 {
        println!("** Retries remaining:  {retries}");
        let restarted = RestartedServer::build_catalog_and_start(table_dir).await;
        match restarted.try_run_sql(sql, namespace).await {
            // if we got results, great, otherwise try again
            Ok(batches) if !batches.is_empty() => {
                assert_batches_sorted_eq!(expected, &batches);
                return;
            }
            _ => {}
        }

        retries -= 1;
    }
}

/// An all in one instance, with data directory of `data_dir`
struct RestartedServer {
    all_in_one: ServerFixture,

    /// data_dir is held so the temp dir is only cleaned on drop
    #[allow(dead_code)]
    data_dir: TempDir,
}

impl RestartedServer {
    async fn try_run_sql(
        &self,
        sql: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Vec<RecordBatch>, FlightError> {
        let (batches, _schema) = try_run_sql(
            sql,
            namespace,
            self.all_in_one.querier_grpc_connection(),
            None,
            false,
        )
        .await?;

        Ok(batches)
    }

    /// builds a catalog from an export directory and starts a all in
    /// one instance with that exported directory.
    async fn build_catalog_and_start(exported_table_dir: &Path) -> Self {
        // directory to rebuild catalog in
        let data_dir = tempfile::tempdir().expect("could not get temporary directory");

        println!("Input directory: {exported_table_dir:?}");
        println!("target_directory: {data_dir:?}");

        // call `influxdb_iox debug build-catalog <table_dir> <new_data_dir>`
        Command::cargo_bin("influxdb_iox")
            .unwrap()
            // use -v to enable logging so we can check the status messages
            .arg("-vv")
            .arg("debug")
            .arg("build-catalog")
            .arg(exported_table_dir.as_os_str().to_str().unwrap())
            .arg(data_dir.path().as_os_str().to_str().unwrap())
            .assert()
            .success()
            .stdout(
                predicate::str::contains("Beginning catalog / object_store build")
                    .and(predicate::str::contains(
                        "Begin importing files total_files=1",
                    ))
                    .and(predicate::str::contains(
                        "Completed importing files total_files=1",
                    )),
            );

        println!("Completed rebuild in {data_dir:?}");

        // now, start up a new server in all-in-one mode
        // using the  newly built data directory
        let test_config = TestConfig::new_all_in_one_with_data_dir(data_dir.path());
        let all_in_one = ServerFixture::create(test_config).await;

        Self {
            all_in_one,
            data_dir,
        }
    }
}

/// Copies only parquet files from the source directory to a new
/// temporary directory
fn copy_only_parquet_files(src: &Path) -> TempDir {
    let target_dir = TempDir::new().expect("can't make temp dir");
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src = entry.path();
        match src.extension() {
            Some(ext) if ext == "parquet" => {
                println!("Copying {ext:?} entry: {entry:?}");
                let dst = target_dir.path().join(src.file_name().unwrap());
                std::fs::copy(src, &dst).expect("error copying");
            }
            Some(ext) => {
                println!("Skipping {ext:?} entry: {entry:?}");
            }
            None => {
                println!("skipping no ext");
            }
        }
    }
    target_dir
}
