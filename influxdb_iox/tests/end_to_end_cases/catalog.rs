//! End to end test of catalog configurations

use std::{io::Write, path::Path};

use tempfile::TempDir;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest};
#[tokio::test]
async fn dsn_file() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // dsn-file only supported for postgres dsns, so skip if not postgres
    if !database_url.starts_with("postgres") {
        println!("SKIPPING dsn file test. Requires postgres catalog, but got {database_url}");
        return;
    }

    // tests using the special `dsn-file://` url for catalog
    // used in IDPE / InfluxCloud production
    //
    // In this case the dsn is `dsn-file://<filename>` and the actual
    // catalog url is stored in <filename> (which is updated from time
    // to time with rotated credentials).
    let tmpdir = TempDir::new().unwrap();
    let dsn_file_path = tmpdir.path().join("catalog.dsn");

    // Write the actual database url to the temporary file
    write_to_file(&dsn_file_path, &database_url);

    let database_url = format!("dsn-file://{}", dsn_file_path.display());
    println!("databse_url is {database_url}");

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_non_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol("my_table val=42i 123456".to_string()),
            Step::Query {
                sql: "select * from my_table".to_string(),
                expected: vec![
                    "+--------------------------------+-----+",
                    "| time                           | val |",
                    "+--------------------------------+-----+",
                    "| 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}

fn write_to_file(path: &Path, contents: &str) {
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
}
