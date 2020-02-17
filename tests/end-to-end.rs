// The test in this file runs the server in a separate thread and makes HTTP requests as a smoke
// test for the integration of the whole system.
//
// As written, only one test of this style can run at a time. Add more data to the existing test to
// test more scenarios rather than adding more tests in the same style.
//
// Or, change the way this test behaves to create isolated instances by:
//
// - Finding an unused port for the server to run on and using that port in the URL
// - Creating a temporary directory for an isolated database path
//
// Or, change the tests to use one server and isolate through `org_id` by:
//
// - Starting one server before all the relevant tests are run
// - Creating a unique org_id per test
// - Stopping the server after all relevant tests are run

use assert_cmd::prelude::*;

use std::env;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime};

const URL_BASE: &str = "http://localhost:8080/api/v2";

async fn read_data(
    client: &reqwest::Client,
    path: &str,
    org_id: u32,
    bucket_name: &str,
    predicate: &str,
    seconds_ago: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let url = format!("{}{}", URL_BASE, path);
    Ok(client
        .get(&url)
        .query(&[
            ("bucket_name", bucket_name),
            ("org_id", &org_id.to_string()),
            ("predicate", predicate),
            ("start", &format!("-{}s", seconds_ago)),
        ])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

async fn write_data(
    client: &reqwest::Client,
    path: &str,
    org_id: u32,
    bucket_name: &str,
    body: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}{}", URL_BASE, path);
    client
        .post(&url)
        .query(&[
            ("bucket_name", bucket_name),
            ("org_id", &org_id.to_string()),
        ])
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

// TODO: if TEST_DELOREAN_DB_DIR is set, create a temporary directory in that directory or
// otherwise isolate the database used in this test with the database used in other tests, rather
// than always ignoring TEST_DELOREAN_DB_DIR
fn get_test_storage_path() -> String {
    let mut path = env::temp_dir();
    path.push("delorean/");
    std::fs::remove_dir_all(&path).unwrap();
    path.into_os_string()
        .into_string()
        .expect("Should have been able to turn temp dir into String")
}

#[tokio::test]
async fn read_and_write_data() -> Result<(), Box<dyn std::error::Error>> {
    let mut server_thread = Command::cargo_bin("delorean")?
        .stdout(Stdio::null())
        .env("DELOREAN_DB_DIR", get_test_storage_path())
        .spawn()?;

    // TODO: poll the server to see if it's ready instead of sleeping
    sleep(Duration::from_secs(3));

    let org_id = 7878;
    let bucket_name = "all";
    let client = reqwest::Client::new();

    let start_time = SystemTime::now();
    let ns_since_epoch = start_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time should have been after the epoch")
        .as_nanos();

    // TODO: make a more extensible way to manage data for tests, such as in external fixture
    // files or with factories.
    write_data(
        &client,
        "/write",
        org_id,
        bucket_name,
        format!(
            "\
cpu_load_short,host=server01,region=us-west value=0.64 {}
cpu_load_short,host=server02,region=us-west value=3.89 {}
cpu_load_short,host=server01,region=us-east value=1234567.891011 {}
cpu_load_short,host=server01,region=us-west value=0.000003 {}",
            ns_since_epoch,
            ns_since_epoch + 1,
            ns_since_epoch + 2,
            ns_since_epoch + 3
        ),
    )
    .await?;

    let end_time = SystemTime::now();
    let duration = end_time
        .duration_since(start_time)
        .expect("End time should have been after start time");
    let seconds_ago = duration.as_secs() + 1;

    let text = read_data(
        &client,
        "/read",
        org_id,
        bucket_name,
        "host=\"server01\"",
        seconds_ago,
    )
    .await?;

    // TODO: make a more sustainable way to manage expected data for tests, such as using the
    // insta crate to manage snapshots.
    assert_eq!(
        text,
        format!(
            "\
_m,host,region,_f,_time,_value
cpu_load_short,server01,us-west,value,{},0.64
cpu_load_short,server01,us-west,value,{},0.000003

_m,host,region,_f,_time,_value
cpu_load_short,server01,us-east,value,{},1234567.891011

",
            ns_since_epoch,
            ns_since_epoch + 3,
            ns_since_epoch + 2
        )
    );

    server_thread
        .kill()
        .expect("Should have been able to kill the test server");

    Ok(())
}
