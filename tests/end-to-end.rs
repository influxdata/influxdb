// The test in this file runs the server in a separate thread and makes HTTP
// requests as a smoke test for the integration of the whole system.
//
// As written, only one test of this style can run at a time. Add more data to
// the existing test to test more scenarios rather than adding more tests in the
// same style.
//
// Or, change the way this test behaves to create isolated instances by:
//
// - Finding an unused port for the server to run on and using that port in the
//   URL
// - Creating a temporary directory for an isolated database path
//
// Or, change the tests to use one server and isolate through `org_id` by:
//
// - Starting one server before all the relevant tests are run
// - Creating a unique org_id per test
// - Stopping the server after all relevant tests are run

use assert_cmd::prelude::*;
use data_types::{database_rules::DatabaseRules, names::org_and_bucket_to_database, DatabaseName};
use futures::prelude::*;
use generated_types::{storage_client::StorageClient, ReadSource, TimestampRange};
use prost::Message;
use std::convert::TryInto;
use std::process::{Child, Command};
use std::str;
use std::time::{Duration, SystemTime};
use std::u32;
use tempfile::TempDir;

const HTTP_BASE: &str = "http://localhost:8080";
const API_BASE: &str = "http://localhost:8080/api/v2";
const GRPC_URL_BASE: &str = "http://localhost:8082/";
const TOKEN: &str = "InfluxDB IOx doesn't have authentication yet";

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

mod end_to_end_cases;
use end_to_end_cases::*;

#[tokio::test]
async fn read_and_write_data() {
    let server = TestServer::new().unwrap();
    server.wait_until_ready().await;

    let http_client = reqwest::Client::new();
    let influxdb2 = influxdb2_client::Client::new(HTTP_BASE, TOKEN);
    let mut storage_client = StorageClient::connect(GRPC_URL_BASE).await.unwrap();

    // These tests share data; TODO: a better way to indicate this
    {
        let scenario = Scenario::default()
            .set_org_id("0000111100001111")
            .set_bucket_id("1111000011110000");

        create_database(&http_client, &scenario.database_name()).await;

        let expected_read_data = load_data(&influxdb2, &scenario).await;
        let sql_query = "select * from cpu_load_short";

        read_api::test(&http_client, &scenario, sql_query, &expected_read_data).await;
        grpc_api::test(&mut storage_client, &scenario).await;
    }

    // These tests manage their own data
    grpc_api::read_group_test(&http_client, &influxdb2, &mut storage_client).await;
    grpc_api::read_window_aggregate_test(&http_client, &influxdb2, &mut storage_client).await;
    test_http_error_messages(&influxdb2).await.unwrap();
}

// TODO: Randomly generate org and bucket ids to ensure test data independence
// where desired

#[derive(Debug)]
pub struct Scenario {
    org_id_str: String,
    bucket_id_str: String,
    ns_since_epoch: i64,
}

impl Default for Scenario {
    fn default() -> Self {
        let ns_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should have been after the epoch")
            .as_nanos()
            .try_into()
            .expect("Unable to represent system time");

        Self {
            ns_since_epoch,
            org_id_str: Default::default(),
            bucket_id_str: Default::default(),
        }
    }
}

impl Scenario {
    fn set_org_id(mut self, org_id: impl Into<String>) -> Self {
        self.org_id_str = org_id.into();
        self
    }

    fn set_bucket_id(mut self, bucket_id: impl Into<String>) -> Self {
        self.bucket_id_str = bucket_id.into();
        self
    }

    fn org_id_str(&self) -> &str {
        &self.org_id_str
    }

    fn bucket_id_str(&self) -> &str {
        &self.bucket_id_str
    }

    fn org_id(&self) -> u64 {
        u64::from_str_radix(&self.org_id_str, 16).unwrap()
    }

    fn bucket_id(&self) -> u64 {
        u64::from_str_radix(&self.bucket_id_str, 16).unwrap()
    }

    fn database_name(&self) -> DatabaseName<'_> {
        org_and_bucket_to_database(&self.org_id_str, &self.bucket_id_str).unwrap()
    }

    fn ns_since_epoch(&self) -> i64 {
        self.ns_since_epoch
    }

    fn read_source(&self) -> Option<prost_types::Any> {
        let partition_id = u64::from(u32::MAX);
        let read_source = ReadSource {
            org_id: self.org_id(),
            bucket_id: self.bucket_id(),
            partition_id,
        };

        let mut d = Vec::new();
        read_source.encode(&mut d).unwrap();
        let read_source = prost_types::Any {
            type_url: "/TODO".to_string(),
            value: d,
        };

        Some(read_source)
    }

    fn timestamp_range(&self) -> Option<TimestampRange> {
        Some(TimestampRange {
            start: self.ns_since_epoch(),
            end: self.ns_since_epoch() + 10,
        })
    }
}

async fn create_database(client: &reqwest::Client, database_name: &str) {
    let rules = DatabaseRules {
        store_locally: true,
        ..Default::default()
    };
    let data = serde_json::to_vec(&rules).unwrap();

    client
        .put(&format!(
            "{}/iox/api/v1/databases/{}",
            HTTP_BASE, database_name
        ))
        .body(data)
        .send()
        .await
        .unwrap();
}

async fn load_data(influxdb2: &influxdb2_client::Client, scenario: &Scenario) -> Vec<String> {
    // TODO: make a more extensible way to manage data for tests, such as in
    // external fixture files or with factories.
    let points = vec![
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("value", 0.64)
            .timestamp(scenario.ns_since_epoch())
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .field("value", 27.99)
            .timestamp(scenario.ns_since_epoch() + 1)
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server02")
            .tag("region", "us-west")
            .field("value", 3.89)
            .timestamp(scenario.ns_since_epoch() + 2)
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-east")
            .field("value", 1234567.891011)
            .timestamp(scenario.ns_since_epoch() + 3)
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("value", 0.000003)
            .timestamp(scenario.ns_since_epoch() + 4)
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("system")
            .tag("host", "server03")
            .field("uptime", 1303385)
            .timestamp(scenario.ns_since_epoch() + 5)
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("swap")
            .tag("host", "server01")
            .tag("name", "disk0")
            .field("in", 3)
            .field("out", 4)
            .timestamp(scenario.ns_since_epoch() + 6)
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("status")
            .field("active", true)
            .timestamp(scenario.ns_since_epoch() + 7)
            .build()
            .unwrap(),
        influxdb2_client::DataPoint::builder("attributes")
            .field("color", "blue")
            .timestamp(scenario.ns_since_epoch() + 8)
            .build()
            .unwrap(),
    ];
    write_data(&influxdb2, scenario, points).await.unwrap();

    substitute_nanos(
        scenario.ns_since_epoch(),
        &[
            "+----------+---------+---------------------+----------------+",
            "| host     | region  | time                | value          |",
            "+----------+---------+---------------------+----------------+",
            "| server01 | us-west | ns0 | 0.64           |",
            "| server01 |         | ns1 | 27.99          |",
            "| server02 | us-west | ns2 | 3.89           |",
            "| server01 | us-east | ns3 | 1234567.891011 |",
            "| server01 | us-west | ns4 | 0.000003       |",
            "+----------+---------+---------------------+----------------+",
        ],
    )
}

async fn write_data(
    client: &influxdb2_client::Client,
    scenario: &Scenario,
    points: Vec<influxdb2_client::DataPoint>,
) -> Result<()> {
    client
        .write(
            scenario.org_id_str(),
            scenario.bucket_id_str(),
            stream::iter(points),
        )
        .await?;
    Ok(())
}

// Don't make a separate #test function so that we can reuse the same
// server process
async fn test_http_error_messages(client: &influxdb2_client::Client) -> Result<()> {
    // send malformed request (bucket id is invalid)
    let result = client
        .write_line_protocol("Bar", "Foo", "arbitrary")
        .await
        .expect_err("Should have errored");

    let expected_error = "HTTP request returned an error: 400 Bad Request, `{\"error\":\"Error parsing line protocol: A generic parsing error occurred: TakeWhile1\",\"error_code\":100}`";
    assert_eq!(result.to_string(), expected_error);

    Ok(())
}

/// substitutes "ns" --> ns_since_epoch, ns1-->ns_since_epoch+1, etc
fn substitute_nanos(ns_since_epoch: i64, lines: &[&str]) -> Vec<String> {
    let substitutions = vec![
        ("ns0", format!("{}", ns_since_epoch)),
        ("ns1", format!("{}", ns_since_epoch + 1)),
        ("ns2", format!("{}", ns_since_epoch + 2)),
        ("ns3", format!("{}", ns_since_epoch + 3)),
        ("ns4", format!("{}", ns_since_epoch + 4)),
        ("ns5", format!("{}", ns_since_epoch + 5)),
        ("ns6", format!("{}", ns_since_epoch + 6)),
    ];

    lines
        .iter()
        .map(|line| {
            let mut line = line.to_string();
            for (from, to) in &substitutions {
                line = line.replace(from, to);
            }
            line
        })
        .collect()
}

struct TestServer {
    server_process: Child,

    // The temporary directory **must** be last so that it is
    // dropped after the database closes.
    #[allow(dead_code)]
    dir: TempDir,
}

impl TestServer {
    fn new() -> Result<Self> {
        let dir = test_helpers::tmp_dir().unwrap();

        let server_process = Command::cargo_bin("influxdb_iox")
            .unwrap()
            // Can enable for debbugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_ID", "1")
            .spawn()
            .unwrap();

        Ok(Self {
            dir,
            server_process,
        })
    }

    #[allow(dead_code)]
    fn restart(&mut self) -> Result<()> {
        self.server_process.kill().unwrap();
        self.server_process.wait().unwrap();
        self.server_process = Command::cargo_bin("influxdb_iox")
            .unwrap()
            // Can enable for debbugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_DB_DIR", self.dir.path())
            .env("INFLUXDB_IOX_ID", "1")
            .spawn()
            .unwrap();
        Ok(())
    }

    async fn wait_until_ready(&self) {
        // Poll the RPC and HTTP servers separately as they listen on
        // different ports but both need to be up for the test to run
        let try_grpc_connect = async {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                match StorageClient::connect(GRPC_URL_BASE).await {
                    Ok(storage_client) => {
                        println!(
                            "Successfully connected storage_client: {:?}",
                            storage_client
                        );
                        return;
                    }
                    Err(e) => {
                        println!("Waiting for gRPC server to be up: {}", e);
                    }
                }
                interval.tick().await;
            }
        };

        let try_http_connect = async {
            let client = reqwest::Client::new();
            let url = format!("{}/ping", HTTP_BASE);
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                match client.get(&url).send().await {
                    Ok(resp) => {
                        println!("Successfully got a response from HTTP: {:?}", resp);
                        return;
                    }
                    Err(e) => {
                        println!("Waiting for HTTP server to be up: {}", e);
                    }
                }
                interval.tick().await;
            }
        };

        let pair = future::join(try_http_connect, try_grpc_connect);

        let capped_check = tokio::time::timeout(Duration::from_secs(3), pair);

        match capped_check.await {
            Ok(_) => println!("Server is up correctly"),
            Err(e) => println!("WARNING: server was not ready: {}", e),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server_process
            .kill()
            .expect("Should have been able to kill the test server");
    }
}
