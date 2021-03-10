// The test in this file runs the server in a separate thread and makes HTTP
// requests as a smoke test for the integration of the whole system.
//
// The servers under test are managed using [`ServerFixture`]
//
// Other rust tests are defined in the various submodules of end_to_end_cases

use std::convert::TryInto;
use std::str;
use std::time::SystemTime;
use std::u32;

use futures::prelude::*;
use prost::Message;

use data_types::{names::org_and_bucket_to_database, DatabaseName};
use end_to_end_cases::*;
use generated_types::{
    influxdata::iox::management::v1::DatabaseRules, storage_client::StorageClient, ReadSource,
    TimestampRange,
};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

pub mod common;

mod end_to_end_cases;

use common::server_fixture::*;

#[tokio::test]
async fn read_and_write_data() {
    let fixture = ServerFixture::create_shared().await;

    let influxdb2 = fixture.influxdb2_client();
    let mut storage_client = StorageClient::new(fixture.grpc_channel());
    let mut management_client =
        influxdb_iox_client::management::Client::new(fixture.grpc_channel());

    // These tests share data; TODO: a better way to indicate this
    {
        let scenario = Scenario::default()
            .set_org_id("0000111100001111")
            .set_bucket_id("1111000011110000");

        create_database(&mut management_client, &scenario.database_name()).await;

        let expected_read_data = load_data(&influxdb2, &scenario).await;
        let sql_query = "select * from cpu_load_short";

        read_api::test(&fixture, &scenario, sql_query, &expected_read_data).await;
        storage_api::test(&mut storage_client, &scenario).await;
        flight_api::test(&fixture, &scenario, sql_query, &expected_read_data).await;
    }

    // These tests manage their own data
    storage_api::read_group_test(&mut management_client, &influxdb2, &mut storage_client).await;
    storage_api::read_window_aggregate_test(
        &mut management_client,
        &influxdb2,
        &mut storage_client,
    )
    .await;
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

    fn read_source(&self) -> Option<generated_types::google::protobuf::Any> {
        let partition_id = u64::from(u32::MAX);
        let read_source = ReadSource {
            org_id: self.org_id(),
            bucket_id: self.bucket_id(),
            partition_id,
        };

        let mut d = bytes::BytesMut::new();
        read_source.encode(&mut d).unwrap();
        let read_source = generated_types::google::protobuf::Any {
            type_url: "/TODO".to_string(),
            value: d.freeze(),
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

async fn create_database(
    client: &mut influxdb_iox_client::management::Client,
    database_name: &str,
) {
    client
        .create_database(DatabaseRules {
            name: database_name.to_string(),
            mutable_buffer_config: Some(Default::default()),
            ..Default::default()
        })
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

#[tokio::test]
async fn test_http_error_messages() {
    let server_fixture = ServerFixture::create_shared().await;
    let client = server_fixture.influxdb2_client();

    // send malformed request (bucket id is invalid)
    let result = client
        .write_line_protocol("Bar", "Foo", "arbitrary")
        .await
        .expect_err("Should have errored");

    let expected_error = "HTTP request returned an error: 400 Bad Request, `{\"error\":\"Error parsing line protocol: A generic parsing error occurred: TakeWhile1\",\"error_code\":100}`";
    assert_eq!(result.to_string(), expected_error);
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
