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
use futures::prelude::*;
use prost::Message;
use std::convert::TryInto;
use std::env;
use std::process::{Command, Stdio};
use std::str;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use std::u32;

const URL_BASE: &str = "http://localhost:8080/api/v2";
const GRPC_URL_BASE: &str = "http://localhost:8081/";

mod grpc {
    tonic::include_proto!("delorean");
}

use grpc::delorean_client::DeloreanClient;
use grpc::storage_client::StorageClient;
use grpc::Organization;
use grpc::ReadSource;
use grpc::{
    node::{Comparison, Value},
    read_response::{frame::Data, DataType},
    Node, Predicate, ReadFilterRequest, Tag, TagKeysRequest, TagValuesRequest, TimestampRange,
};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! assert_unwrap {
    ($e:expr, $p:path) => {
        match $e {
            $p(v) => v,
            _ => panic!("{} was not a {}", stringify!($e), stringify!($p)),
        }
    };
    ($e:expr, $p:path, $extra:tt) => {
        match $e {
            $p(v) => v,
            _ => {
                let extra = format_args!($extra);
                panic!("{} was not a {}: {}", stringify!($e), stringify!($p), extra);
            }
        }
    };
}

async fn read_data(
    client: &reqwest::Client,
    path: &str,
    org_id: u32,
    bucket_name: &str,
    predicate: &str,
    seconds_ago: u64,
) -> Result<String> {
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
) -> Result<()> {
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
async fn read_and_write_data() -> Result<()> {
    let mut server_thread = Command::cargo_bin("delorean")?
        .stdout(Stdio::null())
        .env("DELOREAN_DB_DIR", get_test_storage_path())
        .spawn()?;

    // TODO: poll the server to see if it's ready instead of sleeping
    sleep(Duration::from_secs(3));

    let org_id = 7878;
    let bucket_name = "all";

    let client = reqwest::Client::new();
    let mut grpc_client = DeloreanClient::connect(GRPC_URL_BASE).await?;

    let get_buckets_request = tonic::Request::new(Organization {
        id: org_id,
        name: "test".into(),
        buckets: vec![],
    });
    let get_buckets_response = grpc_client.get_buckets(get_buckets_request).await?;
    let get_buckets_response = get_buckets_response.into_inner();
    let org_buckets = get_buckets_response.buckets;

    // This checks that gRPC is functioning and that we're starting from an org without buckets.
    assert!(org_buckets.is_empty());

    let start_time = SystemTime::now();
    let ns_since_epoch: i64 = start_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time should have been after the epoch")
        .as_nanos()
        .try_into()
        .expect("Unable to represent system time");

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
        r#"host="server01""#,
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

    let mut storage_client = StorageClient::connect(GRPC_URL_BASE).await?;

    let org_id = u64::from(u32::MAX);
    let bucket_id = 1; // TODO: how do we know this?
    let partition_id = u64::from(u32::MAX);
    let read_source = ReadSource {
        org_id,
        bucket_id,
        partition_id,
    };
    let mut d = Vec::new();
    read_source.encode(&mut d)?;
    let read_source = prost_types::Any {
        type_url: "/TODO".to_string(),
        value: d,
    };
    let read_source = Some(read_source);

    let range = TimestampRange {
        start: ns_since_epoch,
        end: ns_since_epoch + 3,
    };
    let range = Some(range);

    let l = Value::TagRefValue("host".into());
    let l = Node {
        children: vec![],
        value: Some(l),
    };

    let r = Value::StringValue("server01".into());
    let r = Node {
        children: vec![],
        value: Some(r),
    };

    let comp = Value::Comparison(Comparison::Equal as _);
    let comp = Some(comp);
    let root = Node {
        children: vec![l, r],
        value: comp,
    };
    let root = Some(root);
    let predicate = Predicate { root };
    let predicate = Some(predicate);

    let read_filter_request = tonic::Request::new(ReadFilterRequest {
        read_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
    });
    let read_response = storage_client.read_filter(read_filter_request).await?;

    let responses: Vec<_> = read_response.into_inner().try_collect().await?;
    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    assert_eq!(
        frames.len(),
        5,
        "expected exactly 5 frames, but there were {}",
        frames.len()
    );

    let f = assert_unwrap!(&frames[0], Data::Series, "in frame 0");
    assert_eq!(f.data_type, DataType::Float as i32, "in frame 0");
    assert_eq!(
        tags_as_strings(&f.tags),
        vec![("host", "server01"), ("region", "us-west")]
    );

    let f = assert_unwrap!(&frames[1], Data::FloatPoints, "in frame 1");
    assert_eq!(f.timestamps, [ns_since_epoch], "in frame 1");
    assert_eq!(f.values, [0.64], "in frame 1");

    let f = assert_unwrap!(&frames[2], Data::FloatPoints, "in frame 2");
    assert_eq!(f.timestamps, [ns_since_epoch + 3], "in frame 2");
    assert_eq!(f.values, [0.000_003], "in frame 2");

    let f = assert_unwrap!(&frames[3], Data::Series, "in frame 3");
    assert_eq!(f.data_type, DataType::Float as i32, "in frame 3");

    assert_eq!(
        tags_as_strings(&f.tags),
        vec![("host", "server01"), ("region", "us-east")]
    );

    let f = assert_unwrap!(&frames[4], Data::FloatPoints, "in frame 4");
    assert_eq!(f.timestamps, [ns_since_epoch + 2], "in frame 4");
    assert_eq!(f.values, [1_234_567.891_011], "in frame 4");

    let tag_keys_request = tonic::Request::new(TagKeysRequest {
        tags_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
    });

    let tag_keys_response = storage_client.tag_keys(tag_keys_request).await?;
    let responses: Vec<_> = tag_keys_response.into_inner().try_collect().await?;

    let keys = &responses[0].values;
    let keys: Vec<_> = keys.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(keys, vec!["_f", "_m", "host", "region"]);

    let tag_values_request = tonic::Request::new(TagValuesRequest {
        tags_source: read_source,
        range,
        predicate,
        tag_key: String::from("host"),
    });

    let tag_values_response = storage_client.tag_values(tag_values_request).await?;
    let responses: Vec<_> = tag_values_response.into_inner().try_collect().await?;

    let values = &responses[0].values;
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(values, vec!["server01", "server02"]);

    server_thread
        .kill()
        .expect("Should have been able to kill the test server");

    Ok(())
}

fn tags_as_strings(tags: &[Tag]) -> Vec<(&str, &str)> {
    tags.iter()
        .map(|t| {
            (
                str::from_utf8(&t.key).unwrap(),
                str::from_utf8(&t.value).unwrap(),
            )
        })
        .collect()
}
