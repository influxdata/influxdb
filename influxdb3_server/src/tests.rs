use crate::{CreateServerArgs, serve};
use crate::{Server, http::HttpApi};
use chrono::DateTime;
use datafusion::parquet::data_type::AsBytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
use http_body_util::BodyExt;
use hyper::StatusCode;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use influxdb3_authz::NoAuthAuthenticator;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_cache::parquet_cache::test_cached_obj_store_and_oracle;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_processing_engine::environment::DisabledManager;
use influxdb3_processing_engine::plugins::ProcessingEngineEnvironmentManager;
use influxdb3_query_executor::{CreateQueryExecutorArgs, QueryExecutorImpl};
use influxdb3_shutdown::ShutdownManager;
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::store::TelemetryStore;
use influxdb3_wal::WalConfig;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::N_SNAPSHOTS_TO_LOAD_ON_START;
use influxdb3_write::write_buffer::persisted_files::PersistedFiles;
use influxdb3_write::{Bufferer, WriteBuffer};
use iox_http_util::{
    RequestBody, RequestBuilder, Response, bytes_to_request_body, empty_request_body,
    read_body_bytes_for_tests,
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, PerQueryMemoryPoolConfig};
use iox_time::{MockProvider, Time};
use object_store::DynObjectStore;
use parquet_file::storage::{ParquetStorage, StorageId};
use pretty_assertions::assert_eq;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

static EMPTY_PATHS: OnceLock<Vec<&'static str>> = OnceLock::new();

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_and_query() {
    let start_time = 0;
    let (server, shutdown, _) = setup_server(start_time).await;

    write_lp(
        &server,
        "foo",
        "cpu,host=a val=1i 123",
        None,
        false,
        "nanosecond",
    )
    .await;

    // Test that we can query the output with a pretty output
    let res = query(
        &server,
        "foo",
        "select host, time, val from cpu",
        "pretty",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let body = String::from_utf8(body.as_bytes().to_vec()).unwrap();
    let expected = vec![
        "+------+-------------------------------+-----+",
        "| host | time                          | val |",
        "+------+-------------------------------+-----+",
        "| a    | 1970-01-01T00:00:00.000000123 | 1   |",
        "+------+-------------------------------+-----+",
    ];
    let actual: Vec<_> = body.split('\n').collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    // Test that we can query the output with a json output
    let res = query(
        &server,
        "foo",
        "select host, time, val from cpu",
        "json",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = r#"[{"host":"a","time":"1970-01-01T00:00:00.000000123","val":1}]"#;
    assert_eq!(actual, expected);
    // Test that we can query the output with a csv output
    let res = query(
        &server,
        "foo",
        "select host, time, val from cpu",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,time,val\na,1970-01-01T00:00:00.000000123,1\n";
    assert_eq!(actual, expected);

    // Test that we can query the output with a parquet
    use arrow::buffer::Buffer;
    use parquet::arrow::arrow_reader;
    let res = query(&server, "foo", "select * from cpu", "parquet", None).await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let batches = arrow_reader::ParquetRecordBatchReaderBuilder::try_new(body)
        .unwrap()
        .build()
        .unwrap();
    let batches = batches.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);

    // Check that we only have the columns we expect
    assert_eq!(batches[0].num_columns(), 3);
    assert!(batches[0].schema().column_with_name("host").is_some());
    assert!(batches[0].schema().column_with_name("time").is_some());
    assert!(batches[0].schema().column_with_name("val").is_some());
    assert!(
        batches[0]
            .schema()
            .column_with_name("random_name")
            .is_none()
    );

    assert_eq!(
        batches[0]["host"].to_data().child_data()[0].buffers()[1],
        Buffer::from([b'a'])
    );

    assert_eq!(
        batches[0]["time"].to_data().buffers(),
        &[Buffer::from([123, 0, 0, 0, 0, 0, 0, 0])]
    );
    assert_eq!(
        batches[0]["val"].to_data().buffers(),
        &[Buffer::from(1_u64.to_le_bytes())]
    );

    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_lp_tests() {
    let start_time = 0;
    let (server, shutdown, _) = setup_server(start_time).await;

    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=a val= 123\ncpu,host=b val=5 124\ncpu,host=b val= 124",
        None,
        false,
        "nanosecond",
    )
    .await;

    let status = resp.status();
    let body =
        String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        "{\
            \"error\":\"parsing failed for write_lp endpoint\",\
            \"data\":{\
                \"error_message\":\"No fields were provided\",\
                \"line_number\":1,\
                \"original_line\":\"cpu,host=a val= 123\"\
            }\
        }"
    );

    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=2 155\ncpu,host=a val= 123\ncpu,host=b val=5 199",
        None,
        true,
        "nanosecond",
    )
    .await;

    let status = resp.status();
    let body =
        String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        "{\
            \"error\":\"partial write of line protocol occurred\",\
            \"data\":[{\
                \"error_message\":\"No fields were provided\",\
                \"line_number\":2,\
                \"original_line\":\"cpu,host=a val= 123\"\
            }]\
        }"
    );

    // Check that the first write did not partially write any data. We
    // should only see 2 values from the above write.
    let res = query(
        &server,
        "foo",
        "select host, time, val from cpu",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,time,val\n\
                    b,1970-01-01T00:00:00.000000155,2.0\n\
                    b,1970-01-01T00:00:00.000000199,5.0\n";
    assert_eq!(actual, expected);

    // Check that invalid database names are rejected
    let resp = write_lp(
        &server,
        "this#_is_fine",
        "cpu,host=b val=2 155\n",
        None,
        true,
        "nanosecond",
    )
    .await;

    let status = resp.status();
    let body =
        String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        "{\
            \"error\":\"invalid character in database or rp name: must be ASCII, containing only letters, numbers, underscores, or hyphens\"\
        }"
    );

    let resp = write_lp(
        &server,
        "?this_is_fine",
        "cpu,host=b val=2 155\n",
        None,
        true,
        "nanosecond",
    )
    .await;

    let status = resp.status();
    let body =
        String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        "{\
            \"error\":\"db name did not start with a number or letter\"\
        }"
    );

    let resp = write_lp(
        &server,
        "",
        "cpu,host=b val=2 155\n",
        None,
        true,
        "nanosecond",
    )
    .await;

    let status = resp.status();
    let body =
        String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        "{\
            \"error\":\"db name cannot be empty\"\
        }"
    );

    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_lp_precision_tests() {
    let start_time = 1708473607000000000;
    let (server, shutdown, _) = setup_server(start_time).await;

    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=5 1708473600",
        None,
        false,
        "auto",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=5 1708473601000",
        None,
        false,
        "auto",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=5 1708473602000000",
        None,
        false,
        "auto",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=5 1708473603000000000",
        None,
        false,
        "auto",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=6 1708473604",
        None,
        false,
        "second",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=6 1708473605000",
        None,
        false,
        "millisecond",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=6 1708473606000000",
        None,
        false,
        "microsecond",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = write_lp(
        &server,
        "foo",
        "cpu,host=b val=6 1708473607000000000",
        None,
        false,
        "nanosecond",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let res = query(
        &server,
        "foo",
        "select host, time, val from cpu",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    // Since a query can come back with data in any order we need to sort it
    // here before we do any assertions
    let mut unsorted = String::from_utf8(body.as_bytes().to_vec())
        .unwrap()
        .lines()
        .skip(1)
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    unsorted.sort();
    let actual = unsorted.join("\n");
    let expected = "b,2024-02-21T00:00:00,5.0\n\
                    b,2024-02-21T00:00:01,5.0\n\
                    b,2024-02-21T00:00:02,5.0\n\
                    b,2024-02-21T00:00:03,5.0\n\
                    b,2024-02-21T00:00:04,6.0\n\
                    b,2024-02-21T00:00:05,6.0\n\
                    b,2024-02-21T00:00:06,6.0\n\
                    b,2024-02-21T00:00:07,6.0";
    assert_eq!(actual, expected);

    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_lp_with_single_member_gzip() {
    let (server, shutdown, _) = setup_server(0).await;

    let lp = "cpu,host=a val=1i 100\ncpu,host=b val=2i 200\ncpu,host=c val=3i 300";
    let payload = create_gzip_bytes(lp);

    let mut headers = hyper::HeaderMap::new();
    headers.insert(&CONTENT_ENCODING, "gzip".parse().unwrap());
    headers.insert(&CONTENT_LENGTH, payload.len().to_string().parse().unwrap());

    let resp = write_lp_raw(
        &server,
        "foo",
        bytes_to_request_body(payload),
        None,
        false,
        "nanosecond",
        Some(headers),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let res = query(
        &server,
        "foo",
        "SELECT host, val, time FROM cpu ORDER BY time",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,val,time\na,1,1970-01-01T00:00:00.000000100\nb,2,1970-01-01T00:00:00.000000200\nc,3,1970-01-01T00:00:00.000000300\n";
    assert_eq!(actual, expected);

    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_lp_with_multi_member_gzip() {
    let (server, shutdown, _) = setup_server(0).await;

    // Member 1: Multiple lines (note trailing newline to ensure proper separation)
    let lp1 = "cpu,host=a val=1i 100\ncpu,host=b val=2i 200\ncpu,host=c val=3i 300\n";
    let gz1 = create_gzip_bytes(lp1);

    // Member 2: Single line
    let lp2 = "mem,host=a used=500i 400\n";
    let gz2 = create_gzip_bytes(lp2);

    // Member 3: Multiple lines
    let lp3 = "disk,host=a free=1000i 500\ndisk,host=b free=2000i 600\n";
    let gz3 = create_gzip_bytes(lp3);

    let mut payload = Vec::new();
    payload.extend_from_slice(&gz1);
    payload.extend_from_slice(&gz2);
    payload.extend_from_slice(&gz3);

    let mut headers = hyper::HeaderMap::new();
    headers.insert(&CONTENT_ENCODING, "gzip".parse().unwrap());
    headers.insert(&CONTENT_LENGTH, payload.len().to_string().parse().unwrap());

    let resp = write_lp_raw(
        &server,
        "foo",
        bytes_to_request_body(payload),
        None,
        false,
        "nanosecond",
        Some(headers),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify cpu table
    let res = query(
        &server,
        "foo",
        "SELECT host, val, time FROM cpu ORDER BY time",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,val,time\na,1,1970-01-01T00:00:00.000000100\nb,2,1970-01-01T00:00:00.000000200\nc,3,1970-01-01T00:00:00.000000300\n";
    assert_eq!(actual, expected);

    // Verify mem table
    let res = query(
        &server,
        "foo",
        "SELECT host, used, time FROM mem ORDER BY time",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,used,time\na,500,1970-01-01T00:00:00.000000400\n";
    assert_eq!(actual, expected);

    // Verify disk table
    let res = query(
        &server,
        "foo",
        "SELECT host, free, time FROM disk ORDER BY time",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,free,time\na,1000,1970-01-01T00:00:00.000000500\nb,2000,1970-01-01T00:00:00.000000600\n";
    assert_eq!(actual, expected);

    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_lp_with_empty_gzip_member() {
    let (server, shutdown, _) = setup_server(0).await;

    // Member 1: Valid data (note trailing newline)
    let lp1 = "cpu,host=a val=1i 100\n";
    let gz1 = create_gzip_bytes(lp1);

    // Member 2: Empty gzip member
    let gz2 = create_gzip_bytes("");

    // Member 3: Valid data
    let lp3 = "mem,host=b used=500i 200\n";
    let gz3 = create_gzip_bytes(lp3);

    let mut payload = Vec::new();
    payload.extend_from_slice(&gz1);
    payload.extend_from_slice(&gz2);
    payload.extend_from_slice(&gz3);

    let mut headers = hyper::HeaderMap::new();
    headers.insert(&CONTENT_ENCODING, "gzip".parse().unwrap());
    headers.insert(&CONTENT_LENGTH, payload.len().to_string().parse().unwrap());

    let resp = write_lp_raw(
        &server,
        "foo",
        bytes_to_request_body(payload),
        None,
        false,
        "nanosecond",
        Some(headers),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify cpu table
    let res = query(
        &server,
        "foo",
        "SELECT host, val, time FROM cpu ORDER BY time",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,val,time\na,1,1970-01-01T00:00:00.000000100\n";
    assert_eq!(actual, expected);

    // Verify mem table
    let res = query(
        &server,
        "foo",
        "SELECT host, used, time FROM mem ORDER BY time",
        "csv",
        None,
    )
    .await;
    let body = read_body_bytes_for_tests(res.into_body()).await;
    let actual = std::str::from_utf8(body.as_bytes()).unwrap();
    let expected = "host,used,time\nb,500,1970-01-01T00:00:00.000000200\n";
    assert_eq!(actual, expected);

    shutdown.cancel();
}

fn create_gzip_bytes(data: &str) -> Vec<u8> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes()).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn delete_table_defaults_to_hard_delete_default() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";
    let table_name = "test_table";

    // Write some data to create the table
    write_lp(
        &server,
        db_name,
        &format!("{table_name},host=a val=1i 123"),
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the table without hard_delete_at parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{server}/api/v3/configure/table?db={db_name}&table={table_name}");

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Access the catalog to verify the table's hard_delete_time is None (which represents Never)
    let catalog = write_buffer.catalog();
    let db_schema = catalog.db_schema(db_name).expect("database should exist");

    // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
    let deleted_table = db_schema
        .tables()
        .find(|table| table.deleted && table.table_name.starts_with(table_name))
        .expect("deleted table should exist");

    // Verify the table is marked as deleted and hard_delete_time is set to default duration
    assert!(deleted_table.deleted, "table should be marked as deleted");
    assert_eq!(
        deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
        start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
        "hard_delete_time should be set to default duration when hard_delete_at is omitted"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_table_with_explicit_hard_delete_never() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";
    let table_name = "test_table";

    // Write some data to create the table
    write_lp(
        &server,
        db_name,
        &format!("{table_name},host=a val=1i 123"),
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the table with explicit hard_delete_at=never parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!(
        "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at=never"
    );

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Access the catalog to verify the table's hard_delete_time is None (which represents Never)
    let catalog = write_buffer.catalog();
    let db_schema = catalog.db_schema(db_name).expect("database should exist");

    // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
    let deleted_table = db_schema
        .tables()
        .find(|table| table.deleted && table.table_name.starts_with(table_name))
        .expect("deleted table should exist");

    // Verify the table is marked as deleted and hard_delete_time is None (Never)
    assert!(deleted_table.deleted, "table should be marked as deleted");
    assert!(
        deleted_table.hard_delete_time.is_none(),
        "hard_delete_time should be None (Never) when hard_delete_at=never is explicitly provided"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_table_with_explicit_hard_delete_now() {
    let start_time = 1000;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";
    let table_name = "test_table";

    // Write some data to create the table
    write_lp(
        &server,
        db_name,
        &format!("{table_name},host=a val=1i 123"),
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the table with explicit hard_delete_at=now parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!(
        "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at=now"
    );

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Access the catalog to verify the table's hard_delete_time is set to a time value (not None)
    let catalog = write_buffer.catalog();
    let db_schema = catalog.db_schema(db_name).expect("database should exist");

    // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
    let deleted_table = db_schema
        .tables()
        .find(|table| table.deleted && table.table_name.starts_with(table_name))
        .expect("deleted table should exist");

    // Verify the table is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted)
    assert!(deleted_table.deleted, "table should be marked as deleted");
    assert_eq!(
        deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
        start_time,
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_table_with_explicit_hard_delete_timestamp() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";
    let table_name = "test_table";

    // Write some data to create the table
    write_lp(
        &server,
        db_name,
        &format!("{table_name},host=a val=1i 123"),
        None,
        false,
        "nanosecond",
    )
    .await;

    let future_timestamp = "2125-12-31T23:59:59Z";
    let expected_time = Time::from_datetime(
        DateTime::parse_from_rfc3339(future_timestamp)
            .unwrap()
            .to_utc(),
    );

    // Make a DELETE request to delete the table with explicit hard_delete_at timestamp
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!(
        "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at={future_timestamp}"
    );

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Access the catalog to verify the table's hard_delete_time matches the expected timestamp
    let catalog = write_buffer.catalog();
    let db_schema = catalog.db_schema(db_name).expect("database should exist");

    // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
    let deleted_table = db_schema
        .tables()
        .find(|table| table.deleted && table.table_name.starts_with(table_name))
        .expect("deleted table should exist");

    // Verify the table is marked as deleted and hard_delete_time matches the expected timestamp
    assert!(deleted_table.deleted, "table should be marked as deleted");
    assert_eq!(
        deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
        expected_time.timestamp_nanos(),
        "hard_delete_time should match the explicitly provided timestamp"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_table_with_explicit_hard_delete_default() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";
    let table_name = "test_table";

    // Write some data to create the table
    write_lp(
        &server,
        db_name,
        &format!("{table_name},host=a val=1i 123"),
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the table with explicit hard_delete_at=default parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!(
        "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at=default"
    );

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Access the catalog to verify the table's hard_delete_time is set to Some (indicating it will be hard deleted after default duration)
    let catalog = write_buffer.catalog();
    let db_schema = catalog.db_schema(db_name).expect("database should exist");

    // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
    let deleted_table = db_schema
        .tables()
        .find(|table| table.deleted && table.table_name.starts_with(table_name))
        .expect("deleted table should exist");

    // Verify the table is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted after default duration)
    assert!(deleted_table.deleted, "table should be marked as deleted");
    assert_eq!(
        deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
        start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_database_with_explicit_hard_delete_never() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";

    // Write some data to create the database
    write_lp(
        &server,
        db_name,
        "cpu,host=a val=1i 123",
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the database with explicit hard_delete_at=never parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{server}/api/v3/configure/database?db={db_name}&hard_delete_at=never");

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
    let all_databases = write_buffer.catalog().list_db_schema();
    let deleted_db = all_databases
        .iter()
        .find(|db| db.deleted && db.name.starts_with(db_name))
        .expect("deleted database should exist");

    // Verify the database is marked as deleted and hard_delete_time is None (indicating it will never be hard deleted)
    assert!(deleted_db.deleted, "database should be marked as deleted");
    assert!(
        deleted_db.hard_delete_time.is_none(),
        "hard_delete_time should be None for never hard delete"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_database_defaults_to_hard_delete_default() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";

    // Write some data to create the database
    write_lp(
        &server,
        db_name,
        "cpu,host=a val=1i 123",
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the database without hard_delete_at parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{server}/api/v3/configure/database?db={db_name}");

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
    let all_databases = write_buffer.catalog().list_db_schema();
    let deleted_db = all_databases
        .iter()
        .find(|db| db.deleted && db.name.starts_with(db_name))
        .expect("deleted database should exist");

    // Verify the database is marked as deleted and hard_delete_time is set to default duration
    assert!(deleted_db.deleted, "database should be marked as deleted");
    assert_eq!(
        deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
        start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
        "hard_delete_time should be set to default duration when hard_delete_at is omitted"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_database_with_explicit_hard_delete_now() {
    let start_time = 1000;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";

    // Write some data to create the database
    write_lp(
        &server,
        db_name,
        "cpu,host=a val=1i 123",
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the database with explicit hard_delete_at=now parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{server}/api/v3/configure/database?db={db_name}&hard_delete_at=now");

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
    let all_databases = write_buffer.catalog().list_db_schema();
    let deleted_db = all_databases
        .iter()
        .find(|db| db.deleted && db.name.starts_with(db_name))
        .expect("deleted database should exist");

    // Verify the database is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted)
    assert!(deleted_db.deleted, "database should be marked as deleted");
    assert_eq!(
        deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
        start_time,
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_database_with_explicit_hard_delete_default() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";

    // Write some data to create the database
    write_lp(
        &server,
        db_name,
        "cpu,host=a val=1i 123",
        None,
        false,
        "nanosecond",
    )
    .await;

    // Make a DELETE request to delete the database with explicit hard_delete_at=default parameter
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{server}/api/v3/configure/database?db={db_name}&hard_delete_at=default");

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
    let all_databases = write_buffer.catalog().list_db_schema();
    let deleted_db = all_databases
        .iter()
        .find(|db| db.deleted && db.name.starts_with(db_name))
        .expect("deleted database should exist");

    // Verify the database is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted after default duration)
    assert!(deleted_db.deleted, "database should be marked as deleted");
    assert_eq!(
        deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
        start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
    );

    shutdown.cancel();
}

#[tokio::test]
async fn delete_database_with_explicit_hard_delete_timestamp() {
    let start_time = 0;
    let (server, shutdown, write_buffer) = setup_server(start_time).await;

    let db_name = "test_db";

    // Write some data to create the database
    write_lp(
        &server,
        db_name,
        "cpu,host=a val=1i 123",
        None,
        false,
        "nanosecond",
    )
    .await;

    let future_timestamp = "2125-12-31T23:59:59Z";
    let expected_time = Time::from_datetime(
        DateTime::parse_from_rfc3339(future_timestamp)
            .unwrap()
            .to_utc(),
    );

    // Make a DELETE request to delete the database with explicit hard_delete_at timestamp
    let client = Client::builder(TokioExecutor::new()).build_http();
    let url = format!(
        "{server}/api/v3/configure/database?db={db_name}&hard_delete_at={future_timestamp}"
    );

    let request = RequestBuilder::new()
        .uri(url)
        .method("DELETE")
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
    let all_databases = write_buffer.catalog().list_db_schema();
    let deleted_db = all_databases
        .iter()
        .find(|db| db.deleted && db.name.starts_with(db_name))
        .expect("deleted database should exist");

    // Verify the database is marked as deleted and hard_delete_time matches the expected timestamp
    assert!(deleted_db.deleted, "database should be marked as deleted");
    assert_eq!(
        deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
        expected_time.timestamp_nanos(),
        "hard_delete_time should match the explicitly provided timestamp"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn query_from_last_cache() {
    let start_time = 0;
    let (url, shutdown, wbuf) = setup_server(start_time).await;
    let db_name = "foo";
    let tbl_name = "cpu";

    // Write to generate a db/table in the catalog:
    let resp = write_lp(
        &url,
        db_name,
        format!("{tbl_name},region=us,host=a usage=50 500"),
        None,
        false,
        "second",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Create the last cache:
    wbuf.catalog()
        .create_last_cache(
            db_name,
            tbl_name,
            None,
            None as Option<&[&str]>,
            None as Option<&[&str]>,
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();

    // Write to put something in the last cache:
    let resp = write_lp(
        &url,
        db_name,
        format!(
            "\
            {tbl_name},region=us,host=a usage=11 1000\n\
            {tbl_name},region=us,host=b usage=22 1000\n\
            {tbl_name},region=us,host=c usage=33 1000\n\
            {tbl_name},region=ca,host=d usage=44 1000\n\
            {tbl_name},region=ca,host=e usage=55 1000\n\
            {tbl_name},region=eu,host=f usage=66 1000\n\
            "
        ),
        None,
        false,
        "second",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    struct TestCase {
        query: &'static str,
        expected: &'static str,
    }

    let test_cases = [
        TestCase {
            query: "SELECT * FROM last_cache('cpu') ORDER BY host",
            expected: "\
                +--------+------+---------------------+-------+\n\
                | region | host | time                | usage |\n\
                +--------+------+---------------------+-------+\n\
                | us     | a    | 1970-01-01T00:16:40 | 11.0  |\n\
                | us     | b    | 1970-01-01T00:16:40 | 22.0  |\n\
                | us     | c    | 1970-01-01T00:16:40 | 33.0  |\n\
                | ca     | d    | 1970-01-01T00:16:40 | 44.0  |\n\
                | ca     | e    | 1970-01-01T00:16:40 | 55.0  |\n\
                | eu     | f    | 1970-01-01T00:16:40 | 66.0  |\n\
                +--------+------+---------------------+-------+",
        },
        TestCase {
            query: "SELECT * FROM last_cache('cpu') WHERE region = 'us' ORDER BY host",
            expected: "\
                +--------+------+---------------------+-------+\n\
                | region | host | time                | usage |\n\
                +--------+------+---------------------+-------+\n\
                | us     | a    | 1970-01-01T00:16:40 | 11.0  |\n\
                | us     | b    | 1970-01-01T00:16:40 | 22.0  |\n\
                | us     | c    | 1970-01-01T00:16:40 | 33.0  |\n\
                +--------+------+---------------------+-------+",
        },
        TestCase {
            query: "SELECT * FROM last_cache('cpu') WHERE region != 'us' ORDER BY host",
            expected: "\
                +--------+------+---------------------+-------+\n\
                | region | host | time                | usage |\n\
                +--------+------+---------------------+-------+\n\
                | ca     | d    | 1970-01-01T00:16:40 | 44.0  |\n\
                | ca     | e    | 1970-01-01T00:16:40 | 55.0  |\n\
                | eu     | f    | 1970-01-01T00:16:40 | 66.0  |\n\
                +--------+------+---------------------+-------+",
        },
        TestCase {
            query: "SELECT * FROM last_cache('cpu') WHERE host IN ('a', 'b') ORDER BY host",
            expected: "\
                +--------+------+---------------------+-------+\n\
                | region | host | time                | usage |\n\
                +--------+------+---------------------+-------+\n\
                | us     | a    | 1970-01-01T00:16:40 | 11.0  |\n\
                | us     | b    | 1970-01-01T00:16:40 | 22.0  |\n\
                +--------+------+---------------------+-------+",
        },
        TestCase {
            query: "SELECT * FROM last_cache('cpu') WHERE host NOT IN ('a', 'b') ORDER BY host",
            expected: "\
                +--------+------+---------------------+-------+\n\
                | region | host | time                | usage |\n\
                +--------+------+---------------------+-------+\n\
                | us     | c    | 1970-01-01T00:16:40 | 33.0  |\n\
                | ca     | d    | 1970-01-01T00:16:40 | 44.0  |\n\
                | ca     | e    | 1970-01-01T00:16:40 | 55.0  |\n\
                | eu     | f    | 1970-01-01T00:16:40 | 66.0  |\n\
                +--------+------+---------------------+-------+",
        },
    ];

    for t in test_cases {
        let res = query(&url, db_name, t.query, "pretty", None).await;
        let body = read_body_bytes_for_tests(res.into_body()).await;
        let body = String::from_utf8(body.as_bytes().to_vec()).unwrap();
        assert_eq!(t.expected, body, "query failed: {}", t.query);
    }
    // Query from the last cache:

    shutdown.cancel();
}

async fn setup_server(start_time: i64) -> (String, CancellationToken, Arc<dyn WriteBuffer>) {
    let server_start_time = tokio::time::Instant::now();
    let trace_header_parser = trace_http::ctx::TraceHeaderParser::new();
    let metrics = Arc::new(metric::Registry::new());
    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(start_time)));
    let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
        object_store,
        Arc::clone(&time_provider) as _,
        Default::default(),
    );
    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
    let exec = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            mem_pool_size: usize::MAX,
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new_testing(),
    ));
    let node_identifier_prefix = "test_host";
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        node_identifier_prefix,
        Arc::clone(&time_provider) as _,
        None,
    ));
    let sample_node_id = Arc::from("sample-host-id");
    let catalog = Arc::new(
        Catalog::new(
            sample_node_id,
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let frontend_shutdown = CancellationToken::new();
    let shutdown_manager = ShutdownManager::new(frontend_shutdown.clone());
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();

    // Start background catalog update for last cache in tests
    {
        use influxdb3_cache::last_cache::background_catalog_update;
        let subscription = catalog.subscribe_to_updates("last_cache_test").await;
        background_catalog_update(Arc::clone(&last_cache), subscription);
    }

    let distinct_cache = DistinctCacheProvider::new_from_catalog(
        Arc::clone(&time_provider) as _,
        Arc::clone(&catalog),
    )
    .await
    .unwrap();

    // Start background catalog update for distinct cache in tests
    {
        use influxdb3_cache::distinct_cache::background_catalog_update;
        let subscription = catalog.subscribe_to_updates("distinct_cache_test").await;
        background_catalog_update(Arc::clone(&distinct_cache), subscription);
    }

    let write_buffer_impl = influxdb3_write::write_buffer::WriteBufferImpl::new(
        influxdb3_write::write_buffer::WriteBufferImplArgs {
            persister: Arc::clone(&persister),
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider) as _,
            executor: Arc::clone(&exec),
            wal_config: WalConfig::test_config(),
            parquet_cache: Some(parquet_cache),
            metric_registry: Arc::clone(&metrics),
            snapshotted_wal_files_to_keep: 100,
            query_file_limit: None,
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            shutdown: shutdown_manager.register("test"),
            wal_replay_concurrency_limit: 1,
            parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
        },
    )
    .await
    .unwrap();

    let sys_events_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider) as _));
    let parquet_metrics_provider: Arc<PersistedFiles> =
        Arc::clone(&write_buffer_impl.persisted_files());
    let processing_engine_metrics_provider: Arc<Catalog> = Arc::clone(&write_buffer_impl.catalog());

    let sample_telem_store = TelemetryStore::new_without_background_runners(
        Some(parquet_metrics_provider),
        processing_engine_metrics_provider,
    );
    let write_buffer: Arc<dyn WriteBuffer> = write_buffer_impl;
    let common_state = crate::CommonServerState::new(
        Arc::clone(&catalog),
        Arc::clone(&metrics),
        None,
        trace_header_parser,
        Arc::clone(&sample_telem_store),
    );
    let query_executor = Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
        catalog: write_buffer.catalog(),
        write_buffer: Arc::clone(&write_buffer),
        exec: Arc::clone(&exec),
        metrics: Arc::clone(&metrics),
        datafusion_config: Default::default(),
        query_log_size: 10,
        telemetry_store: Arc::clone(&sample_telem_store),
        sys_events_store: Arc::clone(&sys_events_store),
        started_with_auth: false,
        time_provider: Arc::clone(&time_provider) as _,
        max_concurrent_queries: cli_types::QUERY_CONCURRENCY_LIMIT_MAX,
    }));

    // bind to port 0 will assign a random available port:
    let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let listener = TcpListener::bind(socket_addr)
        .await
        .expect("bind tcp address");
    let addr = listener.local_addr().unwrap();

    let processing_engine = ProcessingEngineManagerImpl::new(
        ProcessingEngineEnvironmentManager {
            plugin_dir: None,
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
            plugin_repo: None,
        },
        write_buffer.catalog(),
        node_identifier_prefix,
        Arc::clone(&write_buffer) as Arc<dyn influxdb3_write::Bufferer>,
        Arc::clone(&query_executor) as _,
        Arc::clone(&time_provider) as _,
        sys_events_store,
    )
    .await
    .unwrap();

    // We declare this as a static so that the lifetimes workout here and that
    // it lives long enough.
    // Note: TLS12 is not available without the tls12 feature in rustls 0.22
    // Using TLS13 only, which is more secure and widely supported
    static TLS_MIN_VERSION: &[&tokio_rustls::rustls::SupportedProtocolVersion] =
        &[&tokio_rustls::rustls::version::TLS13];

    // Start processing engine triggers
    Arc::clone(&processing_engine)
        .start_triggers()
        .await
        .expect("failed to start processing engine triggers");

    write_buffer
        .wal()
        .add_file_notifier(Arc::clone(&processing_engine) as _);

    let authorizer = Arc::new(NoAuthAuthenticator);
    let http = Arc::new(HttpApi::new(
        common_state.clone(),
        Arc::clone(&time_provider) as _,
        Arc::clone(&write_buffer),
        Arc::clone(&query_executor) as _,
        Arc::clone(&processing_engine),
        usize::MAX,
        Arc::clone(&authorizer) as _,
    ));

    let server = Server::new(CreateServerArgs {
        common_state,
        http,
        authorizer: authorizer as _,
        listener,
        cert_file: None,
        key_file: None,
        tls_minimum_version: TLS_MIN_VERSION,
    });
    let shutdown = frontend_shutdown.clone();
    let paths = EMPTY_PATHS.get_or_init(std::vec::Vec::new);
    tokio::spawn(async move {
        serve(
            server,
            frontend_shutdown,
            server_start_time,
            false,
            paths,
            None,
        )
        .await
    });

    (format!("http://{addr}"), shutdown, write_buffer)
}

pub(crate) async fn write_lp(
    server: impl Into<String> + Send,
    database: impl Into<String> + Send,
    lp: impl Into<String> + Send,
    authorization: Option<&str>,
    accept_partial: bool,
    precision: impl Into<String> + Send,
) -> Response {
    write_lp_raw(
        server,
        database,
        bytes_to_request_body(lp.into()),
        authorization,
        accept_partial,
        precision,
        None,
    )
    .await
}

pub(crate) async fn write_lp_raw(
    server: impl Into<String> + Send,
    database: impl Into<String> + Send,
    payload: RequestBody,
    authorization: Option<&str>,
    accept_partial: bool,
    precision: impl Into<String> + Send,
    headers: Option<hyper::HeaderMap>,
) -> Response {
    let server = server.into();
    let client = Client::builder(TokioExecutor::new()).build_http();
    let database = urlencoding::encode(database.into().as_ref());
    let url = format!(
        "{}/api/v3/write_lp?db={}&accept_partial={accept_partial}&precision={}",
        server,
        database,
        precision.into(),
    );
    println!("{url}");

    let mut builder = RequestBuilder::new().uri(url).method("POST");
    if let Some(authorization) = authorization {
        builder = builder.header(hyper::header::AUTHORIZATION, authorization);
    };
    if let Some(headers) = headers {
        for (key, value) in &headers {
            builder = builder.header(key, value);
        }
    }
    let request = builder
        .body(payload)
        .expect("failed to construct HTTP request");

    let response = client
        .request(request)
        .await
        .expect("http error sending write");

    // Convert Response<Incoming> to Response<UnsyncBoxBody>
    response.map(|body| body.map_err(|e| Box::new(e) as _).boxed_unsync())
}

pub(crate) async fn query(
    server: impl Into<String> + Send,
    database: impl Into<String> + Send,
    query: impl Into<String> + Send,
    format: impl Into<String> + Send,
    authorization: Option<&str>,
) -> Response {
    let client = Client::builder(TokioExecutor::new()).build_http();
    // query escaped for uri
    let query = urlencoding::encode(&query.into());
    let url = format!(
        "{}/api/v3/query_sql?db={}&q={}&format={}",
        server.into(),
        database.into(),
        query,
        format.into()
    );

    println!("query url: {url}");
    let mut builder = RequestBuilder::new().uri(url).method("GET");
    if let Some(authorization) = authorization {
        builder = builder.header(hyper::header::AUTHORIZATION, authorization);
    };
    let request = builder
        .body(empty_request_body())
        .expect("failed to construct HTTP request");

    let response = client
        .request(request)
        .await
        .expect("http error sending query");

    // Convert Response<Incoming> to Response<UnsyncBoxBody>
    response.map(|body| body.map_err(|e| Box::new(e) as _).boxed_unsync())
}
