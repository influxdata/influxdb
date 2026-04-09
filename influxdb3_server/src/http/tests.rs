use http::{HeaderMap, HeaderValue, header::ACCEPT};
use http::{Request, Uri};

use super::{
    MAXIMUM_DATABASE_NAME_LENGTH, extract_client_ip, extract_db_from_query_param,
    truncate_for_logging,
};
use crate::http::{AuthenticationError, Error};

use super::QueryFormat;
use super::ValidateDbNameError;
use super::record_batch_stream_to_body;
use super::token_part_as_bytes;
use super::validate_db_name;
use arrow_array::{Int32Array, RecordBatch, record_batch};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use iox_http_util::read_body_bytes_for_tests;
use pretty_assertions::assert_eq;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str;
use std::sync::Arc;

macro_rules! assert_validate_db_name {
    ($name:expr, $expected:pat) => {
        let actual = validate_db_name($name);
        assert!(matches!(&actual, $expected), "got: {actual:?}",);
    };
}

#[test]
fn test_try_from_headers_default_browser_accept_headers_to_json() {
    let mut map = HeaderMap::new();
    map.append(
        ACCEPT,
        HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    );
    let format = QueryFormat::try_from_headers(&map).unwrap();
    assert!(matches!(format, QueryFormat::Json));
}

#[test]
fn test_validate_db_name() {
    assert!(validate_db_name("foo/bar").is_ok());
    assert!(validate_db_name("foo-bar").is_ok());
    assert!(validate_db_name("foo_bar").is_ok());
    assert!(validate_db_name("f").is_ok());
    assert!(validate_db_name("1").is_ok());
    assert!(validate_db_name("1/2").is_ok());
    assert!(validate_db_name("1/-").is_ok());
    assert!(validate_db_name("1/_").is_ok());
    assert!(validate_db_name(&"1".repeat(MAXIMUM_DATABASE_NAME_LENGTH)).is_ok());
    assert_validate_db_name!(
        &"1".repeat(MAXIMUM_DATABASE_NAME_LENGTH + 1),
        Err(ValidateDbNameError::NameTooLong)
    );
    assert_validate_db_name!(
        "foo/bar/baz",
        Err(ValidateDbNameError::InvalidRetentionPolicy)
    );
    assert_validate_db_name!("foo/", Err(ValidateDbNameError::InvalidRetentionPolicy));
    assert_validate_db_name!("foo///", Err(ValidateDbNameError::InvalidRetentionPolicy));
    assert_validate_db_name!("foo?bar", Err(ValidateDbNameError::InvalidChar));
    assert_validate_db_name!("foo#bar", Err(ValidateDbNameError::InvalidChar));
    assert_validate_db_name!("foo@bar/baz", Err(ValidateDbNameError::InvalidChar));
    assert_validate_db_name!("_foo", Err(ValidateDbNameError::InvalidStartChar));
    assert_validate_db_name!("-foo", Err(ValidateDbNameError::InvalidStartChar));
    assert_validate_db_name!("/", Err(ValidateDbNameError::InvalidStartChar));
    assert_validate_db_name!("", Err(ValidateDbNameError::Empty));
}

#[tokio::test]
async fn test_json_output_empty() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(None), QueryFormat::Json)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "[]");
}

#[tokio::test]
async fn test_json_output_one_record() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(1)), QueryFormat::Json)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "[{\"a\":1}]");
}

#[tokio::test]
async fn test_json_output_all_empties() {
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(
            make_record_stream_with_sizes(vec![0, 0, 0]),
            QueryFormat::Json,
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "[]");
}

#[tokio::test]
async fn test_empty_present_mixture() {
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(
            make_record_stream_with_sizes(vec![0, 0, 1, 1, 0, 1, 0]),
            QueryFormat::Json,
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(
        str::from_utf8(bytes.as_ref()).unwrap(),
        "[{\"a\":1},{\"a\":1},{\"a\":1}]"
    );
}
#[tokio::test]
async fn test_json_output_three_records() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(3)), QueryFormat::Json)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        str::from_utf8(bytes.as_ref()).unwrap(),
        "[{\"a\":1},{\"a\":1},{\"a\":1}]"
    );
}
#[tokio::test]
async fn test_json_output_five_records() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(5)), QueryFormat::Json)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        str::from_utf8(bytes.as_ref()).unwrap(),
        "[{\"a\":1},{\"a\":1},{\"a\":1},{\"a\":1},{\"a\":1}]"
    );
}

#[tokio::test]
async fn test_jsonl_output_empty() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(None), QueryFormat::JsonLines)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "");
}

#[tokio::test]
async fn test_jsonl_output_one_record() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(1)), QueryFormat::JsonLines)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "{\"a\":1}\n");
}
#[tokio::test]
async fn test_jsonl_output_three_records() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(3)), QueryFormat::JsonLines)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        str::from_utf8(bytes.as_ref()).unwrap(),
        "{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n"
    );
}
#[tokio::test]
async fn test_jsonl_output_five_records() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(5)), QueryFormat::JsonLines)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        str::from_utf8(bytes.as_ref()).unwrap(),
        "{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n"
    );
}
#[tokio::test]
async fn test_csv_output_empty() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(None), QueryFormat::Csv)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "");
}

#[tokio::test]
async fn test_csv_output_one_record() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(1)), QueryFormat::Csv)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "a\n1\n");
}
#[tokio::test]
async fn test_csv_output_three_records() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(3)), QueryFormat::Csv)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "a\n1\n1\n1\n");
}
#[tokio::test]
async fn test_csv_output_five_records() {
    // Turn RecordBatches into a Body and then collect into Bytes to assert
    // their validity
    let bytes = read_body_bytes_for_tests(
        record_batch_stream_to_body(make_record_stream(Some(5)), QueryFormat::Csv)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        str::from_utf8(bytes.as_ref()).unwrap(),
        "a\n1\n1\n1\n1\n1\n"
    );
}

#[test]
fn test_basic_auth_token_valid() {
    let token_bytes =
        token_part_as_bytes(
            "PHVzZXJuYW1lPjphcGl2M19Ka2Fsdi1JUEtxSlIyUDdRVDBuMjhnRnBmMWlFd0stZVo3cTZoWHF5enJKdTBrRVBCLVZFODhlR1hUVHo5R0tod0ttMzgtNnFreWtLUGRoTmVkdVM5Zw==")
        .expect("base64 encoded string to be valid");
    let token_string = String::from_utf8_lossy(&token_bytes);
    assert_eq!(
        token_string,
        "apiv3_Jkalv-IPKqJR2P7QT0n28gFpf1iEwK-eZ7q6hXqyzrJu0kEPB-VE88eGXTTz9GKhwKm38-6qkykKPdhNeduS9g"
    );
}

#[test]
fn test_basic_auth_token_invalid() {
    let invalid_token = token_part_as_bytes(
        "YXBpdjNfSmthbHYtSVBLcUpSMlA3UVQwbjI4Z0ZwZjFpRXdLLWVaN3E2aFhxeXpySnUwa0VQQi1WRTg4ZUdYVFR6OUdLaHdLbTM4LTZxa3lrS1BkaE5lZHVTOWc=",
    );
    assert!(matches!(
        invalid_token,
        Err(AuthenticationError::MalformedRequest)
    ));
}

#[test]
fn test_basic_auth_token_should_not_allow_colon_in_username() {
    //  echo -n "foo:bar:$TOKEN" | base64 -w 0
    let invalid_token = token_part_as_bytes(
        "Zm9vOmJhcjphcGl2M19Ka2Fsdi1JUEtxSlIyUDdRVDBuMjhnRnBmMWlFd0stZVo3cTZoWHF5enJKdTBrRVBCLVZFODhlR1hUVHo5R0tod0ttMzgtNnFreWtLUGRoTmVkdVM5Zw==",
    );
    assert!(matches!(
        invalid_token,
        Err(AuthenticationError::MalformedRequest)
    ));
}

fn make_record_stream(records: Option<usize>) -> SendableRecordBatchStream {
    match records {
        None => make_record_stream_with_sizes(vec![]),
        Some(num) => make_record_stream_with_sizes(vec![1; num]),
    }
}

fn make_record_stream_with_sizes(batch_sizes: Vec<usize>) -> SendableRecordBatchStream {
    let batch = record_batch!(("a", Int32, [1])).unwrap();
    let schema = batch.schema();

    // If there are no sizes, return empty stream
    if batch_sizes.is_empty() {
        let stream = futures::stream::iter(Vec::new());
        let adapter = RecordBatchStreamAdapter::new(schema, stream);
        return Box::pin(adapter);
    }

    let batches = batch_sizes
        .into_iter()
        .map(|size| {
            if size == 0 {
                // Create an empty batch
                Ok(RecordBatch::new_empty(Arc::clone(&schema)))
            } else {
                // Create a batch with 'size' rows, all with value 1
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from_iter_values(vec![1; size]))],
                )?)
            }
        })
        .collect::<Vec<_>>();

    let stream = futures::stream::iter(batches);
    let adapter = RecordBatchStreamAdapter::new(schema, stream);
    Box::pin(adapter)
}

#[test]
fn test_client_ip_extraction_from_socket_address() {
    use hyper::Request;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    // Create a request with socket address in extensions
    let mut req = Request::builder()
        .uri("http://example.com/api/v3/write_lp?db=test")
        .body(())
        .unwrap();

    let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 8080);
    req.extensions_mut().insert(Some(socket_addr));

    // Extract client IP - should get socket address since no headers
    let client_ip = extract_client_ip(&req);

    assert_eq!(client_ip, Some("192.168.1.100".to_string()));
}

#[test]
fn test_client_ip_extraction_prefers_headers_over_socket() {
    use hyper::Request;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    // Create a request with both headers and socket address
    let mut req = Request::builder()
        .uri("http://example.com/api/v3/write_lp?db=test")
        .header("x-forwarded-for", "10.0.0.1")
        .body(())
        .unwrap();

    let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 8080);
    req.extensions_mut().insert(Some(socket_addr));

    // Extract client IP - should prefer header over socket
    let client_ip = extract_client_ip(&req);

    assert_eq!(client_ip, Some("10.0.0.1".to_string()));
}

#[test]
fn test_extract_db_from_query_param() {
    // Test with valid db parameter
    let uri = Uri::try_from("http://example.com/api/v3/write?db=mydb").unwrap();
    assert_eq!(extract_db_from_query_param(&uri), Some("mydb".to_string()));

    // Test with db parameter among other parameters
    let uri = Uri::try_from("http://example.com/api/v3/write?foo=bar&db=testdb&baz=qux").unwrap();
    assert_eq!(
        extract_db_from_query_param(&uri),
        Some("testdb".to_string())
    );

    // Test with empty db parameter
    let uri = Uri::try_from("http://example.com/api/v3/write?db=").unwrap();
    assert_eq!(extract_db_from_query_param(&uri), Some("".to_string()));

    // Test without db parameter
    let uri = Uri::try_from("http://example.com/api/v3/write?foo=bar").unwrap();
    assert_eq!(extract_db_from_query_param(&uri), None);

    // Test with no query parameters
    let uri = Uri::try_from("http://example.com/api/v3/write").unwrap();
    assert_eq!(extract_db_from_query_param(&uri), None);

    // Test with URL encoded db name
    let uri = Uri::try_from("http://example.com/api/v3/write?db=my%20database").unwrap();
    assert_eq!(
        extract_db_from_query_param(&uri),
        Some("my database".to_string())
    );

    // Test with special characters in db name
    let uri = Uri::try_from("http://example.com/api/v3/write?db=db-name_123").unwrap();
    assert_eq!(
        extract_db_from_query_param(&uri),
        Some("db-name_123".to_string())
    );

    // Test with multiple db parameters (should return first one)
    let uri = Uri::try_from("http://example.com/api/v3/write?db=first&db=second").unwrap();
    assert_eq!(extract_db_from_query_param(&uri), Some("first".to_string()));
}

#[test]
fn test_extract_client_ip() {
    // Test with x-forwarded-for header (single IP)
    let req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .header("x-forwarded-for", "192.168.1.100")
        .body("")
        .unwrap();
    assert_eq!(extract_client_ip(&req), Some("192.168.1.100".to_string()));

    // Test with x-forwarded-for header (multiple IPs, should take first)
    let req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .header("x-forwarded-for", "10.0.0.1, 172.16.0.1, 192.168.1.1")
        .body("")
        .unwrap();
    assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

    // Test with x-forwarded-for header with spaces
    let req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .header("x-forwarded-for", "  10.0.0.1  ,  172.16.0.1  ")
        .body("")
        .unwrap();
    assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

    // Test with x-real-ip header
    let req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .header("x-real-ip", "192.168.1.50")
        .body("")
        .unwrap();
    assert_eq!(extract_client_ip(&req), Some("192.168.1.50".to_string()));

    // Test with both headers (x-forwarded-for takes precedence)
    let req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .header("x-forwarded-for", "10.0.0.1")
        .header("x-real-ip", "192.168.1.50")
        .body("")
        .unwrap();
    assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

    // Test with socket address in extensions (IPv4)
    let mut req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .body("")
        .unwrap();
    let socket_addr = Some(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    ));
    req.extensions_mut().insert(socket_addr);
    assert_eq!(extract_client_ip(&req), Some("127.0.0.1".to_string()));

    // Test with socket address in extensions (IPv6)
    let mut req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .body("")
        .unwrap();
    let socket_addr = Some(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
        8080,
    ));
    req.extensions_mut().insert(socket_addr);
    assert_eq!(extract_client_ip(&req), Some("::1".to_string()));

    // Test with None socket address in extensions
    let mut req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .body("")
        .unwrap();
    let socket_addr: Option<SocketAddr> = None;
    req.extensions_mut().insert(socket_addr);
    assert_eq!(extract_client_ip(&req), Some("unknown".to_string()));

    // Test with no headers and no socket address
    let req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .body("")
        .unwrap();
    assert_eq!(extract_client_ip(&req), None);

    // Test header precedence: x-forwarded-for > x-real-ip > socket
    let mut req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .header("x-forwarded-for", "10.0.0.1")
        .header("x-real-ip", "192.168.1.50")
        .body("")
        .unwrap();
    let socket_addr = Some(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    ));
    req.extensions_mut().insert(socket_addr);
    assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

    // Test with empty x-forwarded-for header
    let req = Request::builder()
        .uri("http://example.com/api/v3/write")
        .header("x-forwarded-for", "")
        .body("")
        .unwrap();
    // Should return empty string since the header exists but is empty
    assert_eq!(extract_client_ip(&req), Some("".to_string()));
}

#[test]
fn test_truncate_for_logging_utf8() {
    // "中国" is 6 bytes (each Chinese character is 3 bytes in UTF-8)
    let s = "中国";
    assert_eq!(s.len(), 6);

    // max_len = 1 falls in the middle of first character, should return empty string
    assert_eq!(truncate_for_logging(s, 1), "");
}

#[tokio::test]
async fn test_datafusion_plan_error_maps_to_bad_request() {
    let err = Error::Query(super::QueryExecutorError::QueryPlanning(
        DataFusionError::Plan("bad plan".into()),
    ));
    let response = super::IntoResponse::into_response(err);

    assert!(response.status().is_client_error());

    let body = read_body_bytes_for_tests(response.into_body()).await;
    assert_eq!(
        str::from_utf8(body.as_ref()).unwrap(),
        "Error during planning: bad plan"
    );
}

#[tokio::test]
async fn test_influxql_rewrite_error_maps_to_client_error() {
    let rewrite_err = super::rewrite::parse_statements("show tags")
        .expect_err("invalid InfluxQL should fail to parse");
    let err = Error::InfluxqlRewrite(rewrite_err);
    let response = super::IntoResponse::into_response(err);

    assert!(response.status().is_client_error());

    let body = read_body_bytes_for_tests(response.into_body()).await;
    assert_eq!(
        str::from_utf8(body.as_ref()).unwrap(),
        "error in InfluxQL statement: parsing error: invalid SHOW statement, \
        expected DATABASES, FIELD KEYS, MEASUREMENTS, TAG KEYS, TAG VALUES, or \
        RETENTION POLICIES following SHOW at pos 5"
    );
}
