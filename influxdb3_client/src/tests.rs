use std::io::Write as _;

use influxdb3_types::http::{LastCacheSize, LastCacheTtl};
use mockito::{Matcher, Server};
use serde_json::json;

use crate::{Client, Precision, QueryFormat};

/// Produce the gzip encoding the client is expected to put on the wire for the
/// given input. Stays in sync with `WriteRequestBuilder::send` (default
/// compression level), so tests can byte-match the request body.
fn expected_gzip(input: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(input).unwrap();
    enc.finish().unwrap()
}

#[tokio::test]
async fn api_v3_write_lp() {
    let token = "super-secret-token";
    let db = "stats";
    let body = "\
        cpu,host=s1 usage=0.5
        cpu,host=s1,region=us-west usage=0.7";

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/write_lp")
        .match_header("Authorization", format!("Bearer {token}").as_str())
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("precision".into(), "millisecond".into()),
            Matcher::UrlEncoded("db".into(), db.into()),
            Matcher::UrlEncoded("accept_partial".into(), "true".into()),
        ]))
        .match_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false)
        .expect("create client")
        .with_auth_token(token);

    client
        .api_v3_write_lp(db)
        .precision(Precision::Millisecond)
        .accept_partial(true)
        .body(body)
        .send()
        .await
        .expect("send write_lp request");

    mock.assert_async().await;
}

#[tokio::test]
async fn api_v3_write_lp_gzip_sends_compressed_bytes() {
    // Asserts the body on the wire is the actual gzip-encoded payload, not
    // raw LP with a misleading header. A no-op `with_gzip(true)` would fail
    // both the binary body match and the header match below.
    let db = "stats";
    let body = "cpu,host=s1 usage=0.5\ncpu,host=s1,region=us-west usage=0.7";

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/write_lp")
        .match_header("content-encoding", "gzip")
        .match_query(Matcher::UrlEncoded("db".into(), db.into()))
        .match_body(expected_gzip(body.as_bytes()))
        .with_status(200)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    client
        .api_v3_write_lp(db)
        .with_gzip(true)
        .body(body)
        .send()
        .await
        .expect("send gzipped write_lp request");

    mock.assert_async().await;
}

#[tokio::test]
async fn api_v3_write_lp_no_gzip_omits_header() {
    let db = "stats";
    let body = "cpu,host=s1 usage=0.5";

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/write_lp")
        .match_header("content-encoding", Matcher::Missing)
        .match_query(Matcher::UrlEncoded("db".into(), db.into()))
        .match_body(body)
        .with_status(200)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    client
        .api_v3_write_lp(db)
        .body(body)
        .send()
        .await
        .expect("send uncompressed write_lp request");

    mock.assert_async().await;
}

#[tokio::test]
async fn api_v3_query_sql() {
    let token = "super-secret-token";
    let db = "stats";
    let query = "SELECT * FROM foo";
    let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/query_sql")
        .match_header("Authorization", format!("Bearer {token}").as_str())
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "q": query,
            "format": "json",
            "params": null,
        })))
        .with_status(200)
        // TODO - could add content-type header but that may be too brittle
        //        at the moment
        //      - this will be JSON Lines at some point
        .with_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false)
        .expect("create client")
        .with_auth_token(token);

    let r = client
        .api_v3_query_sql(db, query)
        .format(QueryFormat::Json)
        .send()
        .await
        .expect("send request to server");

    assert_eq!(&r, body);

    mock.assert_async().await;
}

#[tokio::test]
async fn api_v3_query_sql_params() {
    let db = "stats";
    let query = "SELECT * FROM foo WHERE bar = $bar";
    let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/query_sql")
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "q": query,
            "params": {
                "bar": "baz",
                "baz": false,
            },
            "format": null
        })))
        .with_status(200)
        .with_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    let r = client
        .api_v3_query_sql(db, query)
        .with_param("bar", "baz")
        .with_param("baz", false)
        .send()
        .await;

    mock.assert_async().await;

    r.expect("sent request successfully");
}

#[tokio::test]
async fn api_v3_query_influxql() {
    let db = "stats";
    let query = "SELECT * FROM foo";
    let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/query_influxql")
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "q": query,
            "format": "json",
            "params": null,
        })))
        .with_status(200)
        .with_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    let r = client
        .api_v3_query_influxql(db, query)
        .format(QueryFormat::Json)
        .send()
        .await
        .expect("send request to server");

    assert_eq!(&r, body);

    mock.assert_async().await;
}
#[tokio::test]
async fn api_v3_query_influxql_params() {
    let db = "stats";
    let query = "SELECT * FROM foo WHERE a = $a AND b < $b AND c > $c AND d = $d";
    let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/query_influxql")
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "q": query,
            "params": {
                "a": "bar",
                "b": 123,
                "c": 1.5,
                "d": false
            },
            "format": null
        })))
        .with_status(200)
        .with_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    let mut builder = client.api_v3_query_influxql(db, query);

    for (name, value) in [
        ("a", json!("bar")),
        ("b", json!(123)),
        ("c", json!(1.5)),
        ("d", json!(false)),
    ] {
        builder = builder.with_try_param(name, value).unwrap();
    }
    let r = builder.send().await;

    mock.assert_async().await;

    r.expect("sent request successfully");
}
#[tokio::test]
async fn api_v3_query_influxql_with_params_from() {
    let db = "stats";
    let query = "SELECT * FROM foo WHERE a = $a AND b < $b AND c > $c AND d = $d";
    let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/query_influxql")
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "q": query,
            "params": {
                "a": "bar",
                "b": 123,
                "c": 1.5,
                "d": false
            },
            "format": null
        })))
        .with_status(200)
        .with_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    let r = client
        .api_v3_query_influxql(db, query)
        .with_params_from([
            ("a", json!("bar")),
            ("b", json!(123)),
            ("c", json!(1.5)),
            ("d", json!(false)),
        ])
        .unwrap()
        .send()
        .await;

    mock.assert_async().await;

    r.expect("sent request successfully");
}

#[tokio::test]
async fn v1_query_influxql_with_format() {
    let db = "stats";
    let query = "SELECT * FROM foo";
    let body = "time,val\n1990-07-23T06:00:00Z,1\n";

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("GET", "/query")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("db".into(), db.into()),
            Matcher::UrlEncoded("q".into(), query.into()),
            Matcher::UrlEncoded("format".into(), "csv".into()),
        ]))
        .with_status(200)
        .with_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    let response = client
        .v1_query_influxql(db, query)
        .format(QueryFormat::Csv)
        .send()
        .await
        .expect("send request to server");

    assert_eq!(&response, body);
    mock.assert_async().await;
}

#[tokio::test]
async fn v1_query_influxql_with_params() {
    let db = "stats";
    let query = "SELECT * FROM foo WHERE host = $host";
    let body = "time,val\n1990-07-23T06:00:00Z,1\n";

    // params are sent as a URL-encoded JSON object string in the `params` field.
    // Use a single key to avoid HashMap ordering nondeterminism.
    let expected_params = serde_json::json!({ "host": "s1" }).to_string();

    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("GET", "/query")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("db".into(), db.into()),
            Matcher::UrlEncoded("q".into(), query.into()),
            Matcher::UrlEncoded("params".into(), expected_params),
        ]))
        .with_status(200)
        .with_body(body)
        .create_async()
        .await;

    let client = Client::new(mock_server.url(), None, false).expect("create client");

    let response = client
        .v1_query_influxql(db, query)
        .with_param("host", "s1")
        .send()
        .await
        .expect("send request to server");

    assert_eq!(&response, body);
    mock.assert_async().await;
}

// NOTE(trevor): these tests are flaky since we need to fabricate the mock response, considering
// removing them in favour of integration tests that use the actual APIs
#[tokio::test]
#[ignore]
async fn api_v3_configure_last_cache_create_201() {
    let db = "db";
    let table = "table";
    let name = "cache_name";
    let key_columns = ["col1", "col2"];
    let val_columns = vec!["col3", "col4"];
    let ttl = LastCacheTtl::from_secs(120);
    let count = LastCacheSize::new(5).unwrap();
    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/configure/last_cache")
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "table": table,
            "name": name,
            "key_columns": key_columns,
            "value_columns": val_columns,
            "count": count,
            "ttl": ttl,
        })))
        .with_status(201)
        .with_body(
            r#"{
                "table": "table",
                "name": "cache_name",
                "key_columns": [0, 1],
                "value_columns": {
                    "explicit": {
                        "columns": [2, 3]
                    }
                },
                "ttl": 120,
                "count": 5
            }"#,
        )
        .create_async()
        .await;
    let client = Client::new(mock_server.url(), None, false).unwrap();
    client
        .api_v3_configure_last_cache_create(db, table)
        .name(name)
        .key_columns(key_columns)
        .value_columns(val_columns)
        .ttl(ttl)
        .count(count)
        .send()
        .await
        .expect("creates last cache and parses response");
    mock.assert_async().await;
}

#[tokio::test]
async fn api_v3_configure_last_cache_create_204() {
    let db = "db";
    let table = "table";
    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("POST", "/api/v3/configure/last_cache")
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "table": table,
            "ttl": 14400,
            "count": 1,
        })))
        .with_status(204)
        .create_async()
        .await;
    let client = Client::new(mock_server.url(), None, false).unwrap();
    let resp = client
        .api_v3_configure_last_cache_create(db, table)
        .send()
        .await
        .unwrap();
    mock.assert_async().await;
    assert!(resp.is_none());
}

#[tokio::test]
async fn api_v3_configure_last_cache_delete() {
    let db = "db";
    let table = "table";
    let name = "cache_name";
    let mut mock_server = Server::new_async().await;
    let mock = mock_server
        .mock("DELETE", "/api/v3/configure/last_cache")
        .match_body(Matcher::Json(serde_json::json!({
            "db": db,
            "table": table,
            "name": name,
        })))
        .with_status(200)
        .create_async()
        .await;
    let client = Client::new(mock_server.url(), None, false).unwrap();
    client
        .api_v3_configure_last_cache_delete(db, table, name)
        .await
        .unwrap();
    mock.assert_async().await;
}
