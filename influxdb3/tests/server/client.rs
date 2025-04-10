//! End-to-end tests for the influxdb3_client
//!
//! This is useful for verifying that the client can parse API responses from the server

use influxdb3_client::Precision;
use influxdb3_types::http::QueryFormat as Format;

use crate::server::TestServer;

#[tokio::test]
async fn write_and_query() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let tbl_name = "bar";
    let client = influxdb3_client::Client::new(
        server.client_addr(),
        Some("../testing-certs/rootCA.pem".into()),
    )
    .unwrap();
    client
        .api_v3_write_lp(db_name)
        .precision(Precision::Nanosecond)
        .accept_partial(false)
        .body(format!("{tbl_name},t1=a,t2=aa f1=123"))
        .send()
        .await
        .expect("make write_lp request");
    client
        .api_v3_query_sql(db_name, format!("SELECT * FROM {tbl_name}"))
        .format(Format::Json)
        .send()
        .await
        .expect("query SQL for JSON response");
    client
        .api_v3_query_influxql(db_name, format!("SELECT * FROM {tbl_name}"))
        .format(Format::Csv)
        .send()
        .await
        .expect("query InfluxQL for CSV response");
}

#[tokio::test]
async fn configure_last_caches() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let tbl_name = "bar";
    let client = influxdb3_client::Client::new(
        server.client_addr(),
        Some("../testing-certs/rootCA.pem".into()),
    )
    .unwrap();
    client
        .api_v3_write_lp(db_name)
        .precision(Precision::Nanosecond)
        .accept_partial(false)
        .body(format!("{tbl_name},t1=a,t2=aa f1=123"))
        .send()
        .await
        .expect("make write_lp request");
    let Some(batch) = client
        .api_v3_configure_last_cache_create(db_name, tbl_name)
        .send()
        .await
        .expect("send create last cache with defaults")
    else {
        panic!("should have created the cache");
    };
    let name = batch
        .into_batch()
        .to_database()
        .and_then(|mut db| db.ops.pop())
        .and_then(|op| op.to_create_last_cache())
        .map(|r| r.name.to_string())
        .unwrap();
    client
        .api_v3_configure_last_cache_delete(db_name, tbl_name, name)
        .await
        .expect("deletes the cache");
    let Some(batch) = client
        .api_v3_configure_last_cache_create(db_name, tbl_name)
        .value_columns(["f1"])
        .send()
        .await
        .expect("send create last cache with explicit value columns")
    else {
        panic!("should have created the cache");
    };
    let name = batch
        .into_batch()
        .to_database()
        .and_then(|mut db| db.ops.pop())
        .and_then(|op| op.to_create_last_cache())
        .map(|r| r.name.to_string())
        .unwrap();
    client
        .api_v3_configure_last_cache_delete(db_name, tbl_name, name)
        .await
        .expect("should delete the cache");
}
