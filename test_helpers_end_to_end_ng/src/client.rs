//! Client helpers for writing end to end ng tests
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use http::Response;
use hyper::{Body, Client, Request};

use influxdb_iox_client::connection::Connection;
use influxdb_iox_client::write_info::generated_types::GetWriteInfoResponse;

/// Writes the line protocol to the write_base/api/v2/write endpoint (typically on the router)
pub async fn write_to_router(
    line_protocol: impl Into<String>,
    org: impl AsRef<str>,
    bucket: impl AsRef<str>,
    write_base: impl AsRef<str>,
) -> Response<Body> {
    let client = Client::new();
    let url = format!(
        "{}/api/v2/write?org={}&bucket={}",
        write_base.as_ref(),
        org.as_ref(),
        bucket.as_ref()
    );

    let request = Request::builder()
        .uri(url)
        .method("POST")
        .body(Body::from(line_protocol.into()))
        .expect("failed to construct HTTP request");

    client
        .request(request)
        .await
        .expect("http error sending write")
}

/// Extracts the write token from the specified response (to the /api/v2/write api)
pub fn get_write_token(response: &Response<Body>) -> String {
    let message = format!("no write token in {:?}", response);
    response
        .headers()
        .get("X-IOx-Write-Token")
        .expect(&message)
        .to_str()
        .expect("Value not a string")
        .to_string()
}

const MAX_QUERY_RETRY_TIME_SEC: u64 = 10;

/// Waits for the specified predicate to return true
pub async fn wait_for_token<F>(
    write_token: impl Into<String>,
    ingester_connection: Connection,
    f: F,
) where
    F: Fn(&GetWriteInfoResponse) -> bool,
{
    let write_token = write_token.into();

    println!("Waiting for Write Token {}", write_token);

    let retry_duration = Duration::from_secs(MAX_QUERY_RETRY_TIME_SEC);
    let mut write_info_client = influxdb_iox_client::write_info::Client::new(ingester_connection);
    tokio::time::timeout(retry_duration, async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            match write_info_client.get_write_info(&write_token).await {
                Ok(res) => {
                    if f(&res) {
                        return;
                    }
                    println!("Retrying; predicate not satistified: {:?}", res);
                }

                Err(e) => {
                    println!("Retrying; Got error getting write_info: {}", e);
                }
            };
            interval.tick().await;
        }
    })
    .await
    .expect("did not get passing predicate on token");
}

/// Waits for the specified write token to be readable
pub async fn wait_for_readable(write_token: impl Into<String>, ingester_connection: Connection) {
    println!("Waiting for Write Token to be readable");

    wait_for_token(write_token, ingester_connection, |res| {
        if res.readable {
            println!("Write is readable: {:?}", res);
            true
        } else {
            false
        }
    })
    .await
}

/// Waits for the write token to be persisted
pub async fn wait_for_persisted(write_token: impl Into<String>, ingester_connection: Connection) {
    println!("Waiting for Write Token to be persisted");

    wait_for_token(write_token, ingester_connection, |res| {
        if res.persisted {
            println!("Write is persisted: {:?}", res);
            true
        } else {
            false
        }
    })
    .await
}

/// Runs a query using the flight API on the specified connection
/// until responses are produced.
///
/// (Will) eventually wait until data from the specified write token
/// is readable, but currently waits for the data to be persisted (as
/// the querier doesn't know how to ask the ingester yet)
///
/// The retry loop is used to wait for writes to become visible
pub async fn query_when_readable(
    sql: impl Into<String>,
    namespace: impl Into<String>,
    write_token: impl Into<String>,
    ingester_connection: Connection,
    querier_connection: Connection,
) -> Vec<RecordBatch> {
    let namespace = namespace.into();
    let sql = sql.into();

    // TODO: this should be "wait_for_readable" once the querier can talk to ingester
    wait_for_persisted(write_token, ingester_connection).await;

    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);

    // This does nothing except test the client handshake implementation.
    client.handshake().await.unwrap();

    let mut response = client
        .perform_query(&namespace, &sql)
        .await
        .expect("Error performing query");

    response.collect().await.expect("Error executing query")
}
