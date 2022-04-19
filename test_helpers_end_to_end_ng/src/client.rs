//! Client helpers for writing end to end ng tests
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use http::Response;
use hyper::{Body, Client, Request};

use influxdb_iox_client::connection::Connection;
use influxdb_iox_client::flight::generated_types::ReadInfo;
use influxdb_iox_client::write_info::generated_types::{
    GetWriteInfoResponse, KafkaPartitionStatus,
};

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

/// returns the write info from ingester_connection for this token
pub async fn token_info(
    write_token: impl AsRef<str>,
    ingester_connection: Connection,
) -> Result<GetWriteInfoResponse, influxdb_iox_client::error::Error> {
    influxdb_iox_client::write_info::Client::new(ingester_connection)
        .get_write_info(write_token.as_ref())
        .await
}

/// returns true if the write for this token is persisted, false if it
/// is not persisted. panic's on error
pub async fn token_is_persisted(
    write_token: impl AsRef<str>,
    ingester_connection: Connection,
) -> bool {
    let res = token_info(write_token, ingester_connection)
        .await
        .expect("Error fetching write info for token");
    all_persisted(&res)
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
    assert!(!write_token.is_empty());

    println!("  write token: {}", write_token);

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
    println!("Waiting for write token to be readable");

    wait_for_token(write_token, ingester_connection, |res| {
        if all_readable(res) {
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
    println!("Waiting for write token to be persisted");

    wait_for_token(write_token, ingester_connection, |res| {
        if all_persisted(res) {
            println!("Write is persisted: {:?}", res);
            true
        } else {
            false
        }
    })
    .await
}

/// returns true if all partitions in the response are readablel
/// TODO: maybe put this in the influxdb_iox_client library / make a
/// proper public facing client API. For now, iterate in the end to end tests.
pub fn all_readable(res: &GetWriteInfoResponse) -> bool {
    res.kafka_partition_infos.iter().all(|info| {
        matches!(
            info.status(),
            KafkaPartitionStatus::Readable | KafkaPartitionStatus::Persisted
        )
    })
}

/// returns true if all partitions in the response are readablel
/// TODO: maybe put this in the influxdb_iox_client library / make a
/// proper public facing client API. For now, iterate in the end to end tests.
pub fn all_persisted(res: &GetWriteInfoResponse) -> bool {
    res.kafka_partition_infos
        .iter()
        .all(|info| matches!(info.status(), KafkaPartitionStatus::Persisted))
}

/// Runs a query using the flight API on the specified connection
pub async fn run_query(
    sql: impl Into<String>,
    namespace: impl Into<String>,
    querier_connection: Connection,
) -> Vec<RecordBatch> {
    let namespace = namespace.into();
    let sql = sql.into();

    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);

    // This does nothing except test the client handshake implementation.
    client.handshake().await.unwrap();

    let mut response = client
        .perform_query(ReadInfo {
            namespace_name: namespace,
            sql_query: sql,
        })
        .await
        .expect("Error performing query");

    response.collect().await.expect("Error executing query")
}
