//! Client helpers for writing end to end ng tests
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use http::Response;
use hyper::{Body, Client, Request};
use influxdb_iox_client::connection::Connection;

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

/// Runs a SQL query using the flight API on the specified connection.
pub async fn try_run_sql(
    sql_query: impl Into<String>,
    namespace: impl Into<String>,
    querier_connection: Connection,
) -> Result<Vec<RecordBatch>, influxdb_iox_client::flight::Error> {
    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);

    // Test the client handshake implementation
    // Normally this would be done one per connection, not per query
    client.handshake().await?;

    client
        .sql(namespace.into(), sql_query.into())
        .await?
        .try_collect()
        .await
}

/// Runs a InfluxQL query using the flight API on the specified connection.
pub async fn try_run_influxql(
    influxql_query: impl Into<String>,
    namespace: impl Into<String>,
    querier_connection: Connection,
) -> Result<Vec<RecordBatch>, influxdb_iox_client::flight::Error> {
    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);

    // Test the client handshake implementation
    // Normally this would be done one per connection, not per query
    client.handshake().await?;

    client
        .influxql(namespace.into(), influxql_query.into())
        .await?
        .try_collect()
        .await
}

/// Runs a SQL query using the flight API on the specified connection.
///
/// Use [`try_run_sql`] if you want to check the error manually.
pub async fn run_sql(
    sql: impl Into<String>,
    namespace: impl Into<String>,
    querier_connection: Connection,
) -> Vec<RecordBatch> {
    try_run_sql(sql, namespace, querier_connection)
        .await
        .expect("Error executing sql query")
}

/// Runs an InfluxQL query using the flight API on the specified connection.
///
/// Use [`try_run_influxql`] if you want to check the error manually.
pub async fn run_influxql(
    influxql: impl Into<String>,
    namespace: impl Into<String>,
    querier_connection: Connection,
) -> Vec<RecordBatch> {
    try_run_influxql(influxql, namespace, querier_connection)
        .await
        .expect("Error executing influxql query")
}
