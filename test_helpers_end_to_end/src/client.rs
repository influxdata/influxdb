//! Client helpers for writing end to end ng tests
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use data_types::{NamespaceId, TableId};
use dml::{DmlMeta, DmlWrite};
use futures::TryStreamExt;
use http::Response;
use hyper::{Body, Client, Request};
use influxdb_iox_client::{
    connection::Connection,
    ingester::generated_types::{write_service_client::WriteServiceClient, WriteRequest},
};
use iox_query_params::StatementParam;
use mutable_batch_lp::lines_to_batches;
use mutable_batch_pb::encode::encode_write;
use std::fmt::Display;
use tonic::IntoRequest;

/// Writes the line protocol to the write_base/api/v2/write endpoint (typically on the router)
pub async fn write_to_router(
    line_protocol: impl Into<String> + Send,
    org: impl AsRef<str> + Send,
    bucket: impl AsRef<str> + Send,
    write_base: impl AsRef<str> + Send,
    authorization: Option<&str>,
) -> Response<Body> {
    let client = Client::new();
    let url = format!(
        "{}/api/v2/write?org={}&bucket={}",
        write_base.as_ref(),
        org.as_ref(),
        bucket.as_ref()
    );

    let mut builder = Request::builder().uri(url).method("POST");
    if let Some(authorization) = authorization {
        builder = builder.header(hyper::header::AUTHORIZATION, authorization);
    };
    let request = builder
        .body(Body::from(line_protocol.into()))
        .expect("failed to construct HTTP request");

    client
        .request(request)
        .await
        .expect("http error sending write")
}

/// Writes the line protocol to the WriteService endpoint (typically on the ingester)
pub async fn write_to_ingester(
    line_protocol: impl Into<String> + Send,
    namespace_id: NamespaceId,
    table_id: TableId,
    ingester_connection: Connection,
) {
    let line_protocol = line_protocol.into();
    let writes = lines_to_batches(&line_protocol, 0).unwrap();
    let writes = writes
        .into_iter()
        .map(|(_name, data)| (table_id, data))
        .collect();

    let mut client = WriteServiceClient::new(ingester_connection.into_grpc_connection());

    let op = DmlWrite::new(
        namespace_id,
        writes,
        "1970-01-01".into(),
        DmlMeta::unsequenced(None),
    );

    client
        .write(
            tonic::Request::new(WriteRequest {
                payload: Some(encode_write(namespace_id.get(), &op)),
            })
            .into_request(),
        )
        .await
        .unwrap();
}

/// Runs a SQL query using the flight API on the specified connection.
pub async fn try_run_sql(
    sql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    try_run_sql_with_params(
        sql_query,
        namespace,
        [],
        querier_connection,
        authorization,
        with_debug,
    )
    .await
}

/// Runs a SQL query using the flight API on the specified connection.
pub async fn try_run_sql_with_params(
    sql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);
    if with_debug {
        client.add_header("iox-debug", "true").unwrap();
    }
    if let Some(authorization) = authorization {
        client.add_header("authorization", authorization).unwrap();
    }

    // Test the client handshake implementation
    // Normally this would be done one per connection, not per query
    client.handshake().await?;

    let mut stream = client
        .query(namespace)
        .sql(sql_query.into())
        .with_params(params)
        .run()
        .await?;

    let batches = (&mut stream).try_collect().await?;

    // read schema AFTER collection, otherwise the stream does not have the schema data yet
    let schema = stream
        .inner()
        .schema()
        .cloned()
        .ok_or(influxdb_iox_client::flight::Error::NoSchema)?;

    Ok((batches, schema))
}

/// Runs a InfluxQL query using the flight API on the specified connection.
pub async fn try_run_influxql(
    influxql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    try_run_influxql_with_params(
        influxql_query,
        namespace,
        [],
        querier_connection,
        authorization,
    )
    .await
}

pub async fn try_run_influxql_with_params(
    influxql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);
    if let Some(authorization) = authorization {
        client.add_header("authorization", authorization).unwrap();
    }

    // Test the client handshake implementation
    // Normally this would be done one per connection, not per query
    client.handshake().await?;

    let mut stream = client
        .query(namespace)
        .influxql(influxql_query.into())
        .with_params(params)
        .run()
        .await?;

    let batches = (&mut stream).try_collect().await?;

    // read schema AFTER collection, otherwise the stream does not have the schema data yet
    let schema = stream
        .inner()
        .schema()
        .cloned()
        .ok_or(influxdb_iox_client::flight::Error::NoSchema)?;

    Ok((batches, schema))
}

/// Runs a SQL query using the flight API on the specified connection.
///
/// Use [`try_run_sql`] if you want to check the error manually.
pub async fn run_sql(
    sql: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_sql(
        sql,
        namespace,
        querier_connection,
        authorization,
        with_debug,
    )
    .await
    .expect("Error executing sql query")
}

/// Runs a SQL query using the flight API on the specified connection.
///
/// Use [`try_run_sql`] if you want to check the error manually.
pub async fn run_sql_with_params(
    sql: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_sql_with_params(
        sql,
        namespace,
        params,
        querier_connection,
        authorization,
        with_debug,
    )
    .await
    .expect("Error executing sql query")
}

/// Runs an InfluxQL query using the flight API on the specified connection.
///
/// Use [`try_run_influxql`] if you want to check the error manually.
pub async fn run_influxql(
    influxql: impl Into<String> + Clone + Display + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_influxql(
        influxql.clone(),
        namespace,
        querier_connection,
        authorization,
    )
    .await
    .unwrap_or_else(|_| panic!("Error executing InfluxQL query: {influxql}"))
}

/// Runs an InfluxQL query using the flight API on the specified connection.
///
/// Use [`try_run_influxql`] if you want to check the error manually.
pub async fn run_influxql_with_params(
    influxql: impl Into<String> + Clone + Display + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_influxql_with_params(
        influxql.clone(),
        namespace,
        params,
        querier_connection,
        authorization,
    )
    .await
    .unwrap_or_else(|_| panic!("Error executing InfluxQL query: {influxql}"))
}
