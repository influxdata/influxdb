//! This module contains a partial implementation of the /v2 HTTP api
//! routes for InfluxDB IOx.
//!
//! Note that these routes are designed to be just helpers for now,
//! and "close enough" to the real /v2 api to be able to test InfluxDB IOx
//! without needing to create and manage a mapping layer from name -->
//! id (this is done by other services in the influx cloud)
//!
//! Long term, we expect to create IOx specific api in terms of
//! database names and may remove this quasi /v2 API.

// Influx crates
use arrow_deps::{arrow, datafusion::physical_plan::collect};
use data_types::{
    database_rules::DatabaseRules,
    names::{org_and_bucket_to_database, OrgBucketMappingError},
    DatabaseName,
};
use influxdb_line_protocol::parse_lines;
use object_store::ObjectStoreApi;
use query::{frontend::sql::SQLQueryPlanner, Database, DatabaseStore};
use server::{ConnectionManager, Server as AppServer};

// External crates
use bytes::{Bytes, BytesMut};
use futures::{self, StreamExt};
use http::header::CONTENT_ENCODING;
use hyper::{Body, Method, Request, Response, StatusCode};
use routerify::{prelude::*, Middleware, RequestInfo, Router, RouterError, RouterService};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{debug, error, info};

use std::{fmt::Debug, str, sync::Arc};

#[derive(Debug, Snafu)]
pub enum ApplicationError {
    // Internal (unexpected) errors
    #[snafu(display(
        "Internal error accessing org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    BucketByName {
        org: String,
        bucket_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Internal error mapping org & bucket: {}", source))]
    BucketMappingError { source: OrgBucketMappingError },

    #[snafu(display(
        "Internal error writing points into org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    WritingPoints {
        org: String,
        bucket_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error planning query {}: {}", query, source))]
    PlanningSQLQuery {
        query: String,
        source: query::frontend::sql::Error,
    },

    #[snafu(display("Internal error reading points from database {}:  {}", db_name, source))]
    Query {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // Application level errors
    #[snafu(display("Bucket {} not found in org {}", bucket, org))]
    BucketNotFound { org: String, bucket: String },

    #[snafu(display("Body exceeds limit of {} bytes", max_body_size))]
    RequestSizeExceeded { max_body_size: usize },

    #[snafu(display("Expected query string in request, but none was provided"))]
    ExpectedQueryString {},

    #[snafu(display("Invalid query string '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("Query error: {}", source))]
    QueryError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid request body: {}", source))]
    InvalidRequestBody { source: serde_json::error::Error },

    #[snafu(display("Invalid response body: {}", source))]
    InternalSerializationError { source: serde_json::error::Error },

    #[snafu(display("Invalid content encoding: {}", content_encoding))]
    InvalidContentEncoding { content_encoding: String },

    #[snafu(display("Error reading request header '{}' as Utf8: {}", header_name, source))]
    ReadingHeaderAsUtf8 {
        header_name: String,
        source: hyper::header::ToStrError,
    },

    #[snafu(display("Error reading request body: {}", source))]
    ReadingBody { source: hyper::Error },

    #[snafu(display("Error reading request body as utf8: {}", source))]
    ReadingBodyAsUtf8 { source: std::str::Utf8Error },

    #[snafu(display("Error parsing line protocol: {}", source))]
    ParsingLineProtocol {
        source: influxdb_line_protocol::Error,
    },

    #[snafu(display("Error decompressing body as gzip: {}", source))]
    ReadingBodyAsGzip { source: std::io::Error },

    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },

    #[snafu(display("Internal error from database {}: {}", database, source))]
    DatabaseError {
        database: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error generating json response: {}", source))]
    JsonGenerationError { source: serde_json::Error },

    #[snafu(display("Error creating database: {}", source))]
    ErrorCreatingDatabase { source: server::Error },

    #[snafu(display("Invalid database name: {}", source))]
    DatabaseNameError {
        source: data_types::DatabaseNameError,
    },

    #[snafu(display("Database {} not found", name))]
    DatabaseNotFound { name: String },
}

impl ApplicationError {
    pub fn response(&self) -> Response<Body> {
        match self {
            Self::BucketByName { .. } => self.internal_error(),
            Self::BucketMappingError { .. } => self.internal_error(),
            Self::WritingPoints { .. } => self.internal_error(),
            Self::PlanningSQLQuery { .. } => self.bad_request(),
            Self::Query { .. } => self.internal_error(),
            Self::QueryError { .. } => self.bad_request(),
            Self::BucketNotFound { .. } => self.not_found(),
            Self::RequestSizeExceeded { .. } => self.bad_request(),
            Self::ExpectedQueryString { .. } => self.bad_request(),
            Self::InvalidQueryString { .. } => self.bad_request(),
            Self::InvalidRequestBody { .. } => self.bad_request(),
            Self::InternalSerializationError { .. } => self.internal_error(),
            Self::InvalidContentEncoding { .. } => self.bad_request(),
            Self::ReadingHeaderAsUtf8 { .. } => self.bad_request(),
            Self::ReadingBody { .. } => self.bad_request(),
            Self::ReadingBodyAsUtf8 { .. } => self.bad_request(),
            Self::ParsingLineProtocol { .. } => self.bad_request(),
            Self::ReadingBodyAsGzip { .. } => self.bad_request(),
            Self::RouteNotFound { .. } => self.not_found(),
            Self::DatabaseError { .. } => self.internal_error(),
            Self::JsonGenerationError { .. } => self.internal_error(),
            Self::ErrorCreatingDatabase { .. } => self.bad_request(),
            Self::DatabaseNameError { .. } => self.bad_request(),
            Self::DatabaseNotFound { .. } => self.not_found(),
        }
    }

    fn bad_request(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(self.body())
            .unwrap()
    }

    fn internal_error(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(self.body())
            .unwrap()
    }

    fn not_found(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    }

    fn body(&self) -> Body {
        let json =
            serde_json::json!({"error": self.to_string(), "error_code": self.api_error_code()})
                .to_string();
        Body::from(json)
    }

    /// Map the error type into an API error code.
    fn api_error_code(&self) -> u32 {
        use influxdb_iox_client::errors::ApiErrorCode;

        match self {
            Self::DatabaseNameError { .. } => ApiErrorCode::DB_INVALID_NAME,
            Self::DatabaseNotFound { .. } => ApiErrorCode::DB_NOT_FOUND,

            // Some errors are wrapped
            Self::ErrorCreatingDatabase {
                source: server::Error::InvalidDatabaseName { .. },
            } => ApiErrorCode::DB_INVALID_NAME,

            Self::ErrorCreatingDatabase {
                source: server::Error::DatabaseNotFound { .. },
            } => ApiErrorCode::DB_NOT_FOUND,

            Self::ErrorCreatingDatabase {
                source: server::Error::DatabaseAlreadyExists { .. },
            } => ApiErrorCode::DB_ALREADY_EXISTS,

            // A "catch all" error code
            _ => ApiErrorCode::UNKNOWN,
        }
        .into()
    }
}

const MAX_SIZE: usize = 10_485_760; // max write request size of 10MB

fn router<M>(server: Arc<AppServer<M>>) -> Router<Body, ApplicationError>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    // Create a router and specify the the handlers.
    Router::builder()
        .data(server)
        .middleware(Middleware::pre(|req| async move {
            info!(request = ?req, "Processing request");
            Ok(req)
        }))
        .middleware(Middleware::post(|res| async move {
            info!(response = ?res, "Successfully processed request");
            Ok(res)
        })) // this endpoint is for API backward compatibility with InfluxDB 2.x
        .post("/api/v2/write", write::<M>)
        .get("/ping", ping)
        .get("/api/v2/read", read::<M>)
        .get("/iox/api/v1/databases", list_databases::<M>)
        .put("/iox/api/v1/databases/:name", create_database::<M>)
        .get("/iox/api/v1/databases/:name", get_database::<M>)
        .put("/iox/api/v1/id", set_writer::<M>)
        .get("/api/v1/partitions", list_partitions::<M>)
        .post("/api/v1/snapshot", snapshot_partition::<M>)
        // Specify the error handler to handle any errors caused by
        // a route or any middleware.
        .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}

// The API-global error handler, handles ApplicationErrors originating from
// individual routes and middlewares, along with errors from the router itself
async fn error_handler(err: RouterError<ApplicationError>, req: RequestInfo) -> Response<Body> {
    match err {
        RouterError::HandleRequest(e, _)
        | RouterError::HandlePreMiddlewareRequest(e)
        | RouterError::HandlePostMiddlewareWithInfoRequest(e)
        | RouterError::HandlePostMiddlewareWithoutInfoRequest(e) => {
            error!(error = ?e, error_message = ?e.to_string(), "Error while handling request");
            e.response()
        }
        _ => {
            let method = req.method().clone();
            let uri = req.uri().clone();
            error!(error = ?err, error_message = ?err.to_string(), method = ?method, uri = ?uri, "Error while handling request");

            let json = serde_json::json!({"error": err.to_string()}).to_string();
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(json))
                .unwrap()
        }
    }
}

#[derive(Debug, Deserialize)]
/// Body of the request to the /write endpoint
struct WriteInfo {
    org: String,
    bucket: String,
}

/// Parse the request's body into raw bytes, applying size limits and
/// content encoding as needed.
async fn parse_body(req: hyper::Request<Body>) -> Result<Bytes, ApplicationError> {
    // clippy says the const needs to be assigned to a local variable:
    // error: a `const` item with interior mutability should not be borrowed
    let header_name = CONTENT_ENCODING;
    let ungzip = match req.headers().get(&header_name) {
        None => false,
        Some(content_encoding) => {
            let content_encoding = content_encoding.to_str().context(ReadingHeaderAsUtf8 {
                header_name: header_name.as_str(),
            })?;
            match content_encoding {
                "gzip" => true,
                _ => InvalidContentEncoding { content_encoding }.fail()?,
            }
        }
    };

    let mut payload = req.into_body();

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.expect("Should have been able to read the next chunk");
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(ApplicationError::RequestSizeExceeded {
                max_body_size: MAX_SIZE,
            });
        }
        body.extend_from_slice(&chunk);
    }
    let body = body.freeze();

    // apply any content encoding needed
    if ungzip {
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(&body[..]);

        // Read at most MAX_SIZE bytes to prevent a decompression bomb based
        // DoS.
        let mut decoder = decoder.take(MAX_SIZE as u64);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .context(ReadingBodyAsGzip)?;
        Ok(decoded_data.into())
    } else {
        Ok(body)
    }
}

#[tracing::instrument(level = "debug")]
async fn write<M>(req: Request<Body>) -> Result<Response<Body>, ApplicationError>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();

    let query = req.uri().query().context(ExpectedQueryString)?;

    let write_info: WriteInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: String::from(query),
    })?;

    let db_name = org_and_bucket_to_database(&write_info.org, &write_info.bucket)
        .context(BucketMappingError)?;

    let body = parse_body(req).await?;

    let body = str::from_utf8(&body).context(ReadingBodyAsUtf8)?;

    let lines = parse_lines(body)
        .collect::<Result<Vec<_>, influxdb_line_protocol::Error>>()
        .context(ParsingLineProtocol)?;

    debug!(
        "Inserting {} lines into database {} (org {} bucket {})",
        lines.len(),
        db_name,
        write_info.org,
        write_info.bucket
    );

    server
        .write_lines(&db_name, &lines)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(WritingPoints {
            org: write_info.org.clone(),
            bucket_name: write_info.bucket.clone(),
        })?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

#[derive(Deserialize, Debug)]
/// Body of the request to the /read endpoint
struct ReadInfo {
    org: String,
    bucket: String,
    // TODO This is currently a "SQL" request -- should be updated to conform
    // to the V2 API for reading (using timestamps, etc).
    sql_query: String,
}

// TODO: figure out how to stream read results out rather than rendering the
// whole thing in mem
#[tracing::instrument(level = "debug")]
async fn read<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let read_info: ReadInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let planner = SQLQueryPlanner::default();
    let executor = server.executor();

    let db_name = org_and_bucket_to_database(&read_info.org, &read_info.bucket)
        .context(BucketMappingError)?;

    let db = server.db(&db_name).await.context(BucketNotFound {
        org: read_info.org.clone(),
        bucket: read_info.bucket.clone(),
    })?;

    let physical_plan = planner
        .query(db.as_ref(), &read_info.sql_query, executor.as_ref())
        .await
        .context(PlanningSQLQuery { query })?;

    let batches = collect(physical_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(Query { db_name })?;

    let results = arrow::util::pretty::pretty_format_batches(&batches).unwrap();

    Ok(Response::new(Body::from(results.into_bytes())))
}

#[derive(Serialize, Deserialize, Debug)]
/// Body of the response to the /databases endpoint.
struct ListDatabasesResponse {
    names: Vec<String>,
}

#[tracing::instrument(level = "debug")]
async fn list_databases<M>(req: Request<Body>) -> Result<Response<Body>, ApplicationError>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();

    let names = server.db_names_sorted().await;
    let json = serde_json::to_string(&ListDatabasesResponse { names })
        .context(InternalSerializationError)?;
    Ok(Response::new(Body::from(json)))
}

#[tracing::instrument(level = "debug")]
async fn create_database<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();

    // with routerify, we shouldn't have gotten here without this being set
    let db_name = req
        .param("name")
        .expect("db name must have been set")
        .clone();
    let body = parse_body(req).await?;

    let rules: DatabaseRules = serde_json::from_slice(body.as_ref()).context(InvalidRequestBody)?;

    server
        .create_database(db_name, rules)
        .await
        .context(ErrorCreatingDatabase)?;

    Ok(Response::new(Body::empty()))
}

#[tracing::instrument(level = "debug")]
async fn get_database<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();

    // with routerify, we shouldn't have gotten here without this being set
    let db_name_str = req
        .param("name")
        .expect("db name must have been set")
        .clone();
    let db_name = DatabaseName::new(&db_name_str).context(DatabaseNameError)?;
    let db = server
        .db_rules(&db_name)
        .await
        .context(DatabaseNotFound { name: &db_name_str })?;

    let data = serde_json::to_string(&db).context(JsonGenerationError)?;
    let response = Response::builder()
        .header("Content-Type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(data))
        .expect("builder should be successful");

    Ok(response)
}

#[tracing::instrument(level = "debug")]
async fn set_writer<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();

    // Read the request body
    let body = parse_body(req).await?;

    // Parse the JSON body into a structure
    #[derive(Serialize, Deserialize)]
    struct WriterIdBody {
        id: u32,
    };
    let req: WriterIdBody = serde_json::from_slice(body.as_ref()).context(InvalidRequestBody)?;

    // Set the writer ID
    server.set_id(req.id);

    // Build a HTTP 200 response
    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(
            serde_json::to_string(&req).expect("json encoding should not fail"),
        ))
        .expect("builder should be successful");

    Ok(response)
}

// Route to test that the server is alive
#[tracing::instrument(level = "debug")]
async fn ping(req: Request<Body>) -> Result<Response<Body>, ApplicationError> {
    let response_body = "PONG";
    Ok(Response::new(Body::from(response_body.to_string())))
}

#[derive(Deserialize, Debug)]
/// Arguments in the query string of the request to /partitions
struct DatabaseInfo {
    org: String,
    bucket: String,
}

#[tracing::instrument(level = "debug")]
async fn list_partitions<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let info: DatabaseInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let db_name =
        org_and_bucket_to_database(&info.org, &info.bucket).context(BucketMappingError)?;

    let db = server.db(&db_name).await.context(BucketNotFound {
        org: &info.org,
        bucket: &info.bucket,
    })?;

    let partition_keys = db
        .partition_keys()
        .await
        .map_err(|e| Box::new(e) as _)
        .context(BucketByName {
            org: &info.org,
            bucket_name: &info.bucket,
        })?;

    let result = serde_json::to_string(&partition_keys).context(JsonGenerationError)?;

    Ok(Response::new(Body::from(result)))
}

#[derive(Deserialize, Debug)]
/// Arguments in the query string of the request to /snapshot
struct SnapshotInfo {
    org: String,
    bucket: String,
    partition: String,
}

#[tracing::instrument(level = "debug")]
async fn snapshot_partition<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    use object_store::path::ObjectStorePath;

    let server = req
        .data::<Arc<AppServer<M>>>()
        .expect("server state")
        .clone();
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let snapshot: SnapshotInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let db_name =
        org_and_bucket_to_database(&snapshot.org, &snapshot.bucket).context(BucketMappingError)?;

    // TODO: refactor the rest of this out of the http route and into the server
    // crate.
    let db = server.db(&db_name).await.context(BucketNotFound {
        org: &snapshot.org,
        bucket: &snapshot.bucket,
    })?;

    let store = server.store.clone();

    let mut metadata_path = store.new_path();
    metadata_path.push_dir(&db_name.to_string());
    let mut data_path = metadata_path.clone();
    metadata_path.push_dir("meta");
    data_path.push_all_dirs(&["data", &snapshot.partition]);

    let partition_key = &snapshot.partition;
    let chunk = db.rollover_partition(partition_key).await.unwrap();
    let snapshot = server::snapshot::snapshot_chunk(
        metadata_path,
        data_path,
        store,
        partition_key,
        chunk,
        None,
    )
    .unwrap();

    let ret = format!("{}", snapshot.id);
    Ok(Response::new(Body::from(ret)))
}

pub fn router_service<M: ConnectionManager + Send + Sync + Debug + 'static>(
    server: Arc<AppServer<M>>,
) -> RouterService<Body, ApplicationError> {
    let router = router(server);
    RouterService::new(router).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use arrow_deps::{arrow::record_batch::RecordBatch, assert_table_eq};
    use http::header;
    use query::exec::Executor;
    use reqwest::{Client, Response};

    use hyper::Server;

    use data_types::database_rules::DatabaseRules;
    use data_types::DatabaseName;
    use object_store::{memory::InMemory, ObjectStore};
    use server::{db::Db, ConnectionManagerImpl};

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn test_ping() -> Result<()> {
        let test_storage = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        let server_url = test_server(test_storage.clone());

        let client = Client::new();
        let response = client.get(&format!("{}/ping", server_url)).send().await;

        // Print the response so if the test fails, we have a log of what went wrong
        check_response("ping", response, StatusCode::OK, "PONG").await;
        Ok(())
    }

    #[tokio::test]
    async fn test_write() -> Result<()> {
        let test_storage = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        test_storage.set_id(1);
        let rules = DatabaseRules {
            store_locally: true,
            ..Default::default()
        };
        test_storage
            .create_database("MyOrg_MyBucket", rules)
            .await
            .unwrap();
        let server_url = test_server(test_storage.clone());

        let client = Client::new();

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, "").await;

        // Check that the data got into the right bucket
        let test_db = test_storage
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .await
            .expect("Database exists");

        let batches = run_query(test_db.as_ref(), "select * from h2o_temperature").await;
        let expected = vec![
            "+----------------+--------------+-------+-----------------+------------+",
            "| bottom_degrees | location     | state | surface_degrees | time       |",
            "+----------------+--------------+-------+-----------------+------------+",
            "| 50.4           | santa_monica | CA    | 65.2            | 1568756160 |",
            "+----------------+--------------+-------+-----------------+------------+",
        ];
        assert_table_eq!(expected, &batches);

        Ok(())
    }

    fn gzip_str(s: &str) -> Vec<u8> {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        write!(encoder, "{}", s).expect("writing into encoder");
        encoder.finish().expect("successfully encoding gzip data")
    }

    #[tokio::test]
    async fn test_gzip_write() -> Result<()> {
        let test_storage = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        test_storage.set_id(1);
        let rules = DatabaseRules {
            store_locally: true,
            ..Default::default()
        };
        test_storage
            .create_database("MyOrg_MyBucket", rules)
            .await
            .unwrap();
        let server_url = test_server(test_storage.clone());

        let client = Client::new();
        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160";

        // send write data encoded with gzip
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .header(header::CONTENT_ENCODING, "gzip")
            .body(gzip_str(lp_data))
            .send()
            .await;

        check_response("gzip_write", response, StatusCode::NO_CONTENT, "").await;

        // Check that the data got into the right bucket
        let test_db = test_storage
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .await
            .expect("Database exists");

        let batches = run_query(test_db.as_ref(), "select * from h2o_temperature").await;

        let expected = vec![
            "+----------------+--------------+-------+-----------------+------------+",
            "| bottom_degrees | location     | state | surface_degrees | time       |",
            "+----------------+--------------+-------+-----------------+------------+",
            "| 50.4           | santa_monica | CA    | 65.2            | 1568756160 |",
            "+----------------+--------------+-------+-----------------+------------+",
        ];
        assert_table_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn set_writer_id() {
        let server = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        server.set_id(1);
        let server_url = test_server(server.clone());

        let data = r#"{"id":42}"#;

        let client = Client::new();
        let response = client
            .put(&format!("{}/iox/api/v1/id", server_url))
            .body(data)
            .send()
            .await;

        check_response("set_writer_id", response, StatusCode::OK, data).await;

        assert_eq!(server.require_id().expect("should be set"), 42);
    }

    #[tokio::test]
    async fn list_databases() {
        let server = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        server.set_id(1);
        let server_url = test_server(server.clone());

        let database_names: Vec<String> = vec!["foo_bar", "foo_baz"]
            .iter()
            .map(|i| i.to_string())
            .collect();

        for database_name in &database_names {
            let rules = DatabaseRules {
                name: database_name.clone(),
                store_locally: true,
                ..Default::default()
            };
            server.create_database(database_name, rules).await.unwrap();
        }

        let client = Client::new();
        let response = client
            .get(&format!("{}/iox/api/v1/databases", server_url))
            .send()
            .await;

        let data = serde_json::to_string(&ListDatabasesResponse {
            names: database_names,
        })
        .unwrap();
        check_response("list_databases", response, StatusCode::OK, &data).await;
    }

    #[tokio::test]
    async fn create_database() {
        let server = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        server.set_id(1);
        let server_url = test_server(server.clone());

        let data = r#"{"store_locally": true}"#;

        let database_name = DatabaseName::new("foo_bar").unwrap();

        let client = Client::new();
        let response = client
            .put(&format!(
                "{}/iox/api/v1/databases/{}",
                server_url, database_name
            ))
            .body(data)
            .send()
            .await;

        check_response("create_database", response, StatusCode::OK, "").await;

        server.db(&database_name).await.unwrap();
        let db_rules = server.db_rules(&database_name).await.unwrap();
        assert_eq!(db_rules.store_locally, true);
    }

    #[tokio::test]
    async fn get_database() {
        let server = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        server.set_id(1);
        let server_url = test_server(server.clone());

        let database_name = "foo_bar";
        let rules = DatabaseRules {
            name: database_name.to_owned(),
            store_locally: true,
            ..Default::default()
        };
        let data = serde_json::to_string(&rules).unwrap();

        server.create_database(database_name, rules).await.unwrap();

        let client = Client::new();
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/{}",
                server_url, database_name
            ))
            .send()
            .await;

        check_response("get_database", response, StatusCode::OK, &data).await;
    }

    /// checks a http response against expected results
    async fn check_response(
        description: &str,
        response: Result<Response, reqwest::Error>,
        expected_status: StatusCode,
        expected_body: &str,
    ) {
        // Print the response so if the test fails, we have a log of
        // what went wrong
        println!("{} response: {:?}", description, response);

        if let Ok(response) = response {
            let status = response.status();
            let body = response
                .text()
                .await
                .expect("Converting request body to string");

            assert_eq!(status, expected_status);
            assert_eq!(body, expected_body);
        } else {
            panic!("Unexpected error response: {:?}", response);
        }
    }

    /// creates an instance of the http service backed by a in-memory
    /// testable database.  Returns the url of the server
    fn test_server(server: Arc<AppServer<ConnectionManagerImpl>>) -> String {
        let make_svc = router_service(server);

        // NB: specify port 0 to let the OS pick the port.
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let server = Server::bind(&bind_addr).serve(make_svc);
        let server_url = format!("http://{}", server.local_addr());
        tokio::task::spawn(server);
        println!("Started server at {}", server_url);
        server_url
    }

    /// Run the specified SQL query and return formatted results as a string
    async fn run_query(db: &Db, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();
        let physical_plan = planner.query(db, query, &executor).await.unwrap();

        collect(physical_plan).await.unwrap()
    }
}
