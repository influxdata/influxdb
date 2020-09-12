//! This module contains a parallel implementation of the /v2 HTTP api
//! routes for Delorean.
//!
//! The goal is that eventually the implementation in these routes
//! will replace the implementation in http_routes.rs

#![deny(rust_2018_idioms)]

use http::header::CONTENT_ENCODING;
use tracing::{debug, error, info};

use delorean::storage::{org_and_bucket_to_database, Database, DatabaseStore};
use delorean_line_parser::parse_lines;

use bytes::{Bytes, BytesMut};
use futures::{self, StreamExt};
use hyper::{Body, Method, StatusCode};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use std::str;
use std::sync::Arc;

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

    #[snafu(display(
        "Internal error reading points from database {}:  {}",
        database,
        source
    ))]
    Query {
        database: String,
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

    #[snafu(display("Invalid request body '{}': {}", request_body, source))]
    InvalidRequestBody {
        request_body: String,
        source: serde_json::error::Error,
    },

    #[snafu(display("Invalid content encoding: {}", content_encoding))]
    InvalidContentEncoding { content_encoding: String },

    #[snafu(display("Error reading request header '{}' as Utf8: {}", header_name, source))]
    ReadingHeaderAsUtf8 {
        header_name: String,
        source: hyper::header::ToStrError,
    },

    #[snafu(display("Error reading request body: {}", source))]
    ReadingBody { source: hyper::error::Error },

    #[snafu(display("Error reading request body as utf8: {}", source))]
    ReadingBodyAsUtf8 { source: std::str::Utf8Error },

    #[snafu(display("Error parsing line protocol: {}", source))]
    ParsingLineProtocol { source: delorean_line_parser::Error },

    #[snafu(display("Error decompressing body as gzip: {}", source))]
    ReadingBodyAsGzip { source: std::io::Error },

    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },

    #[snafu(display("Internal error creating gzip decoder: {:?}", source))]
    CreatingGzipDecoder { source: std::io::Error },
}

impl ApplicationError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BucketByName { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::WritingPoints { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Query { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::QueryError { .. } => StatusCode::BAD_REQUEST,
            Self::BucketNotFound { .. } => StatusCode::NOT_FOUND,
            Self::RequestSizeExceeded { .. } => StatusCode::BAD_REQUEST,
            Self::ExpectedQueryString { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidQueryString { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidRequestBody { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidContentEncoding { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingHeaderAsUtf8 { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingBody { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingBodyAsUtf8 { .. } => StatusCode::BAD_REQUEST,
            Self::ParsingLineProtocol { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingBodyAsGzip { .. } => StatusCode::BAD_REQUEST,
            Self::RouteNotFound { .. } => StatusCode::NOT_FOUND,
            Self::CreatingGzipDecoder { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

const MAX_SIZE: usize = 10_485_760; // max write request size of 10MB

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
        use libflate::gzip::Decoder;
        use std::io::Read;
        let mut decoder = Decoder::new(&body[..]).context(CreatingGzipDecoder)?;
        // TODO cap the size of the decoded data (right
        // now this could decompress some crazy large
        // request)
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
async fn write<T: DatabaseStore>(
    req: hyper::Request<Body>,
    storage: Arc<T>,
) -> Result<Option<Body>, ApplicationError> {
    let query = req.uri().query().context(ExpectedQueryString)?;

    let write_info: WriteInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: String::from(query),
    })?;

    let db_name = org_and_bucket_to_database(&write_info.org, &write_info.bucket);

    let db = storage
        .db_or_create(&db_name)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(BucketByName {
            org: write_info.org.clone(),
            bucket_name: write_info.bucket.clone(),
        })?;

    let body = parse_body(req).await?;

    let body = str::from_utf8(&body).context(ReadingBodyAsUtf8)?;

    let lines = parse_lines(body)
        .collect::<Result<Vec<_>, delorean_line_parser::Error>>()
        .context(ParsingLineProtocol)?;

    debug!("Parsed {} lines", lines.len());

    db.write_lines(&lines)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(WritingPoints {
            org: write_info.org.clone(),
            bucket_name: write_info.bucket.clone(),
        })?;

    Ok(None)
}

#[derive(Deserialize, Debug)]
/// Body of the request to the /read endpoint
struct ReadInfo {
    org: String,
    bucket: String,
    // TODL This is currently a "SQL" request -- should be updated to conform
    // to the V2 API for reading (using timestamps, etc).
    query: String,
}

// TODO: figure out how to stream read results out rather than rendering the whole thing in mem
#[tracing::instrument(level = "debug")]
async fn read<T: DatabaseStore>(
    req: hyper::Request<Body>,
    storage: Arc<T>,
) -> Result<Option<Body>, ApplicationError> {
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let read_info: ReadInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let db_name = org_and_bucket_to_database(&read_info.org, &read_info.bucket);

    let db = storage.db(&db_name).await.context(BucketNotFound {
        org: read_info.org.clone(),
        bucket: read_info.bucket.clone(),
    })?;

    let results = db
        .query(&read_info.query)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(QueryError {})?;
    let results = arrow::util::pretty::pretty_format_batches(&results).unwrap();

    Ok(Some(results.into_bytes().into()))
}

// Route to test that the server is alive
#[tracing::instrument(level = "debug")]
async fn ping(req: hyper::Request<Body>) -> Result<Option<Body>, ApplicationError> {
    let response_body = "PONG";
    Ok(Some(response_body.into()))
}

fn no_op(name: &str) -> Result<Option<Body>, ApplicationError> {
    info!("NOOP: {}", name);
    Ok(None)
}

pub async fn service<T: DatabaseStore>(
    req: hyper::Request<Body>,
    storage: Arc<T>,
) -> http::Result<hyper::Response<Body>> {
    let method = req.method().clone();
    let uri = req.uri().clone();

    let response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/api/v2/write") => write(req, storage).await,
        (&Method::POST, "/api/v2/buckets") => no_op("create bucket"),
        (&Method::GET, "/ping") => ping(req).await,
        (&Method::GET, "/api/v2/read") => read(req, storage).await,
        _ => Err(ApplicationError::RouteNotFound {
            method: method.clone(),
            path: uri.to_string(),
        }),
    };

    let result = match response {
        Ok(Some(body)) => hyper::Response::builder()
            .body(body)
            .expect("Should have been able to construct a response"),
        Ok(None) => hyper::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("Should have been able to construct a response"),
        Err(e) => {
            error!(error = ?e, method = ?method, uri = ?uri, "Error while handing request");
            let json = serde_json::json!({"error": e.to_string()}).to_string();
            hyper::Response::builder()
                .status(e.status_code())
                .body(json.into())
                .expect("Should have been able to construct a response")
        }
    };
    info!(method = ?method, uri = ?uri, status = ?result.status(), "Handled request");
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, net::SocketAddr};

    use arrow::record_batch::RecordBatch;
    use delorean::storage::{Database, DatabaseStore};
    use delorean_line_parser::ParsedLine;
    use reqwest::{Client, Response};
    use tonic::async_trait;

    use hyper::service::{make_service_fn, service_fn};
    use hyper::Server;
    use tokio::sync::Mutex;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn test_write() -> Result<()> {
        let test_storage = Arc::new(TestDatabaseStore::new());
        let server_url = test_server(test_storage.clone());
        println!("Started server at {}", server_url);
        // now, make a http client and send some requests

        let client = Client::new();
        let response = client
            .request(Method::GET, &format!("{}/ping", server_url))
            .send()
            .await;

        // Print the response so if the test fails, we have a log of what went wrong
        check_response("ping", response, StatusCode::OK, "PONG").await;

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .request(
                Method::POST,
                &format!(
                    "{}/api/v2/write?bucket={}&org={}",
                    server_url, bucket_name, org_name
                ),
            )
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, "").await;

        // Check that the data got into the right bucket
        let test_db = test_storage
            .db("MyOrg_MyBucket")
            .await
            .expect("Database exists");

        // Ensure the same line protocol data gets through
        assert_eq!(test_db.get_lines().await, vec![lp_data]);
        Ok(())
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
    fn test_server(storage: Arc<TestDatabaseStore>) -> String {
        let make_svc = make_service_fn(move |_conn| {
            let storage = storage.clone();
            async move {
                Ok::<_, http::Error>(service_fn(move |req| {
                    let state = storage.clone();
                    super::service(req, state)
                }))
            }
        });

        // TODO pick the port dynamically and return it
        let bind_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
        let server = Server::bind(&bind_addr).serve(make_svc);
        let server_url = format!("http://{}", bind_addr);
        tokio::task::spawn(server);
        server_url
    }

    #[derive(Debug)]
    struct TestDatabase {
        // lines which have been written to this database, in order
        saved_lines: Mutex<Vec<String>>,
    }

    #[derive(Snafu, Debug)]
    enum TestError {}

    impl TestDatabase {
        fn new() -> Self {
            Self {
                saved_lines: Mutex::new(Vec::new()),
            }
        }

        /// Get all lines written to this database
        async fn get_lines(&self) -> Vec<String> {
            self.saved_lines.lock().await.clone()
        }
    }

    #[async_trait]
    impl Database for TestDatabase {
        type Error = TestError;

        /// writes parsed lines into this database
        async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error> {
            let mut saved_lines = self.saved_lines.lock().await;
            for line in lines {
                saved_lines.push(line.to_string())
            }
            Ok(())
        }

        /// Execute the specified query and return arrow record batches with the result
        async fn query(&self, _query: &str) -> Result<Vec<RecordBatch>, Self::Error> {
            unimplemented!("query Not yet implemented");
        }

        /// Fetch the specified table names and columns as Arrow RecordBatches
        async fn table_to_arrow(
            &self,
            _table_name: &str,
            _columns: &[&str],
        ) -> Result<Vec<RecordBatch>, Self::Error> {
            unimplemented!("table_to_arrow Not yet implemented");
        }
    }

    #[derive(Debug)]
    struct TestDatabaseStore {
        databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    }

    impl TestDatabaseStore {
        fn new() -> Self {
            Self {
                databases: Mutex::new(BTreeMap::new()),
            }
        }
    }

    #[async_trait]
    impl DatabaseStore for TestDatabaseStore {
        type Database = TestDatabase;
        type Error = TestError;
        /// Retrieve the database specified name
        async fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
            let databases = self.databases.lock().await;

            databases.get(name).cloned()
        }

        /// Retrieve the database specified by name, creating it if it
        /// doesn't exist.
        async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
            let mut databases = self.databases.lock().await;

            if let Some(db) = databases.get(name) {
                Ok(db.clone())
            } else {
                let new_db = Arc::new(TestDatabase::new());
                databases.insert(name.to_string(), new_db.clone());
                Ok(new_db)
            }
        }
    }
}
