#![deny(rust_2018_idioms)]

use tracing::{debug, error, info};

use delorean::generated_types::{Bucket, TimestampRange};
use delorean::id::Id;
use delorean::line_parser;
use delorean::line_parser::index_pairs;
use delorean::storage::{
    database::Error as DatabaseError, partitioned_store::ReadValues, predicate,
};
use delorean::time::{parse_duration, time_as_i64_nanos};

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use csv::Writer;
use futures::{self, StreamExt};
use hyper::{Body, Method, StatusCode};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use std::str;

use crate::server::App;

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
        source: DatabaseError,
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
        source: DatabaseError,
    },

    #[snafu(display(
        "Internal error reading points from org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    ReadingPoints {
        org: String,
        bucket_name: String,
        source: DatabaseError,
    },

    #[snafu(display(
        "Internal error creating csv from org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    ConvertingToCSV {
        org: String,
        bucket_name: String,
        source: Box<dyn std::error::Error>,
    },

    #[snafu(display(
        "Internal error creating org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    CreatingBucket {
        org: String,
        bucket_name: String,
        source: DatabaseError,
    },

    // Application level errors
    #[snafu(display("Bucket {} not found in org {}", bucket_name, org))]
    BucketNotFound { org: String, bucket_name: String },

    #[snafu(display("Body exceeds limit of {} bytes", max_body_size))]
    RequestSizeExceeded { max_body_size: usize },

    #[snafu(display("Expected query string in request, but none was provided"))]
    ExpectedQueryString {},

    #[snafu(display("Invalid query string '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("Invalid request body '{}': {}", request_body, source))]
    InvalidRequestBody {
        request_body: String,
        source: serde_json::error::Error,
    },

    #[snafu(display("Invalid duration '{}': {}", duration, source))]
    InvalidDuration {
        duration: String,
        source: delorean::Error,
    },

    #[snafu(display("Could not parse predicate '{}':  {}", predicate, source))]
    InvalidPredicate {
        predicate: String,
        source: predicate::Error,
    },

    #[snafu(display("Error reading request body: {}", source))]
    ReadingBody { source: hyper::error::Error },

    #[snafu(display("Error reading request body as utf8: {}", source))]
    ReadingBodyAsUtf8 { source: std::str::Utf8Error },

    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },
}

impl ApplicationError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BucketByName { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::WritingPoints { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ReadingPoints { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ConvertingToCSV { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::CreatingBucket { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BucketNotFound { .. } => StatusCode::NOT_FOUND,
            Self::RequestSizeExceeded { .. } => StatusCode::BAD_REQUEST,
            Self::ExpectedQueryString { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidQueryString { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidRequestBody { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidDuration { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidPredicate { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingBody { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingBodyAsUtf8 { .. } => StatusCode::BAD_REQUEST,
            Self::RouteNotFound { .. } => StatusCode::NOT_FOUND,
        }
    }
}

const MAX_SIZE: usize = 1_048_576; // max write request size of 1MB

#[derive(Debug, Deserialize)]
struct WriteInfo {
    org: Id,
    bucket: Id,
}

#[tracing::instrument(level = "debug")]
async fn write(req: hyper::Request<Body>, app: Arc<App>) -> Result<Option<Body>, ApplicationError> {
    let query = req.uri().query().context(ExpectedQueryString)?;

    let write_info: WriteInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: String::from(query),
    })?;

    // Even though tools like `inch` and `storectl query` pass bucket IDs, treat them as
    // `bucket_name` in delorean because MemDB sets auto-incrementing IDs for buckets.
    let bucket_name = write_info.bucket.to_string();
    let org = write_info.org;

    let bucket_id = app
        .db
        .get_bucket_id_by_name(org, &bucket_name)
        .await
        .context(BucketByName {
            org,
            bucket_name: bucket_name.clone(),
        })?
        .context(BucketNotFound {
            org,
            bucket_name: bucket_name.clone(),
        })?;

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
    let body = str::from_utf8(&body).unwrap();

    let mut points = line_parser::parse(body).expect("TODO: Unable to parse lines");
    debug!("Parsed {} points", points.len());

    app.db
        .write_points(write_info.org, bucket_id, &mut points)
        .await
        .context(WritingPoints { org, bucket_name })?;

    Ok(None)
}

#[derive(Deserialize, Debug)]
struct ReadInfo {
    org: Id,
    bucket: Id,
    predicate: String,
    start: Option<String>,
    stop: Option<String>,
}

// TODO: Move this to src/time.rs, better error surfacing
fn duration_to_nanos_or_default(
    duration_param: Option<&str>,
    now: std::time::SystemTime,
    default: std::time::SystemTime,
) -> Result<i64, ApplicationError> {
    let time = match duration_param {
        Some(duration) => parse_duration(duration)
            .context(InvalidDuration { duration })?
            .from_time(now)
            .context(InvalidDuration { duration })?,
        None => default,
    };
    Ok(time_as_i64_nanos(&time))
}

// TODO: figure out how to stream read results out rather than rendering the whole thing in mem
#[tracing::instrument(level = "debug")]
async fn read(req: hyper::Request<Body>, app: Arc<App>) -> Result<Option<Body>, ApplicationError> {
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let read_info: ReadInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    // Even though tools like `inch` and `storectl query` pass bucket IDs, treat them as
    // `bucket_name` in delorean because MemDB sets auto-incrementing IDs for buckets.
    let org = read_info.org;
    let bucket_name = read_info.bucket.to_string();

    let predicate = predicate::parse_predicate(&read_info.predicate).context(InvalidPredicate {
        predicate: &read_info.predicate,
    })?;

    let now = std::time::SystemTime::now();

    let start = duration_to_nanos_or_default(
        read_info.start.as_deref(),
        now,
        now.checked_sub(Duration::from_secs(10)).unwrap(),
    )?;

    let end = duration_to_nanos_or_default(read_info.stop.as_deref(), now, now)?;

    let range = TimestampRange { start, end };

    let bucket_id = app
        .db
        .get_bucket_id_by_name(org, &bucket_name)
        .await
        .context(BucketByName {
            org,
            bucket_name: bucket_name.clone(),
        })?
        .context(BucketNotFound {
            org,
            bucket_name: bucket_name.clone(),
        })?;

    let batches = app
        .db
        .read_points(read_info.org, bucket_id, &predicate, &range)
        .await
        .context(ReadingPoints {
            org,
            bucket_name: bucket_name.clone(),
        })?;

    let mut response_body = vec![];

    for batch in batches {
        let mut wtr = Writer::from_writer(vec![]);

        let pairs = index_pairs(&batch.key);

        let mut cols = Vec::with_capacity(pairs.len() + 2);
        let mut vals = Vec::with_capacity(pairs.len() + 2);

        for p in &pairs {
            cols.push(p.key.clone());
            vals.push(p.value.clone());
        }
        let tcol = "_time".to_string();
        let vcol = "_value".to_string();

        cols.push(tcol.clone());
        cols.push(vcol.clone());
        vals.push(tcol);
        vals.push(vcol);
        let tcol = cols.len() - 2;
        let vcol = cols.len() - 1;

        wtr.write_record(&cols).unwrap();

        match batch.values {
            ReadValues::I64(values) => {
                for val in values {
                    let t = val.time.to_string();
                    let v = val.value.to_string();
                    vals[vcol] = v;
                    vals[tcol] = t;
                    wtr.write_record(&vals).unwrap();
                }
            }
            ReadValues::F64(values) => {
                for val in values {
                    let t = val.time.to_string();
                    let v = val.value.to_string();
                    vals[vcol] = v;
                    vals[tcol] = t;
                    wtr.write_record(&vals).unwrap();
                }
            }
        }

        let mut data = wtr
            .into_inner()
            .map_err(|e| ApplicationError::ConvertingToCSV {
                org: org.into(),
                bucket_name: bucket_name.clone(),
                source: Box::new(e),
            })?;

        response_body.append(&mut data);
        response_body.append(&mut b"\n".to_vec());
    }

    Ok(Some(response_body.into()))
}

// Route to test that the server is alive
#[tracing::instrument(level = "debug")]
async fn ping(req: hyper::Request<Body>) -> Result<Option<Body>, ApplicationError> {
    let response_body = "PONG";
    Ok(Some(response_body.into()))
}

#[derive(Deserialize, Debug)]
struct CreateBucketInfo {
    #[serde(rename = "orgID")]
    org: Id,
    #[serde(rename = "name")]
    bucket: Id,
}

#[tracing::instrument(level = "debug")]
async fn create_bucket(
    req: hyper::Request<Body>,
    app: Arc<App>,
) -> Result<Option<Body>, ApplicationError> {
    let body = hyper::body::to_bytes(req).await.context(ReadingBody)?;

    let request_body = str::from_utf8(&body).context(ReadingBodyAsUtf8)?;

    let create_bucket_info: CreateBucketInfo =
        serde_json::from_str(request_body).context(InvalidRequestBody { request_body })?;

    let org = create_bucket_info.org;
    let bucket_name = create_bucket_info.bucket.to_string();

    let bucket = Bucket {
        org_id: org.into(),
        id: 0,
        name: bucket_name.clone(),
        retention: "0".to_string(),
        posting_list_rollover: 10_000,
        index_levels: vec![],
    };

    app.db
        .create_bucket_if_not_exists(create_bucket_info.org, bucket)
        .await
        .context(CreatingBucket { org, bucket_name })?;

    Ok(None)
}

pub async fn service(
    req: hyper::Request<Body>,
    app: Arc<App>,
) -> http::Result<hyper::Response<Body>> {
    let method = req.method().clone();
    let uri = req.uri().clone();

    let response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/api/v2/write") => write(req, app).await,
        (&Method::GET, "/ping") => ping(req).await,
        (&Method::GET, "/api/v2/read") => read(req, app).await,
        (&Method::POST, "/api/v2/buckets") => create_bucket(req, app).await,
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
