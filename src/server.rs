#![deny(rust_2018_idioms)]

use tracing::{debug, info, warn};

use delorean::generated_types::{
    delorean_server::DeloreanServer, storage_server::StorageServer, Bucket, TimestampRange,
};
use delorean::id::Id;
use delorean::line_parser;
use delorean::line_parser::index_pairs;
use delorean::storage::database::Database;
use delorean::storage::partitioned_store::ReadValues;
use delorean::storage::predicate::parse_predicate;
use delorean::time::{parse_duration, time_as_i64_nanos};

use std::env::VarError;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, str};

use bytes::BytesMut;
use csv::Writer;
use futures::{self, StreamExt};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Server, StatusCode};
use serde::Deserialize;

use crate::rpc::GrpcServer;

#[derive(Debug)]
pub struct App {
    pub db: Database,
}

const MAX_SIZE: usize = 1_048_576; // max write request size of 1MB

#[derive(Debug, Deserialize)]
struct WriteInfo {
    org: Id,
    bucket: Id,
}

#[tracing::instrument]
async fn write(req: hyper::Request<Body>, app: Arc<App>) -> Result<Option<Body>, ApplicationError> {
    let query = req.uri().query().ok_or(StatusCode::BAD_REQUEST)?;
    let write_info: WriteInfo =
        serde_urlencoded::from_str(query).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Even though tools like `inch` and `storectl query` pass bucket IDs, treat them as
    // `bucket_name` in delorean because MemDB sets auto-incrementing IDs for buckets.
    let bucket_name = write_info.bucket.to_string();

    let bucket_id = app
        .db
        .get_bucket_id_by_name(write_info.org, &bucket_name)
        .await
        .map_err(|e| {
            debug!("Error getting bucket id: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or_else(|| {
            ApplicationError::new(
                StatusCode::NOT_FOUND,
                &format!("bucket {} not found", bucket_name),
            )
        })?;

    let mut payload = req.into_body();

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.expect("Should have been able to read the next chunk");
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(ApplicationError::new(
                StatusCode::BAD_REQUEST,
                "Body exceeds limit of 1MB",
            ));
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
        .map_err(|e| {
            debug!("Error writing points: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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
) -> Result<i64, delorean::Error> {
    let time = match duration_param {
        Some(duration) => {
            let d = parse_duration(duration)?;
            d.from_time(now)?
        }
        None => default,
    };
    Ok(time_as_i64_nanos(&time))
}

// TODO: figure out how to stream read results out rather than rendering the whole thing in mem
#[tracing::instrument]
async fn read(req: hyper::Request<Body>, app: Arc<App>) -> Result<Option<Body>, ApplicationError> {
    let query = req
        .uri()
        .query()
        .expect("Should have been query parameters");
    let read_info: ReadInfo =
        serde_urlencoded::from_str(query).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Even though tools like `inch` and `storectl query` pass bucket IDs, treat them as
    // `bucket_name` in delorean because MemDB sets auto-incrementing IDs for buckets.
    let bucket_name = read_info.bucket.to_string();

    let predicate = parse_predicate(&read_info.predicate).map_err(|_| StatusCode::BAD_REQUEST)?;

    let now = std::time::SystemTime::now();

    let start = duration_to_nanos_or_default(
        read_info.start.as_deref(),
        now,
        now.checked_sub(Duration::from_secs(10)).unwrap(),
    )
    .map_err(|_| StatusCode::BAD_REQUEST)?;

    let end = duration_to_nanos_or_default(read_info.stop.as_deref(), now, now)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let range = TimestampRange { start, end };

    let bucket_id = app
        .db
        .get_bucket_id_by_name(read_info.org, &bucket_name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or_else(|| {
            ApplicationError::new(
                StatusCode::NOT_FOUND,
                &format!("bucket {} not found", bucket_name),
            )
        })?;

    let batches = app
        .db
        .read_points(read_info.org, bucket_id, &predicate, &range)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        response_body.append(&mut data);
        response_body.append(&mut b"\n".to_vec());
    }

    Ok(Some(response_body.into()))
}

// Route to test that the server is alive
#[tracing::instrument]
async fn ping(req: hyper::Request<Body>) -> Result<Option<Body>, ApplicationError> {
    let response_body = "PONG";
    Ok(Some(response_body.into()))
}

#[derive(Deserialize, Debug)]
struct CreateBucketInfo {
    org: Id,
    bucket: Id,
}

#[tracing::instrument]
async fn create_bucket(
    req: hyper::Request<Body>,
    app: Arc<App>,
) -> Result<Option<Body>, ApplicationError> {
    let body = hyper::body::to_bytes(req)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let body = str::from_utf8(&body).unwrap();

    let create_bucket_info: CreateBucketInfo =
        serde_urlencoded::from_str(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let bucket_name = create_bucket_info.bucket.to_string();

    let bucket = Bucket {
        org_id: create_bucket_info.org.into(),
        id: 0,
        name: bucket_name,
        retention: "0".to_string(),
        posting_list_rollover: 10_000,
        index_levels: vec![],
    };

    app.db
        .create_bucket_if_not_exists(create_bucket_info.org, bucket)
        .await
        .map_err(|err| {
            ApplicationError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("error creating bucket: {}", err),
            )
        })?;

    Ok(None)
}

async fn service(req: hyper::Request<Body>, app: Arc<App>) -> http::Result<hyper::Response<Body>> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/api/v2/write") => write(req, app).await,
        (&Method::GET, "/ping") => ping(req).await,
        (&Method::GET, "/api/v2/read") => read(req, app).await,
        (&Method::POST, "/api/v2/create_bucket") => create_bucket(req, app).await,
        _ => Err(ApplicationError::new(
            StatusCode::NOT_FOUND,
            format!("route not found: {}", req.uri()),
        )),
    };

    match response {
        Ok(Some(body)) => Ok(hyper::Response::builder()
            .body(body)
            .expect("Should have been able to construct a response")),
        Ok(None) => Ok(hyper::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("Should have been able to construct a response")),
        Err(e) => {
            let json = serde_json::json!({"error": e.to_string()}).to_string();
            Ok(hyper::Response::builder()
                .status(e.status_code())
                .body(json.into())
                .expect("Should have been able to construct a response"))
        }
    }
}

#[derive(Debug)]
struct ApplicationError {
    status_code: StatusCode,
    message: String,
}

impl ApplicationError {
    fn new(status_code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status_code,
            message: message.into(),
        }
    }

    fn status_code(&self) -> StatusCode {
        self.status_code
    }
}

impl std::error::Error for ApplicationError {}

impl fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<StatusCode> for ApplicationError {
    fn from(other: StatusCode) -> Self {
        match other {
            StatusCode::BAD_REQUEST => Self::new(StatusCode::BAD_REQUEST, "Bad request"),
            StatusCode::INTERNAL_SERVER_ERROR => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            }
            _ => unimplemented!(),
        }
    }
}

fn warn_if_db_dir_does_not_exist(path: &std::path::Path) {
    match fs::metadata(path) {
        Ok(metadata) => {
            if metadata.is_file() {
                warn!("{:?} seems to be a file, not a directory as needed", path);
            }
        }
        Err(e) => {
            warn!("Can't read db_dir {:?}: {}", path, e);
        }
    }
}

#[tokio::main]
/// Main entrypoint of the Delorean server loop
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv::dotenv().ok();

    let db_dir = match std::env::var("DELOREAN_DB_DIR") {
        Ok(val) => val,
        Err(_) => {
            // default database path is $HOME/.delorean
            let mut path = dirs::home_dir().unwrap();
            path.push(".delorean/");
            warn_if_db_dir_does_not_exist(&path);

            path.into_os_string().into_string().unwrap()
        }
    };
    debug!("Delorean Server using database directory: {:?}", db_dir);

    let db = Database::new(&db_dir);
    db.restore_from_wal().await?;

    let state = Arc::new(App { db });
    let bind_addr: SocketAddr = match std::env::var("DELOREAN_BIND_ADDR") {
        Ok(addr) => addr
            .parse()
            .expect("DELOREAN_BIND_ADDR environment variable not a valid SocketAddr"),
        Err(VarError::NotPresent) => "127.0.0.1:8080".parse().unwrap(),
        Err(VarError::NotUnicode(_)) => {
            panic!("DELOREAN_BIND_ADDR environment variable not a valid unicode string")
        }
    };
    let grpc_bind_addr: SocketAddr = match std::env::var("DELOREAN_GRPC_BIND_ADDR") {
        Ok(addr) => addr
            .parse()
            .expect("DELOREAN_GRPC_BIND_ADDR environment variable not a valid SocketAddr"),
        Err(VarError::NotPresent) => "127.0.0.1:8082".parse().unwrap(),
        Err(VarError::NotUnicode(_)) => {
            panic!("DELOREAN_GRPC_BIND_ADDR environment variable not a valid unicode string")
        }
    };

    let grpc_server = tonic::transport::Server::builder()
        .add_service(DeloreanServer::new(GrpcServer { app: state.clone() }))
        .add_service(StorageServer::new(GrpcServer { app: state.clone() }))
        .serve(grpc_bind_addr);

    let make_svc = make_service_fn(move |_conn| {
        let state = state.clone();
        async move {
            Ok::<_, http::Error>(service_fn(move |req| {
                let state = state.clone();
                service(req, state)
            }))
        }
    });

    let server = Server::bind(&bind_addr).serve(make_svc);

    info!("gRPC server listening on http://{}", grpc_bind_addr);
    info!("Listening on http://{}", bind_addr);

    let (grpc_server, server) = futures::future::join(grpc_server, server).await;
    grpc_server?;
    server?;

    Ok(())
}
