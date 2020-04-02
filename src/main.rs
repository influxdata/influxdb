#![deny(rust_2018_idioms)]

#[macro_use]
extern crate log;

use delorean::delorean::Bucket;
use delorean::delorean::{
    delorean_server::DeloreanServer, storage_server::StorageServer, TimestampRange,
};
use delorean::line_parser;
use delorean::line_parser::index_pairs;
use delorean::storage::database::Database;
use delorean::storage::predicate::parse_predicate;
use delorean::storage::SeriesDataType;
use delorean::time::{parse_duration, time_as_i64_nanos};

use std::env::VarError;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fmt, str};

use bytes::BytesMut;
use csv::Writer;
use futures::{self, StreamExt};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Server, StatusCode};
use serde::Deserialize;

mod rpc;

use crate::rpc::GrpcServer;

pub struct App {
    db: Database,
}

const MAX_SIZE: usize = 1_048_576; // max write request size of 1MB

#[derive(Deserialize)]
struct WriteInfo {
    org_id: u32,
    bucket_name: String,
}

async fn write(req: hyper::Request<Body>, app: Arc<App>) -> Result<Body, ApplicationError> {
    let query = req.uri().query().ok_or(StatusCode::BAD_REQUEST)?;
    let write_info: WriteInfo =
        serde_urlencoded::from_str(query).map_err(|_| StatusCode::BAD_REQUEST)?;

    let maybe_bucket = app
        .db
        .get_bucket_by_name(write_info.org_id, &write_info.bucket_name)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let bucket = match maybe_bucket {
        Some(b) => b,
        None => {
            // create this as the default bucket
            let b = Bucket {
                org_id: write_info.org_id,
                id: 0,
                name: write_info.bucket_name.clone(),
                retention: "0".to_string(),
                posting_list_rollover: 10_000,
                index_levels: vec![],
            };

            app.db
                .create_bucket_if_not_exists(write_info.org_id, &b)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            app.db
                .get_bucket_by_name(write_info.org_id, &write_info.bucket_name)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .expect("Bucket should have just been created")
        }
    };

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

    app.db
        .write_points(write_info.org_id, &bucket, &mut points)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(serde_json::json!(()).to_string().into())
}

#[derive(Deserialize, Debug)]
struct ReadInfo {
    org_id: u32,
    bucket_name: String,
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
async fn read(req: hyper::Request<Body>, app: Arc<App>) -> Result<Body, ApplicationError> {
    let query = req
        .uri()
        .query()
        .expect("Should have been query parameters");
    let read_info: ReadInfo =
        serde_urlencoded::from_str(query).map_err(|_| StatusCode::BAD_REQUEST)?;

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

    let maybe_bucket = app
        .db
        .get_bucket_by_name(read_info.org_id, &read_info.bucket_name)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let bucket = match maybe_bucket {
        Some(b) => b,
        None => {
            return Err(ApplicationError::new(
                StatusCode::NOT_FOUND,
                &format!("bucket {} not found", read_info.bucket_name),
            ));
        }
    };

    let series = app
        .db
        .read_series_matching_predicate_and_range(&bucket, Some(&predicate), Some(&range))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let db = &app.db;

    let mut response_body = vec![];

    for s in series {
        let mut wtr = Writer::from_writer(vec![]);

        let pairs = index_pairs(&s.key).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

        match s.series_type {
            SeriesDataType::I64 => {
                let points = db
                    .read_i64_range(&bucket, &s, &range, 10)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                for batch in points {
                    for p in batch {
                        let t = p.time.to_string();
                        let v = p.value.to_string();
                        vals[vcol] = v;
                        vals[tcol] = t;

                        wtr.write_record(&vals).unwrap();
                    }
                }
            }
            SeriesDataType::F64 => {
                let points = db
                    .read_f64_range(&bucket, &s, &range, 10)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                for batch in points {
                    for p in batch {
                        let t = p.time.to_string();
                        let v = p.value.to_string();
                        vals[vcol] = v;
                        vals[tcol] = t;

                        wtr.write_record(&vals).unwrap();
                    }
                }
            }
        };

        let mut data = wtr
            .into_inner()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        response_body.append(&mut data);
        response_body.append(&mut b"\n".to_vec());
    }

    Ok(response_body.into())
}

async fn service(req: hyper::Request<Body>, app: Arc<App>) -> http::Result<hyper::Response<Body>> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/api/v2/write") => write(req, app).await,
        (&Method::GET, "/api/v2/read") => read(req, app).await,
        _ => Err(ApplicationError::new(
            StatusCode::NOT_FOUND,
            "route not found",
        )),
    };

    match response {
        Ok(body) => Ok(hyper::Response::builder()
            .body(body)
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
    fn new(status_code: StatusCode, message: impl Into<String>) -> ApplicationError {
        ApplicationError {
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
    fn from(other: StatusCode) -> ApplicationError {
        match other {
            StatusCode::BAD_REQUEST => {
                ApplicationError::new(StatusCode::BAD_REQUEST, "Bad request")
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                ApplicationError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            }
            _ => unimplemented!(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv::dotenv().ok();

    env::set_var("RUST_LOG", "delorean=debug,hyper=info");
    env_logger::init();

    let db_dir = match std::env::var("DELOREAN_DB_DIR") {
        Ok(val) => val,
        Err(_) => {
            // default database path is $HOME/.delorean
            let mut path = dirs::home_dir().unwrap();
            path.push(".delorean/");
            path.into_os_string().into_string().unwrap()
        }
    };

    let db = Database::new(&db_dir);
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
        Err(VarError::NotPresent) => "127.0.0.1:8081".parse().unwrap(),
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
