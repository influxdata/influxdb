use delorean::delorean::Bucket;
use delorean::line_parser;
use delorean::line_parser::index_pairs;
use delorean::storage::database::Database;
use delorean::storage::predicate::parse_predicate;
use delorean::storage::{Range, SeriesDataType};
use delorean::time::{parse_duration, time_as_i64_nanos};

use std::env::VarError;
use std::sync::Arc;
use std::{env, io, str};

use actix_web::web::BytesMut;
use actix_web::{error, guard, middleware, web, App, Error as AWError, HttpResponse, HttpServer};
use csv::Writer;
use failure::_core::time::Duration;
use futures::{self, StreamExt};
use serde::Deserialize;
use serde_json;

struct Server {
    db: Database,
}

const MAX_SIZE: usize = 1_048_576; // max write request size of 1MB

#[derive(Deserialize)]
struct WriteInfo {
    org_id: u32,
    bucket_name: String,
}

// TODO: write end to end test of write
async fn write(
    mut payload: web::Payload,
    write_info: web::Query<WriteInfo>,
    s: web::Data<Arc<Server>>,
) -> Result<HttpResponse, AWError> {
    let bucket = match s
        .db
        .get_bucket_by_name(write_info.org_id, &write_info.bucket_name)?
    {
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

            let _ = s.db.create_bucket_if_not_exists(write_info.org_id, &b)?;
            s.db.get_bucket_by_name(write_info.org_id, &write_info.bucket_name)?
                .unwrap()
        }
    };

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }
    let body = body.freeze();
    let body = str::from_utf8(&body).unwrap();

    let mut points = line_parser::parse(body);

    if let Err(err) = s.db.write_points(write_info.org_id, &bucket, &mut points) {
        return Ok(HttpResponse::InternalServerError()
            .json(serde_json::json!({ "error": format!("{}", err) })));
    }

    Ok(HttpResponse::Ok().json({}))
}

#[derive(Deserialize, Debug)]
struct ReadInfo {
    org_id: u32,
    bucket_name: String,
    predicate: String,
    start: Option<String>,
    stop: Option<String>,
}

//struct ReadResponseBody<'a> {
//    series: SeriesIterator<'a>,
//    current_points_iterator: PointsIterator<'a>,
//}
//
//impl Iterator for ReadResponseBody<'_> {
//    type Item = Vec<u8>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//    }
//}
//
//impl Stream for ReadResponseBody {
//    type Item = Result<Bytes, AWError>;
//
//    fn poll_next(
//        &mut self,
//        cx: &mut Context<'_>,
//    ) -> Poll<Option<Self::Item>> {
//        if self.iters > 10 {
//            Poll::Ready(None)
//        } else {
//            Poll::Ready(Some(Ok(Bytes::from_static("this is a line in the feed\n"))))
//        }
//    }
//}
//
//impl Stream for ReadResponseBody<'_> {
//    fn poll_next(
//        &mut self,
//        cx: &mut Context
//    ) -> Result<Async<Option<Self::Item>>, Self::Error> {
//        if self.iters > 10_000_000 {
//            Ok(Async::Ready(None))
//        } else {
//            Ok(Async::Ready(Some("this is a line in the feed\n".to_string())))
//        }
//    }
//}

//struct Record<T: Serialize> {
//    pairs: Vec<Pair>,
//    time: i64,
//    value: T,
//}
//
//impl<T: Serialize> Serialize for Record<T> {
//    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//        where
//            S: Serializer,
//    {
//        let mut state = serializer.serialize_struct("Record", self.pairs.len() + 2)?;
//        for p in &self.pairs {
//            state.serialize_field(&p.key, &p.value)?;
//        }
//
//        state.serialize_field("_value", &self.value)?;
//        state.serialize_field("_time", &self.time)?;
//
//        state.end()
//    }
//}

// TODO: write end to end test of read
// TODO: figure out how to stream read results out rather than rendering the whole thing in mem
async fn read(
    read_info: web::Query<ReadInfo>,
    s: web::Data<Arc<Server>>,
) -> Result<HttpResponse, AWError> {
    let predicate = parse_predicate(&read_info.predicate)?;

    let now = std::time::SystemTime::now();

    let start = match &read_info.start {
        Some(duration) => {
            let d = parse_duration(duration)?;
            d.from_time(now)?
        }
        None => {
            // default to 10s in the past
            now.checked_sub(Duration::from_secs(10)).unwrap()
        }
    };

    let stop = match &read_info.stop {
        Some(duration) => {
            let d = parse_duration(duration)?;
            d.from_time(now)?
        }
        None => now,
    };

    let start = time_as_i64_nanos(&start);
    let stop = time_as_i64_nanos(&stop);

    let range = Range { start, stop };

    let bucket = match s
        .db
        .get_bucket_by_name(read_info.org_id, &read_info.bucket_name)?
    {
        Some(b) => b,
        None => {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": format!("bucket {} not found", read_info.bucket_name)
            })))
        }
    };

    let series =
        s.db.read_series_matching_predicate_and_range(&bucket, Some(&predicate), Some(&range))?;

    let db = &s.db;

    let mut response_body = vec![];

    for s in series {
        let mut wtr = Writer::from_writer(vec![]);

        let pairs = index_pairs(&s.key)?;
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
                let points = db.read_i64_range(&bucket, &s, &range, 10)?;

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
                let points = db.read_f64_range(&bucket, &s, &range, 10)?;

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

        let mut data = match wtr.into_inner() {
            Ok(d) => d,
            Err(e) => {
                return Ok(HttpResponse::InternalServerError()
                    .json(serde_json::json!({ "error": format!("{}", e) })))
            }
        };
        response_body.append(&mut data);
        response_body.append(&mut b"\n".to_vec());
    }

    Ok(HttpResponse::Ok().body(response_body))
}

async fn not_found() -> Result<HttpResponse, AWError> {
    Ok(HttpResponse::NotFound().json(serde_json::json!({"error": "not found"})))
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    dotenv::dotenv().ok();

    env::set_var("RUST_LOG", "delorean=debug,actix_server=info");
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
    let state = Arc::new(Server { db });
    let bind_addr = match std::env::var("DELOREAN_BIND_ADDR") {
        Ok(addr) => addr,
        Err(VarError::NotPresent) => "127.0.0.1:8080".to_string(),
        Err(VarError::NotUnicode(_)) => {
            panic!("DELOREAN_BIND_ADDR environment variable not a valid unicode string")
        }
    };

    HttpServer::new(move || {
        App::new()
            .data(state.clone())
            // enable logger
            .wrap(middleware::Logger::default())
            .service(
                web::scope("/api/v2")
                    .service(web::resource("/write").route(web::post().to(write)))
                    .service(web::resource("/read").route(web::get().to(read))),
            )
            // default
            .default_service(
                // 404 for GET request
                web::resource("")
                    .route(web::get().to(not_found))
                    // all requests that are not `GET`
                    .route(
                        web::route()
                            .guard(guard::Not(guard::Get()))
                            .to(HttpResponse::MethodNotAllowed),
                    ),
            )
    })
    .bind(bind_addr)?
    .run()
    .await
}
