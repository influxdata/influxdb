use delorean::storage::rocksdb::Database;

use std::{env, io, str};
use std::sync::Arc;

use actix_web::{App, middleware, HttpServer, web, HttpResponse, Error as AWError, guard, error};
use serde_json;
use serde::Deserialize;
use actix_web::web::{BytesMut};
use futures::StreamExt;
use delorean::{line_parser, storage};
use std::env::VarError;


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
async fn write(mut payload: web::Payload, write_info: web::Query<WriteInfo>, s: web::Data<Arc<Server>>) -> Result<HttpResponse, AWError> {
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

    let points = line_parser::parse(body);

    if let Err(err) = s.db.write_points(write_info.org_id, &write_info.bucket_name, points) {
        return Ok(HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("{}", err)})))
    }

    Ok(HttpResponse::Ok().json({}))
}

async fn series_match() -> Result<HttpResponse, AWError> {
    Ok(HttpResponse::InternalServerError().json(serde_json::json!({"error": "not implemented"})))
}

async fn not_found() -> Result<HttpResponse, AWError> {
    Ok(HttpResponse::NotFound().json(serde_json::json!({"error": "not found"})))
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    dotenv::dotenv().ok();

    env::set_var("RUST_LOG", "delorean=debug,actix_server=info");
    env_logger::init();

    let db_dir = std::env::var("DELOREAN_DB_DIR").expect("DELOREAN_DB_DIR must be set");
    let db = Database::new(&db_dir);
    let state = Arc::new(Server{db});
    let bind_addr = match std::env::var("DELOREAN_BIND_ADDR") {
        Ok(addr) => addr,
        Err(VarError::NotPresent) => "127.0.0.1:8080".to_string(),
        Err(VarError::NotUnicode(_)) => panic!("DELOREAN_BIND_ADDR environment variable not a valid unicode string"),
    };

    HttpServer::new(move || {
        App::new()
            .data(state.clone())
            // enable logger
            .wrap(middleware::Logger::default())
            .service(web::resource("/api/v2/write")
                .route(web::post().to(write))
            )
            .service(
                web::scope("/api/v3")
                    .service(web::resource("/series_match")
                        .route(web::get().to(series_match))
                    )
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
