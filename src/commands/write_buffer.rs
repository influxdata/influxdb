#![deny(rust_2018_idioms)]

use tracing::{debug, info};

use std::env::VarError;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::server::write_buffer_routes;
use delorean::storage::write_buffer_database::{Db, WriteBufferDatabases};

use hyper::service::{make_service_fn, service_fn};
use hyper::Server;

pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv::dotenv().ok();

    let db_dir = match std::env::var("DELOREAN_DB_DIR") {
        Ok(val) => val,
        Err(_) => {
            // default database path is $HOME/.delorean
            let mut path = dirs::home_dir().unwrap();
            path.push(".delorean/");
            path.into_os_string().into_string().unwrap()
        }
    };
    debug!("Delorean Server using database directory: {:?}", db_dir);

    let storage = WriteBufferDatabases::new(&db_dir);
    let dirs = storage.wal_dirs()?;

    // TODO: make recovery of multiple databases multi-threaded
    for dir in dirs {
        let db = Db::restore_from_wal(dir).await?;
        storage.add_db(db).await;
    }

    let bind_addr: SocketAddr = match std::env::var("DELOREAN_BIND_ADDR") {
        Ok(addr) => addr
            .parse()
            .expect("DELOREAN_BIND_ADDR environment variable not a valid SocketAddr"),
        Err(VarError::NotPresent) => "127.0.0.1:8080".parse().unwrap(),
        Err(VarError::NotUnicode(_)) => {
            panic!("DELOREAN_BIND_ADDR environment variable not a valid unicode string")
        }
    };

    let storage = Arc::new(storage);
    let make_svc = make_service_fn(move |_conn| {
        let storage = storage.clone();
        async move {
            Ok::<_, http::Error>(service_fn(move |req| {
                let state = storage.clone();
                write_buffer_routes::service(req, state)
            }))
        }
    });

    let server = Server::bind(&bind_addr).serve(make_svc);

    info!("Listening on http://{}", bind_addr);

    server.await?;

    Ok(())
}
