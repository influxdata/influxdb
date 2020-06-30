#![deny(rust_2018_idioms)]
use delorean::storage::database::Database;

pub mod http_routes;
pub mod rpc;

#[derive(Debug)]
pub struct App {
    pub db: Database,
}
