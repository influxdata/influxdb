#![deny(rust_2018_idioms)]
use delorean::storage::database::Database;

pub mod http_routes;
pub mod rpc;
pub mod write_buffer_routes;

#[derive(Debug)]
pub struct App {
    pub db: Database,
}
