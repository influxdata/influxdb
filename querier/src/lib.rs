//! IOx Query Server Implementation.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

mod cache;
mod database;
mod handler;
mod ingester;
mod namespace;
mod parquet;
mod poison;
mod query_log;
mod server;
mod system_tables;
mod table;
mod tombstone;

pub use cache::CatalogCache as QuerierCatalogCache;
pub use database::{Error as QuerierDatabaseError, QuerierDatabase};
pub use handler::{QuerierHandler, QuerierHandlerImpl};
pub use ingester::{
    create_ingester_connection_for_testing, create_ingester_connections,
    flight_client::{
        Error as IngesterFlightClientError, IngesterFlightClient,
        QueryData as IngesterFlightClientQueryData,
    },
    Error as IngesterError, IngesterConnection, IngesterConnectionImpl, IngesterPartition,
};
pub use namespace::QuerierNamespace;
pub use server::QuerierServer;
