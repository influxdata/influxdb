pub mod catalog;
pub mod error;
pub mod id;
pub mod log;
pub mod object_store;
pub mod serialize;
pub mod snapshot;

pub use error::CatalogError;
pub(crate) type Result<T, E = CatalogError> = std::result::Result<T, E>;
