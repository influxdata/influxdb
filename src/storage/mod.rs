use actix_web::http::StatusCode;
use actix_web::ResponseError;
use std::error;
use std::fmt;

pub mod config_store;
pub mod database;
pub mod inverted_index;
pub mod predicate;
pub mod rocksdb;
pub mod series_store;

pub struct Range {
    pub start: i64,
    pub stop: i64,
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SeriesDataType {
    I64,
    F64,
    //    U64,
    //    String,
    //    Bool,
}

#[derive(Debug, Clone)]
pub struct StorageError {
    pub description: String,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for StorageError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl ResponseError for StorageError {
    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}
