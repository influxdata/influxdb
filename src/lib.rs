extern crate num_cpus;

use actix_web::http::StatusCode;
use actix_web::ResponseError;
use std::{error, fmt};

pub mod encoders;
pub mod line_parser;
pub mod storage;
pub mod time;

pub mod delorean {
    include!(concat!(env!("OUT_DIR"), "/delorean.rs"));
}

// TODO: audit all errors and make ones that can differentiate between 400 and 500 and otehrs

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub description: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}
