use snafu::Snafu;

use parquet::errors::ParquetError;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"{}, underlying parquet error {}"#, message, source))]
    ParquetLibraryError {
        message: String,
        source: ParquetError,
    },
    Unsupported,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
