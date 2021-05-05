use snafu::Snafu;

use parquet::errors::ParquetError;

#[derive(Debug, Snafu)]
pub enum IOxParquetError {
    #[snafu(display(r#"{}, underlying parquet error {}"#, message, source))]
    #[snafu(visibility(pub(crate)))]
    ParquetLibraryError {
        message: String,
        source: ParquetError,
    },
    Unsupported,
}

pub type Result<T, E = IOxParquetError> = std::result::Result<T, E>;
