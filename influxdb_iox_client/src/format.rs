//! Output formatting utilities for Arrow record batches

use std::{fmt::Display, str::FromStr};

use thiserror::Error;

use arrow::{
    self, csv::WriterBuilder, error::ArrowError, json::ArrayWriter, record_batch::RecordBatch,
};

/// Error type for results formatting
#[derive(Debug, Error)]
pub enum Error {
    /// Unknown formatting type
    #[error("Unknown format type: {}. Expected one of 'pretty', 'csv' or 'json'", .0)]
    Invalid(String),

    /// Error pretty printing
    #[error("Arrow pretty printing error: {}", .0)]
    PrettyArrow(ArrowError),

    /// Error during CSV conversion
    #[error("Arrow csv printing error: {}", .0)]
    CsvArrow(ArrowError),

    /// Error during JSON conversion
    #[error("Arrow json printing error: {}", .0)]
    JsonArrow(ArrowError),

    /// Error converting CSV output to utf-8
    #[error("Error converting CSV output to UTF-8: {}", .0)]
    CsvUtf8(std::string::FromUtf8Error),

    /// Error converting JSON output to utf-8
    #[error("Error converting JSON output to UTF-8: {}", .0)]
    JsonUtf8(std::string::FromUtf8Error),
}
type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Copy, Clone, PartialEq)]
/// Requested output format for the query endpoint
pub enum QueryOutputFormat {
    /// Arrow pretty printer format (default)
    Pretty,
    /// Comma separated values
    Csv,
    /// Arrow JSON format
    Json,
}

impl Display for QueryOutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryOutputFormat::Pretty => write!(f, "pretty"),
            QueryOutputFormat::Csv => write!(f, "csv"),
            QueryOutputFormat::Json => write!(f, "json"),
        }
    }
}

impl Default for QueryOutputFormat {
    fn default() -> Self {
        Self::Pretty
    }
}

impl FromStr for QueryOutputFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "pretty" => Ok(Self::Pretty),
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            _ => Err(Error::Invalid(s.to_string())),
        }
    }
}

impl QueryOutputFormat {
    /// Return the Mcontent-type of this format
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Pretty => "text/plain",
            Self::Csv => "text/csv",
            Self::Json => "application/json",
        }
    }
}

impl QueryOutputFormat {
    /// Format the [`RecordBatch`]es into a String in one of the
    /// following formats:
    ///
    /// Pretty:
    /// ```text
    /// +----------------+--------------+-------+-----------------+------------+
    /// | bottom_degrees | location     | state | surface_degrees | time       |
    /// +----------------+--------------+-------+-----------------+------------+
    /// | 50.4           | santa_monica | CA    | 65.2            | 1568756160 |
    /// +----------------+--------------+-------+-----------------+------------+
    /// ```
    ///
    /// CSV:
    /// ```text
    /// bottom_degrees,location,state,surface_degrees,time
    /// 50.4,santa_monica,CA,65.2,1568756160
    /// ```
    ///
    /// JSON:
    ///
    /// Example (newline + whitespace added for clarity):
    /// ```text
    /// [
    ///  {"bottom_degrees":50.4,"location":"santa_monica","state":"CA","surface_degrees":65.2,"time":1568756160},
    ///  {"location":"Boston","state":"MA","surface_degrees":50.2,"time":1568756160}
    /// ]
    /// ```
    pub fn format(&self, batches: &[RecordBatch]) -> Result<String> {
        match self {
            Self::Pretty => batches_to_pretty(&batches),
            Self::Csv => batches_to_csv(&batches),
            Self::Json => batches_to_json(&batches),
        }
    }
}

fn batches_to_pretty(batches: &[RecordBatch]) -> Result<String> {
    arrow_util::display::pretty_format_batches(batches).map_err(Error::PrettyArrow)
}

fn batches_to_csv(batches: &[RecordBatch]) -> Result<String> {
    let mut bytes = vec![];

    {
        let mut writer = WriterBuilder::new().has_headers(true).build(&mut bytes);

        for batch in batches {
            writer.write(batch).map_err(Error::CsvArrow)?;
        }
    }
    let csv = String::from_utf8(bytes).map_err(Error::CsvUtf8)?;
    Ok(csv)
}

fn batches_to_json(batches: &[RecordBatch]) -> Result<String> {
    let mut bytes = vec![];

    {
        let mut writer = ArrayWriter::new(&mut bytes);
        writer.write_batches(batches).map_err(Error::CsvArrow)?;

        writer.finish().map_err(Error::CsvArrow)?;
    }

    let json = String::from_utf8(bytes).map_err(Error::JsonUtf8)?;

    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(
            QueryOutputFormat::from_str("pretty").unwrap(),
            QueryOutputFormat::Pretty
        );
        assert_eq!(
            QueryOutputFormat::from_str("pRetty").unwrap(),
            QueryOutputFormat::Pretty
        );

        assert_eq!(
            QueryOutputFormat::from_str("csv").unwrap(),
            QueryOutputFormat::Csv
        );
        assert_eq!(
            QueryOutputFormat::from_str("CSV").unwrap(),
            QueryOutputFormat::Csv
        );

        assert_eq!(
            QueryOutputFormat::from_str("json").unwrap(),
            QueryOutputFormat::Json
        );
        assert_eq!(
            QueryOutputFormat::from_str("JSON").unwrap(),
            QueryOutputFormat::Json
        );

        assert_eq!(
            QueryOutputFormat::from_str("un").unwrap_err().to_string(),
            "Unknown format type: un. Expected one of 'pretty', 'csv' or 'json'"
        );
    }

    #[test]
    fn test_from_roundtrip() {
        assert_eq!(
            QueryOutputFormat::from_str(&QueryOutputFormat::Pretty.to_string()).unwrap(),
            QueryOutputFormat::Pretty
        );

        assert_eq!(
            QueryOutputFormat::from_str(&QueryOutputFormat::Csv.to_string()).unwrap(),
            QueryOutputFormat::Csv
        );

        assert_eq!(
            QueryOutputFormat::from_str(&QueryOutputFormat::Json.to_string()).unwrap(),
            QueryOutputFormat::Json
        );
    }
}
