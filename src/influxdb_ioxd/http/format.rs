//! Output formatting utilities for query endpoint

use serde::Deserialize;
use snafu::{ResultExt, Snafu};

use arrow_deps::arrow::{
    self, csv::WriterBuilder, error::ArrowError, json::ArrayWriter, record_batch::RecordBatch,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Arrow pretty printing error: {}", source))]
    PrettyArrow { source: ArrowError },

    #[snafu(display("Arrow csv printing error: {}", source))]
    CsvArrow { source: ArrowError },

    #[snafu(display("Arrow json printing error: {}", source))]
    JsonArrow { source: ArrowError },

    #[snafu(display("Error converting CSV output to UTF-8: {}", source))]
    CsvUtf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Error converting JSON output to UTF-8: {}", source))]
    JsonUtf8 { source: std::string::FromUtf8Error },
}
type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Deserialize, Debug, Copy, Clone, PartialEq)]
/// Requested output format for the query endpoint
pub enum QueryOutputFormat {
    /// Arrow pretty printer format (default)
    #[serde(rename = "pretty")]
    Pretty,
    /// Comma separated values
    #[serde(rename = "csv")]
    CSV,
    /// Arrow JSON format
    #[serde(rename = "json")]
    JSON,
}

impl Default for QueryOutputFormat {
    fn default() -> Self {
        Self::Pretty
    }
}

impl QueryOutputFormat {
    /// Return the content type of the relevant format
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Pretty => "text/plain",
            Self::CSV => "text/csv",
            Self::JSON => "application/json",
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
            Self::CSV => batches_to_csv(&batches),
            Self::JSON => batches_to_json(&batches),
        }
    }
}

fn batches_to_pretty(batches: &[RecordBatch]) -> Result<String> {
    arrow::util::pretty::pretty_format_batches(batches).context(PrettyArrow)
}

fn batches_to_csv(batches: &[RecordBatch]) -> Result<String> {
    let mut bytes = vec![];

    {
        let mut writer = WriterBuilder::new().has_headers(true).build(&mut bytes);

        for batch in batches {
            writer.write(batch).context(CsvArrow)?;
        }
    }
    let csv = String::from_utf8(bytes).context(CsvUtf8)?;
    Ok(csv)
}

fn batches_to_json(batches: &[RecordBatch]) -> Result<String> {
    let mut bytes = vec![];

    {
        let mut writer = ArrayWriter::new(&mut bytes);
        writer.write_batches(batches).context(CsvArrow)?;
        writer.finish().context(CsvArrow)?;
    }

    let json = String::from_utf8(bytes).context(JsonUtf8)?;

    Ok(json)
}
