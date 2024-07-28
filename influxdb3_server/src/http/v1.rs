use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{bail, Context as AnyhowContext};
use arrow::{
    array::{as_string_array, ArrayRef, AsArray},
    compute::{cast_with_options, CastOptions},
    datatypes::{
        DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::{format::SecondsFormat, DateTime};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{future::FusedFuture, stream::FusedStream};
use futures::{pin_mut, ready, stream::Fuse, stream::Peekable, FutureExt, Stream, StreamExt};
use hyper::http::HeaderValue;
use hyper::{
    header::ACCEPT, header::CONTENT_TYPE, header::TRANSFER_ENCODING, Body, Request, Response,
    StatusCode,
};
use influxdb3_write::WriteBuffer;
use iox_time::TimeProvider;
use observability_deps::tracing::info;
use schema::{INFLUXQL_MEASUREMENT_COLUMN_NAME, TIME_COLUMN_NAME};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::QueryExecutor;

use super::{Error, HttpApi, Result};

const DEFAULT_CHUNK_SIZE: usize = 10_000;
const TRANSFER_ENCODING_CHUNKED: &str = "chunked";

impl<W, Q, T> HttpApi<W, Q, T>
where
    W: WriteBuffer,
    Q: QueryExecutor,
    T: TimeProvider,
    Error: From<<Q as QueryExecutor>::Error>,
{
    /// Implements the v1 query API for InfluxDB
    ///
    /// Accepts the URL parameters, defined by [`QueryParams`]), and returns a stream
    /// of [`QueryResponse`]s. If the `chunked` parameter is set to `true`, then the
    /// response stream will be chunked into chunks of size `chunk_size`, if provided,
    /// or 10,000. For InfluxQL queries that select from multiple measurements, chunks
    /// will be split on the `chunk_size`, or series, whichever comes first.
    pub(super) async fn v1_query(&self, req: Request<Body>) -> Result<Response<Body>> {
        let params = QueryParams::from_request(&req)?;
        info!(?params, "handle v1 query API");
        let QueryParams {
            chunk_size,
            chunked,
            database,
            epoch,
            pretty,
            query,
        } = params;

        let format = QueryFormat::from_request(&req, pretty)?;
        info!(?format, "handle v1 format API");

        let chunk_size = chunked.then(|| chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE));

        // TODO - Currently not supporting parameterized queries, see
        //        https://github.com/influxdata/influxdb/issues/24805
        let stream = self.query_influxql_inner(database, &query, None).await?;
        let stream =
            QueryResponseStream::new(0, stream, chunk_size, format, epoch).map_err(QueryError)?;
        let body = Body::wrap_stream(stream);

        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, format.as_content_type());

        // Check if the response is chunked.
        // If it is, add the "Transfer-Encoding: chunked" header to the response builder.
        if chunked {
            builder = builder.header(TRANSFER_ENCODING, TRANSFER_ENCODING_CHUNKED);
        }

        Ok(builder.body(body).unwrap())
    }
}

/// Query parameters for the v1/query API
///
/// The original API supports a `u` parameter, for "username", as well as a `p`,
/// for "password". The password is extracted upstream, and username is ignored.
#[derive(Debug, Deserialize)]
struct QueryParams {
    /// Chunk the response into chunks of size `chunk_size`, or 10,000, or by series
    #[serde(default)]
    chunked: bool,
    /// Define the number of records that will go into a chunk
    chunk_size: Option<usize>,
    /// Database to perform the query against
    ///
    /// This is optional because the query string may specify the database
    #[serde(rename = "db")]
    database: Option<String>,
    /// Map timestamps to UNIX epoch time, with the given precision
    #[allow(dead_code)]
    epoch: Option<Precision>,
    /// Format the JSON outputted in pretty format
    #[serde(default)]
    pretty: bool,
    /// The InfluxQL query string
    #[serde(rename = "q")]
    query: String,
}

impl QueryParams {
    /// Extract [`QueryParams`] from an HTTP [`Request`]
    fn from_request(req: &Request<Body>) -> Result<Self> {
        let query = req.uri().query().ok_or(Error::MissingQueryParams)?;
        serde_urlencoded::from_str(query).map_err(Into::into)
    }
}

/// Enum representing the query format for the v1/query API.
///
/// The original API supports CSV, JSON, and "pretty" JSON formats.
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueryFormat {
    Csv,
    Json,
    JsonPretty,
}

impl QueryFormat {
    /// Returns the content type as a string slice for the query format.
    ///
    /// Maps the `QueryFormat` variants to their corresponding MIME types as strings.
    /// This is useful for setting the `Content-Type` header in HTTP responses.
    fn as_content_type(&self) -> &str {
        match self {
            Self::Csv => "application/csv",
            Self::Json | Self::JsonPretty => "application/json",
        }
    }

    /// Checks if the query format is 'JsonPretty'.
    ///
    /// Determines if the `QueryFormat` is `JsonPretty`, which indicates that the JSON
    /// output should be formatted in a human-readable way. Returns `true` if the
    /// format is `JsonPretty`, otherwise returns `false`.
    fn is_pretty(&self) -> bool {
        match self {
            Self::Csv | Self::Json => false,
            Self::JsonPretty => true,
        }
    }

    /// Extracts the [`QueryFormat`] from an HTTP [`Request`].
    ///
    /// Parses the HTTP request to determine the desired query format. The `pretty`
    /// parameter indicates if the pretty format is requested via a query parameter.
    /// The function inspects the `Accept` header of the request to determine the
    /// format, defaulting to JSON if no specific format is requested. If the format
    /// is invalid or non-UTF8, an error is returned.
    fn from_request(req: &Request<Body>, pretty: bool) -> Result<Self> {
        let mime_type = req.headers().get(ACCEPT).map(HeaderValue::as_bytes);

        match mime_type {
            Some(b"application/csv" | b"text/csv") => Ok(Self::Csv),
            Some(b"application/json" | b"*/*") | None => {
                // If no specific format is requested via the Accept header,
                // and the 'pretty' parameter is true, use the pretty JSON format.
                // Otherwise, default to the regular JSON format.
                if pretty {
                    Ok(Self::JsonPretty)
                } else {
                    Ok(Self::Json)
                }
            }
            Some(mime_type) => match String::from_utf8(mime_type.to_vec()) {
                Ok(s) => Err(Error::InvalidMimeType(s)),
                Err(e) => Err(Error::NonUtf8MimeType(e)),
            },
        }
    }
}

/// UNIX epoch precision
#[derive(Debug, Deserialize, Clone, Copy)]
enum Precision {
    #[serde(rename = "ns")]
    Nanoseconds,
    #[serde(rename = "u", alias = "µ")]
    Microseconds,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "m")]
    Minutes,
    #[serde(rename = "h")]
    Hours,
}

/// Error type for the v1 API
///
/// This is used to catch errors that occur during the streaming process.
/// [`anyhow::Error`] is used as a catch-all because if anything fails during
/// that process it will result in a 500 INTERNAL ERROR.
#[derive(Debug, thiserror::Error)]
#[error("unexpected query error: {0}")]
pub struct QueryError(#[from] anyhow::Error);

/// The response structure returned by the v1 query API
///
/// The `pretty` parameter is used during serizliaztion to determine if JSON
/// is pretty formatted or not.
#[derive(Debug, Serialize)]
struct QueryResponse {
    results: Vec<StatementResponse>,
    #[serde(skip_serializing)]
    format: QueryFormat,
}

/// Convert [`QueryResponse`] to [`Bytes`] for `hyper`'s [`Body::wrap_stream`] method
impl From<QueryResponse> for Bytes {
    fn from(s: QueryResponse) -> Self {
        /// Convert a [`QueryResponse`] to a JSON byte vector.
        ///
        /// This function serializes the `QueryResponse` to JSON. If the format is
        /// `JsonPretty`, it will produce human-readable JSON, otherwise it produces
        /// compact JSON.
        fn to_json(s: QueryResponse) -> Vec<u8> {
            if s.format.is_pretty() {
                serde_json::to_vec_pretty(&s)
                    .expect("Failed to serialize QueryResponse to pretty JSON")
            } else {
                serde_json::to_vec(&s).expect("Failed to serialize QueryResponse to JSON")
            }
        }

        /// Convert a [`QueryResponse`] to a CSV byte vector.
        ///
        /// This function serializes the `QueryResponse` to CSV format. It dynamically
        /// extracts column names from the first series and writes the header and data
        /// rows to the CSV writer.
        fn to_csv(s: QueryResponse) -> Vec<u8> {
            let mut wtr = csv::WriterBuilder::new()
                .quote_style(csv::QuoteStyle::Never)
                .from_writer(vec![]);
            // Extract column names dynamically from the first series
            let mut headers = vec!["name", "tags"];
            if let Some(first_statement) = s.results.first() {
                if let Some(first_series) = first_statement.series.first() {
                    headers.extend(first_series.columns.iter().map(|s| s.as_str()));
                }
            }
            // Write the header
            wtr.write_record(&headers)
                .expect("Failed to write CSV header");

            // Iterate through the hierarchical structure of QueryResponse to write data
            // to the CSV writer. The loop processes each statement, series, and row to
            // build and write CSV records. Each record is initialized with the series name
            // and an empty tag field, followed by the string representations of the row's values.
            // Finally, the record is written to the CSV writer
            for statement in s.results {
                for series in statement.series {
                    for row in series.values {
                        let mut record = vec![series.name.clone(), "".to_string()];
                        for v in row.0 {
                            record.push(match v {
                                Value::String(s) => s.clone(),
                                _ => v.to_string(),
                            });
                        }
                        wtr.write_record(&record)
                            .expect("Failed to write CSV record");
                    }
                }
            }

            // Flush the CSV writer to ensure all data is written
            wtr.flush().expect("flush csv writer");

            wtr.into_inner().expect("into_inner from csv writer")
        }
        /// Extend a byte vector with CRLF and convert it to [`Bytes`].
        ///
        /// This function appends a CRLF (`\r\n`) sequence to the given byte vector
        /// and converts it to a `Bytes` object.
        fn extend_with_crlf(mut bytes: Vec<u8>) -> Bytes {
            bytes.extend_from_slice(b"\r\n");
            Bytes::from(bytes)
        }

        match s.format {
            QueryFormat::Json | QueryFormat::JsonPretty => extend_with_crlf(to_json(s)),
            QueryFormat::Csv => extend_with_crlf(to_csv(s)),
        }
    }
}

/// The response to an individual InfluxQL query
#[derive(Debug, Serialize)]
struct StatementResponse {
    statement_id: usize,
    series: Vec<Series>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partial: Option<bool>,
}

/// The records produced for a single time series (measurement)
#[derive(Debug, Serialize)]
struct Series {
    name: String,
    columns: Vec<String>,
    values: Vec<Row>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partial: Option<bool>,
}

/// A single row, or record in a time series
#[derive(Debug, Serialize)]
struct Row(Vec<Value>);

/// A buffer for storing records from a stream of [`RecordBatch`]es
///
/// The optional `size` indicates whether this is operating in `chunked` mode (see
/// [`QueryResponseStream`]), and when specified, gives the size of chunks that will
/// be emitted.
struct ChunkBuffer {
    size: Option<usize>,
    series: VecDeque<(String, Vec<Row>)>,
}

impl ChunkBuffer {
    fn new(size: Option<usize>) -> Self {
        Self {
            size,
            series: VecDeque::new(),
        }
    }

    /// Get the name of the current measurement [`Series`] being streamed
    fn current_measurement_name(&self) -> Option<&str> {
        self.series.front().map(|(n, _)| n.as_str())
    }

    /// For queries that produce multiple [`Series`], this will be called when
    /// the current series is completed streaming
    fn push_next_measurement<S: Into<String>>(&mut self, name: S) {
        self.series.push_front((name.into(), vec![]));
    }

    /// Push a new [`Row`] into the current measurement [`Series`]
    fn push_row(&mut self, row: Row) -> Result<(), anyhow::Error> {
        self.series
            .front_mut()
            .context("tried to push row with no measurements buffered")?
            .1
            .push(row);
        Ok(())
    }

    /// Flush a single chunk from the [`ChunkBuffer`], if possible
    fn flush_one(&mut self) -> Option<(String, Vec<Row>)> {
        if !self.can_flush() {
            return None;
        }
        // we can flush, so unwrap is safe:
        let size = self.size.unwrap();
        if self
            .series
            .back()
            .is_some_and(|(_, rows)| rows.len() <= size)
        {
            // the back series is smaller than the chunk size, so we just
            // pop and take the whole thing:
            self.series.pop_back()
        } else {
            // only drain a chunk's worth from the back series:
            self.series
                .back_mut()
                .map(|(name, rows)| (name.to_owned(), rows.drain(..size).collect()))
        }
    }

    /// The [`ChunkBuffer`] is operating in chunked mode, and can flush a chunk
    fn can_flush(&self) -> bool {
        if let (Some(size), Some(m)) = (self.size, self.series.back()) {
            m.1.len() >= size || self.series.len() > 1
        } else {
            false
        }
    }

    /// The [`ChunkBuffer`] is empty
    fn is_empty(&self) -> bool {
        self.series.is_empty()
    }
}

/// A wrapper around a [`SendableRecordBatchStream`] that buffers streamed
/// [`RecordBatch`]es and outputs [`QueryResponse`]s
///
/// Can operate in `chunked` mode, in which case, the `chunk_size` provided
/// will determine how often [`QueryResponse`]s are emitted. When not in
/// `chunked` mode, the entire input stream of [`RecordBatch`]es will be buffered
/// into memory before being emitted.
///
/// `format` will emit CSV, JSON or pretty formatted JSON.
///
/// Providing an `epoch` [`Precision`] will have the `time` column values emitted
/// as UNIX epoch times with the given precision.
///
/// The input stream is wrapped in [`Fuse`], because of the [`Stream`] implementation
/// below, it is possible that the input stream is polled after completion.

struct QueryResponseStream {
    buffer: ChunkBuffer,
    input: Peekable<Fuse<SendableRecordBatchStream>>,
    column_map: HashMap<String, usize>,
    statement_id: usize,
    format: QueryFormat,
    epoch: Option<Precision>,
}

impl QueryResponseStream {
    /// Create a new [`QueryResponseStream`]
    ///
    /// Specifying a `chunk_size` will have the stream operate in `chunked` mode.
    fn new(
        statement_id: usize,
        input: SendableRecordBatchStream,
        chunk_size: Option<usize>,
        format: QueryFormat,
        epoch: Option<Precision>,
    ) -> Result<Self, anyhow::Error> {
        let buffer = ChunkBuffer::new(chunk_size);
        let schema = input.schema();
        let column_map = schema
            .fields
            .iter()
            .map(|f| f.name().to_owned())
            .enumerate()
            .flat_map(|(i, n)| {
                if n != INFLUXQL_MEASUREMENT_COLUMN_NAME && i > 0 {
                    Some((n, i - 1))
                } else {
                    None
                }
            })
            .collect();
        Ok(Self {
            buffer,
            column_map,
            input: input.fuse().peekable(),
            format,
            statement_id,
            epoch,
        })
    }

    fn buffer_record_batch(&mut self, mut batch: RecordBatch) -> Result<(), anyhow::Error> {
        if self.epoch.is_some() {
            // If the `epoch` is specified, then we cast the `time` column into an Int64.
            // This will be in nanoseconds. The conversion to the given epoch precision
            // happens below, when processing the JSON rows
            batch = RecordBatch::try_from_iter(batch.schema().fields.iter().map(|f| {
                let name = f.name();
                let column = batch.column_by_name(name).unwrap();
                (
                    name,
                    if name == TIME_COLUMN_NAME {
                        // unwrap should be safe here because the time column cast to Int64
                        cast_with_options(column, &DataType::Int64, &CastOptions::default())
                            .unwrap()
                    } else {
                        Arc::clone(column)
                    },
                )
            }))
            .context("failed to cast batch time column with `epoch` parameter specified")?;
        }
        let column_map = &self.column_map;
        let columns = batch.columns();
        let schema = batch.schema();

        for row_index in 0..batch.num_rows() {
            let mut row = vec![Value::Null; column_map.len()];

            for (col_index, column) in columns.iter().enumerate() {
                let field = schema.field(col_index);
                let column_name = field.name();

                let mut cell_value = if !column.is_valid(row_index) {
                    continue;
                } else {
                    cast_column_value(column, row_index)?
                };

                // Handle the special case for the measurement column
                if column_name == INFLUXQL_MEASUREMENT_COLUMN_NAME {
                    if let Value::String(ref measurement_name) = cell_value {
                        if self.buffer.current_measurement_name().is_none()
                            || self
                                .buffer
                                .current_measurement_name()
                                .is_some_and(|n| n != measurement_name)
                        {
                            // we are on the "iox::measurement" column, which gives the name of the time series
                            // if we are on the first row, or if the measurement changes, we push into the
                            // buffer queue
                            self.buffer.push_next_measurement(measurement_name);
                        }
                    }
                    continue;
                }
                if column_name == TIME_COLUMN_NAME {
                    if let Some(precision) = self.epoch {
                        cell_value = convert_ns_epoch(cell_value, precision)?
                    }
                }
                let col_position = column_map
                    .get(column_name)
                    .context("failed to retrieve column position")?;
                row[*col_position] = cell_value;
            }
            self.buffer.push_row(Row(row))?;
        }
        Ok(())
    }

    fn columns(&self) -> Vec<String> {
        let mut columns = vec!["".to_string(); self.column_map.len()];
        self.column_map
            .iter()
            .for_each(|(k, i)| k.clone_into(&mut columns[*i]));
        columns
    }

    /// Flush a single chunk, or time series, when operating in chunked mode
    fn flush_one(&mut self, has_more_data: bool, stream_finished: bool) -> QueryResponse {
        let columns = self.columns();
        let partial_series = if has_more_data { Some(true) } else { None };
        let partial_results = if stream_finished { Some(true) } else { None };

        // This unwrap is okay because we only ever call flush_one after calling can_flush on the buffer
        let (name, values) = self.buffer.flush_one().unwrap();
        let series = vec![Series {
            name,
            columns,
            values,
            partial: partial_series,
        }];
        QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series,
                partial: partial_results,
            }],
            format: self.format,
        }
    }

    /// Flush the entire buffer
    fn flush_all(&mut self) -> Result<QueryResponse, anyhow::Error> {
        let columns = self.columns();
        let series = self
            .buffer
            .series
            .drain(..)
            .map(|(name, values)| Series {
                name,
                columns: columns.clone(),
                values,
                partial: None,
            })
            .collect();
        Ok(QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series,
                partial: None,
            }],
            format: self.format,
        })
    }
}

/// Convert an epoch time in nanoseconds to the provided precision
fn convert_ns_epoch(value: Value, precision: Precision) -> Result<Value, anyhow::Error> {
    let epoch_ns = value
        .as_i64()
        .context("the provided nanosecond epoch time was not a valid i64")?;
    Ok(match precision {
        Precision::Nanoseconds => epoch_ns,
        Precision::Microseconds => epoch_ns / 1_000,
        Precision::Milliseconds => epoch_ns / 1_000_000,
        Precision::Seconds => epoch_ns / 1_000_000_000,
        Precision::Minutes => epoch_ns / (1_000_000_000 * 60),
        Precision::Hours => epoch_ns / (1_000_000_000 * 60 * 60),
    }
    .into())
}

/// Converts a value from an Arrow `ArrayRef` at a given row index into a `serde_json::Value`.
///
/// This function handles various Arrow data types, converting them into their corresponding
/// JSON representations. For unsupported data types, it returns an error using the `anyhow` crate.
fn cast_column_value(column: &ArrayRef, row_index: usize) -> Result<Value, anyhow::Error> {
    let value = match column.data_type() {
        DataType::Boolean => Value::Bool(column.as_boolean().value(row_index)),
        DataType::Null => Value::Null,
        DataType::Int8 => Value::Number(column.as_primitive::<Int8Type>().value(row_index).into()),
        DataType::Int16 => {
            Value::Number(column.as_primitive::<Int16Type>().value(row_index).into())
        }
        DataType::Int32 => {
            Value::Number(column.as_primitive::<Int32Type>().value(row_index).into())
        }
        DataType::Int64 => {
            Value::Number(column.as_primitive::<Int64Type>().value(row_index).into())
        }
        DataType::UInt8 => {
            Value::Number(column.as_primitive::<UInt8Type>().value(row_index).into())
        }
        DataType::UInt16 => {
            Value::Number(column.as_primitive::<UInt16Type>().value(row_index).into())
        }
        DataType::UInt32 => {
            Value::Number(column.as_primitive::<UInt32Type>().value(row_index).into())
        }
        DataType::UInt64 => {
            Value::Number(column.as_primitive::<UInt64Type>().value(row_index).into())
        }
        DataType::Float16 => Value::Number(
            serde_json::Number::from_f64(
                column
                    .as_primitive::<Float16Type>()
                    .value(row_index)
                    .to_f64(),
            )
            .context("failed to downcast Float16 column")?,
        ),
        DataType::Float32 => Value::Number(
            serde_json::Number::from_f64(
                column.as_primitive::<Float32Type>().value(row_index).into(),
            )
            .context("failed to downcast Float32 column")?,
        ),
        DataType::Float64 => Value::Number(
            serde_json::Number::from_f64(column.as_primitive::<Float64Type>().value(row_index))
                .context("failed to downcast Float64 column")?,
        ),
        DataType::Utf8 => Value::String(column.as_string::<i32>().value(row_index).to_string()),
        DataType::LargeUtf8 => {
            Value::String(column.as_string::<i64>().value(row_index).to_string())
        }
        DataType::Dictionary(key, value) => match (key.as_ref(), value.as_ref()) {
            (DataType::Int32, DataType::Utf8) => {
                let dict_array = column.as_dictionary::<Int32Type>();
                let keys = dict_array.keys();
                let values = as_string_array(dict_array.values());
                Value::String(values.value(keys.value(row_index) as usize).to_string())
            }
            _ => Value::Null,
        },
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Value::String(
            DateTime::from_timestamp_nanos(
                column
                    .as_primitive::<TimestampNanosecondType>()
                    .value(row_index),
            )
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Value::String(
            DateTime::from_timestamp_micros(
                column
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row_index),
            )
            .context("failed to downcast TimestampMicrosecondType column")?
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Value::String(
            DateTime::from_timestamp_millis(
                column
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row_index),
            )
            .context("failed to downcast TimestampNillisecondType column")?
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        DataType::Timestamp(TimeUnit::Second, None) => Value::String(
            DateTime::from_timestamp(
                column
                    .as_primitive::<TimestampSecondType>()
                    .value(row_index),
                0,
            )
            .context("failed to downcast TimestampSecondType column")?
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        t => bail!("Unsupported data type: {:?}", t),
    };
    Ok(value)
}

impl Stream for QueryResponseStream {
    type Item = Result<QueryResponse, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.buffer.can_flush() {
            let has_more_data = ready!(Pin::new(&mut self.input).poll_peek(cx)).is_some();
            let stream_finished = false;
            return Poll::Ready(Some(Ok(self.flush_one(has_more_data, stream_finished))));
        }
        // poll the input record batch stream:
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if let Err(e) = self.buffer_record_batch(batch) {
                    return Poll::Ready(Some(Err(e)));
                }
                if self.buffer.can_flush() {
                    let has_more_data = ready!(Pin::new(&mut self.input).poll_peek(cx)).is_some();
                    Poll::Ready(Some(Ok(self.flush_one(has_more_data, true))))
                } else {
                    // otherwise, we want to poll again in order to pull more
                    // batches from the input record batch stream:
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => {
                if !self.buffer.is_empty() {
                    // we only get here if we are not operating in chunked mode and
                    // we need to flush the entire buffer at once, OR if we are in chunked
                    // mode, and there is less than a chunk's worth of records left
                    //
                    // this is why the input stream is fused, because we will end up
                    // polling the input stream again if we end up here.
                    Poll::Ready(Some(self.flush_all()))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream::{self, StreamExt};
    use serde_json::json;
    use std::sync::Arc;

    fn times(vals: &[i64]) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from_iter_values(
            vals.iter().cloned(),
        ))
    }

    fn strs<T: AsRef<str>>(vals: &[Option<T>]) -> ArrayRef {
        Arc::new(StringArray::from_iter(vals))
    }

    fn create_test_record_batch() -> Vec<Result<RecordBatch, DataFusionError>> {
        let meta = serde_json::to_string(&json!({
            "measurement_column_index": 0,
            "tag_key_columns": [],
        }))
        .unwrap();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("iox::measurement", DataType::Utf8, false),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("value", DataType::Utf8, true),
            ],
            HashMap::from([("iox::influxql::group_key::metadata".to_owned(), meta)]),
        ));
        let record_batch_0 = Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                strs(&[Some("cpu"), Some("cpu")]),
                times(&[1157082300000000000, 1157082310000000000]),
                strs(&[Some("cpu0"), Some("cpu0")]),
            ],
        )
        .unwrap());

        let record_batch_1 = Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                strs(&[Some("cpu"), Some("cpu"), Some("cpu")]),
                times(&[
                    1157082300000000000,
                    1157082400000000000,
                    1157082320000000000,
                ]),
                strs(&[Some("cpu0"), Some("cpu0"), Some("cpu2")]),
            ],
        )
        .unwrap());

        let record_batch_2 = Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                strs(&[Some("mem"), Some("mem")]),
                times(&[1157082500000000000, 1157082420000000000]),
                strs(&[Some("mem0"), Some("mem2")]),
            ],
        )
        .unwrap());

        vec![record_batch_0, record_batch_1, record_batch_2]
    }

    #[tokio::test]
    async fn test_partial_flag() {
        let batch = create_test_record_batch();
        let schema = batch[0].as_ref().unwrap().schema();
        let input_stream = stream::iter(batch);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(input_stream),
        ));
        let chunk_size = Some(2);
        let mut query_response_stream =
            QueryResponseStream::new(0, input, chunk_size, QueryFormat::Json, None).unwrap();

        // Counters for assertions
        let mut counter = 0;

        while let Some(response) = query_response_stream.next().await {
            match response {
                Ok(resp) => {
                    println!("Received response: {:?}", resp);

                    match counter {
                        0 => {
                            assert!(resp.results[0].partial.unwrap());
                            assert!(resp.results[0].series[0].partial.unwrap());
                            assert_eq!(resp.results[0].series[0].name, "cpu");
                            assert_eq!(resp.results[0].series[0].values.len(), 2);
                        }
                        1 => {
                            assert!(resp.results[0].partial.unwrap());
                            assert!(resp.results[0].series[0].partial.unwrap());
                            assert_eq!(resp.results[0].series[0].name, "cpu");
                            assert_eq!(resp.results[0].series[0].values.len(), 2);
                        }
                        2 => {
                            assert!(resp.results[0].partial.unwrap());
                            assert_eq!(resp.results[0].series[0].partial, None);
                            assert_eq!(resp.results[0].series[0].name, "cpu");
                            assert_eq!(resp.results[0].series[0].values.len(), 1);
                        }
                        3 => {
                            assert_eq!(resp.results[0].partial, None);
                            assert_eq!(resp.results[0].series[0].partial, None);
                            assert_eq!(resp.results[0].series[0].name, "mem");
                            assert_eq!(resp.results[0].series[0].values.len(), 2);
                        }
                        _ => (),
                    }

                    counter += 1;
                }
                Err(err) => panic!("Error while polling stream: {:?}", err),
            }
        }

        // Ensure we received exactly 4 responses
        assert_eq!(counter, 4, "Expected 4 responses, but received {}", counter);
    }

    #[tokio::test]
    async fn test_partial_flag_one_stream() {
        let meta = serde_json::to_string(&json!({
            "measurement_column_index": 0,
            "tag_key_columns": [],
        }))
        .unwrap();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("iox::measurement", DataType::Utf8, false),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("value", DataType::Utf8, true),
            ],
            HashMap::from([("iox::influxql::group_key::metadata".to_owned(), meta)]),
        ));
        let record_batch_0 = Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                strs(&[Some("cpu"), Some("cpu")]),
                times(&[1157082300000000000, 1157082310000000000]),
                strs(&[Some("cpu0"), Some("cpu0")]),
            ],
        )
        .unwrap());

        let batch = vec![record_batch_0];
        let schema = batch[0].as_ref().unwrap().schema();
        let input_stream = stream::iter(batch);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(input_stream),
        ));
        let chunk_size = Some(1);
        let mut query_response_stream =
            QueryResponseStream::new(0, input, chunk_size, QueryFormat::Json, None).unwrap();

        // Counters for assertions
        let mut counter = 0;

        while let Some(response) = query_response_stream.next().await {
            match response {
                Ok(resp) => {
                    println!("Received response: {:?}", resp);

                    match counter {
                        0 => {
                            assert!(resp.results[0].partial.unwrap());
                            assert!(resp.results[0].series[0].partial.unwrap());
                            assert_eq!(resp.results[0].series[0].name, "cpu");
                            assert_eq!(resp.results[0].series[0].values.len(), 1);
                        }
                        1 => {
                            assert_eq!(resp.results[0].partial, None);
                            assert_eq!(resp.results[0].series[0].partial, None);
                            assert_eq!(resp.results[0].series[0].name, "cpu");
                            assert_eq!(resp.results[0].series[0].values.len(), 1);
                        }
                        _ => (),
                    }

                    counter += 1;
                }
                Err(err) => panic!("Error while polling stream: {:?}", err),
            }
        }

        assert_eq!(counter, 2, "Expected 2 responses, but received {}", counter);
    }
}
