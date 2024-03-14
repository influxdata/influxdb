use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Context as AnyhowContext;
use arrow::{
    compute::{cast_with_options, CastOptions},
    record_batch::RecordBatch,
};
use arrow_json::writer::record_batches_to_json_rows;

use arrow_schema::DataType;
use bytes::Bytes;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{ready, stream::Fuse, Stream, StreamExt};
use hyper::{Body, Request, Response};
use influxdb3_write::WriteBuffer;
use iox_time::TimeProvider;
use observability_deps::tracing::debug;
use schema::{INFLUXQL_MEASUREMENT_COLUMN_NAME, TIME_COLUMN_NAME};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::QueryExecutor;

use super::{Error, HttpApi, Result};

const DEFAULT_CHUNK_SIZE: usize = 10_000;

impl<W, Q, T> HttpApi<W, Q, T>
where
    W: WriteBuffer,
    Q: QueryExecutor,
    T: TimeProvider,
    Error: From<<Q as QueryExecutor>::Error>,
{
    pub(super) async fn v1_query(&self, req: Request<Body>) -> Result<Response<Body>> {
        let QueryParams {
            chunk_size,
            chunked,
            database,
            epoch,
            pretty,
            query,
        } = QueryParams::from_request(&req)?;
        debug!(
            ?chunk_size,
            chunked,
            ?database,
            query,
            ?epoch,
            "handle v1 query API"
        );

        let chunk_size = if chunked {
            chunk_size.or(Some(DEFAULT_CHUNK_SIZE))
        } else {
            None
        };

        let stream = self.query_influxql_inner(database, &query).await?;
        let stream = QueryResponseStream::new(0, stream, chunk_size, pretty, epoch)
            .map_err(QueryError::Unexpected)?;
        let body = Body::wrap_stream(stream);

        Ok(Response::builder().status(200).body(body).unwrap())
    }
}

/// Query parameters for the v1/query API
///
/// The original API supports a `u` parameter, for "username", as well as a `p`,
/// for "password". The password is extracted upstream, and username is ignored.
#[derive(Debug, Deserialize)]
struct QueryParams {
    #[serde(default)]
    chunked: bool,
    chunk_size: Option<usize>,
    #[serde(rename = "db")]
    database: Option<String>,
    #[allow(dead_code)]
    epoch: Option<Precision>,
    #[serde(default)]
    pretty: bool,
    #[serde(rename = "q")]
    query: String,
}

impl QueryParams {
    fn from_request(req: &Request<Body>) -> Result<Self> {
        let query = req.uri().query().ok_or(Error::MissingQueryParams)?;
        serde_urlencoded::from_str(query).map_err(Into::into)
    }
}

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

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("unexpected query error: {0}")]
    Unexpected(#[from] anyhow::Error),
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    results: Vec<StatementResponse>,
    #[serde(skip_serializing)]
    pretty: bool,
}

#[derive(Debug, Serialize)]
struct StatementResponse {
    statement_id: usize,
    series: Vec<Series>,
}

impl From<QueryResponse> for Bytes {
    fn from(s: QueryResponse) -> Self {
        if s.pretty {
            serde_json::to_vec_pretty(&s)
        } else {
            serde_json::to_vec(&s)
        }
        .map(|mut b| {
            b.extend_from_slice(b"\r\n");
            b
        })
        .expect("valid bytes in statement result")
        .into()
    }
}

#[derive(Debug, Serialize)]
struct Series {
    name: String,
    columns: Vec<String>,
    values: Vec<Row>,
}

#[derive(Debug, Serialize)]
struct Row(Vec<Value>);

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

    fn current_measurement_name(&self) -> Option<&str> {
        self.series.front().map(|(n, _)| n.as_str())
    }

    fn push_next_measurement<S: Into<String>>(&mut self, name: S) {
        self.series.push_front((name.into(), vec![]));
    }

    fn push_row(&mut self, row: Row) -> Result<(), anyhow::Error> {
        self.series
            .front_mut()
            .context("tried to push row with no measurements buffered")?
            .1
            .push(row);
        Ok(())
    }

    fn flush_measurement(&mut self) -> Option<(String, Vec<Row>)> {
        self.series.pop_back()
    }

    fn should_flush(&self) -> bool {
        if let (Some(size), Some(m)) = (self.size, self.series.front()) {
            m.1.len() >= size || self.series.len() > 1
        } else {
            false
        }
    }

    fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    fn calculate_batch_slice_len(&self, batch_row_count: usize) -> usize {
        if let (Some(size), Some(m)) = (self.size, self.series.front()) {
            size.checked_sub(m.1.len())
                .unwrap_or(batch_row_count)
                .min(batch_row_count)
        } else {
            batch_row_count
        }
    }
}

struct QueryResponseStream {
    buffer: ChunkBuffer,
    current_batch: Option<RecordBatch>,
    input: Fuse<SendableRecordBatchStream>,
    column_map: HashMap<String, usize>,
    statement_id: usize,
    pretty: bool,
    epoch: Option<Precision>,
}

impl QueryResponseStream {
    fn new(
        statement_id: usize,
        input: SendableRecordBatchStream,
        chunk_size: Option<usize>,
        pretty: bool,
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
            current_batch: None,
            input: input.fuse(),
            pretty,
            statement_id,
            epoch,
        })
    }

    fn stream_batch(&mut self, batch: RecordBatch) -> Result<(), anyhow::Error> {
        let batch_slice_length = self.buffer.calculate_batch_slice_len(batch.num_rows());
        let mut batch_slice = batch.slice(0, batch_slice_length);
        if self.epoch.is_some() {
            batch_slice = RecordBatch::try_from_iter(batch_slice.schema().fields.iter().map(|f| {
                let name = f.name();
                let column = batch.column_by_name(name).unwrap();
                (
                    name,
                    if name == TIME_COLUMN_NAME {
                        // unwrap is safe here because the time column can cast cheaply to Int64
                        cast_with_options(column, &DataType::Int64, &CastOptions::default())
                            .unwrap()
                    } else {
                        column.clone()
                    },
                )
            }))
            .context("failed to cast batch time column")?;
        }
        let json_rows = record_batches_to_json_rows(&[&batch_slice])
            .context("failed to convert RecordBatch slice to JSON")?;
        for json_row in json_rows {
            let mut row = vec![Value::Null; self.column_map.len()];
            for (k, v) in json_row {
                if k == INFLUXQL_MEASUREMENT_COLUMN_NAME
                    && self.buffer.current_measurement_name().is_none()
                {
                    self.buffer.push_next_measurement(
                        v.as_str()
                            .context("iox::measurement value was not a string")?,
                    );
                } else if k == INFLUXQL_MEASUREMENT_COLUMN_NAME
                    && self
                        .buffer
                        .current_measurement_name()
                        .is_some_and(|n| *n != v)
                {
                    // here we are starting on the next measurement, so push it into the buffer
                    self.buffer
                        .push_next_measurement(v.as_str().unwrap().to_owned());
                } else if k == INFLUXQL_MEASUREMENT_COLUMN_NAME {
                    continue;
                } else {
                    let j = self.column_map.get(&k).unwrap();
                    row[*j] = if let (Some(precision), TIME_COLUMN_NAME) = (self.epoch, k.as_str())
                    {
                        convert_ns_epoch(v, precision)?
                    } else {
                        v
                    };
                }
            }
            self.buffer.push_row(Row(row))?;
        }
        debug!(
            batch_slice_length,
            batch_num_rows = batch.num_rows(),
            "done streaming"
        );
        if batch_slice_length < batch.num_rows() {
            debug!("replacing");
            self.current_batch
                .replace(batch.slice(batch_slice_length, batch.num_rows() - batch_slice_length));
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<QueryResponse, anyhow::Error> {
        let mut columns = vec!["".to_string(); self.column_map.len()];
        self.column_map
            .iter()
            .for_each(|(k, i)| columns[*i] = k.to_owned());
        let (name, values) = self
            .buffer
            .flush_measurement()
            .context("no values to flush")?;
        let series = vec![Series {
            name,
            columns,
            values,
        }];
        // reset the chunk position:
        Ok(QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series,
            }],
            pretty: self.pretty,
        })
    }

    fn flush_all(&mut self) -> Result<QueryResponse, anyhow::Error> {
        let mut columns = vec!["".to_string(); self.column_map.len()];
        self.column_map
            .iter()
            .for_each(|(k, i)| columns[*i] = k.to_owned());
        let mut series = Vec::new();
        while let Some((name, values)) = self.buffer.flush_measurement() {
            series.push(Series {
                name,
                columns: columns.clone(),
                values,
            });
        }
        Ok(QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series,
            }],
            pretty: self.pretty,
        })
    }
}

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

impl Stream for QueryResponseStream {
    type Item = Result<QueryResponse, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check if there are any records left in the current batch, we want to buffer them
        // into the current chunk and potentially stream them before polling the input stream
        if let Some(batch) = self.current_batch.take() {
            debug!(?batch, "stream current batch");
            if let Err(e) = self.stream_batch(batch) {
                return Poll::Ready(Some(Err(e)));
            }
            if self.buffer.should_flush() {
                debug!("flush current batch");
                return Poll::Ready(Some(self.flush()));
            }
        }
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                debug!(?batch, "stream input batch");
                if let Err(e) = self.stream_batch(batch) {
                    return Poll::Ready(Some(Err(e)));
                }
                if self.buffer.should_flush() {
                    debug!("flush recent batch");
                    Poll::Ready(Some(self.flush()))
                } else {
                    debug!("pending");
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => {
                // If we have data remaining in the buffer, we need to flush one more time,
                // returning Some(...), and then when the stream gets polled again, the chunk
                // buffer will be empty, and we return None.
                //
                // This is the reason the input stream is fused, because we may end up polling
                // again after it has finished.
                if !self.buffer.is_empty() {
                    debug!("flushing buffer");
                    Poll::Ready(Some(self.flush_all()))
                } else {
                    debug!("we are done");
                    Poll::Ready(None)
                }
            }
        }
    }
}
