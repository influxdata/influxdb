use std::{
    collections::HashMap,
    iter,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Context as AnyhowContext;
use arrow::record_batch::RecordBatch;
use arrow_json::writer::record_batches_to_json_rows;
use bytes::Bytes;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{ready, stream::Fuse, Stream, StreamExt};
use hyper::{Body, Request, Response};
use influxdb3_write::WriteBuffer;
use iox_time::TimeProvider;
use observability_deps::tracing::debug;
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
            // TODO - support conversion to epoch times
            epoch: _,
            pretty,
            query,
        } = QueryParams::from_request(&req)?;
        debug!(
            ?chunk_size,
            chunked,
            ?database,
            query,
            "handle v1 query API"
        );

        let chunk_size = if chunked {
            chunk_size.or(Some(DEFAULT_CHUNK_SIZE))
        } else {
            None
        };

        let stream = self.query_influxql_inner(database, &query).await?;
        let stream = StatementResultStream::new(0, stream, chunk_size, pretty)
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

#[derive(Debug, Deserialize)]
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
    results: Vec<StatementResult>,
}

#[derive(Debug, Serialize)]
struct StatementResult {
    statement_id: usize,
    series: Vec<Series>,
    #[serde(skip_serializing)]
    pretty: bool,
}

impl From<StatementResult> for Bytes {
    fn from(s: StatementResult) -> Self {
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

struct StatementResultStream {
    chunk_name: Option<String>,
    chunk_columns: Vec<String>,
    chunk_buffer: Vec<Row>,
    chunk_pos: usize,
    chunk_size: usize,
    current_batch: Option<RecordBatch>,
    input: Fuse<SendableRecordBatchStream>,
    statement_id: usize,
    pretty: bool,
}

impl StatementResultStream {
    fn new(
        statement_id: usize,
        input: SendableRecordBatchStream,
        chunk_size: Option<usize>,
        pretty: bool,
    ) -> Result<Self, anyhow::Error> {
        let chunk_buffer = chunk_size
            .map(|size| Vec::with_capacity(size))
            .unwrap_or_else(|| Vec::new());
        let chunk_size = chunk_size.unwrap_or(usize::MAX);
        let schema = input.schema();
        debug!(?schema, "input schema");
        let chunk_columns = schema
            .fields()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect();
        Ok(Self {
            chunk_name: None,
            chunk_columns,
            chunk_buffer,
            chunk_pos: 0,
            chunk_size,
            current_batch: None,
            input: input.fuse(),
            statement_id,
            pretty,
        })
    }

    fn stream_batch(&mut self, batch: RecordBatch) -> Result<(), anyhow::Error> {
        let batch_row_count = batch.num_rows();
        let batch_offset = 0;
        let batch_slice_length =
            (self.chunk_size - self.chunk_pos).min(batch_row_count - batch_offset);
        let base = self.chunk_pos;
        debug!(?batch, %batch_offset, %batch_row_count, %batch_slice_length, %base, "stream batch");
        let batch_slice = batch.slice(batch_offset, batch_slice_length);
        self.chunk_pos = self.chunk_pos + batch_slice_length;
        let json_rows = record_batches_to_json_rows(&[&batch_slice])
            .context("failed to convert RecordBatch slice to JSON")?;
        let column_positions =
            self.chunk_columns
                .iter()
                .enumerate()
                .fold(HashMap::new(), |mut acc, (j, name)| {
                    acc.insert(name.to_owned(), j);
                    acc
                });
        let mut name = None;
        for json_row in json_rows {
            let mut row: Vec<Value> = iter::repeat(Value::Null)
                .take(column_positions.len().checked_sub(1).unwrap_or(0))
                .collect();
            debug!(?row, "new row");
            json_row.into_iter().for_each(|(k, v)| {
                debug!(key = %k, value = %v, ?column_positions, "JSON Row");
                if k == "iox::measurement" {
                    name.replace(v.as_str().unwrap().to_owned());
                } else {
                    let j = column_positions.get(&k).unwrap();
                    row[*j - 1] = v;
                }
            });
            debug!(?row, "updated row");
            self.chunk_buffer.push(Row(row));
        }
        if let Some(name) = name {
            self.chunk_name.replace(name);
        }
        let new_batch_offset = batch_offset + batch_slice_length;
        if new_batch_offset < batch.num_rows() {
            self.current_batch
                .replace(batch.slice(new_batch_offset, batch.num_rows() - new_batch_offset));
        }
        Ok(())
    }

    fn flush(&mut self) -> StatementResult {
        let series = vec![Series {
            name: self.chunk_name.clone().unwrap(),
            columns: self
                .chunk_columns
                .iter()
                .filter_map(|c| {
                    if c == "iox::measurement" {
                        None
                    } else {
                        Some(c)
                    }
                })
                .cloned()
                .collect(),
            values: self.chunk_buffer.drain(..).collect(),
        }];
        self.chunk_pos = 0;
        StatementResult {
            statement_id: self.statement_id,
            series,
            pretty: self.pretty,
        }
    }
}

impl Stream for StatementResultStream {
    type Item = Result<StatementResult, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check if there are any records left in the current batch, we want to stream them
        // or buffer them into the current chunk before polling the input stream for more...
        if let Some(batch) = self.current_batch.take() {
            if let Err(e) = self.stream_batch(batch) {
                return Poll::Ready(Some(Err(e)));
            }
            if !self.chunk_buffer.is_empty() && self.chunk_buffer.len() == self.chunk_size {
                return Poll::Ready(Some(Ok(self.flush())));
            }
        }
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if let Err(e) = self.stream_batch(batch) {
                    return Poll::Ready(Some(Err(e)));
                }
                if self.chunk_buffer.len() < self.chunk_size {
                    // We don't have a full chunk yet, hold data in the buffer and continue polling:
                    Poll::Pending
                } else {
                    // We have a full chunk, so flush and return it:
                    Poll::Ready(Some(Ok(self.flush())))
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
                if !self.chunk_buffer.is_empty() {
                    Poll::Ready(Some(Ok(self.flush())))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}
