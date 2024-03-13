use std::{
    collections::HashMap,
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
use observability_deps::tracing::{self, debug, trace};
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
        let stream = QueryResponseStream::new(0, stream, chunk_size, pretty)
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

struct QueryResponseStream {
    chunk_name: Option<String>,
    chunk_name_next: Option<String>,
    chunk_buffer: Vec<Row>,
    chunk_pos: usize,
    chunk_size: usize,
    current_batch: Option<RecordBatch>,
    input: Fuse<SendableRecordBatchStream>,
    column_map: HashMap<String, usize>,
    statement_id: usize,
    pretty: bool,
}

const IOX_MEASUREMENT_KEY: &str = "iox::measurement";

impl QueryResponseStream {
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
        let column_map = schema
            .fields
            .iter()
            .map(|f| f.name().to_owned())
            .enumerate()
            .flat_map(|(i, n)| {
                if n != IOX_MEASUREMENT_KEY && i > 0 {
                    Some((n, i - 1))
                } else {
                    None
                }
            })
            .collect();
        Ok(Self {
            chunk_buffer,
            chunk_name: None,
            chunk_name_next: None,
            chunk_pos: 0,
            chunk_size,
            column_map,
            current_batch: None,
            input: input.fuse(),
            pretty,
            statement_id,
        })
    }

    #[tracing::instrument(level = "TRACE", skip_all, ret, err)]
    fn stream_batch(&mut self, batch: RecordBatch) -> Result<bool, anyhow::Error> {
        let batch_row_count = batch.num_rows();
        let batch_slice_length = (self.chunk_size - self.chunk_pos).min(batch_row_count);
        let batch_slice = batch.slice(0, batch_slice_length);
        self.chunk_pos = self.chunk_pos + batch_slice_length;
        let json_rows = record_batches_to_json_rows(&[&batch_slice])
            .context("failed to convert RecordBatch slice to JSON")?;
        for (i, json_row) in json_rows.into_iter().enumerate() {
            let mut row = vec![Value::Null; self.column_map.len()];
            for (k, v) in json_row {
                if k == IOX_MEASUREMENT_KEY && self.chunk_name.is_none() {
                    trace!(k, ?v, "setting measurement name for first time");
                    self.chunk_name.replace(
                        v.as_str()
                            .context("iox::measurement value was not a string")?
                            .to_string(),
                    );
                } else if k == IOX_MEASUREMENT_KEY
                    && self.chunk_name.as_ref().is_some_and(|n| *n != v)
                {
                    trace!(k, ?v, "updating measurement name");
                    // we are starting a new measurement, so we want to stop here, and flush
                    // what is currently in the buffer for the previous measurement before
                    // streaming from this point on
                    self.current_batch
                        .replace(batch.slice(i, batch.num_rows() - i));
                    self.chunk_name_next.replace(v.as_str().unwrap().to_owned());
                    return Ok(true);
                } else if k == IOX_MEASUREMENT_KEY {
                    trace!(k, ?v, "ignoring measurement name");
                    continue;
                } else {
                    trace!(k, ?v, "setting column value");
                    let j = self.column_map.get(&k).unwrap();
                    row[*j] = v;
                }
            }
            self.chunk_buffer.push(Row(row));
        }
        if batch_slice_length < batch.num_rows() {
            self.current_batch
                .replace(batch.slice(batch_slice_length, batch.num_rows() - batch_slice_length));
        }
        Ok(false)
    }

    fn flush(&mut self) -> Result<QueryResponse, anyhow::Error> {
        let mut columns = vec!["".to_string(); self.column_map.len()];
        self.column_map
            .iter()
            .for_each(|(k, i)| columns[*i] = k.to_owned());
        let name = if let Some(n) = self.chunk_name_next.take() {
            self.chunk_name.replace(n)
        } else {
            self.chunk_name.clone()
        }
        .context("missing iox::measurement name on flush")?;
        let series = vec![Series {
            name,
            columns,
            values: self.chunk_buffer.drain(..).collect(),
        }];
        // reset the chunk position:
        self.chunk_pos = 0;
        Ok(QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series,
            }],
            pretty: self.pretty,
        })
    }
}

impl Stream for QueryResponseStream {
    type Item = Result<QueryResponse, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check if there are any records left in the current batch, we want to buffer them
        // into the current chunk and potentially stream them before polling the input stream
        if let Some(batch) = self.current_batch.take() {
            match self.stream_batch(batch) {
                Ok(true) => return Poll::Ready(Some(self.flush())),
                Ok(false) => (),
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
            if !self.chunk_buffer.is_empty() && self.chunk_buffer.len() == self.chunk_size {
                return Poll::Ready(Some(self.flush()));
            }
        }
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                debug!(?batch, "received batch from input stream");
                match self.stream_batch(batch) {
                    Ok(true) => return Poll::Ready(Some(self.flush())),
                    Ok(false) => (),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
                if self.chunk_buffer.len() < self.chunk_size {
                    debug!("chunk buffer is not full yet");
                    // We don't have a full chunk yet, hold data in the buffer and continue polling:
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    debug!("chunk buffer is ready to flush");
                    // We have a full chunk, so flush and return it:
                    Poll::Ready(Some(self.flush()))
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => {
                debug!("input stream is empty");
                // If we have data remaining in the buffer, we need to flush one more time,
                // returning Some(...), and then when the stream gets polled again, the chunk
                // buffer will be empty, and we return None.
                //
                // This is the reason the input stream is fused, because we may end up polling
                // again after it has finished.
                if !self.chunk_buffer.is_empty() {
                    debug!("flushing buffer");
                    Poll::Ready(Some(self.flush()))
                } else {
                    debug!("we are done");
                    Poll::Ready(None)
                }
            }
        }
    }
}
