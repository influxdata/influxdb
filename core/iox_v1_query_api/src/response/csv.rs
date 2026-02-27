//! InfluxDB v1 compatible CSV streaming output for InfluxQL queries.

use super::{SeriesChunk, SeriesChunkStream};
use crate::error::Error;
use crate::types::Precision;
use crate::{Result, StatementFuture, types::Statement, value::ValueType};
use bytes::{Bytes, BytesMut};
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, ready};
use iox_query::query_log::PermitAndToken;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::warn;

/// A stream of CSV data produced by executing InfluxQL statements.
pub(crate) struct CsvStream {
    statements: Vec<Pin<StatementFuture>>,
    statement_id: usize,
    current_statement: Option<(
        Option<PermitAndToken>,
        SeriesChunkStream<SendableRecordBatchStream>,
    )>,
    add_headers: bool,
    add_newline: bool,
    epoch: Precision,
}

impl CsvStream {
    pub(crate) fn new(statements: Vec<StatementFuture>) -> Self {
        let statements = statements.into_iter().map(Box::into_pin).collect();
        Self {
            statements,
            statement_id: 0,
            current_statement: None,
            add_headers: false,
            add_newline: false,
            epoch: Precision::Nanoseconds,
        }
    }

    pub(crate) fn with_epoch(mut self, epoch: Option<Precision>) -> Self {
        self.epoch = epoch.unwrap_or(Precision::Nanoseconds);
        self
    }

    fn poll_statement(&mut self, cx: &mut Context<'_>) -> Poll<Result<Statement>> {
        self.statements[self.statement_id].as_mut().poll(cx)
    }

    fn poll_next_chunk(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<SeriesChunk>>> {
        let (_, stream) = self
            .current_statement
            .as_mut()
            .expect("no active SeriesChunkStream");
        Pin::new(stream).poll_next(cx).map_err(Error::from)
    }
}

impl Stream for CsvStream {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.statement_id >= this.statements.len() {
            return Poll::Ready(None);
        }

        if this.current_statement.is_none() {
            let res = ready!(this.poll_statement(cx));
            let Statement {
                schema,
                permit_state,
                stream,
            } = match res {
                Ok(v) => v,
                Err(e) => {
                    warn!(error=%e, "Error executing query");
                    this.statement_id += 1;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            };
            let stream = match SeriesChunkStream::try_new(stream, schema) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error=%e, "Error creating SeriesChunk stream");
                    if let Some(permit_state) = permit_state {
                        permit_state.query_completed_token.fail();
                    }
                    this.statement_id += 1;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            };
            this.add_headers = true;
            this.add_newline = this.statement_id != 0;
            this.current_statement = Some((permit_state, stream));
        }
        assert!(this.current_statement.is_some());
        match ready!(this.poll_next_chunk(cx)) {
            None => {
                let (permit_state, _) = this.current_statement.take().unwrap();
                if let Some(permit_state) = permit_state {
                    permit_state.query_completed_token.success();
                }
                this.statement_id += 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Ok(chunk)) => {
                let mut chunk = CsvSeriesChunk::new(chunk, this.epoch);
                if this.add_newline {
                    chunk = chunk.with_newline();
                    this.add_newline = false;
                }
                if this.add_headers {
                    chunk = chunk.with_headers();
                    this.add_headers = false;
                }
                Poll::Ready(Some(chunk.into()))
            }
            Some(Err(e)) => {
                warn!(error=%e, "Error streaming SeriesChunk");
                let (permit_state, _) = this.current_statement.take().unwrap();
                if let Some(permit_state) = permit_state {
                    permit_state.query_completed_token.fail();
                }
                this.statement_id += 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// A chunk of CSV data which represents part of the result of executing
/// an InfluxQL statement. Each chunk is for a single series, but may
/// not contain a complete series.
#[derive(Debug)]
pub(crate) struct CsvSeriesChunk {
    chunk: SeriesChunk,
    emit_headers: bool,
    emit_newline: bool,
    epoch: Precision,
}

impl CsvSeriesChunk {
    fn new(chunk: SeriesChunk, epoch: Precision) -> Self {
        Self {
            chunk,
            emit_headers: false,
            emit_newline: false,
            epoch,
        }
    }

    fn with_headers(mut self) -> Self {
        self.emit_headers = true;
        self
    }

    fn with_newline(mut self) -> Self {
        self.emit_newline = true;
        self
    }
}

impl From<CsvSeriesChunk> for Bytes {
    fn from(value: CsvSeriesChunk) -> Self {
        let mut bytes = BytesMut::new();
        let epoch = value.epoch;
        if value.emit_newline {
            bytes.extend_from_slice(b"\n");
        }
        if value.emit_headers {
            // Measurement name and tag headers are always present.
            bytes.extend_from_slice(b"name,tags");
            for column in value.chunk.columns() {
                bytes.extend_from_slice(format!(",{column}").as_bytes());
            }
            bytes.extend_from_slice(b"\n");
        }
        let measurement = csv_escape(value.chunk.measurement());
        let mut tags = String::new();
        for (k, v) in value.chunk.tags() {
            tags.push_str(k.as_ref());
            tags.push('=');
            tags.push_str(&v);
            tags.push(',');
        }
        if !tags.is_empty() {
            tags.pop(); // Remove trailing comma.
        }
        let tags = csv_escape(tags);

        for row in value.chunk.into_iter() {
            bytes.extend_from_slice(measurement.as_bytes());
            bytes.extend_from_slice(b",");
            bytes.extend_from_slice(tags.as_bytes());
            for value in row.into_iter() {
                bytes.extend_from_slice(b",");
                match value.value_type() {
                    // NOTE(hiltontj): legacy /query API respects the `epoch` parameter, but only
                    // returns timestamps in epoch format, not in RFC3339.
                    //
                    // See: <https://docs.influxdata.com/influxdb/v1/tools/api/#request-query-results-in-csv-format>
                    ValueType::Timestamp(_) => {
                        let ts = value.as_timestamp_opt().unwrap_or_default();
                        let ts = match epoch {
                            Precision::Nanoseconds => ts.timestamp_nanos_opt().unwrap_or_default(),
                            Precision::Microseconds => ts.timestamp_micros(),
                            Precision::Milliseconds => ts.timestamp_millis(),
                            Precision::Seconds => ts.timestamp(),
                            Precision::Minutes => ts.timestamp() / 60,
                            Precision::Hours => ts.timestamp() / (60 * 60),
                            Precision::Days => ts.timestamp() / (60 * 60 * 24),
                            Precision::Weeks => ts.timestamp() / (60 * 60 * 24 * 7),
                        };
                        bytes.extend_from_slice(ts.to_string().as_bytes());
                    }
                    _ => {
                        bytes.extend_from_slice(csv_escape(format!("{value}")).as_bytes());
                    }
                }
            }
            bytes.extend_from_slice(b"\n");
        }

        bytes.into()
    }
}

fn csv_escape(s: String) -> String {
    if s.contains(',') || s.contains('"') {
        format!("\"{}\"", s.replace("\"", "\"\""))
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::tests::{Column, make_statement};
    use super::*;
    use arrow::{
        array::{DictionaryArray, Float64Array, StringArray, TimestampNanosecondArray},
        datatypes::Int32Type,
    };
    use datafusion::error::DataFusionError;
    use futures::StreamExt;
    use iox_query::QueryDatabase;
    use iox_query::exec::IOxSessionContext;
    use iox_query::query_log::QueryLog;
    use iox_query::test::TestDatabaseStore;
    use iox_time::SystemProvider;

    #[tokio::test]
    async fn no_statements() {
        let mut stream = CsvStream::new(vec![]);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn single_statement_no_group() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t1",
                    group_by: false,
                    projected: false,
                },
                Column::Field { name: "f1" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 1000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1a", "t1a",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let stream = CsvStream::new(vec![statement]);
        let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
        let output = String::from_utf8(output).unwrap();
        insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000,t1a,1
        m1,,2000000000,t1a,2
        m1,,3000000000,t1a,3
        m2,,1000000000,t1a,4
        ");
    }

    #[tokio::test]
    async fn single_statement_tag_group() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "f1" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 1000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1a", "t1a",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let stream = CsvStream::new(vec![statement]);
        let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
        let output = String::from_utf8(output).unwrap();
        insta::assert_snapshot!(output, @r"
        name,tags,time,f1
        m1,t1=t1a,1000000000,1
        m1,t1=t1a,2000000000,2
        m1,t1=t1a,3000000000,3
        m2,t1=t1a,1000000000,4
        ");
    }

    #[tokio::test]
    async fn single_statement_tag_group_projected() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t1",
                    group_by: true,
                    projected: true,
                },
                Column::Field { name: "f1" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 1000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1a", "t1a",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let stream = CsvStream::new(vec![statement]);
        let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
        let output = String::from_utf8(output).unwrap();
        insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,t1=t1a,1000000000,t1a,1
        m1,t1=t1a,2000000000,t1a,2
        m1,t1=t1a,3000000000,t1a,3
        m2,t1=t1a,1000000000,t1a,4
        ");
    }

    #[tokio::test]
    async fn single_statement_tag_group_multiple_tags() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t1",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "t2",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "f1" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 1000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1a", "t1a",
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "\"t2a\"", "\"t2a\"", "\"t2b\"", "\"t2a\"",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let stream = CsvStream::new(vec![statement]);
        let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
        let output = String::from_utf8(output).unwrap();
        insta::assert_snapshot!(output, @r#"
        name,tags,time,f1
        m1,"t1=t1a,t2=""t2a""",1000000000,1
        m1,"t1=t1a,t2=""t2a""",2000000000,2
        m1,"t1=t1a,t2=""t2b""",3000000000,3
        m2,"t1=t1a,t2=""t2a""",1000000000,4
        "#);
    }

    #[tokio::test]
    async fn single_statement_infinite_value() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t1",
                    group_by: false,
                    projected: false,
                },
                Column::Field { name: "f1" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 1000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1a", "t1a",
                ])),
                Arc::new(Float64Array::from(vec![
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::NAN,
                ])),
            ],
        );
        let stream = CsvStream::new(vec![statement]);
        let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
        let output = String::from_utf8(output).unwrap();
        insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000,t1a,-inf
        m1,,2000000000,t1a,inf
        m1,,3000000000,t1a,-inf
        m2,,1000000000,t1a,NaN
        ");
    }

    #[tokio::test]
    async fn multiple_statements() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement1 = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t1",
                    group_by: false,
                    projected: false,
                },
                Column::Field { name: "f1" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 1000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1a", "t1a",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let statement2 = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t2",
                    group_by: false,
                    projected: false,
                },
                Column::Field { name: "f2" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m3", "m3", "m3", "m3"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 4000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t2a", "t2a", "t2a", "t2a",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let stream = CsvStream::new(vec![statement1, statement2]);
        let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
        let output = String::from_utf8(output).unwrap();
        insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000,t1a,1
        m1,,2000000000,t1a,2
        m1,,3000000000,t1a,3
        m2,,1000000000,t1a,4

        name,tags,time,t2,f2
        m3,,1000000000,t2a,1
        m3,,2000000000,t2a,2
        m3,,3000000000,t2a,3
        m3,,4000000000,t2a,4
        ");
    }

    #[tokio::test]
    async fn multiple_statements_with_error() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement1 = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t1",
                    group_by: false,
                    projected: false,
                },
                Column::Field { name: "f1" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 1000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1a", "t1a",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let statement2 = make_statement(
            &db,
            &ctx,
            &log,
            [
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "t2",
                    group_by: false,
                    projected: false,
                },
                Column::Field { name: "f2" },
            ],
            vec![
                Arc::new(StringArray::from(vec!["m3", "m3", "m3", "m3"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000, 4000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t2a", "t2a", "t2a", "t2a",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        );
        let stream = CsvStream::new(vec![
            statement1,
            Box::new(async { Err(DataFusionError::Internal("test error".to_string()))? }),
            statement2,
        ]);
        let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
        let output = String::from_utf8(output).unwrap();
        insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000,t1a,1
        m1,,2000000000,t1a,2
        m1,,3000000000,t1a,3
        m2,,1000000000,t1a,4

        name,tags,time,t2,f2
        m3,,1000000000,t2a,1
        m3,,2000000000,t2a,2
        m3,,3000000000,t2a,3
        m3,,4000000000,t2a,4
        ");
    }

    #[tokio::test]
    async fn test_csv_epoch_handling() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let statement = || {
            make_statement(
                &db,
                &ctx,
                &log,
                [
                    Column::Measurement,
                    Column::Time,
                    Column::Tag {
                        name: "t1",
                        group_by: false,
                        projected: false,
                    },
                    Column::Field { name: "f1" },
                ],
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1_000_000_000_000_000,
                        2_000_000_000_000_000,
                        3_000_000_000_000_000,
                        4_000_000_000_000_000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "t1a", "t1a", "t1a", "t1a",
                    ])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
            )
        };
        {
            let stream = CsvStream::new(vec![statement()]).with_epoch(None);
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000000000,t1a,1
        m1,,2000000000000000,t1a,2
        m1,,3000000000000000,t1a,3
        m2,,4000000000000000,t1a,4
        ");
        }
        {
            let stream = CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Nanoseconds));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000000000,t1a,1
        m1,,2000000000000000,t1a,2
        m1,,3000000000000000,t1a,3
        m2,,4000000000000000,t1a,4
        ");
        }
        {
            let stream =
                CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Microseconds));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000000,t1a,1
        m1,,2000000000000,t1a,2
        m1,,3000000000000,t1a,3
        m2,,4000000000000,t1a,4
        ");
        }
        {
            let stream =
                CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Milliseconds));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r"
        name,tags,time,t1,f1
        m1,,1000000000,t1a,1
        m1,,2000000000,t1a,2
        m1,,3000000000,t1a,3
        m2,,4000000000,t1a,4
        ");
        }
        {
            let stream = CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Seconds));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r#"
            name,tags,time,t1,f1
            m1,,1000000,t1a,1
            m1,,2000000,t1a,2
            m1,,3000000,t1a,3
            m2,,4000000,t1a,4
            "#);
        }
        {
            let stream = CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Minutes));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r#"
            name,tags,time,t1,f1
            m1,,16666,t1a,1
            m1,,33333,t1a,2
            m1,,50000,t1a,3
            m2,,66666,t1a,4
            "#);
        }
        {
            let stream = CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Hours));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r#"
            name,tags,time,t1,f1
            m1,,277,t1a,1
            m1,,555,t1a,2
            m1,,833,t1a,3
            m2,,1111,t1a,4
            "#);
        }
        {
            let stream = CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Days));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r#"
            name,tags,time,t1,f1
            m1,,11,t1a,1
            m1,,23,t1a,2
            m1,,34,t1a,3
            m2,,46,t1a,4
            "#);
        }
        {
            let stream = CsvStream::new(vec![statement()]).with_epoch(Some(Precision::Weeks));
            let output = stream.map(Bytes::from).map(Vec::<u8>::from).concat().await;
            let output = String::from_utf8(output).unwrap();
            insta::assert_snapshot!(output, @r#"
            name,tags,time,t1,f1
            m1,,1,t1a,1
            m1,,3,t1a,2
            m1,,4,t1a,3
            m2,,6,t1a,4
            "#);
        }
    }
}
