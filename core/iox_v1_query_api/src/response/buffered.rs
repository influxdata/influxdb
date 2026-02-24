//! Streams for producing responses where chunking is not enabled.

use super::{Response, SeriesChunk, SeriesChunkMergeStream, SeriesChunkStream, StatementResult};
use crate::{Result, types::Statement};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, ready};
use iox_query::query_log::PermitAndToken;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// A stream of one [Response] value where the response contains the
/// result data for all the corresponding statements.
pub(crate) struct BufferedResponseStream {
    statements: Vec<Pin<Box<dyn Future<Output = Result<Statement>> + Send>>>,
    statement_id: usize,
    current_statement: Option<(
        Option<PermitAndToken>,
        BufferedResultStream<SeriesChunkStream<SendableRecordBatchStream>>,
    )>,
    // Buffer the statment results for all the statements in one response
    response: Option<Response>,
}

impl BufferedResponseStream {
    pub(crate) fn new(statements: Vec<Box<dyn Future<Output = Result<Statement>> + Send>>) -> Self {
        let response = (!statements.is_empty()).then_some(Response::default());

        let statements = statements.into_iter().map(Box::into_pin).collect();
        Self {
            statements,
            statement_id: 0,
            current_statement: None,
            response,
        }
    }

    fn poll_statement(&mut self, cx: &mut Context<'_>) -> Poll<Result<Statement>> {
        self.statements[self.statement_id].as_mut().poll(cx)
    }

    fn poll_next_result(&mut self, cx: &mut Context<'_>) -> Poll<Option<StatementResult>> {
        let (_, stream) = self
            .current_statement
            .as_mut()
            .expect("current_statement is None");
        Pin::new(stream).poll_next(cx)
    }

    fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<Response>> {
        if self.statement_id >= self.statements.len() {
            return Poll::Ready(self.response.take());
        }

        if self.current_statement.is_some() {
            match ready!(self.poll_next_result(cx)) {
                Some(result) => {
                    if result.is_error() {
                        let (permit_state, _) = self.current_statement.take().unwrap(); // safe to unwrap because we just checked above
                        if let Some(permit_state) = permit_state {
                            permit_state.query_completed_token.fail();
                        }
                        self.statement_id += 1;
                    }
                    self.response.as_mut().unwrap().add_result(result); // safe to unwrap because we just checked above
                }
                None => {
                    let (permit_state, _) = self.current_statement.take().unwrap(); // safe to unwrap because we just checked above
                    if let Some(permit_state) = permit_state {
                        permit_state.query_completed_token.success();
                    }
                    self.statement_id += 1;
                }
            }
        } else {
            match ready!(self.poll_statement(cx)) {
                Ok(Statement {
                    schema,
                    permit_state,
                    stream,
                }) => match SeriesChunkStream::try_new(stream, schema) {
                    Ok(stream) => {
                        self.current_statement = Some((
                            permit_state,
                            BufferedResultStream::new(stream, self.statement_id),
                        ));
                    }
                    Err(e) => {
                        if let Some(permit_state) = permit_state {
                            permit_state.query_completed_token.fail();
                        }
                        let mut result = StatementResult::new(self.statement_id);
                        result.set_error(e.to_string());
                        self.statement_id += 1;
                        self.response.as_mut().unwrap().add_result(result); // safe to unwrap because we just checked above
                    }
                },
                Err(e) => {
                    let mut result = StatementResult::new(self.statement_id);
                    result.set_error(e.to_string());
                    self.statement_id += 1;
                    self.response.as_mut().unwrap().add_result(result); // safe to unwrap because we just checked above
                }
            }
        }
        self.poll_next_unpin(cx)
    }
}

impl Stream for BufferedResponseStream {
    type Item = Response;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.poll_next_unpin(cx)
    }
}

pub(super) struct BufferedResultStream<S> {
    inner: SeriesChunkMergeStream<S>,
    result: Option<StatementResult>,
}

impl<S> BufferedResultStream<S> {
    pub(super) fn new(stream: S, statement_id: usize) -> Self {
        let inner = SeriesChunkMergeStream::new(stream, None);
        Self {
            inner,
            result: Some(StatementResult::new(statement_id)),
        }
    }
}

impl<S> BufferedResultStream<S>
where
    S: Stream<Item = Result<SeriesChunk>> + Unpin,
{
    fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<StatementResult>> {
        loop {
            if self.result.is_none() {
                return Poll::Ready(None);
            }

            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(chunk)) => {
                    self.result.as_mut().unwrap().add_series(chunk); // safe to unwrap because we just checked above
                }
                Some(Err(e)) => {
                    let mut result = self.result.take().unwrap(); // safe to unwrap because we just checked above
                    result.set_error(e.to_string());
                    return Poll::Ready(Some(result));
                }
                None => return Poll::Ready(self.result.take()),
            }
        }
    }
}

impl<S> Stream for BufferedResultStream<S>
where
    S: Stream<Item = Result<SeriesChunk>> + Unpin,
{
    type Item = StatementResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.poll_next_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use crate::response::ResponseSerializer;
    use crate::types::Precision;

    use super::super::tests::{Column, make_statement};
    use super::*;
    use arrow::array::{DictionaryArray, Float64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::Int32Type;
    use futures::StreamExt;
    use iox_query::QueryDatabase;
    use iox_query::exec::IOxSessionContext;
    use iox_query::query_log::QueryLog;
    use iox_query::test::TestDatabaseStore;
    use iox_time::SystemProvider;
    use serde::ser::Serialize;
    use serde_json::ser::Serializer;
    use std::sync::Arc;

    #[tokio::test]
    async fn no_statements() {
        let mut stream = BufferedResponseStream::new(vec![]);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn single_statement_single_series() {
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
                Arc::new(StringArray::from(vec!["m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![1000000000, 2000000000])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["t1a", "t1a"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        );
        insta::assert_snapshot!(collect_output(BufferedResponseStream::new(vec![statement])).await);
    }

    #[tokio::test]
    async fn single_statement_multi_series() {
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
                Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1b",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        );

        insta::assert_snapshot!(collect_output(BufferedResponseStream::new(vec![statement])).await);
    }

    #[tokio::test]
    async fn many_statements() {
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
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 1.0])),
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
                    "t1a", "t1a", "t1b", "t1b",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 1.0])),
            ],
        );

        insta::assert_snapshot!(
            collect_output(BufferedResponseStream::new(vec![statement1, statement2])).await
        );
    }

    #[tokio::test]
    async fn test_epoch() {
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
                Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000,
                ])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                    "t1a", "t1a", "t1b",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        );

        insta::assert_snapshot!(
            collect_output_epoch(
                BufferedResponseStream::new(vec![statement]),
                Some(Precision::Milliseconds)
            )
            .await
        );
    }

    async fn collect_output(stream: impl Stream<Item = Response> + Send) -> String {
        collect_output_epoch(stream, None).await
    }

    async fn collect_output_epoch(
        stream: impl Stream<Item = Response> + Send,
        epoch: Option<Precision>,
    ) -> String {
        stream
            .map(|r| {
                let mut w = vec![];
                let mut ser = Serializer::new(&mut w);
                let r = ResponseSerializer::new(&r, epoch, true);
                r.serialize(&mut ser).unwrap();
                w.push(b'\n');
                String::from_utf8(w).unwrap()
            })
            .collect()
            .await
    }
}
