//! Message Pack encoding of InfluxQL query results.
use crate::types::Precision;

use super::{BufferedResponseStream, ChunkedResponseStream, Response, ResponseSerializer};
use bytes::buf::BufMut;
use bytes::{Bytes, BytesMut};
use futures::Stream;
use rmp_serde::Serializer;
use serde::Serialize;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tracing::warn;

/// A generic Message Pack-encoded [Response] stream.
pub(crate) struct MessagePackStream<S> {
    stream: S,
    epoch: Option<Precision>,
}

impl<S> MessagePackStream<S> {
    pub(crate) fn new(stream: S, epoch: Option<Precision>) -> Self {
        Self { stream, epoch }
    }

    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Response>>
    where
        S: Stream<Item = Response> + Unpin,
    {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl<S> Stream for MessagePackStream<S>
where
    S: Stream<Item = Response> + Unpin,
{
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.poll_next_inner(cx).map(|opt| {
            opt.and_then(|resp| {
                let mut w = BytesMut::new().writer();
                let mut serializer = Serializer::new(&mut w);
                let resp = ResponseSerializer::new(&resp, this.epoch, true);
                if let Err(e) = resp.serialize(&mut serializer) {
                    warn!(error = %e, "failed to serialize response");
                    None
                } else {
                    Some(w.into_inner().freeze())
                }
            })
        })
    }
}

pub(crate) type ChunkedMessagePackStream = MessagePackStream<ChunkedResponseStream>;
pub(crate) type BufferedMessagePackStream = MessagePackStream<BufferedResponseStream>;

#[cfg(test)]
mod tests {
    use crate::response::buffered::BufferedResponseStream;

    use super::super::tests::{Column, make_statement};
    use super::*;
    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, StringArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::Int32Type;
    use futures::StreamExt;
    use iox_query::QueryDatabase;
    use iox_query::exec::IOxSessionContext;
    use iox_query::query_log::QueryLog;
    use iox_query::test::TestDatabaseStore;
    use iox_time::SystemProvider;
    use rmp_serde::Deserializer;
    use serde::Deserialize;
    use serde_json::Value;
    use std::sync::Arc;

    #[tokio::test]
    async fn empty_stream() {
        let stream = ChunkedResponseStream::new(vec![], 2);
        let mut stream = ChunkedMessagePackStream::new(stream, None);
        assert!(stream.next().await.is_none());

        let stream = BufferedResponseStream::new(vec![]);
        let mut stream = BufferedMessagePackStream::new(stream, None);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn single_chunk() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let columns = [
            Column::Measurement,
            Column::Time,
            Column::Tag {
                name: "t1",
                group_by: false,
                projected: false,
            },
            Column::Field { name: "f1" },
        ];
        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["m1", "m1"])),
            Arc::new(TimestampNanosecondArray::from(vec![1000000000, 2000000000])),
            Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["t1a", "t1a"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ];

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = ChunkedResponseStream::new(vec![statement], 2);
        let stream = ChunkedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = BufferedResponseStream::new(vec![statement]);
        let stream = BufferedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);
    }

    #[tokio::test]
    async fn many_chunks() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let columns = [
            Column::Measurement,
            Column::Time,
            Column::Tag {
                name: "t1",
                group_by: false,
                projected: false,
            },
            Column::Field { name: "f1" },
        ];
        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1000000000, 2000000000, 3000000000,
            ])),
            Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                "t1a", "t1a", "t1a",
            ])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ];

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = ChunkedResponseStream::new(vec![statement], 2);
        let stream = ChunkedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = BufferedResponseStream::new(vec![statement]);
        let stream = BufferedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);
    }

    #[tokio::test]
    async fn many_chunks_many_measurments() {
        let db: Arc<dyn QueryDatabase> = Arc::new(TestDatabaseStore::default());
        let ctx = Arc::new(IOxSessionContext::with_testing());
        let log = Arc::new(QueryLog::new(
            1,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        ));
        let columns = [
            Column::Measurement,
            Column::Time,
            Column::Tag {
                name: "t1",
                group_by: false,
                projected: false,
            },
            Column::Field { name: "f1" },
        ];
        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1000000000, 2000000000, 3000000000, 1000000000,
            ])),
            Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                "t1a", "t1a", "t1a", "t1a",
            ])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 1.0])),
        ];

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = ChunkedResponseStream::new(vec![statement], 2);
        let stream = ChunkedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = BufferedResponseStream::new(vec![statement]);
        let stream = BufferedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);
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
        let columns1 = [
            Column::Measurement,
            Column::Time,
            Column::Tag {
                name: "t1",
                group_by: false,
                projected: false,
            },
            Column::Field { name: "f1" },
        ];
        let data1: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1000000000, 2000000000, 3000000000, 1000000000,
            ])),
            Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                "t1a", "t1a", "t1a", "t1a",
            ])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 1.0])),
        ];

        let columns2 = [
            Column::Measurement,
            Column::Time,
            Column::Tag {
                name: "t1",
                group_by: true,
                projected: false,
            },
            Column::Field { name: "f1" },
        ];
        let data2: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m2"])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1000000000, 2000000000, 3000000000, 1000000000,
            ])),
            Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                "t1a", "t1a", "t1b", "t1b",
            ])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 1.0])),
        ];

        let statement1 = make_statement(&db, &ctx, &log, columns1.clone(), data1.clone());
        let statement2 = make_statement(&db, &ctx, &log, columns2.clone(), data2.clone());
        let stream = ChunkedResponseStream::new(vec![statement1, statement2], 2);
        let stream = ChunkedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);

        let statement1 = make_statement(&db, &ctx, &log, columns1.clone(), data1.clone());
        let statement2 = make_statement(&db, &ctx, &log, columns2.clone(), data2.clone());
        let stream = BufferedResponseStream::new(vec![statement1, statement2]);
        let stream = BufferedMessagePackStream::new(stream, None);
        insta::assert_snapshot!(collect_output(stream).await);
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
        let columns = [
            Column::Measurement,
            Column::Time,
            Column::Tag {
                name: "t1",
                group_by: false,
                projected: false,
            },
            Column::Field { name: "f1" },
        ];
        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["m1", "m1"])),
            Arc::new(TimestampNanosecondArray::from(vec![1000000000, 2000000000])),
            Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["t1a", "t1a"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ];

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = ChunkedResponseStream::new(vec![statement], 2);
        let stream = ChunkedMessagePackStream::new(stream, Some(Precision::Microseconds));
        insta::assert_snapshot!(collect_output(stream).await);

        let statement = make_statement(&db, &ctx, &log, columns.clone(), data.clone());
        let stream = BufferedResponseStream::new(vec![statement]);
        let stream = BufferedMessagePackStream::new(stream, Some(Precision::Microseconds));
        insta::assert_snapshot!(collect_output(stream).await);
    }

    async fn collect_output<S: Stream<Item = Bytes> + Send>(stream: S) -> String {
        String::from_utf8(
            stream
                .map(Vec::<u8>::from)
                .map(|v| {
                    // Docode the msgpack and recode as JSON to make it
                    // easier to validate.
                    let mut de = Deserializer::new(&v[..]);
                    let value = Value::deserialize(&mut de).unwrap();
                    let mut v = serde_json::to_vec(&value).unwrap();
                    v.push(b'\n');
                    v
                })
                .concat()
                .await,
        )
        .unwrap()
    }
}
