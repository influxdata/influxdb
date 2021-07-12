//! This module contains a `Stream` wrapper that fully consumes (slurps) a
//! `Stream` so it can compute its size, while saving it to a backing store for
//! later replay.
use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};
use std::io::{Cursor, Result, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{copy, AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite};
use tokio_util::io::{ReaderStream, StreamReader};

/// Returns a BufferedStream backed by a temporary file.
///
/// The temporary file will be deleted when the result stream
/// is dropped.
#[allow(dead_code)]
pub async fn slurp_stream_tempfile<S>(bytes: S) -> Result<BufferedStream<File>>
where
    S: Stream<Item = Result<Bytes>> + Send + Sync,
{
    let tmp = File::from_std(tempfile::tempfile()?);
    BufferedStream::new(tmp, bytes).await
}

/// Returns a BufferedStream backed by a in-memory buffer.
#[allow(dead_code)]
pub async fn slurp_stream_memory<S>(bytes: S) -> Result<BufferedStream<Cursor<Vec<u8>>>>
where
    S: Stream<Item = Result<Bytes>> + Send + Sync,
{
    BufferedStream::new(Cursor::new(Vec::new()), bytes).await
}

// A stream fully buffered by a backing store..
pub struct BufferedStream<R>
where
    R: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin,
{
    size: usize,
    inner: ReaderStream<R>,
}

impl<R> BufferedStream<R>
where
    R: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin,
{
    /// Consumes the bytes stream fully and writes its content into file.
    /// It returns a Stream implementation that reads the same content from the
    /// buffered file.
    ///
    /// The granularity of stream "chunks" will not be preserved.
    pub async fn new<S>(mut backing_store: R, bytes: S) -> Result<Self>
    where
        S: Stream<Item = Result<Bytes>> + Send + Sync,
    {
        pin_mut!(bytes);
        let mut read = StreamReader::new(bytes);
        let size = copy(&mut read, &mut backing_store).await? as usize;
        backing_store.seek(SeekFrom::Start(0)).await?;

        Ok(Self {
            size,
            inner: ReaderStream::new(backing_store),
        })
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl<R> Stream for BufferedStream<R>
where
    R: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::{self, TryStreamExt};
    use futures_test::stream::StreamTestExt;

    fn test_data() -> impl Stream<Item = Result<Bytes>> + Send + Sync {
        stream::iter(vec!["foo", "bar", "baz"])
            .map(|i| Ok(Bytes::from(i)))
            .interleave_pending()
    }

    async fn check_stream<R>(buf_stream: BufferedStream<R>)
    where
        R: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin,
    {
        assert_eq!(buf_stream.size(), 9);
        assert_eq!(buf_stream.size_hint(), (9, Some(9)));

        let content = buf_stream
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();

        assert_eq!(content, "foobarbaz");
    }

    #[tokio::test]
    async fn test_buffered_stream() {
        let backing_store = std::io::Cursor::new(Vec::new()); // in-memory buffer
        check_stream(
            BufferedStream::new(backing_store, test_data())
                .await
                .unwrap(),
        )
        .await;
    }
}
