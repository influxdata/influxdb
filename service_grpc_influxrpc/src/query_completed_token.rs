use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Stream, StreamExt};
use iox_query::QueryCompletedToken;

/// Wraps an inner query stream, calling the `QueryCompletedToken::set_success` on success
#[derive(Debug)]
pub struct QueryCompletedTokenStream<S, T, E>
where
    S: Stream<Item = Result<T, E>> + Unpin + Send,
{
    inner: S,
    token: QueryCompletedToken,
    found_err: bool,
}

impl<S, T, E> QueryCompletedTokenStream<S, T, E>
where
    S: Stream<Item = Result<T, E>> + Unpin + Send,
{
    pub fn new(inner: S, token: QueryCompletedToken) -> Self {
        Self {
            inner,
            token,
            found_err: false,
        }
    }
}

impl<S, T, E> Stream for QueryCompletedTokenStream<S, T, E>
where
    S: Stream<Item = Result<T, E>> + Unpin + Send,
{
    type Item = Result<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        match ready!(this.inner.poll_next_unpin(cx)) {
            None => {
                if !this.found_err {
                    this.token.set_success();
                }
                Poll::Ready(None)
            }
            Some(Ok(x)) => Poll::Ready(Some(Ok(x))),
            Some(Err(e)) => {
                this.found_err = true;
                Poll::Ready(Some(Err(e)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::*;

    #[tokio::test]
    async fn test_empty() {
        let (res, token) = token();
        let stream =
            QueryCompletedTokenStream::new(futures::stream::empty::<Result<(), ()>>(), token);

        assert_eq!(stream.collect::<Vec<_>>().await, vec![],);
        assert_eq!(*res.lock(), Some(true));
    }

    #[tokio::test]
    async fn test_not_finished() {
        let (res, token) = token();
        QueryCompletedTokenStream::new(futures::stream::empty::<Result<(), ()>>(), token);
        assert_eq!(*res.lock(), Some(false));
    }

    #[tokio::test]
    async fn test_err() {
        let (res, token) = token();
        let stream =
            QueryCompletedTokenStream::new(futures::stream::iter([Ok(()), Err(()), Ok(())]), token);

        assert_eq!(
            stream.collect::<Vec<_>>().await,
            vec![Ok(()), Err(()), Ok(())],
        );
        assert_eq!(*res.lock(), Some(false));
    }

    fn token() -> (Arc<Mutex<Option<bool>>>, QueryCompletedToken) {
        let token = Arc::new(Mutex::new(None));
        let token_captured = Arc::clone(&token);
        let qct = QueryCompletedToken::new(move |success| {
            *token_captured.lock() = Some(success);
        });
        (token, qct)
    }
}
