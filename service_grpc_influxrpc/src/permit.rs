use futures::Stream;
use pin_project::pin_project;
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// Helper to keep a semaphore permit attached to a stream.
#[derive(Debug)]
#[pin_project]
pub struct StreamWithPermit<S> {
    #[pin]
    stream: S,
    #[allow(dead_code)]
    permit: InstrumentedAsyncOwnedSemaphorePermit,
}

impl<S> StreamWithPermit<S> {
    pub fn new(stream: S, permit: InstrumentedAsyncOwnedSemaphorePermit) -> Self {
        Self { stream, permit }
    }
}

impl<S> Stream for StreamWithPermit<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}
