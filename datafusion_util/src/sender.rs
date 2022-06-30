use async_trait::async_trait;
use tokio::sync::mpsc::{error::SendError, Sender, UnboundedSender};

/// Trait to abstract over [bounded](Sender) and [unbounded](UnboundedSender) tokio [MPSC](tokio::sync::mpsc) senders.
#[async_trait]
pub trait AbstractSender: Clone + Send + Sync + 'static {
    /// Channel payload type.
    type T;

    /// Send data.
    async fn send(&self, value: Self::T) -> Result<(), SendError<Self::T>>;
}

#[async_trait]
impl<T> AbstractSender for Sender<T>
where
    T: Send + 'static,
{
    type T = T;

    async fn send(&self, value: Self::T) -> Result<(), SendError<Self::T>> {
        self.send(value).await
    }
}

#[async_trait]
impl<T> AbstractSender for UnboundedSender<T>
where
    T: Send + 'static,
{
    type T = T;

    async fn send(&self, value: Self::T) -> Result<(), SendError<Self::T>> {
        self.send(value)
    }
}
