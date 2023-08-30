use async_trait::async_trait;
use tracing::{debug, warn};

// Re-export the bytes type to ensure upstream users of this crate are
// interacting with the same type.
pub use prost::bytes::Bytes;

use crate::peers::Identity;

/// A delegate abstraction through which the gossip subsystem propagates
/// application-level messages received from other peers.
#[async_trait]
pub trait Dispatcher<T>: Send + Sync
where
    T: TryFrom<u64>,
{
    /// Invoked when an application-level payload is received from a peer.
    ///
    /// This call should not block / should complete quickly to avoid blocking
    /// the gossip reactor loop - if a long-running job must be started within
    /// this call, consider spawning a separate task.
    ///
    /// # Topics
    ///
    /// Messages are tagged with an application-defined "topic" identifying the
    /// type of message being transmitted. The provided topic was set by the
    /// peer when sending the message, and MAY be a topic the local node is
    /// uninterested in.
    ///
    /// Implementations SHOULD return an error from the [`TryFrom`] conversion
    /// for unknown topics or otherwise gracefully handle the message.
    async fn dispatch(&self, topic: T, payload: Bytes, sender: Identity);
}

#[async_trait]
impl<T> Dispatcher<T> for tokio::sync::mpsc::Sender<(T, Bytes)>
where
    T: TryFrom<u64> + Send + Sync + 'static,
{
    async fn dispatch(&self, topic: T, payload: Bytes, _sender: Identity) {
        if let Err(e) = self.send((topic, payload)).await {
            warn!(error=%e, "error dispatching payload to application handler");
        }
    }
}

/// A no-op [`Dispatcher`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NopDispatcher;

#[async_trait::async_trait]
impl<T> Dispatcher<T> for NopDispatcher
where
    T: TryFrom<u64> + Send + Sync + 'static,
{
    async fn dispatch(&self, _topic: T, _payload: crate::Bytes, _sender: Identity) {
        debug!("received no-op message payload");
    }
}
