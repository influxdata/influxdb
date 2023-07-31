use async_trait::async_trait;
use tracing::{debug, warn};

// Re-export the bytes type to ensure upstream users of this crate are
// interacting with the same type.
pub use prost::bytes::Bytes;

/// A delegate abstraction through which the gossip subsystem propagates
/// application-level messages received from other peers.
#[async_trait]
pub trait Dispatcher: Send + Sync {
    /// Invoked when an application-level payload is received from a peer.
    ///
    /// This call should not block / should complete quickly to avoid blocking
    /// the gossip reactor loop - if a long-running job must be started within
    /// this call, consider spawning a separate task.
    async fn dispatch(&self, payload: Bytes);
}

#[async_trait]
impl Dispatcher for tokio::sync::mpsc::Sender<Bytes> {
    async fn dispatch(&self, payload: Bytes) {
        if let Err(e) = self.send(payload).await {
            warn!(error=%e, "error dispatching payload to application handler");
        }
    }
}

/// A no-op [`Dispatcher`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NopDispatcher;

#[async_trait::async_trait]
impl Dispatcher for NopDispatcher {
    async fn dispatch(&self, _payload: crate::Bytes) {
        debug!("received no-op message payload");
    }
}
