use crate::Bytes;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::peers::Identity;

/// Requests sent to the [`Reactor`] actor task.
///
/// [`Reactor`]: crate::reactor::Reactor
#[derive(Debug)]
pub(crate) enum Request {
    /// Broadcast the given payload to all known peers.
    Broadcast(Bytes),

    /// Get a snapshot of the peer identities.
    GetPeers(oneshot::Sender<Vec<Uuid>>),
}

/// A handle to the gossip subsystem.
///
/// All resources used by the gossip system will be released once this
/// [`GossipHandle`] is dropped. To share the handle, wrap it in an [`Arc`].
///
/// [`Arc`]: std::sync::Arc
#[derive(Debug)]
pub struct GossipHandle {
    tx: mpsc::Sender<Request>,
    identity: Identity,
}

impl GossipHandle {
    pub(crate) fn new(tx: mpsc::Sender<Request>, identity: Identity) -> Self {
        Self { tx, identity }
    }

    /// Return the randomly generated identity of this gossip instance.
    pub fn identity(&self) -> Uuid {
        *self.identity
    }

    /// Broadcast `payload` to all known peers.
    ///
    /// This is a best-effort operation - peers are not guaranteed to receive
    /// this broadcast.
    pub async fn broadcast<T>(&self, payload: T)
    where
        T: Into<Bytes> + Send,
    {
        self.tx
            .send(Request::Broadcast(payload.into()))
            .await
            .unwrap()
    }

    /// Retrieve a snapshot of the connected peer list.
    pub async fn get_peers(&self) -> Vec<Uuid> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Request::GetPeers(tx)).await.unwrap();
        rx.await.unwrap()
    }
}
