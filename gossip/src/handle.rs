use std::{marker::PhantomData, net::SocketAddr};

use crate::{topic_set::Topic, Bytes, MAX_USER_PAYLOAD_BYTES};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::peers::Identity;

/// An error indicating a send was attempted with a payload that exceeds
/// [`MAX_USER_PAYLOAD_BYTES`].
#[derive(Error, Debug)]
#[error("max allowed payload size exceeded")]
#[allow(missing_copy_implementations)]
pub struct PayloadSizeError {}

/// Specify the target of a broadcast.
#[derive(Debug)]
pub(crate) enum BroadcastType {
    /// All interested peers.
    AllPeers,

    /// A subset of peers of the interested peers.
    PeerSubset,
}

/// Requests sent to the [`Reactor`] actor task.
///
/// [`Reactor`]: crate::reactor::Reactor
#[derive(Debug)]
pub(crate) enum Request {
    /// Broadcast the given payload to all known peers.
    Broadcast(Bytes, Topic, BroadcastType),

    /// Get a snapshot of the peer identities.
    GetPeers(oneshot::Sender<Vec<Identity>>),

    /// Get the [`SocketAddr`] associated with a given peer [`Identity`].
    GetPeerAddr(Identity, oneshot::Sender<Option<SocketAddr>>),
}

/// A handle to the gossip subsystem.
///
/// All resources used by the gossip system will be released once this
/// [`GossipHandle`] is dropped. To share the handle, wrap it in an [`Arc`].
///
/// [`Arc`]: std::sync::Arc
#[derive(Debug)]
pub struct GossipHandle<S = u64> {
    tx: mpsc::Sender<Request>,
    identity: Identity,
    _topic_type: PhantomData<S>,
}

impl<S> GossipHandle<S>
where
    S: Into<u64> + Send + Sync,
{
    pub(crate) fn new(tx: mpsc::Sender<Request>, identity: Identity) -> Self {
        Self {
            tx,
            identity,
            _topic_type: PhantomData,
        }
    }

    /// Return the randomly generated identity of this gossip instance.
    pub fn identity(&self) -> Identity {
        self.identity.clone()
    }

    /// Broadcast `payload` to all known peers.
    ///
    /// This is a best-effort operation - peers are not guaranteed to receive
    /// this broadcast.
    ///
    /// If the outgoing message queue is full, this method blocks and waits for
    /// space to become available.
    ///
    /// # Topics
    ///
    /// Messages are tagged with an application-defined "topic" identifying the
    /// type of message being transmitted. The provided topic will transmitted
    /// alongside the message and passed to peer [`Dispatcher`] implementations
    /// with the provided payload.
    ///
    /// A topic MUST be convertable into a `u64` in the range 0 to 63 inclusive.
    ///
    /// # Panics
    ///
    /// Panics if the topic ID is outside the range 0 to 63 inclusive once
    /// converted to a `u64`.
    ///
    /// [`Dispatcher`]: crate::dispatcher::Dispatcher
    pub async fn broadcast<T>(&self, payload: T, topic: S) -> Result<(), PayloadSizeError>
    where
        T: Into<Bytes> + Send,
    {
        self.push_broadcast(payload, topic, BroadcastType::AllPeers)
            .await
    }

    /// Broadcast the specified `payload` in the provided `topic` to a random
    /// subset of gossip peers.
    ///
    /// See [`GossipHandle::broadcast()`] for documentation.
    pub async fn broadcast_subset<T>(&self, payload: T, topic: S) -> Result<(), PayloadSizeError>
    where
        T: Into<Bytes> + Send,
    {
        self.push_broadcast(payload, topic, BroadcastType::PeerSubset)
            .await
    }

    async fn push_broadcast<T>(
        &self,
        payload: T,
        topic: S,
        subset: BroadcastType,
    ) -> Result<(), PayloadSizeError>
    where
        T: Into<Bytes> + Send,
    {
        let payload = payload.into();
        if payload.len() > MAX_USER_PAYLOAD_BYTES {
            return Err(PayloadSizeError {});
        }

        self.tx
            .send(Request::Broadcast(payload, Topic::encode(topic), subset))
            .await
            .unwrap();

        Ok(())
    }

    /// Retrieve a snapshot of the connected peer list.
    pub async fn get_peers(&self) -> Vec<Identity> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Request::GetPeers(tx)).await.unwrap();
        rx.await.unwrap()
    }

    /// Return the [`SocketAddr`] being used by the specified `peer`.
    ///
    /// This method returns [`None`] if the `peer` is no longer in the local
    /// peer list.
    pub async fn get_peer_addr(&self, peer: Identity) -> Option<SocketAddr> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Request::GetPeerAddr(peer, tx)).await.unwrap();
        rx.await.unwrap()
    }
}
