use std::{future, io, net::SocketAddr};

use futures::{stream::FuturesUnordered, Future, StreamExt};
use hashbrown::{hash_map::RawEntryMut, HashMap};
use metric::U64Counter;
use prost::bytes::Bytes;
use rand::seq::SliceRandom;
use tokio::net::UdpSocket;
use tracing::{info, trace, warn};
use uuid::Uuid;

use crate::{
    metric::{SentBytes, SentFrames},
    topic_set::{Topic, TopicSet},
    BroadcastType, MAX_FRAME_BYTES, MAX_PING_UNACKED,
};

/// A fractional value between 0 and 1 that specifies the proportion of cluster
/// members a subset broadcast is sent to.
const SUBSET_FRACTION: f32 = 0.33;

/// A unique peer identity containing 128 bits of randomness (V4 UUID).
#[derive(Debug, Eq, Clone, PartialEq)]
pub struct Identity(Bytes);

impl std::hash::Hash for Identity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_uuid().fmt(f)
    }
}

impl TryFrom<Bytes> for Identity {
    type Error = uuid::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        // Validate bytes are a UUID
        Uuid::from_slice(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<Vec<u8>> for Identity {
    type Error = uuid::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        // Validate bytes
        Uuid::from_slice(&value)?;
        Ok(Self(Bytes::from(value)))
    }
}

impl Identity {
    /// Generate a new random identity.
    pub(crate) fn new() -> Self {
        let id = Uuid::new_v4();
        Self(Bytes::from(id.as_bytes().to_vec()))
    }

    pub(crate) fn as_bytes(&self) -> &Bytes {
        &self.0
    }

    /// Parse this identity into a UUID.
    pub fn as_uuid(&self) -> Uuid {
        Uuid::from_slice(&self.0).unwrap()
    }
}

/// A discovered peer within the gossip cluster.
#[derive(Debug, Clone)]
pub(crate) struct Peer {
    identity: Identity,
    addr: SocketAddr,
    interests: TopicSet,

    /// The number of PING messages sent to this peer since the last message
    /// observed from it.
    ///
    /// This value is reset to 0 each time a message from this peer is received.
    unacked_ping_count: usize,
}

impl Peer {
    pub(crate) async fn send(
        &self,
        buf: &[u8],
        socket: &UdpSocket,
        frames_sent: &SentFrames,
        bytes_sent: &SentBytes,
    ) -> Result<usize, io::Error> {
        // If the frame is larger than the allowed maximum, then the receiver
        // will truncate the frame when reading the socket.
        //
        // Never send frames that will be unprocessable.
        if buf.len() > MAX_FRAME_BYTES {
            warn!(
                n_bytes = buf.len(),
                "not sending oversized packet - receiver would truncate"
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max frame size exceeded",
            ));
        }

        let ret = socket.send_to(buf, self.addr).await;
        match &ret {
            Ok(n_bytes) => {
                frames_sent.inc(1);
                bytes_sent.inc(*n_bytes);
                trace!(identity=%self.identity, n_bytes, peer_addr=%self.addr, "socket write")
            }
            Err(e) => {
                warn!(error=%e, identity=%self.identity, peer_addr=%self.addr, "frame send error")
            }
        }
        ret
    }

    /// Record that a PING has been sent to this peer, incrementing the unacked
    /// PING count and returning the new value.
    pub(crate) fn record_ping(&mut self) -> usize {
        self.unacked_ping_count += 1;
        self.unacked_ping_count
    }

    pub(crate) fn mark_observed(&mut self) {
        self.unacked_ping_count = 0;
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl From<&Peer> for crate::proto::Peer {
    fn from(p: &Peer) -> Self {
        Self {
            identity: p.identity.as_bytes().clone(), // Ref-count
            address: p.addr.to_string(),
            interests: u64::from(p.interests),
        }
    }
}

/// The set of currently active/known peers.
#[derive(Debug, Default)]
pub(crate) struct PeerList {
    list: HashMap<Identity, Peer>,

    /// The number of peers discovered.
    metric_peer_discovered_count: metric::U64Counter,
    /// The number of peers removed due to health checks.
    metric_peer_removed_count: metric::U64Counter,
    // The difference between these two metrics will always match the number of
    // entries in the peer list (ignoring races & lack of thread
    // synchronisation).
}

impl PeerList {
    /// Initialise the [`PeerList`] with capacity for `cap` number of [`Peer`]
    /// instances.
    pub(crate) fn with_capacity(cap: usize, metrics: &metric::Registry) -> Self {
        let metric_peer_discovered_count = metrics
            .register_metric::<U64Counter>(
                "gossip_peers_discovered",
                "number of peers discovered by this node",
            )
            .recorder(&[]);
        let metric_peer_removed_count = metrics
            .register_metric::<U64Counter>(
                "gossip_peers_removed",
                "number of peers removed due to health check failures",
            )
            .recorder(&[]);

        Self {
            list: HashMap::with_capacity(cap),
            metric_peer_discovered_count,
            metric_peer_removed_count,
        }
    }

    /// Return the UUIDs of all known peers.
    pub(crate) fn peer_identities(&self) -> Vec<Identity> {
        self.list.keys().cloned().collect()
    }

    /// Returns an iterator of all known peers in the peer list.
    pub(crate) fn peers(&self) -> impl Iterator<Item = &'_ Peer> {
        self.list.values()
    }

    /// Returns true if `identity` is already in the peer list.
    pub(crate) fn contains(&self, identity: &Identity) -> bool {
        self.list.contains_key(identity)
    }

    /// Returns the [`Peer`] for the provided [`Identity`], if any.
    pub(crate) fn get(&self, identity: &Identity) -> Option<&Peer> {
        self.list.get(identity)
    }

    /// Upsert a peer identified by `identity` to the peer list, associating it
    /// with the provided `peer_addr` and interested in `interests`.
    ///
    /// If this peer was already known, `interests` is discarded and the
    /// existing value is used.
    pub(crate) fn upsert(
        &mut self,
        identity: &Identity,
        peer_addr: SocketAddr,
        interests: TopicSet,
    ) -> &mut Peer {
        let p = match self.list.raw_entry_mut().from_key(identity) {
            RawEntryMut::Vacant(v) => {
                info!(%identity, %peer_addr, "discovered new peer");
                self.metric_peer_discovered_count.inc(1);
                v.insert(
                    identity.to_owned(),
                    Peer {
                        addr: peer_addr,
                        identity: identity.to_owned(),
                        unacked_ping_count: 0,
                        interests,
                    },
                )
                .1
            }
            RawEntryMut::Occupied(v) => v.into_mut(),
        };

        p.addr = peer_addr;
        p
    }

    /// Broadcast `buf` to all known peers over `socket`, returning the number
    /// of bytes sent in total.
    ///
    /// If `topic` is provided, this frame is broadcast to only those peers with
    /// an interest in `topic`. If `topic` is [`None`], this frame is broadcast
    /// to all peers.
    pub(crate) async fn broadcast(
        &self,
        buf: &[u8],
        socket: &UdpSocket,
        frames_sent: &SentFrames,
        bytes_sent: &SentBytes,
        topic: Option<Topic>,
        subset: BroadcastType,
    ) {
        // Depending on the request, this message should either be sent to all
        // peers, or a random subset.
        match subset {
            BroadcastType::AllPeers => {
                self.list
                    .values()
                    .filter_map(|v| filtered_send(topic, v, buf, socket, frames_sent, bytes_sent))
                    .collect::<FuturesUnordered<_>>()
                    .for_each(|_| future::ready(()))
                    .await
            }
            BroadcastType::PeerSubset => {
                // Generate a list of futures that send the payload to candidate
                // peers that are interested in this topic (if any).
                let mut candidates = self
                    .list
                    .values()
                    .filter_map(|v| filtered_send(topic, v, buf, socket, frames_sent, bytes_sent))
                    .collect::<Vec<_>>();

                // Figure out how many interested peers to send this payload to.
                let n = (candidates.len() as f32 * SUBSET_FRACTION).ceil() as usize;

                // Shuffle the candidate list
                candidates.shuffle(&mut rand::thread_rng());

                // And execute the first N futures to send the payload to random
                // peers.
                candidates
                    .into_iter()
                    .take(n)
                    .collect::<FuturesUnordered<_>>()
                    .for_each(|_| future::ready(()))
                    .await
            }
        };
    }

    /// Send PING frames to all known nodes, and remove any that have not
    /// responded after [`MAX_PING_UNACKED`] attempts.
    pub(crate) async fn ping_gc(
        &mut self,
        ping_frame: &[u8],
        socket: &UdpSocket,
        frames_sent: &SentFrames,
        bytes_sent: &SentBytes,
    ) {
        // First broadcast the PING frames.
        //
        // If a peer has exceeded the allowed maximum, it will be removed next,
        // but if it responds to this PING, it will be re-added again.
        self.broadcast(
            ping_frame,
            socket,
            frames_sent,
            bytes_sent,
            None,
            BroadcastType::AllPeers,
        )
        .await;

        // Increment the PING counts and remove all peers that have not
        // responded to more than the allowed number of PINGs.
        let dead = self.list.extract_if(|_ident, p| {
            // Increment the counter and grab the incremented value.
            let missed = p.record_ping();
            missed > MAX_PING_UNACKED
        });

        for (identity, p) in dead {
            warn!(%identity, addr=%p.addr, "removed unreachable peer");
            self.metric_peer_removed_count.inc(1);
        }
    }
}

/// Send `buf` to `peer`, optionally applying a topic filter.
fn filtered_send<'a>(
    topic: Option<Topic>,
    peer: &'a Peer,
    buf: &'a [u8],
    socket: &'a UdpSocket,
    frames_sent: &'a SentFrames,
    bytes_sent: &'a SentBytes,
) -> Option<impl Future<Output = Result<usize, io::Error>> + 'a> {
    if let Some(topic) = topic {
        if !peer.interests.is_interested(topic) {
            return None;
        }
        trace!(
            identity = %peer.identity,
            peer_addr=%peer.addr,
            ?topic,
            "selected interested peer as candidate for broadcast"
        );
    }

    Some(peer.send(buf, socket, frames_sent, bytes_sent))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use super::*;

    #[test]
    fn test_identity_round_trip() {
        let a = Identity::new();

        let encoded = a.as_bytes().to_owned();
        let decoded = Identity::try_from(encoded).unwrap();

        assert_eq!(decoded, a);
    }

    #[test]
    fn test_identity_length_mismatch() {
        let v = Bytes::from_static(&[42, 42, 42, 42]);
        let _ = Identity::try_from(v).expect_err("short ID should fail");

        let v = Bytes::from_static(&[
            42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
        ]);
        let _ = Identity::try_from(v).expect_err("long ID should fail");
    }

    #[test]
    fn test_identity_eq() {
        let v = Identity::new();
        assert_eq!(v.clone(), v);
        assert_eq!(hash_identity(&v), hash_identity(&v));

        let other = Identity::new();
        assert_ne!(v, other);
        assert_ne!(hash_identity(&other), hash_identity(&v));
    }

    #[test]
    fn test_identity_display() {
        let v = Identity::new();
        let text = v.to_string();

        let uuid = Uuid::try_parse(&text).expect("display impl should output valid uuids");
        assert_eq!(v.as_uuid(), uuid);
    }

    fn hash_identity(v: &Identity) -> u64 {
        let mut h = DefaultHasher::default();
        v.hash(&mut h);
        h.finish()
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_subset_fraction() {
        // This would be silly.
        assert!(SUBSET_FRACTION < 1.0);
    }
}
