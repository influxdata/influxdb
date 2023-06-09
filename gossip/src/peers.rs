use std::net::SocketAddr;

use futures::{stream::FuturesUnordered, StreamExt};
use hashbrown::{hash_map::RawEntryMut, HashMap};
use prost::bytes::Bytes;
use tokio::net::UdpSocket;
use tracing::{trace, warn};
use uuid::Uuid;

/// A unique generated identity containing 128 bits of randomness (V4 UUID).
#[derive(Debug, Eq, Clone)]
pub(crate) struct Identity(Bytes, Uuid);

impl std::ops::Deref for Identity {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl std::hash::Hash for Identity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        debug_assert!((self.1 == other.1) == (self.0 == other.0));
        self.0 == other.0
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.1.fmt(f)
    }
}

impl TryFrom<Bytes> for Identity {
    type Error = uuid::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let uuid = Uuid::from_slice(&value)?;
        Ok(Self(value, uuid))
    }
}

impl Identity {
    /// Generate a new random identity.
    pub(crate) fn new() -> Self {
        let id = Uuid::new_v4();
        Self(Bytes::from(id.as_bytes().to_vec()), id)
    }

    pub(crate) fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

/// A discovered peer within the gossip cluster.
#[derive(Debug, Clone)]
pub(crate) struct Peer {
    identity: Identity,
    addr: SocketAddr,
}

impl Peer {
    pub(crate) async fn send(
        &self,
        buf: &[u8],
        socket: &UdpSocket,
    ) -> Result<usize, std::io::Error> {
        let ret = socket.send_to(buf, self.addr).await;
        match &ret {
            Ok(n_bytes) => {
                trace!(identity=%self.identity, n_bytes, peer_addr=%self.addr, "send frame")
            }
            Err(e) => {
                warn!(error=%e, identity=%self.identity, peer_addr=%self.addr, "frame send error")
            }
        }
        ret
    }
}

/// The set of currently active/known peers.
#[derive(Debug, Default)]
pub(crate) struct PeerList {
    list: HashMap<Identity, Peer>,
}

impl PeerList {
    /// Initialise the [`PeerList`] with capacity for `cap` number of [`Peer`]
    /// instances.
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            list: HashMap::with_capacity(cap),
        }
    }

    /// Return the UUIDs of all known peers.
    pub(crate) fn peer_uuids(&self) -> Vec<Uuid> {
        self.list.keys().map(|v| **v).collect()
    }

    /// Upsert a peer identified by `identity` to the peer list, associating it
    /// with the provided `peer_addr`.
    pub(crate) fn upsert(&mut self, identity: &Identity, peer_addr: SocketAddr) -> &mut Peer {
        let p = match self.list.raw_entry_mut().from_key(identity) {
            RawEntryMut::Vacant(v) => {
                v.insert(
                    identity.to_owned(),
                    Peer {
                        addr: peer_addr,
                        identity: identity.to_owned(),
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
    pub(crate) async fn broadcast(&self, buf: &[u8], socket: &UdpSocket) -> usize {
        self.list
            .values()
            .map(|v| v.send(buf, socket))
            .collect::<FuturesUnordered<_>>()
            .fold(0, |acc, res| async move {
                match res {
                    Ok(n) => acc + n,
                    Err(_) => acc,
                }
            })
            .await
    }
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
        let _ = Identity::try_from(v).expect_err("short ID should fail");
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
        assert_eq!(*v, uuid);
    }

    fn hash_identity(v: &Identity) -> u64 {
        let mut h = DefaultHasher::default();
        v.hash(&mut h);
        h.finish()
    }
}
