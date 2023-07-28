use std::{net::SocketAddr, sync::Arc};

use prost::{bytes::BytesMut, Message};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self},
    time,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    metric::*,
    peers::{Identity, PeerList},
    proto::{self, frame_message::Payload, FrameMessage},
    seed::{seed_ping_task, Seed},
    Dispatcher, Request, MAX_FRAME_BYTES, PEER_PING_INTERVAL,
};

#[derive(Debug)]
enum Error {
    NoPayload {
        peer: Identity,
        addr: SocketAddr,
    },

    Deserialise {
        addr: SocketAddr,
        source: prost::DecodeError,
    },

    Identity {
        addr: SocketAddr,
    },

    Io(std::io::Error),

    MaxSize(usize),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort()
    }
}

/// An event loop actor for gossip frame processing.
///
/// This actor task is responsible for driving peer discovery, managing the set
/// of known peers and exchanging gossip frames between peers.
///
/// A user interacts with a [`Reactor`] through a [`GossipHandle`].
///
/// [`GossipHandle`]: crate::GossipHandle
#[derive(Debug)]
pub(crate) struct Reactor<T> {
    dispatch: T,

    /// The random identity of this gossip instance.
    identity: Identity,

    /// A cached wire frame, used to generate outgoing messages.
    cached_frame: proto::Frame,
    /// A cached, immutable ping frame.
    cached_ping_frame: Arc<[u8]>,
    /// A re-used buffer for serialising outgoing messages into.
    serialisation_buf: Vec<u8>,

    /// The immutable list of seed addresses provided by the user, periodically
    /// pinged.
    seed_list: Arc<[Seed]>,
    /// A task that periodically sends PING frames to all seeds, executing in a
    /// separate task so that DNS resolution does not block the reactor loop.
    _seed_ping_task: AbortOnDrop,

    /// The set of active peers this node has communicated with and believes to
    /// be (recently) healthy.
    ///
    /// Depending on the perceived availability of the seed nodes, this may
    /// contain less peers than the number of initial seeds.
    peer_list: PeerList,

    /// The UDP socket used for communication with peers.
    socket: Arc<UdpSocket>,

    /// The count of frames sent and received.
    metric_frames_sent: SentFrames,
    metric_frames_received: ReceivedFrames,

    /// The sum of bytes sent and received.
    metric_bytes_sent: SentBytes,
    metric_bytes_received: ReceivedBytes,
}

impl<T> Reactor<T>
where
    T: Dispatcher,
{
    pub(crate) fn new(
        seed_list: Vec<String>,
        socket: UdpSocket,
        dispatch: T,
        metrics: &metric::Registry,
    ) -> Self {
        // Generate a unique UUID for this Reactor instance, and cache the wire
        // representation.
        let identity = Identity::new();

        let seed_list = seed_list.into_iter().map(Seed::new).collect();
        let socket = Arc::new(socket);
        let mut serialisation_buf = Vec::with_capacity(1024);

        // Generate a pre-populated frame header.
        let mut cached_frame = proto::Frame {
            identity: identity.as_bytes().clone(),
            messages: Vec::with_capacity(1),
        };

        // A ping frame is static over the lifetime of a Reactor instance, so it
        // can be pre-serialised, cached, and reused for every ping.
        let cached_ping_frame = {
            populate_frame(
                &mut cached_frame,
                vec![new_payload(Payload::Ping(proto::Ping {}))],
                &mut serialisation_buf,
            )
            .unwrap();
            Arc::from(serialisation_buf.clone())
        };

        // Initialise the various metrics with wrappers to help distinguish
        // between the (very similar) counters.
        let (metric_frames_sent, metric_frames_received, metric_bytes_sent, metric_bytes_received) =
            new_metrics(metrics);

        // Spawn a task that periodically pings all known seeds.
        //
        // Pinging all seeds announces this node as alive, propagating the
        // instance UUID, and requesting PONG responses to drive population of
        // the active peer list.
        let seed_ping_task = AbortOnDrop(tokio::spawn(seed_ping_task(
            Arc::clone(&seed_list),
            Arc::clone(&socket),
            Arc::clone(&cached_ping_frame),
            metric_frames_sent.clone(),
            metric_bytes_sent.clone(),
        )));

        Self {
            dispatch,
            identity,
            cached_frame,
            cached_ping_frame,
            serialisation_buf,
            peer_list: PeerList::with_capacity(seed_list.len(), metrics),
            seed_list,
            _seed_ping_task: seed_ping_task,
            socket,
            metric_frames_sent,
            metric_frames_received,
            metric_bytes_sent,
            metric_bytes_received,
        }
    }

    /// Execute the reactor event loop, handing requests from the
    /// [`GossipHandle`] over `rx`.
    ///
    /// [`GossipHandle`]: crate::GossipHandle
    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<Request>) {
        info!(
            identity = %self.identity,
            seed_list = ?self.seed_list,
            "gossip reactor started",
        );

        // Start a timer to periodically perform health check PINGs and remove
        // dead nodes.
        let mut gc_interval = time::interval(PEER_PING_INTERVAL);
        gc_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                msg = self.read() => {
                    // Process a message received from a peer.
                    match msg {
                        Ok(()) => {},
                        Err(Error::NoPayload { peer, addr }) => {
                            warn!(%peer, %addr, "message contains no payload");
                            continue;
                        }
                        Err(Error::Deserialise { addr, source }) => {
                            warn!(error=%source, %addr, "error deserialising frame");
                            continue;
                        }
                        Err(Error::Identity { addr }) => {
                            warn!(%addr, "invalid identity value in frame");
                            continue;
                        }
                        Err(Error::Io(error)) => {
                            error!(%error, "i/o error");
                            continue;
                        }
                        Err(Error::MaxSize(_)) => {
                            // Logged at source
                            continue;
                        }
                    }
                }
                op = rx.recv() => {
                    // Process an operation from the handle.
                    match op {
                        None => {
                            info!("stopping gossip reactor");
                            return;
                        }
                        Some(Request::GetPeers(tx)) => {
                            let _ = tx.send(self.peer_list.peer_uuids());
                        },
                        Some(Request::Broadcast(payload)) => {
                            // The user is guaranteed MAX_USER_PAYLOAD_BYTES to
                            // be send-able, so send this frame without packing
                            // others with it for simplicity.
                            populate_frame(
                                &mut self.cached_frame,
                                vec![new_payload(Payload::UserData(proto::UserPayload{payload}))],
                                &mut self.serialisation_buf
                            ).expect("size validated in handle at enqueue time");

                            self.peer_list.broadcast(
                                &self.serialisation_buf,
                                &self.socket,
                                &self.metric_frames_sent,
                                &self.metric_bytes_sent
                            ).await;
                        }
                    }
                }
                _ = gc_interval.tick() => {
                    // Perform a periodic PING & prune dead peers.
                    debug!("peer ping & gc sweep");
                    self.peer_list.ping_gc(
                        &self.cached_ping_frame,
                        &self.socket,
                        &self.metric_frames_sent,
                        &self.metric_bytes_sent
                    ).await;
                }
            };
        }
    }

    /// Read a gossip frame from the socket and potentially respond.
    ///
    /// This method waits for a frame to be made available by the OS, enumerates
    /// the contents, batches any responses to those frames and if non-empty,
    /// returns the result to the sender of the original frame.
    ///
    /// Returns the bytes read and bytes sent during execution of this method.
    async fn read(&mut self) -> Result<(), Error> {
        // Read a frame into buf.
        let (bytes_read, frame, peer_addr) = read_frame(&self.socket).await?;
        self.metric_frames_received.inc(1);
        self.metric_bytes_received.inc(bytes_read as _);

        // Read the peer identity from the frame
        let identity =
            Identity::try_from(frame.identity).map_err(|_| Error::Identity { addr: peer_addr })?;

        // Don't process messages from this node.
        //
        // It's expected that all N servers will be included in a peer list,
        // rather than the N-1 peers to this node. By dropping messages from
        // this node, pings sent by this node will go unprocessed and therefore
        // this node will not be added to the active peer list.
        if identity == self.identity {
            debug!(%identity, %peer_addr, bytes_read, "dropping frame from self");
            return Ok(());
        }

        let mut out_messages = Vec::new();
        for msg in frame.messages {
            // Extract the payload from the frame message
            let payload = msg.payload.ok_or_else(|| Error::NoPayload {
                peer: identity.clone(),
                addr: peer_addr,
            })?;

            // Handle the frame message from the peer, optionally returning a
            // response frame to be sent back.
            let response = match payload {
                Payload::Ping(_) => Some(Payload::Pong(proto::Pong {
                    peers: self.peer_list.peers().map(proto::Peer::from).collect(),
                })),
                Payload::Pong(pex) => {
                    debug!(
                        %identity,
                        %peer_addr,
                        pex_nodes=pex.peers.len(),
                        "pong"
                    );
                    // If handling the PEX frame fails, the sender sent a bad
                    // frame and is not added to the peer list / marked as
                    // healthy.
                    self.handle_pex(pex).await?;
                    None
                }
                Payload::UserData(data) => {
                    let data = data.payload;
                    debug!(%identity, %peer_addr, n_bytes=data.len(), "dispatch payload");
                    self.dispatch.dispatch(data).await;
                    None
                }
            };

            if let Some(payload) = response {
                out_messages.push(new_payload(payload));
            }
        }

        // Find or create the peer in the peer list.
        let peer = self.peer_list.upsert(&identity, peer_addr);

        // Track that this peer has been observed as healthy.
        peer.mark_observed();

        // Sometimes no message will be returned to the peer - there's no need
        // to send an empty frame.
        if out_messages.is_empty() {
            return Ok(());
        }

        // Serialise the frame into the serialisation buffer.
        populate_frame(
            &mut self.cached_frame,
            out_messages,
            &mut self.serialisation_buf,
        )?;

        peer.send(
            &self.serialisation_buf,
            &self.socket,
            &self.metric_frames_sent,
            &self.metric_bytes_sent,
        )
        .await?;

        Ok(())
    }

    /// The PONG response to a PING contains the set of peers known to the sender
    /// - this is the peer exchange mechanism.
    async fn handle_pex(&mut self, pex: proto::Pong) -> Result<(), Error> {
        // Process the peers from the remote, ignoring the local node and known
        // peers.
        for p in pex.peers {
            let pex_identity = match Identity::try_from(p.identity) {
                // Ignore any references to this node's identity, or known
                // peers.
                Ok(v) if v == self.identity => continue,
                Ok(v) if self.peer_list.contains(&v) => continue,
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        error=%e,
                        "received invalid identity via PEX",
                    );
                    continue;
                }
            };

            let pex_addr = match p.address.parse() {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        %pex_identity,
                        error=%e,
                        "received invalid peer address via PEX",
                    );
                    continue;
                }
            };

            // Send a PING frame to this peer without adding it to the local
            // peer list.
            //
            // The peer will be added to the local peer list if it responds to
            // this solicitation (or otherwise communicates with the local
            // node), ensuring only live and reachable peers are added.
            //
            // Immediately ping this new peer if new (a fast UDP send).
            ping(
                &self.cached_ping_frame,
                &self.socket,
                pex_addr,
                &self.metric_frames_sent,
                &self.metric_bytes_sent,
            )
            .await;
        }

        Ok(())
    }

    /// Return the randomised identity assigned to this instance.
    pub(crate) fn identity(&self) -> &Identity {
        &self.identity
    }
}

/// Wait for a UDP datagram to become ready, and read it entirely into `buf`.
async fn recv(socket: &UdpSocket, buf: &mut BytesMut) -> (usize, SocketAddr) {
    let (n_bytes, addr) = socket
        .recv_buf_from(buf)
        .await
        // These errors are libc's recvfrom() or converting the kernel-provided
        // socket structure to rust's SocketAddr - neither should ever happen.
        .expect("invalid recvfrom");

    trace!(%addr, n_bytes, "socket read");
    (n_bytes, addr)
}

/// Wait for a UDP datagram to arrive, and decode it into a gossip Frame.
///
/// Clears the contents of `buf` before reading the frame.
async fn read_frame(socket: &UdpSocket) -> Result<(usize, proto::Frame, SocketAddr), Error> {
    // Pre-allocate a buffer large enough to hold the maximum message size.
    //
    // Reading data from a UDP socket silently truncates if there's not enough
    // buffer space to write the full packet payload (tokio doesn't support
    // MSG_TRUNC-like flags on reads).
    let mut buf = BytesMut::with_capacity(MAX_FRAME_BYTES);

    let (n_bytes, addr) = recv(socket, &mut buf).await;

    // Decode the frame, re-using byte arrays from the underlying buffer.
    match proto::Frame::decode(buf.freeze()) {
        Ok(frame) => {
            debug!(?frame, %addr, n_bytes, "read frame");
            Ok((n_bytes, frame, addr))
        }
        Err(e) => Err(Error::Deserialise { addr, source: e }),
    }
}

/// Given a pre-allocated `frame`, clear and populate it with the provided
/// `payload` containing a set of [`FrameMessage`], serialising it to `buf`.
fn populate_frame(
    frame: &mut proto::Frame,
    payload: Vec<FrameMessage>,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    frame.messages = payload;

    // Reading data from a UDP socket silently truncates if there's not enough
    // buffer space to write the full packet payload. This library will
    // pre-allocate a buffer of this size to read packets into, therefore all
    // messages must be shorter than this value.
    if frame.encoded_len() > MAX_FRAME_BYTES {
        error!(
            n_bytes=frame.encoded_len(),
            n_max=%MAX_FRAME_BYTES,
            "attempted to send frame larger than configured maximum"
        );
        return Err(Error::MaxSize(frame.encoded_len()));
    }

    buf.clear();
    frame.encode(buf).expect("buffer should grow");

    debug_assert!(proto::Frame::decode(crate::Bytes::from(buf.clone())).is_ok());

    Ok(())
}

/// Instantiate a new [`FrameMessage`] from the given [`Payload`].
fn new_payload(p: Payload) -> proto::FrameMessage {
    proto::FrameMessage { payload: Some(p) }
}

/// Send a PING message to `socket`.
pub(crate) async fn ping(
    ping_frame: &[u8],
    socket: &UdpSocket,
    addr: SocketAddr,
    sent_frames: &SentFrames,
    sent_bytes: &SentBytes,
) -> usize {
    // Check the payload length as a primitive/best-effort way of ensuring only
    // ping frames are sent via this mechanism.
    //
    // Normal messaging should be performed through a Peer's send() method.
    debug_assert_eq!(ping_frame.len(), 22);

    match socket.send_to(ping_frame, &addr).await {
        Ok(n_bytes) => {
            debug!(n_bytes, %addr, "ping");
            sent_frames.inc(1);
            sent_bytes.inc(n_bytes);
            n_bytes
        }
        Err(e) => {
            warn!(error=%e, %addr, "ping failed");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{MAX_USER_PAYLOAD_BYTES, USER_PAYLOAD_OVERHEAD};

    use super::*;

    #[test]
    fn test_user_frame_overhead() {
        let identity = Identity::new();

        // Generate a pre-populated frame header.
        let mut frame = proto::Frame {
            identity: identity.as_bytes().clone(),
            messages: vec![],
        };

        let mut buf = Vec::new();
        populate_frame(
            &mut frame,
            vec![new_payload(Payload::UserData(proto::UserPayload {
                payload: crate::Bytes::new(), // Empty/0-sized
            }))],
            &mut buf,
        )
        .unwrap();

        // The proto type should self-report the same size.
        assert_eq!(buf.len(), frame.encoded_len());

        // The overhead const should be accurate
        assert_eq!(buf.len(), USER_PAYLOAD_OVERHEAD);

        // The max user payload size should be accurate.
        assert_eq!(MAX_FRAME_BYTES - buf.len(), MAX_USER_PAYLOAD_BYTES);
    }
}
