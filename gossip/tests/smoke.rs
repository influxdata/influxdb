#![allow(clippy::default_constructed_unit_structs)]

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use test_helpers::{maybe_start_logging, timeout::FutureTimeout};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self, error::TryRecvError},
};

use gossip::*;

// How long to wait for various time-limited test loops to complete.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Bind a UDP socket on a random port and return it alongside the socket
/// address.
async fn random_udp() -> (UdpSocket, SocketAddr) {
    // Bind a UDP socket to a random port
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("failed to bind UDP socket");
    let addr = socket.local_addr().expect("failed to read local addr");

    (socket, addr)
}

/// An example gossip topic type.
#[derive(Debug, PartialEq)]
enum Topic {
    Bananas,
    Donkey,
    Goose,
}

impl From<Topic> for u64 {
    fn from(value: Topic) -> Self {
        value as u64
    }
}

impl TryFrom<u64> for Topic {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(match value {
            x if x == Topic::Bananas as u64 => Self::Bananas,
            x if x == Topic::Donkey as u64 => Self::Donkey,
            x if x == Topic::Goose as u64 => Self::Goose,
            _ => return Err(()),
        })
    }
}

/// Assert that starting up a reactor performs the initial peer discovery
/// from a set of seeds, resulting in both peers known of one another.
#[tokio::test]
async fn test_payload_exchange() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;

    // Initialise the dispatchers for the reactors
    let (a_tx, mut a_rx) = mpsc::channel(5);
    let (b_tx, mut b_rx) = mpsc::channel(5);

    // Initialise both reactors
    let addrs = vec![a_addr.to_string(), b_addr.to_string()];
    let a = Builder::new(addrs.clone(), a_tx, Arc::clone(&metrics)).build(a_socket);
    let b = Builder::new(addrs, b_tx, Arc::clone(&metrics)).build(b_socket);

    // Wait for peer discovery to occur
    async {
        loop {
            if a.get_peers().await.len() == 1 {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Send the payload through peer A
    let a_payload = Bytes::from_static(b"bananas");
    a.broadcast(a_payload.clone(), Topic::Bananas)
        .await
        .unwrap();

    // Assert it was received by peer B
    let (topic, got) = b_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, a_payload);
    assert_eq!(topic, Topic::Bananas);

    // Do the reverse - send from B to A
    let b_payload = Bytes::from_static(b"platanos");
    b.broadcast(b_payload.clone(), Topic::Bananas)
        .await
        .unwrap();
    let (topic, got) = a_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, b_payload);
    assert_eq!(topic, Topic::Bananas);

    // Send another payload through peer A (ensuring scratch buffers are
    // correctly wiped, etc)
    let a_payload = Bytes::from_static(b"platanos");
    a.broadcast(a_payload.clone(), Topic::Bananas)
        .await
        .unwrap();
    let (topic, got) = b_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, a_payload);
    assert_eq!(topic, Topic::Bananas);
}

/// Construct a set of peers such that peer exchange has to occur for them to
/// know the full set of peers.
///
/// This configures the following topology:
///
/// ```
///     A <--> B <--> C
/// ```
///
/// Where only node B knows of both A & C - in order for A to discover C, it has
/// to perform PEX with B, and the same for C to discover A.
///
/// To further drive the discovery mechanism, B is not informed of any peer - it
/// discovers A & B through their announcement PINGs at startup.
#[tokio::test]
async fn test_peer_exchange() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, _a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;
    let (c_socket, _c_addr) = random_udp().await;

    let (a_tx, mut a_rx) = mpsc::channel(5);

    let a = Builder::<_, Topic>::new(vec![b_addr.to_string()], a_tx, Arc::clone(&metrics))
        .build(a_socket);
    let b = Builder::<_, Topic>::new(vec![], NopDispatcher::default(), Arc::clone(&metrics))
        .build(b_socket);
    let c = Builder::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(c_socket);

    // At this point, A is configured with B as a seed, B knows of no other
    // peers, and C knows of B.

    // Wait for peer exchange to occur
    async {
        loop {
            if a.get_peers().await.len() == 2
                && b.get_peers().await.len() == 2
                && c.get_peers().await.len() == 2
            {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    let a_peers = a.get_peers().await;
    assert!(a_peers.contains(&b.identity()));
    assert!(a_peers.contains(&c.identity()));
    assert!(!a_peers.contains(&a.identity()));

    let b_peers = b.get_peers().await;
    assert!(b_peers.contains(&a.identity()));
    assert!(b_peers.contains(&c.identity()));
    assert!(!b_peers.contains(&b.identity()));

    let c_peers = c.get_peers().await;
    assert!(c_peers.contains(&a.identity()));
    assert!(c_peers.contains(&b.identity()));
    assert!(!c_peers.contains(&c.identity()));

    // Prove that C has discovered A, which would only be possible through PEX
    // with B.
    let payload = Bytes::from_static(b"bananas");
    c.broadcast(payload.clone(), Topic::Bananas).await.unwrap();

    let (topic, got) = a_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, payload);
    assert_eq!(topic, Topic::Bananas);
}

/// Construct a set of peers such that a delayed peer exchange has to occur for
/// them to know the full set of peers.
///
/// This configures the following topology:
///
/// ```
///     A <--> B
/// ```
///
/// And once PEX has completed between them, a new node C is introduced with B
/// as a seed:
///
/// ```
///     A <--> B <-- C
/// ```
///
/// At which point A will discover C when C sends PING messages to all peers it
/// discovers from B.
#[tokio::test]
async fn test_delayed_peer_exchange() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;
    let (c_socket, _c_addr) = random_udp().await;

    let a = Builder::<_, Topic>::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::<_, Topic>::new(
        vec![a_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(b_socket);

    // At this point, A is configured with B as a seed, B knows of no other
    // peers, and C knows of B.

    // Wait for peer exchange to occur between A and B
    async {
        loop {
            if a.get_peers().await.len() == 1 {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    let a_peers = a.get_peers().await;
    assert!(a_peers.contains(&b.identity()));

    let b_peers = b.get_peers().await;
    assert!(b_peers.contains(&a.identity()));

    // Start C now the initial PEX is complete, seeding it with the address of
    // B.
    let c = Builder::<_, Topic>::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(c_socket);

    // C will perform PEX with B, learning of A.
    async {
        loop {
            if c.get_peers().await.len() == 2 {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    let c_peers = c.get_peers().await;
    assert!(c_peers.contains(&a.identity()));
    assert!(c_peers.contains(&b.identity()));

    // And eventually A should discover C through B.
    async {
        loop {
            if a.get_peers().await.len() == 2 {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    let a_peers = a.get_peers().await;
    assert!(a_peers.contains(&b.identity()));
    assert!(a_peers.contains(&c.identity()));
}

/// Initialise a pair of nodes A & B with an unreachable third node C listed as
/// a seed.
///
/// Ensure this seed C is not added to the local peer list.
#[tokio::test]
async fn test_seed_health_check() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;
    let (_c_socket, c_addr) = random_udp().await;

    let seeds = vec![a_addr.to_string(), b_addr.to_string(), c_addr.to_string()];
    let a = Builder::<_, Topic>::new(
        seeds.to_vec(),
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::<_, Topic>::new(seeds, NopDispatcher::default(), Arc::clone(&metrics))
        .build(b_socket);

    // Wait for peer exchange to occur between A and B
    async {
        loop {
            if !a.get_peers().await.is_empty() {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Assert that only the live peers were added.
    let a_peers = a.get_peers().await;
    assert_eq!(a_peers.len(), 1);
    assert!(a_peers.contains(&b.identity()));

    let b_peers = b.get_peers().await;
    assert_eq!(b_peers.len(), 1);
    assert!(b_peers.contains(&a.identity()));
}

/// Initialise a pair of nodes A & B, kill B after PEX, and then introduce a new
/// node C.
///
/// This test ensures that dead peers are not added to the joiner's local peer
/// list, causing them to age out of the cluster and not be propagated forever.
#[tokio::test]
async fn test_discovery_health_check() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;

    let a = Builder::<_, Topic>::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::<_, Topic>::new(
        vec![a_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(b_socket);

    // Wait for peer exchange to occur between A and B
    async {
        loop {
            if a.get_peers().await.len() == 1 {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Stop B.
    let b_identity = b.identity();
    drop(b);

    // Introduce C to the cluster, seeding it with A
    let (c_socket, _c_addr) = random_udp().await;

    let c = Builder::<_, Topic>::new(
        vec![a_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(c_socket);

    // Wait for peer exchange to occur between A and B
    async {
        loop {
            if !c.get_peers().await.is_empty() {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Assert that only the live peer A was added.
    let c_peers = c.get_peers().await;
    assert_eq!(c_peers.len(), 1);
    assert!(c_peers.contains(&a.identity()));
    assert!(!c_peers.contains(&b_identity));
}

/// Drive peer removal / age-out.
///
/// Start two nodes, wait for PEX to complete, kill one node, and wait for it to
/// eventually remove the dead peer. Because this is a periodic/time-based
/// action, this test uses the tokio's "auto-advance time" test functionality.
#[tokio::test]
async fn test_peer_removal() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;

    let a = Builder::<_, Topic>::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::<_, Topic>::new(
        vec![a_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(b_socket);

    // Wait for peer exchange to occur between A and B
    async {
        loop {
            let peers = a.get_peers().await;
            if peers.len() == 1 {
                assert!(peers.contains(&b.identity()));
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Stop B.
    drop(b);

    // Make time tick as fast as necessary to advance to the next timer event.
    tokio::time::pause();
    for _ in 0..50 {
        tokio::time::advance(Duration::from_secs(35)).await;
    }

    // Use the stdlib time to avoid tokio's paused time and bound the loop time.
    let started_at = Instant::now();
    loop {
        let peers = a.get_peers().await;
        if peers.is_empty() {
            break;
        }

        if Instant::now().duration_since(started_at) > TIMEOUT {
            panic!("timeout waiting for peer removal");
        }
    }
}

/// Topic subscriptions / interests.
///
/// Ensure a node that is subscribed to one topic is sent only messages for that
/// topic by peers.
///
/// Covers multiple interested topics (multiple values in the topic set) and a
/// single non-interested topic.
#[tokio::test]
async fn test_topic_send_filter() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;

    let (a_tx, mut a_rx) = mpsc::channel(5);

    // Configure A to be only interested in messages in the "bananas" and
    // "goose" topics.
    let a = Builder::<_, Topic>::new(vec![b_addr.to_string()], a_tx, Arc::clone(&metrics))
        .with_topic_filter(
            TopicInterests::default()
                .with_topic(Topic::Bananas)
                .with_topic(Topic::Goose),
        )
        .build(a_socket);

    let b = Builder::<_, Topic>::new(
        vec![a_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(b_socket);

    // Wait for peer exchange to occur between A and B
    async {
        loop {
            let peers = a.get_peers().await;
            if peers.len() == 1 {
                assert!(peers.contains(&b.identity()));
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Have B send a message for a topic A is not interested in.
    let topic_donkey = Bytes::from_static(b"donkey");
    b.broadcast(topic_donkey, Topic::Donkey).await.unwrap();

    // Followed by a message for a topic it is interested in.
    let topic_bananas = Bytes::from_static(b"bananas");
    b.broadcast(topic_bananas.clone(), Topic::Bananas)
        .await
        .unwrap();

    // Which should be received.
    let got = a_rx.recv().await.unwrap();
    assert_eq!(got, (Topic::Bananas, topic_bananas));

    // There should be no other messages enqueued for A to process.
    assert_eq!(a_rx.try_recv(), Err(TryRecvError::Empty));

    // Have B send a message for a different topic A is interested in.
    let topic_goose = Bytes::from_static(b"goose");
    b.broadcast(topic_goose.clone(), Topic::Goose)
        .await
        .unwrap();

    // Which should be received.
    let got = a_rx.recv().await.unwrap();
    assert_eq!(got, (Topic::Goose, topic_goose));

    // There should be no other messages enqueued for A to process.
    assert_eq!(a_rx.try_recv(), Err(TryRecvError::Empty));
}

/// Unknown / backwards-compatible / graceful handling of unknown topics.
///
/// Ensure a node that receives an unknown topic and returns an error in the
/// TryFrom<u64> conversion doesn't panic and continues to function.
#[tokio::test]
async fn test_unknown_topic() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;

    let (a_tx, mut a_rx) = mpsc::channel(5);

    // Configure A to be only interested in messages in the "bananas" and
    // "goose" topics.
    let a = Builder::<_, Topic>::new(vec![b_addr.to_string()], a_tx, Arc::clone(&metrics))
        .with_topic_filter(
            TopicInterests::default()
                .with_topic(Topic::Bananas)
                .with_topic(Topic::Goose),
        )
        .build(a_socket);

    // Have B send topics using raw u64 instead of the enum.
    let b = Builder::<_, u64>::new(
        vec![a_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(b_socket);

    // Wait for peer exchange to occur between A and B
    async {
        loop {
            let peers = a.get_peers().await;
            if peers.len() == 1 {
                assert!(peers.contains(&b.identity()));
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Have B send a message for a topic A doesn't know about.
    let topic_wat = Bytes::from_static(b"wat");
    b.broadcast(topic_wat.clone(), 42).await.unwrap();

    // This message should not be handed to the dispatcher for A.
    assert_eq!(a_rx.try_recv(), Err(TryRecvError::Empty));

    // But A should continue chugging along.

    // Have B send a message for a topic A is interested in.
    let topic_goose = Bytes::from_static(b"goose");
    b.broadcast(topic_goose.clone(), Topic::Goose as u64)
        .await
        .unwrap();

    // Which should be received.
    let got = a_rx.recv().await.unwrap();
    assert_eq!(got, (Topic::Goose, topic_goose));

    // There should be no other messages enqueued for A to process.
    assert_eq!(a_rx.try_recv(), Err(TryRecvError::Empty));
}

/// Assert that subset broadcasts are sent to a portion of the peers.
#[tokio::test]
async fn test_broadcast_subset() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;
    let (c_socket, c_addr) = random_udp().await;

    // Initialise the dispatchers for the reactors
    let (a_tx, _a_rx) = mpsc::channel(5);
    let (b_tx, mut b_rx) = mpsc::channel(5);
    let (c_tx, mut c_rx) = mpsc::channel(5);

    // Initialise all reactors
    let addrs = vec![a_addr.to_string(), b_addr.to_string(), c_addr.to_string()];
    let a = Builder::<_, Topic>::new(addrs.clone(), a_tx, Arc::clone(&metrics)).build(a_socket);
    // B and C are only interested in the "bananas" topic.
    let b = Builder::<_, Topic>::new(addrs.clone(), b_tx, Arc::clone(&metrics))
        .with_topic_filter(TopicInterests::default().with_topic(Topic::Bananas))
        .build(b_socket);
    let c = Builder::<_, Topic>::new(addrs, c_tx, Arc::clone(&metrics))
        .with_topic_filter(TopicInterests::default().with_topic(Topic::Bananas))
        .build(c_socket);

    // Wait for peer discovery to occur
    async {
        loop {
            if a.get_peers().await.len() == 2
                && b.get_peers().await.len() == 2
                && c.get_peers().await.len() == 2
            {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Send the payload through peer A to a subset of peers.
    let a_payload = Bytes::from_static(b"bananas");
    a.broadcast_subset(a_payload.clone(), Topic::Bananas)
        .await
        .unwrap();

    // Assert it was received by either peer B OR C.
    let (other, (topic, payload)) = tokio::select! {
        Some(got) = b_rx .recv().with_timeout_panic(TIMEOUT) => (&mut c_rx, got),
        Some(got) = c_rx.recv().with_timeout_panic(TIMEOUT) => (&mut b_rx, got),
        else => panic!("no channel is alive"),
    };
    assert_eq!(payload, a_payload);
    assert_eq!(topic, Topic::Bananas);

    // The other peer should not receive the message.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(other.try_recv(), Err(TryRecvError::Empty));

    // Now dispatch a message for a topic with no interested peers, ensuring it
    // doesn't cause a crash/div 0 error/etc.
    a.broadcast_subset(a_payload.clone(), Topic::Goose)
        .await
        .unwrap();

    // Neither peer should receive anything.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(b_rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(c_rx.try_recv(), Err(TryRecvError::Empty));
}

/// Assert that subset broadcasts are sent to a portion of the peers.
#[tokio::test]
async fn test_sender_ident() {
    maybe_start_logging();

    struct Dispatch(mpsc::Sender<Identity>);
    #[async_trait::async_trait]
    impl<T: TryFrom<u64> + Send + Sync + 'static> Dispatcher<T> for Dispatch {
        async fn dispatch(&self, _topic: T, _payload: Bytes, sender: Identity) {
            self.0.send(sender).await.unwrap();
        }
    }

    let metrics = Arc::new(metric::Registry::default());

    let (a_socket, a_addr) = random_udp().await;
    let (b_socket, b_addr) = random_udp().await;

    // Initialise the dispatchers for the reactors
    let (a_tx, _a_rx) = mpsc::channel(5);
    let (b_tx, mut b_rx) = mpsc::channel(5);

    // Initialise all reactors
    let addrs = vec![a_addr.to_string(), b_addr.to_string()];
    let a = Builder::<_, Topic>::new(addrs.clone(), a_tx, Arc::clone(&metrics)).build(a_socket);
    let b = Builder::<_, Topic>::new(addrs.clone(), Dispatch(b_tx), Arc::clone(&metrics))
        .build(b_socket);

    // Wait for peer discovery to occur
    async {
        loop {
            if a.get_peers().await.len() == 1 && b.get_peers().await.len() == 1 {
                break;
            }
        }
    }
    .with_timeout_panic(TIMEOUT)
    .await;

    // Send the payload through peer A to a subset of peers.
    let a_payload = Bytes::from_static(b"bananas");
    a.broadcast_subset(a_payload.clone(), Topic::Bananas)
        .await
        .unwrap();

    let got_ident = b_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor should be running");

    assert_eq!(a.identity(), got_ident);

    // Validate the "get socket address" API using the identity
    let expect_addr = b
        .get_peer_addr(got_ident)
        .await
        .expect("a must exist in peer list");

    assert_eq!(expect_addr, a_addr);
}
