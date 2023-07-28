#![allow(clippy::default_constructed_unit_structs)]

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use test_helpers::{maybe_start_logging, timeout::FutureTimeout};
use tokio::{net::UdpSocket, sync::mpsc};

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
    a.broadcast(a_payload.clone()).await.unwrap();

    // Assert it was received by peer B
    let got = b_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, a_payload);

    // Do the reverse - send from B to A
    let b_payload = Bytes::from_static(b"platanos");
    b.broadcast(b_payload.clone()).await.unwrap();
    let got = a_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, b_payload);

    // Send another payload through peer A (ensuring scratch buffers are
    // correctly wiped, etc)
    let a_payload = Bytes::from_static(b"platanos");
    a.broadcast(a_payload.clone()).await.unwrap();
    let got = b_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, a_payload);
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

    let a = Builder::new(vec![b_addr.to_string()], a_tx, Arc::clone(&metrics)).build(a_socket);
    let b = Builder::new(vec![], NopDispatcher::default(), Arc::clone(&metrics)).build(b_socket);
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
    c.broadcast(payload.clone()).await.unwrap();

    let got = a_rx
        .recv()
        .with_timeout_panic(TIMEOUT)
        .await
        .expect("reactor stopped");
    assert_eq!(got, payload);
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

    let a = Builder::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::new(
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
    let c = Builder::new(
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
    let a = Builder::new(
        seeds.to_vec(),
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::new(seeds, NopDispatcher::default(), Arc::clone(&metrics)).build(b_socket);

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

    let a = Builder::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::new(
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

    let c = Builder::new(
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

    let a = Builder::new(
        vec![b_addr.to_string()],
        NopDispatcher::default(),
        Arc::clone(&metrics),
    )
    .build(a_socket);
    let b = Builder::new(
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
