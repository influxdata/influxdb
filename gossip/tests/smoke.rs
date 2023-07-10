use std::{sync::Arc, time::Duration};

use test_helpers::{maybe_start_logging, timeout::FutureTimeout};
use tokio::{net::UdpSocket, sync::mpsc};

use gossip::*;

/// Assert that starting up a reactor performs the initial peer discovery
/// from a set of seeds, resulting in both peers known of one another.
#[tokio::test]
async fn test_payload_exchange() {
    maybe_start_logging();

    let metrics = Arc::new(metric::Registry::default());

    // How long to wait for peer discovery to complete.
    const TIMEOUT: Duration = Duration::from_secs(5);

    // Bind a UDP socket to a random port
    let a_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("failed to bind UDP socket");
    let a_addr = a_socket.local_addr().expect("failed to read local addr");

    // And a socket for the second reactor
    let b_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("failed to bind UDP socket");
    let b_addr = b_socket.local_addr().expect("failed to read local addr");

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
}
