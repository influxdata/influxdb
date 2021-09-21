//! Captures UDP packets

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// UDP listener server that captures UDP messages (e.g. Jaeger spans)
/// for use in tests
use parking_lot::Mutex;
use tokio::{net::UdpSocket, select};
use tokio_util::sync::CancellationToken;

/// Maximum time to wait for a message, in seconds
const MAX_WAIT_TIME_SEC: u64 = 2;

/// A UDP message received by this server
#[derive(Clone)]
pub struct Message {
    data: Vec<u8>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Message({} bytes: {}", self.data.len(), self.to_string())
    }
}

impl ToString for Message {
    fn to_string(&self) -> String {
        String::from_utf8_lossy(&self.data).to_string()
    }
}

pub struct UdpCapture {
    socket_addr: std::net::SocketAddr,
    join_handle: tokio::task::JoinHandle<()>,
    token: CancellationToken,
    messages: Arc<Mutex<Vec<Message>>>,
}

impl UdpCapture {
    // Create a new server, listening for Udp messages
    pub async fn new() -> Self {
        // Bind to some address, letting the OS pick
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind udp listener");

        let socket_addr = socket.local_addr().unwrap();

        println!(
            "UDP server listening at {} port {}",
            socket_addr.ip(),
            socket_addr.port()
        );

        let token = CancellationToken::new();
        let messages = Arc::new(Mutex::new(vec![]));

        // Spawns a background task that listens on the
        let captured_messages = Arc::clone(&messages);
        let captured_token = token.clone();

        let join_handle = tokio::spawn(async move {
            println!("Starting udp listen");
            loop {
                let mut data = vec![0; 1024];

                select! {
                    _ = captured_token.cancelled() => {
                        println!("Received shutdown request");
                        return;
                    },
                    res  = socket.recv_from(&mut data) => {
                        let (sz, _origin) = res.expect("successful socket read");
                        data.resize(sz, 0);
                        let mut messages = captured_messages.lock();
                        messages.push(Message { data });
                    }
                }
            }
        });

        Self {
            socket_addr,
            join_handle,
            token,
            messages,
        }
    }

    /// return the ip on which this server is listening
    pub fn ip(&self) -> String {
        self.socket_addr.ip().to_string()
    }

    /// return the port on which this server is listening
    pub fn port(&self) -> String {
        self.socket_addr.port().to_string()
    }

    /// stop and wait for succesful shutdown of this server
    pub async fn stop(self) {
        self.token.cancel();
        if let Err(e) = self.join_handle.await {
            println!("Error waiting for shutdown of udp server: {}", e);
        }
    }

    // Return all messages this server has seen so far
    pub fn messages(&self) -> Vec<Message> {
        let messages = self.messages.lock();
        messages.clone()
    }

    // wait for a message to appear that passes `pred` or the timeout expires
    pub fn wait_for<P>(&self, mut pred: P)
    where
        P: FnMut(&Message) -> bool,
    {
        let end = Instant::now() + Duration::from_secs(MAX_WAIT_TIME_SEC);

        while Instant::now() < end {
            if self.messages.lock().iter().any(|m| pred(m)) {
                return;
            }
        }
        panic!(
            "Timeout expired before finding find messages that matches predicate. Messages:\n{:#?}",
            self.messages.lock()
        )
    }
}
