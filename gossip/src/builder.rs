use std::sync::Arc;

use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc,
};

use crate::{handle::GossipHandle, reactor::Reactor, Dispatcher};

/// Gossip subsystem configuration and initialisation.
#[derive(Debug)]
pub struct Builder<T> {
    seed_addrs: Vec<String>,
    dispatcher: T,
    metric: Arc<metric::Registry>,
}

impl<T> Builder<T> {
    /// Use `seed_addrs` as seed peer addresses, and dispatch any application
    /// messages to `dispatcher`.
    ///
    /// Each address in `seed_addrs` is re-resolved periodically and the first
    /// resolved IP address is used for peer communication.
    pub fn new(seed_addrs: Vec<String>, dispatcher: T, metric: Arc<metric::Registry>) -> Self {
        Self {
            seed_addrs,
            dispatcher,
            metric,
        }
    }
}

impl<T> Builder<T>
where
    T: Dispatcher + 'static,
{
    /// Initialise the gossip subsystem using `socket` for communication.
    ///
    /// # Panics
    ///
    /// This call spawns a tokio task, and as such must be called from within a
    /// tokio runtime.
    #[must_use = "gossip reactor stops when handle drops"]
    pub fn build(self, socket: UdpSocket) -> GossipHandle {
        // Obtain a channel to communicate between the actor, and all handles
        let (tx, rx) = mpsc::channel(1000);

        // Initialise the reactor
        let reactor = Reactor::new(self.seed_addrs, socket, self.dispatcher, &self.metric);
        let identity = reactor.identity().clone();

        // Start the message reactor.
        tokio::spawn(reactor.run(rx));

        GossipHandle::new(tx, identity)
    }

    /// Bind to the provided socket address and initialise the gossip subsystem.
    pub async fn bind<A>(self, bind_addr: A) -> Result<GossipHandle, std::io::Error>
    where
        A: ToSocketAddrs + Send,
    {
        Ok(self.build(UdpSocket::bind(bind_addr).await?))
    }
}
