use std::{marker::PhantomData, sync::Arc};

use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc,
};

use crate::{
    handle::GossipHandle,
    reactor::Reactor,
    topic_set::{Topic, TopicSet},
    Dispatcher,
};

/// Gossip subsystem configuration and initialisation.
#[derive(Debug)]
pub struct Builder<T, S = u64> {
    seed_addrs: Vec<String>,
    dispatcher: T,
    metric: Arc<metric::Registry>,
    topic_set: TopicSet,
    _topic_type: PhantomData<S>,
}

impl<T, S> Builder<T, S> {
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
            topic_set: TopicSet::default(),
            _topic_type: PhantomData,
        }
    }
}

impl<T, S> Builder<T, S> {
    /// Configure the gossip instance to subscribe to the specified set of
    /// topics. If not called, the gossip instance defaults to "all topics".
    ///
    /// This causes the local node to advertise it is interested in the
    /// specified topics, causing remote peers to only send relevant messages.
    ///
    /// Remotes MAY send messages for topics that do not appear in this set.
    pub fn with_topic_filter<U>(self, topics: TopicInterests<U>) -> Builder<T, U>
    where
        U: Into<u64> + TryFrom<u64>,
    {
        Builder {
            seed_addrs: self.seed_addrs,
            dispatcher: self.dispatcher,
            metric: self.metric,
            topic_set: topics.0,
            _topic_type: PhantomData,
        }
    }
}

impl<T, S, E> Builder<T, S>
where
    T: Dispatcher<S> + 'static,
    S: Into<u64> + TryFrom<u64, Error = E> + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    /// Initialise the gossip subsystem using `socket` for communication.
    ///
    /// # Panics
    ///
    /// This call spawns a tokio task, and as such must be called from within a
    /// tokio runtime.
    #[must_use = "gossip reactor stops when handle drops"]
    pub fn build(self, socket: UdpSocket) -> GossipHandle<S> {
        // Obtain a channel to communicate between the actor, and all handles
        let (tx, rx) = mpsc::channel(1000);

        // Initialise the reactor
        let reactor = Reactor::<_, S>::new(
            self.seed_addrs,
            socket,
            self.dispatcher,
            &self.metric,
            self.topic_set,
        );
        let identity = reactor.identity().clone();

        // Start the message reactor.
        tokio::spawn(reactor.run(rx));

        GossipHandle::new(tx, identity)
    }

    /// Bind to the provided socket address and initialise the gossip subsystem.
    pub async fn bind<A>(self, bind_addr: A) -> Result<GossipHandle<S>, std::io::Error>
    where
        A: ToSocketAddrs + Send,
    {
        Ok(self.build(UdpSocket::bind(bind_addr).await?))
    }
}

/// A set of topic interests for the local node, defaulting to "none".
#[derive(Debug)]
pub struct TopicInterests<T>(TopicSet, PhantomData<T>);

impl<T> Default for TopicInterests<T> {
    fn default() -> Self {
        Self(TopicSet::empty(), PhantomData)
    }
}

impl<T> TopicInterests<T>
where
    T: Into<u64> + TryFrom<u64>,
{
    /// Add the specified topic ID to the local interests.
    ///
    /// A topic ID MUST be covertable into a `u64` in the range 0 to 63
    /// inclusive, and MUST be unique per topic.
    ///
    /// # Panics
    ///
    /// Panics if the topic ID is not in the inclusive range 0 to 63.
    pub fn with_topic(mut self, topic: T) -> Self {
        self.0.set_interested(Topic::encode(topic));
        self
    }
}
