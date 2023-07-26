//! A deserialiser and dispatcher of [`gossip`] messages.

use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use generated_types::influxdata::iox::gossip::v1::{gossip_message::Msg, GossipMessage};
use generated_types::prost::Message;
use observability_deps::tracing::{info, warn};
use tokio::{sync::mpsc, task::JoinHandle};

/// A handler of [`Msg`] received via gossip.
#[async_trait]
pub trait GossipMessageHandler: Send + Sync + Debug {
    /// Process `message`.
    async fn handle(&self, message: Msg);
}

/// An async gossip message dispatcher.
///
/// This type is responsible for deserialising incoming gossip payloads and
/// passing them off to the provided [`GossipMessageHandler`] implementation.
/// This decoupling allow the handler to deal strictly in terms of messages,
/// abstracting it from the underlying message transport / format.
///
/// This type provides a buffer between incoming events, and processing,
/// preventing processing time from blocking the gossip reactor. Once the buffer
/// is full, incoming events are dropped until space is made through processing
/// of outstanding events.
#[derive(Debug)]
pub struct GossipMessageDispatcher {
    tx: mpsc::Sender<Bytes>,
    task: JoinHandle<()>,
}

impl GossipMessageDispatcher {
    /// Initialise a new dispatcher, buffering up to `buffer` number of events.
    ///
    /// The provided `handler` does not block the gossip reactor during
    /// execution.
    pub fn new<T>(handler: T, buffer: usize) -> Self
    where
        T: GossipMessageHandler + 'static,
    {
        // Initialise a buffered channel to decouple the two halves.
        let (tx, rx) = mpsc::channel(buffer);

        // And run a receiver loop to pull the events from the channel.
        let task = tokio::spawn(dispatch_loop(rx, handler));

        Self { tx, task }
    }
}

#[async_trait]
impl gossip::Dispatcher for GossipMessageDispatcher {
    async fn dispatch(&self, payload: Bytes) {
        if let Err(e) = self.tx.try_send(payload) {
            warn!(error=%e, "failed to buffer gossip event");
        }
    }
}

impl Drop for GossipMessageDispatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn dispatch_loop<T>(mut rx: mpsc::Receiver<Bytes>, handler: T)
where
    T: GossipMessageHandler,
{
    while let Some(payload) = rx.recv().await {
        // Deserialise the payload into the appropriate proto type.
        let msg = match GossipMessage::decode(payload).map(|v| v.msg) {
            Ok(Some(v)) => v,
            Ok(None) => {
                warn!("valid frame contains no message");
                continue;
            }
            Err(e) => {
                warn!(error=%e, "failed to deserialise gossip message");
                continue;
            }
        };

        // Pass this message off to the handler to process.
        handler.handle(msg).await;
    }

    info!("stopping gossip dispatcher");
}
