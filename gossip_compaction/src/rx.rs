//! A deserialiser and dispatcher of [gossip] messages for the
//! [`Topic::CompactionEvents`] topic.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use generated_types::influxdata::iox::gossip::{v1::CompactionEvent, Topic};
use generated_types::prost::Message;
use gossip::Identity;
use observability_deps::tracing::{info, warn};
use tokio::{sync::mpsc, task::JoinHandle};

/// A [`CompactionEvent`] notification handler received via gossip.
#[async_trait]
pub trait CompactionEventHandler: Send + Sync + Debug {
    /// Process `message`.
    async fn handle(&self, event: CompactionEvent);
}

#[async_trait]
impl<T> CompactionEventHandler for Arc<T>
where
    T: CompactionEventHandler,
{
    async fn handle(&self, event: CompactionEvent) {
        T::handle(self, event).await
    }
}

/// An async gossip message dispatcher.
///
/// This type is responsible for deserialising incoming gossip
/// [`Topic::NewParquetFiles`] payloads and passing them off to the provided
/// [`CompactionEventHandler`] implementation.
///
/// This decoupling allow the handler to deal strictly in terms of messages,
/// abstracting it from the underlying message transport / format.
///
/// This type also provides a buffer between incoming events, and processing,
/// preventing processing time from blocking the gossip reactor. Once the buffer
/// is full, incoming events are dropped until space is made through processing
/// of outstanding events. Dropping the [`CompactionEventRx`] stops the background
/// event loop.
#[derive(Debug)]
pub struct CompactionEventRx {
    tx: mpsc::Sender<Bytes>,
    task: JoinHandle<()>,
}

impl CompactionEventRx {
    /// Initialise a new dispatcher, buffering up to `buffer` number of events.
    ///
    /// The provided `handler` does not block the gossip reactor during
    /// execution.
    pub fn new<T>(handler: T, buffer: usize) -> Self
    where
        T: CompactionEventHandler + 'static,
    {
        // Initialise a buffered channel to decouple the two halves.
        let (tx, rx) = mpsc::channel(buffer);

        // And run a receiver loop to pull the events from the channel.
        let task = tokio::spawn(dispatch_loop(rx, handler));

        Self { tx, task }
    }
}

#[async_trait]
impl gossip::Dispatcher<Topic> for CompactionEventRx {
    async fn dispatch(&self, topic: Topic, payload: Bytes, _identity: Identity) {
        if topic != Topic::CompactionEvents {
            return;
        }
        if let Err(e) = self.tx.try_send(payload) {
            warn!(error=%e, "failed to buffer gossip event");
        }
    }
}

impl Drop for CompactionEventRx {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn dispatch_loop<T>(mut rx: mpsc::Receiver<Bytes>, handler: T)
where
    T: CompactionEventHandler,
{
    while let Some(payload) = rx.recv().await {
        // Deserialise the payload into the appropriate proto type.
        let event = match CompactionEvent::decode(payload) {
            Ok(v) => v,
            Err(e) => {
                warn!(error=%e, "failed to deserialise gossip message");
                continue;
            }
        };

        // Pass this message off to the handler to process.
        handler.handle(event).await;
    }

    info!("stopping gossip dispatcher");
}
