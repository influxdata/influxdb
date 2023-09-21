//! A deserialiser and dispatcher of [gossip] messages for the
//! [`Topic::NewParquetFiles`] topic.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use generated_types::influxdata::iox::{
    catalog::v1::ParquetFile,
    gossip::{v1::NewParquetFile, Topic},
};
use generated_types::prost::Message;
use gossip::Identity;
use observability_deps::tracing::{info, warn};
use tokio::{sync::mpsc, task::JoinHandle};

/// A [`ParquetFile`] notification handler received via gossip.
#[async_trait]
pub trait ParquetFileEventHandler: Send + Sync + Debug {
    /// Process `message`.
    async fn handle(&self, event: ParquetFile);
}

#[async_trait]
impl<T> ParquetFileEventHandler for Arc<T>
where
    T: ParquetFileEventHandler,
{
    async fn handle(&self, event: ParquetFile) {
        T::handle(self, event).await
    }
}

/// An async gossip message dispatcher.
///
/// This type is responsible for deserialising incoming gossip
/// [`Topic::NewParquetFiles`] payloads and passing them off to the provided
/// [`ParquetFileEventHandler`] implementation.
///
/// This decoupling allow the handler to deal strictly in terms of messages,
/// abstracting it from the underlying message transport / format.
///
/// This type also provides a buffer between incoming events, and processing,
/// preventing processing time from blocking the gossip reactor. Once the buffer
/// is full, incoming events are dropped until space is made through processing
/// of outstanding events. Dropping the [`ParquetFileRx`] stops the background
/// event loop.
#[derive(Debug)]
pub struct ParquetFileRx {
    tx: mpsc::Sender<Bytes>,
    task: JoinHandle<()>,
}

impl ParquetFileRx {
    /// Initialise a new dispatcher, buffering up to `buffer` number of events.
    ///
    /// The provided `handler` does not block the gossip reactor during
    /// execution.
    pub fn new<T>(handler: T, buffer: usize) -> Self
    where
        T: ParquetFileEventHandler + 'static,
    {
        // Initialise a buffered channel to decouple the two halves.
        let (tx, rx) = mpsc::channel(buffer);

        // And run a receiver loop to pull the events from the channel.
        let task = tokio::spawn(dispatch_loop(rx, handler));

        Self { tx, task }
    }
}

#[async_trait]
impl gossip::Dispatcher<Topic> for ParquetFileRx {
    async fn dispatch(&self, topic: Topic, payload: Bytes, _sender: Identity) {
        if topic != Topic::NewParquetFiles {
            return;
        }
        if let Err(e) = self.tx.try_send(payload) {
            warn!(error=%e, "failed to buffer gossip event");
        }
    }
}

impl Drop for ParquetFileRx {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn dispatch_loop<T>(mut rx: mpsc::Receiver<Bytes>, handler: T)
where
    T: ParquetFileEventHandler,
{
    while let Some(payload) = rx.recv().await {
        // Deserialise the payload into the appropriate proto type.
        let event = match NewParquetFile::decode(payload).map(|v| v.file) {
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
        handler.handle(event).await;
    }

    info!("stopping gossip dispatcher");
}
