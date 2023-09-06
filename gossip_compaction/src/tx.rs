//! A serialiser and broadcaster of [`gossip`] messages for the
//! [`Topic::CompactionEvents`] topic.

use std::fmt::Debug;

use generated_types::{
    influxdata::iox::gossip::{v1::CompactionEvent, Topic},
    prost::Message,
};
use observability_deps::tracing::{debug, error, warn};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinHandle,
};

/// A gossip broadcast primitive specialised for compaction completion
/// notifications.
///
/// This type accepts any type that converts into a [`CompactionEvent`] from the
/// application logic, serialises the message (applying any necessary
/// transformations due to the underlying transport limitations) and broadcasts
/// the result to all listening peers.
///
/// Serialisation and processing of the [`CompactionEvent`] given to the
/// [`CompactionEventTx::broadcast()`] method happens in a background actor task,
/// decoupling the caller from the latency of processing each frame. Dropping
/// the [`CompactionEventTx`] stops this background actor task.
#[derive(Debug)]
pub struct CompactionEventTx<T = CompactionEvent> {
    tx: mpsc::Sender<T>,
    task: JoinHandle<()>,
}

impl<T> Drop for CompactionEventTx<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<T> CompactionEventTx<T>
where
    T: Into<CompactionEvent> + Debug + Send + Sync + 'static,
{
    /// Construct a new [`CompactionEventTx`] that publishes gossip messages over
    /// `gossip`.
    pub fn new(gossip: gossip::GossipHandle<Topic>) -> Self {
        let (tx, rx) = mpsc::channel(100);

        let task = tokio::spawn(actor_loop(rx, gossip));

        Self { tx, task }
    }

    /// Asynchronously broadcast `file` to all interested peers.
    ///
    /// This method enqueues `file` into the serialisation queue, and processed
    /// & transmitted asynchronously.
    pub fn broadcast(&self, file: T) {
        debug!(?file, "sending new compaction notification");
        match self.tx.try_send(file) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {
                panic!("compaction notification serialisation actor not running")
            }
            Err(TrySendError::Full(_)) => {
                warn!("compaction notification serialisation queue full, dropping message")
            }
        }
    }
}

async fn actor_loop<T>(mut rx: mpsc::Receiver<T>, gossip: gossip::GossipHandle<Topic>)
where
    T: Into<CompactionEvent> + Send + Sync,
{
    while let Some(file) = rx.recv().await {
        let file: CompactionEvent = file.into();

        if let Err(e) = gossip
            .broadcast(file.encode_to_vec(), Topic::CompactionEvents)
            .await
        {
            error!(error=%e, "failed to broadcast payload");
        }
    }

    debug!("stopping compaction gossip serialisation actor");
}
