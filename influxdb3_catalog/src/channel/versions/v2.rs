use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Context;
use futures::future::try_join_all;
use observability_deps::tracing::warn;
use tokio::sync::{mpsc, oneshot};

use crate::channel::{CATALOG_SUBSCRIPTION_BUFFER_SIZE, SubscriptionError};
use crate::{catalog::versions::v2::CatalogUpdate, log::versions::v4::CatalogBatch};

#[derive(Debug, Clone)]
struct CatalogUpdateSender {
    tx: mpsc::Sender<CatalogUpdateMessage>,
    stopped: Arc<AtomicBool>,
}

impl CatalogUpdateSender {
    /// Check the `stopped` state on this channel
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl Deref for CatalogUpdateSender {
    type Target = mpsc::Sender<CatalogUpdateMessage>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for CatalogUpdateSender {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

#[derive(Debug)]
pub struct CatalogUpdateReceiver {
    rx: mpsc::Receiver<CatalogUpdateMessage>,
    stopped: Arc<AtomicBool>,
}

impl CatalogUpdateReceiver {
    /// Set the state of this receiver to stopped
    ///
    /// This will have broadcasted messages from the catalog ignore this channel. This is useful
    /// for cases like application shutdown, where the subscriber is closing the receiver, so we
    /// do not want to attempt to send a message to it, because doing so would return an error due
    /// to the channel being closed.
    ///
    /// This allows us to use the `stopped` state to filter out channels that have been
    /// intentionally closed before application shutdown when broadcasting, vs. those that have been
    /// closed unexpectedly, for which we want to have an error appear in the logs when broadcast
    /// fails.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
}

impl Deref for CatalogUpdateReceiver {
    type Target = mpsc::Receiver<CatalogUpdateMessage>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for CatalogUpdateReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

/// A message containing a set of `CatalogUpdate`s that can be handled by subscribers to the
/// `Catalog`.
///
/// The response is sent in the `Drop` implementation of this type, so that the consumer of these
/// messages does not need to worry about sending the response back to the catalog on broadcast.
pub struct CatalogUpdateMessage {
    update: Arc<CatalogUpdate>,
    tx: Option<oneshot::Sender<()>>,
}

impl CatalogUpdateMessage {
    /// Create a new `CatalogUpdateMessage`
    fn new(update: Arc<CatalogUpdate>, tx: oneshot::Sender<()>) -> Self {
        Self {
            update,
            tx: Some(tx),
        }
    }

    /// Iterate over the `CatalogBatch`s in the update
    pub fn batches(&self) -> impl Iterator<Item = &CatalogBatch> {
        self.update.batches()
    }
}

impl Drop for CatalogUpdateMessage {
    /// Send the response to the catalog via the oneshot sender
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx
                .send(())
                .inspect_err(|error| warn!(?error, "unable to send ACK for catalog update"));
        }
    }
}

impl std::fmt::Debug for CatalogUpdateMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogUpdateMessage")
            .field("update", &self.update)
            .finish()
    }
}

#[derive(Debug, Default)]
pub(crate) struct CatalogSubscriptions {
    subscriptions: hashbrown::HashMap<Arc<str>, CatalogUpdateSender>,
}

impl CatalogSubscriptions {
    /// Subscribe to the catalog for updates.
    ///
    /// This allows components in the system to listen for updates made to the catalog
    /// and handle/apply them as needed. A `subscription_name` is provided that identifies the
    /// component subscribing.
    ///
    /// # Panics
    ///
    /// If the provided `subscription_name` has already been used, this will panic.
    pub(crate) fn subscribe(&mut self, subscription_name: &'static str) -> CatalogUpdateReceiver {
        let (tx, rx) = mpsc::channel(CATALOG_SUBSCRIPTION_BUFFER_SIZE);
        let stopped = Arc::new(AtomicBool::new(false));
        assert!(
            self.subscriptions
                .insert(
                    Arc::from(subscription_name),
                    CatalogUpdateSender {
                        tx,
                        stopped: Arc::clone(&stopped)
                    }
                )
                .is_none(),
            "attempted to subscribe to catalog with same component name more than once, \
            name: {subscription_name}"
        );
        CatalogUpdateReceiver {
            rx,
            stopped: Arc::clone(&stopped),
        }
    }

    pub(crate) async fn send_update(
        &self,
        update: Arc<CatalogUpdate>,
    ) -> Result<(), SubscriptionError> {
        let mut responses = vec![];
        for (name, sub) in self
            .subscriptions
            .iter()
            .filter(|(_, sub)| !sub.is_stopped())
            .map(|(n, s)| (Arc::clone(n), s.clone()))
        {
            let update_cloned = Arc::clone(&update);
            responses.push(tokio::spawn(async move {
                let (tx, rx) = oneshot::channel();
                sub.send(CatalogUpdateMessage::new(update_cloned, tx))
                    .await
                    .with_context(|| format!("failed to send update to {name}"))?;
                rx.await
                    .with_context(|| format!("failed to receive response from {name}"))?;
                Ok(())
            }));
        }

        try_join_all(responses)
            .await
            .context("failed to collect responses from catalog subscribers")?
            .into_iter()
            .collect::<Result<Vec<()>, anyhow::Error>>()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
